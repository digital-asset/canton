// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts, TopologyConfig}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.HasTopologyTransactionTestFactory
import com.digitalasset.canton.synchronizer.block.*
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.*
import com.digitalasset.canton.synchronizer.block.data.memory.InMemorySequencerBlockStore
import com.digitalasset.canton.synchronizer.block.update.{
  BlockProcessingParameters,
  BlockUpdateGeneratorImpl,
  InFlightAggregations,
}
import com.digitalasset.canton.synchronizer.metrics.{SequencerMetrics, SequencerTestMetrics}
import com.digitalasset.canton.synchronizer.sequencer.block.BlockOrderer
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.{
  InvalidAcknowledgementSignature,
  InvalidAcknowledgementTimestamp,
}
import com.digitalasset.canton.synchronizer.sequencer.store.{
  InMemorySequencerStore,
  SequencerMemberValidator,
}
import com.digitalasset.canton.synchronizer.sequencer.{
  BlockSequencerStreamInstrumentationConfig,
  SequencerInitialState,
  SequencerIntegration,
  SequencerPruningStatus,
  SequencerSnapshot,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.RateLimitManagerTesting
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.memory.InMemoryTrafficConsumedStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.StoreBasedSynchronizerTopologyClient
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{NoPackageDependencies, ValidatedTopologyTransaction}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  HasExecutorService,
  ProtocolVersionChecksAsyncWordSpec,
  config,
}
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.Try

class SequencerStateManagerTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasExecutorService
    with HasExecutionContext
    with HasTopologyTransactionTestFactory
    with RateLimitManagerTesting {

  private val suppressingLoggerF = loggerFactory
  private val initialHeight = BlockOrderer.DefaultInitialBlockHeight + 1

  private lazy val alice = topologyTransactionFactory.participant1
  private lazy val synchronizerId = DefaultTestIdentities.physicalSynchronizerId
  private lazy val sequencer = DefaultTestIdentities.sequencerId

  private def withEnv[T](
      initial: Option[SequencerInitialState] = None
  )(test: Environment => Future[T]): Future[T] = {
    val env = new Environment(initial)
    complete {
      test(env)
    } lastly env.close()
  }

  private def newTimestamp() = CantonTimestamp.now()

  private val metrics = SequencerTestMetrics(this.getClass.getSimpleName)

  "SequencerStateManager" can {

    "process blocks" should {
      "fail if getting blocks out of order" in withEnv() { implicit env =>
        import env.*

        for {
          submissionReq1 <- senderSignedSubmissionRequest(alice)
          _ = handleBlock(initialHeight, Send(newTimestamp(), submissionReq1, sequencer))
          _ <- suppressingLoggerF.assertThrowsAndLogsAsync[SequencerUnexpectedStateChange](
            {
              handleBlock(initialHeight + 2)
              processingTermination
            },
            _ => succeed,
            _.errorMessage should include(
              s"Received block of height ${initialHeight + 2} while the last processed block only had height $initialHeight"
            ),
            _.throwable.value.getMessage should include(
              s"Received block of height ${initialHeight + 2} while the last processed block only had height $initialHeight"
            ),
          )
        } yield succeed
      }

      "allow first block ever received to have an arbitrary initial height" in withEnv() {
        implicit env =>
          import env.*
          handleBlock(initialHeight + 2)
          succeed
      }
    }

    "acknowledge" should {
      val sequencerInitialStateAtT1 =
        SequencerInitialState(
          synchronizerId,
          SequencerSnapshot(
            ts1,
            UninitializedBlockHeight,
            Map.empty,
            SequencerPruningStatus(CantonTimestamp.Epoch, ts1, Set.empty),
            InFlightAggregations.empty,
            None,
            testedProtocolVersion,
            Seq.empty,
            Seq.empty,
          ),
          None,
          None,
          None,
        )

      "resolve all earlier valid acknowledgements" in withEnv(
        Some(sequencerInitialStateAtT1)
      ) { implicit env =>
        import env.*
        for {
          submissionReq1 <- senderSignedSubmissionRequest(alice)
          _ = handleBlock(initialHeight, Send(newTimestamp(), submissionReq1, sequencer))
          wait1F = stateManager.waitForAcknowledgementToComplete(
            alice,
            CantonTimestamp.MinValue.immediateSuccessor,
          )
          ts1 = stateManager.getPersistenceHeadState.block.lastTs.immediatePredecessor
          ack = signedAcknowledgement(alice, ts1)
          wait2F = stateManager.waitForAcknowledgementToComplete(alice, ts1)
          _ = loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
            handleBlock(initialHeight + 1, ack),
            forAtLeast(1, _)(_.warningMessage should include("Using approximate topology snapshot")),
          )
          _ <- wait2F
          _ <- wait1F
        } yield succeed
      }

      "error on acknowledgements with timestamps in the future" in withEnv(
        Some(sequencerInitialStateAtT1)
      ) { implicit env =>
        import env.*
        for {
          submissionReq1 <- senderSignedSubmissionRequest(alice)
          _ = handleBlock(initialHeight, Send(newTimestamp(), submissionReq1, sequencer))
          ts1 = stateManager.getPersistenceHeadState.block.lastTs
          ts0 = ts1.immediatePredecessor
          ts2 = ts1.immediateSuccessor
          wait0F = stateManager.waitForAcknowledgementToComplete(alice, ts0)
          wait1F = stateManager.waitForAcknowledgementToComplete(alice, ts1)
          wait2F = stateManager.waitForAcknowledgementToComplete(alice, ts2)
          signedAck0 = signedAcknowledgement(alice, ts0)
          signedAck2 = signedAcknowledgement(alice, ts2)
          _ = loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
            handleBlock(initialHeight + 1, signedAck0, signedAck2),
            logs => {
              forAtLeast(1, logs)(
                _.warningMessage should include("Using approximate topology snapshot")
              )
              forAtLeast(1, logs)(
                _.warningMessage should include(InvalidAcknowledgementTimestamp.code.id)
              )
            },
          )
          _ <- wait0F
          err <- wait2F.failed
          _ = wait1F.isCompleted shouldBe false
        } yield {
          err.getMessage should include(LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API)
        }
      }

      "error on acknowledgements with invalid signatures" in withEnv(
        Some(sequencerInitialStateAtT1)
      ) { implicit env =>
        import env.*
        for {
          submissionReq1 <- senderSignedSubmissionRequest(alice)
          _ = handleBlock(initialHeight, Send(newTimestamp(), submissionReq1, sequencer))
          ts1 = stateManager.getPersistenceHeadState.block.lastTs
          ts0 = ts1.immediatePredecessor
          wait0F = stateManager.waitForAcknowledgementToComplete(alice, ts0)
          wait1F = stateManager.waitForAcknowledgementToComplete(alice, ts1)
          _ = loggerFactory.assertLogs(
            {
              val signedAck0 = signedAcknowledgement(alice, ts0)
              val signedAck1 = badSigAcknowledgement(alice, ts1)
              handleBlock(initialHeight + 1, signedAck0, signedAck1)
            },
            _.warningMessage should include("Using approximate topology snapshot"),
            _.warningMessage should include("Using approximate topology snapshot"),
            _.warningMessage should include(InvalidAcknowledgementSignature.code.id),
          )
          _ <- wait0F
          err <- wait1F.failed
        } yield err.getMessage should include(LogEntry.SECURITY_SENSITIVE_MESSAGE_ON_API)
      }
    }

  }

  private class Environment(
      initial: Option[SequencerInitialState] = None
  ) extends AutoCloseable {
    private val actorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(actorSystem)

    val synchronizerId: PhysicalSynchronizerId =
      topologyTransactionFactory.synchronizerId1.toPhysical
    val sequencer1: SequencerId = topologyTransactionFactory.sequencer1
    val topologyStore =
      new InMemoryTopologyStore(
        SynchronizerStore(synchronizerId),
        predecessor = None,
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      )

    val initialSynchronizerParametersMapping =
      SynchronizerParametersState(synchronizerId.logical, TestSynchronizerParameters.defaultDynamic)
    val syncParamTx = topologyTransactionFactory.mkAdd[SynchronizerParametersState](
      initialSynchronizerParametersMapping,
      topologyTransactionFactory.SigningKeys.key1,
    )
    topologyStore
      .update(
        SequencedTime(CantonTimestamp.Epoch),
        EffectiveTime(CantonTimestamp.Epoch),
        removals = Map.empty,
        additions = Seq(
          topologyTransactionFactory.ns1k1_k1,
          topologyTransactionFactory.okmS1k7_k1,
          topologyTransactionFactory.dtcp1_k1,
          syncParamTx,
          topologyTransactionFactory.okm1bk5k1E_k1, // this one to allow verification of the sender's signature
        ).map(ValidatedTopologyTransaction(_, rejectionReason = None)),
      )
      .futureValueUS

    val topologyClient = new StoreBasedSynchronizerTopologyClient(
      mock[Clock],
      defaultStaticSynchronizerParameters,
      topologyStore,
      NoPackageDependencies,
      topologyConfig = TopologyConfig(),
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )
    topologyClient.updateHead(
      SequencedTime(CantonTimestamp.Epoch),
      EffectiveTime(CantonTimestamp.Epoch),
      ApproximateTime(CantonTimestamp.Epoch),
    )
    private val cryptoApi = SynchronizerCryptoClient.create(
      member = sequencer1,
      synchronizerId.logical,
      topologyClient,
      defaultStaticSynchronizerParameters,
      topologyTransactionFactory.syncCryptoClient.crypto,
      CachingConfigs.defaultPublicKeyConversionCache,
      DefaultProcessingTimeouts.testing,
      FutureSupervisor.Noop,
      loggerFactory,
    )

    private val store = new InMemorySequencerBlockStore(
      new InMemorySequencerStore(
        testedProtocolVersion,
        sequencer1,
        blockSequencerMode = true,
        loggerFactory,
        timeouts = timeouts,
        sequencerMetrics = SequencerMetrics.noop("sequencer-state-manager-test"),
      ),
      loggerFactory,
    )
    private val trafficConsumedStore = new InMemoryTrafficConsumedStore(loggerFactory)
    config
      .NonNegativeFiniteDuration(1.second)
      .awaitUS("set initial state")(
        initial.fold(FutureUnlessShutdown.unit)(store.setInitialState(_, None))
      )

    val closeContext = CloseContext(cryptoApi)
    private val updateGenerator = new BlockUpdateGeneratorImpl(
      cryptoApi,
      sequencer1,
      defaultRateLimiter,
      drSequencingTimeUpperBound = None,
      getAnnouncedLsu = None,
      consistencyChecks = true,
      parameters = BlockProcessingParameters(
        orderingTimeFixMode = OrderingTimeFixMode.MakeStrictlyIncreasing,
        lsuSequencingBounds = None,
        parallelism = PositiveInt.two,
        enablePrevalidation = true,
        enableAsyncLoggingWithOutcome = false,
      ),
      metrics,
      memberValidator = new SequencerMemberValidator {
        override def isMemberRegisteredAt(member: Member, time: CantonTimestamp)(implicit
            tc: TraceContext
        ): FutureUnlessShutdown[Boolean] = FutureUnlessShutdown.pure(true)

        override def areMembersRegisteredAt(members: Seq[Member], time: CantonTimestamp)(implicit
            tc: TraceContext
        ): FutureUnlessShutdown[Map[Member, Boolean]] = FutureUnlessShutdown.pure(
          members.map(m => m -> true).toMap
        )
      },
      loggerFactory = loggerFactory,
    )(closeContext, NoReportingTracerProvider.tracer)

    def signedAcknowledgement(
        member: Member,
        timestamp: CantonTimestamp,
    ): Acknowledgment = {
      val ack = AcknowledgeRequest(member, timestamp, testedProtocolVersion)
      val hash =
        topologyTransactionFactory.syncCryptoClient.crypto.pureCrypto.digest(
          HashPurpose.AcknowledgementSignature,
          ack.getCryptographicEvidence,
        )
      topologyTransactionFactory.syncCryptoClient.crypto.privateCrypto
        .sign(
          hash,
          participant1Key.fingerprint,
          SigningKeyUsage.ProtocolOnly,
        )
        .map(signature =>
          SignedContent(
            ack,
            signature,
            Some(ts1),
            testedProtocolVersion,
          )
        )
        .leftMap(_.toString)
        .value
        .failOnShutdown
        .map(_.value)
        .map(Acknowledgment(newTimestamp(), _))
        .futureValue
    }

    def badSigAcknowledgement(member: Member, timestamp: CantonTimestamp): Acknowledgment =
      Acknowledgment(
        newTimestamp(),
        SignedContent(
          AcknowledgeRequest(member, timestamp, testedProtocolVersion),
          Signature.noSignature,
          None,
          testedProtocolVersion,
        ),
      )

    val initialHeadO = store.readHeadUnbounded().futureValueUS
    val stateManager: BlockSequencerStateManager =
      BlockSequencerStateManager
        .create(
          initialHeadO,
          store,
          trafficConsumedStore,
          asyncWriterParameters = AsyncWriterParameters(),
          BlockSequencerStreamInstrumentationConfig(),
          enableInvariantCheck = true,
          enablePrevalidation = true,
          prevalidationParallelism = PositiveInt.two,
          metrics.block,
          timeouts,
          futureSupervisor,
          loggerFactory,
        )(executorService, traceContext)

    private val processingTimestampWatermark =
      new AtomicReference[CantonTimestamp](CantonTimestamp.MinValue)
    val (processingQueue, processingDone) =
      PekkoUtil.runSupervised(
        Source
          .queue[Traced[BlockEvents]](10)
          .via(stateManager.processBlock(updateGenerator))
          .via(stateManager.applyBlockUpdate(SequencerIntegration.Noop))
          .map(ts => processingTimestampWatermark.updateAndGet(_ max ts.value))
          .toMat(Sink.ignore)(Keep.both),
        errorLogMessagePrefix = "",
      )
    def processingTermination: Future[Done] = processingDone

    def handleBlock(height: Long, events: LedgerBlockEvent*): Unit = {
      logger.info(s"Processing block $height")
      val maxTs = events
        .collect { case Send(ts, _, _, _) => ts }
        .maxOption
        .getOrElse(processingTimestampWatermark.get())
      processingQueue.offer(
        Traced(
          BlockEvents(
            height,
            CantonTimestamp.Epoch, // Not relevant for the test
            events.map(Traced(_)),
          )
        )
      )
      eventually() {
        processingTimestampWatermark.get() should be >= maxTs
      }
    }

    override def close(): Unit = {
      // If the processing pipeline fails, we'll get an exception here that we don't care about.
      Try {
        processingQueue.complete()
        processingDone.futureValue
      }
      stateManager.close()
      topologyClient.close()
      topologyStore.close()
      materializer.shutdown()
      Await.result(actorSystem.terminate(), 10.seconds)
    }
  }
}
