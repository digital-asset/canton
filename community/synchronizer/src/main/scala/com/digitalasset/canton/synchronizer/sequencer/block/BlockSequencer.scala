// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.RichGeneratedMessage
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SyncCryptoClient,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.CantonPrettyPrinter
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  EncryptedViewMessage,
  LsuSequencingTestMessage,
  LsuSequencingTestMessageContent,
}
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, Storage}
import com.digitalasset.canton.sequencer.admin.v30.{EnvelopeTrafficSummary, TrafficSummary}
import com.digitalasset.canton.sequencing.GroupAddressResolver
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.ErrorCode
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  SendAsyncClientError,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.{
  Overloaded,
  SubmissionRequestRefused,
}
import com.digitalasset.canton.sequencing.traffic.TrafficControlErrors.{
  TrafficControlDisabled,
  TrafficControlError,
  TrafficStateNotFound,
}
import com.digitalasset.canton.sequencing.traffic.{
  EventCostCalculator,
  TrafficConsumed,
  TrafficControlErrors,
  TrafficPurchasedSubmissionHandler,
}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.synchronizer.block.BlockSequencerStateManagerBase
import com.digitalasset.canton.synchronizer.block.data.SequencerBlockStore
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGeneratorImpl
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.*
import com.digitalasset.canton.synchronizer.sequencer.PruningError.UnsafePruningPoint
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.admin.data.SequencerHealthStatus
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.{
  BlockNotFound,
  ExceededMaxSequencingTime,
  InvalidTrafficState,
  LsuSequencerError,
  LsuTrafficAlreadyInitialized,
  MissingSynchronizerPredecessor,
  SequencerPastUpgradeTime,
}
import com.digitalasset.canton.synchronizer.sequencer.store.{PayloadId, SequencerStore}
import com.digitalasset.canton.synchronizer.sequencer.time.LsuSequencingBounds
import com.digitalasset.canton.synchronizer.sequencer.traffic.TimestampSelector.*
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  LsuTrafficState,
  SequencerRateLimitError,
  SequencerRateLimitManager,
  SequencerTrafficStatus,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficPurchasedStore
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown
import com.digitalasset.canton.util.retry.Pause
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil, PekkoUtil, SimpleExecutionQueue}
import io.grpc.ServerServiceDefinition
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import BlockSequencerFactory.OrderingTimeFixMode

class BlockSequencer(
    blockOrderer: BlockOrderer,
    name: String,
    cryptoApi: SynchronizerCryptoClient,
    sequencerId: SequencerId,
    stateManager: BlockSequencerStateManagerBase,
    store: SequencerBlockStore,
    dbSequencerStore: SequencerStore,
    blockSequencerConfig: BlockSequencerConfig,
    producePostOrderingTopologyTicks: Boolean,
    trafficPurchasedStore: TrafficPurchasedStore,
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    health: Option[SequencerHealthConfig],
    clock: Clock,
    blockRateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    lsuSequencingBounds: Option[LsuSequencingBounds],
    processingTimeouts: ProcessingTimeout,
    logEventDetails: Boolean,
    prettyPrinter: CantonPrettyPrinter,
    metrics: SequencerMetrics,
    batchingConfig: BatchingConfig,
    loggerFactory: NamedLoggerFactory,
    exitOnFatalFailures: Boolean,
    runtimeReady: FutureUnlessShutdown[Unit],
)(implicit executionContext: ExecutionContext, materializer: Materializer, val tracer: Tracer)
    extends DatabaseSequencer(
      SequencerWriterStoreFactory.singleInstance,
      dbSequencerStore,
      blockSequencerConfig.toDatabaseSequencerConfig,
      None,
      TotalNodeCountValues.SingleSequencerTotalNodeCount,
      new LocalSequencerStateEventSignaller(
        processingTimeouts,
        loggerFactory,
      ),
      None,
      None,
      processingTimeouts,
      storage,
      None,
      health,
      clock,
      sequencerId,
      cryptoApi,
      metrics,
      loggerFactory,
      blockSequencerMode = true,
      lsuSequencingBounds,
      rateLimitManagerO = Some(blockRateLimitManager),
    )
    with DatabaseSequencerIntegration
    with NamedLogging
    with FlagCloseableAsync {

  private val protocolVersion = cryptoApi.protocolVersion

  private[sequencer] val pruningQueue = new SimpleExecutionQueue(
    "block-sequencer-pruning-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )
  private val sequencerPruningPromises = TrieMap[CantonTimestamp, Promise[Unit]]()

  // We use this promise on an LSU successor sequencer to wait until the traffic data
  // has been transferred from the predecessor and is available for use.
  private val lsuTrafficInitialized: PromiseUnlessShutdown[Unit] =
    TraceContext.withNewTraceContext("lsu-successor-traffic-initialized") {
      implicit tc: TraceContext =>
        PromiseUnlessShutdown
          .supervised[Unit]("lsu-successor-traffic-initialized", futureSupervisor)
    }
  // If the lower bound is not set (this sequencer is not an LSU successor),
  // we don't need to wait for traffic initialization.
  lsuSequencingBounds match {
    case None =>
      logger.info(
        s"Sequencer $sequencerId is not an LSU successor; traffic data initialization is not required."
      )(TraceContext.empty)
      lsuTrafficInitialized.success(UnlessShutdown.unit)
    case Some(lsuSequencingBounds) =>
      // check that traffic control is enabled in the current topology
      implicit val traceContext: TraceContext = TraceContext.createNew(
        "block-sequencer-lsu-traffic-control-check"
      )
      val checkLsuTrafficInitializedFUS = for {
        snapshot <- cryptoApi.ipsSnapshot(lsuSequencingBounds.upgradeTime)
        trafficControlParametersO <- snapshot.trafficControlParameters(protocolVersion)
        trafficPurchasedInitialized <- trafficPurchasedStore.getInitialTimestamp
      } yield {
        (trafficControlParametersO, trafficPurchasedInitialized) match {
          case (Some(_), None) =>
            logger.info(
              s"Sequencer $sequencerId is an LSU successor with traffic control enabled; awaiting traffic data initialization."
            )
          case (None, _) =>
            logger.info(
              s"Sequencer $sequencerId is an LSU successor with traffic control disabled; traffic data initialization is not required."
            )
            lsuTrafficInitialized.success(UnlessShutdown.unit)
            ()
          case (Some(_), Some(_)) =>
            logger.info(
              s"Sequencer $sequencerId is an LSU successor and traffic data has already been initialized."
            )
            lsuTrafficInitialized.success(UnlessShutdown.unit)
            ()
        }
      }

      doNotAwaitUnlessShutdown(
        checkLsuTrafficInitializedFUS,
        s"Failure during traffic initialization check for LSU successor sequencer",
      )
  }

  override lazy val rateLimitManager: Option[SequencerRateLimitManager] = Some(
    blockRateLimitManager
  )

  private val trafficPurchasedSubmissionHandler =
    new TrafficPurchasedSubmissionHandler(clock, loggerFactory)

  override protected def resetWatermarkTo: SequencerWriter.ResetWatermark =
    lsuSequencingBounds match {
      case Some(lsuSequencingBounds) =>
        SequencerWriter.ResetWatermarkToTimestamp(
          stateManager.getHeadState.block.lastTs
            .max(lsuSequencingBounds.lowerBoundSequencingTimeExclusive)
        )

      case None =>
        SequencerWriter.ResetWatermarkToTimestamp(
          stateManager.getHeadState.block.lastTs
        )
    }

  private val circuitBreaker = BlockSequencerCircuitBreaker(
    blockSequencerConfig.circuitBreaker,
    clock,
    metrics,
    materializer,
    loggerFactory,
  )
  private val throughputCap =
    new BlockSequencerThroughputCap(
      blockSequencerConfig.throughputCap,
      clock,
      materializer.system.scheduler,
      metrics,
      timeouts,
      loggerFactory,
    )

  private[sequencer] val announcedLsu: AtomicReference[Option[AnnouncedLsu]] =
    new AtomicReference(None)

  private val (killSwitchF, done) = {
    val headState = stateManager.getHeadState
    noTracingLogger.info(s"Subscribing to block source from ${headState.block.height + 1}")

    val updateGenerator = new BlockUpdateGeneratorImpl(
      cryptoApi,
      sequencerId,
      blockRateLimitManager,
      orderingTimeFixMode,
      lsuSequencingBounds = lsuSequencingBounds,
      getAnnouncedLsu = announcedLsu.get(),
      producePostOrderingTopologyTicks,
      metrics,
      batchingConfig,
      loggerFactory,
      memberValidator = memberValidator,
    )(CloseContext(cryptoApi), tracer)

    implicit val traceContext: TraceContext =
      TraceContext.createNew("block-sequencer-driver-source")

    val driverSource = Source
      .futureSource(runtimeReady.unwrap.map {
        case UnlessShutdown.AbortedDueToShutdown =>
          noTracingLogger.debug("Not initiating subscription to block source due to shutdown")
          Source.empty.viaMat(KillSwitches.single)(Keep.right)
        case UnlessShutdown.Outcome(_) =>
          noTracingLogger.debug("Subscribing to block source")
          blockOrderer.subscribe()
      })
      // Explicit async to make sure that the block processing runs in parallel with the block retrieval
      .async
      .map(updateGenerator.extractBlockEvents)
      .via(stateManager.processBlock(updateGenerator))
      .wireTap { update =>
        throughputCap.addBlockUpdate(update.value)
      }
      .async
      .via(stateManager.applyBlockUpdate(this))
      .wireTap { lastTs =>
        circuitBreaker.registerLastBlockTimestamp(lastTs)
      }
    PekkoUtil.runSupervised(
      driverSource.toMat(Sink.ignore)(Keep.both),
      errorLogMessagePrefix = "Fatally failed to handle state changes",
    )
  }

  done onComplete {
    case Success(_) => noTracingLogger.debug("Sequencer flow has shutdown")
    case Failure(ex) => noTracingLogger.error("Sequencer flow has failed", ex)
  }

  private def validateMaxSequencingTime(
      submission: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] =
    for {
      estimatedSequencingTimestampO <- EitherT.right(sequencingTime)
      estimatedSequencingTimestamp = estimatedSequencingTimestampO.getOrElse(clock.now)
      _ = logger.debug(
        s"Estimated sequencing time $estimatedSequencingTimestamp for submission with id ${submission.messageId}"
      )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        submission.maxSequencingTime > estimatedSequencingTimestamp,
        ExceededMaxSequencingTime.Error(
          estimatedSequencingTimestamp,
          submission.maxSequencingTime,
          s"Estimation for message id ${submission.messageId}",
        ),
      )
      _ <- submission.aggregationRule.traverse_ { _ =>
        // We can't easily use snapshot(topologyTimestamp), because the effective last snapshot transaction
        // visible in the BlockSequencer can be behind the topologyTimestamp and tracking that there's an
        // intermediate topology change is impossible here (will need to communicate with the BlockUpdateGenerator).
        // If topologyTimestamp happens to be ahead of current topology's timestamp we grab the latter
        // to prevent a deadlock.
        val topologyTimestamp = cryptoApi.approximateTimestamp.min(
          submission.topologyTimestamp.getOrElse(CantonTimestamp.MaxValue)
        )
        for {
          snapshot <- EitherT.right(cryptoApi.snapshot(topologyTimestamp))
          synchronizerParameters <- EitherT(
            snapshot.ipsSnapshot.findDynamicSynchronizerParameters()
          ).leftMap(error =>
            SequencerErrors.Internal(
              s"Could not fetch dynamic synchronizer parameters: $error"
            ): CantonBaseError
          )
          maxSequencingTimeUpperBound = estimatedSequencingTimestamp.add(
            synchronizerParameters.parameters.sequencerAggregateSubmissionTimeout.duration
          )
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            submission.maxSequencingTime < maxSequencingTimeUpperBound,
            SequencerErrors.SubmissionRequestRefused(
              s"Max sequencing time ${submission.maxSequencingTime} for submission with id ${submission.messageId} is too far in the future, currently bounded at $maxSequencingTimeUpperBound"
            ): CantonBaseError,
          )
        } yield ()
      }
    } yield ()

  override protected def sendAsyncInternal(
      submission: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] = {
    val signedContent = SignedContent(submission, Signature.noSignature, None, protocolVersion)
    sendAsyncSignedInternal(signedContent)
  }

  override def adminServices: Seq[ServerServiceDefinition] = blockOrderer.adminServices

  private def enforceRateLimiting(
      request: SignedSubmissionRequest
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    blockRateLimitManager
      .validateRequestAtSubmissionTime(
        request.content,
        request.timestampOfSigningKey,
        // Use the timestamp of the latest chunk here, such that top ups that happened in an earlier chunk of the
        // current block can be reflected in the traffic state used to validate the request
        {
          val headChunkLastTs = stateManager.getHeadState.chunk.lastTs
          lsuSequencingBounds
            .map(_.upgradeTime)
            .getOrElse(headChunkLastTs)
            .max(headChunkLastTs)
        },
        stateManager.getHeadState.chunk.latestSequencerEventTimestamp
          .orElse(lsuSequencingBounds.map(_.upgradeTime)),
      )
      .leftMap {
        // If the cost is outdated, we bounce the request with a specific SendAsyncError so the
        // sender has the required information to retry the request with the correct cost
        case notEnoughTraffic: SequencerRateLimitError.AboveTrafficLimit =>
          logger.debug(
            s"Rejecting submission request because not enough traffic is available: $notEnoughTraffic"
          )
          SequencerErrors.TrafficCredit(
            s"Submission was rejected because no traffic is available: $notEnoughTraffic"
          )
        // If the cost is outdated, we bounce the request with a specific SendAsyncError so the
        // sender has the required information to retry the request with the correct cost
        case outdated: SequencerRateLimitError.OutdatedEventCost =>
          logger.debug(
            s"Rejecting submission request because the cost was computed using an outdated topology: $outdated"
          )
          SequencerErrors.OutdatedTrafficCost(
            s"Submission was refused because traffic cost was outdated. Re-submit after having observed the validation timestamp and processed its topology state: $outdated"
          )
        case error =>
          SequencerErrors.SubmissionRequestRefused(
            s"Submission was refused because traffic control validation failed: $error"
          )
      }

  private def enforceThroughputCap(
      submission: SubmissionRequest
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    EitherT
      .fromEither[FutureUnlessShutdown](
        throughputCap.shouldRejectTransaction(submission.requestType, submission.sender, 0)
      )
      .leftMap { msg =>
        SequencerErrors.Overloaded(
          s"Member ${submission.sender} has reached its throughput cap: $msg"
        )
      }

  /** This method rejects submissions before or at the lower bound of sequencing time. It compares
    * clock.now against the configured sequencingTimeLowerBoundExclusive. It cannot use the time
    * from the head state, because any blocks before sequencingTimeLowerBoundExclusive are filtered
    * out and therefore the time would never advance. The ordering service is expected to lag a bit
    * behind the (synchronized) wall clock, therefore this method does not reject submissions that
    * would be sequenced after sequencingTimeLowerBoundExclusive. It could however pass through
    * submissions that then get dropped, because they end up getting sequenced before
    * sequencingTimeLowerBoundExclusive.
    */
  private def rejectSubmissionsBeforeOrAtSequencingTimeLowerBound()
      : EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] = {
    val currentTime = clock.now

    lsuSequencingBounds.map(_.upgradeTime) match {
      case Some(boundExclusive) =>
        EitherTUtil.condUnitET[FutureUnlessShutdown](
          currentTime > boundExclusive,
          SubmissionRequestRefused(
            s"Cannot submit before or at the lower bound for sequencing time $boundExclusive; time is currently at $currentTime"
          ),
        )

      case None => EitherTUtil.unitUS
    }
  }

  private def rejectSubmissionsIfOverloaded(
      submission: SubmissionRequest
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    if (circuitBreaker.shouldRejectRequests(submission))
      EitherT.leftT(
        Overloaded("Sequencer can't take requests because it is behind on processing events")
      )
    else EitherTUtil.unitUS

  private def rejectAcknowledgementIfOverloaded()
      : EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    if (circuitBreaker.shouldRejectAcknowledgements)
      EitherT.leftT(
        Overloaded("Sequencer can't take requests because it is behind on processing events")
      )
    else EitherTUtil.unitUS

  override protected def sendAsyncSignedInternal(
      signedSubmission: SignedContent[SubmissionRequest],
      skipLsuChecks: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] = {
    val submission = signedSubmission.content
    val SubmissionRequest(
      sender,
      _,
      batch,
      maxSequencingTime,
      _,
      _,
      _,
    ) = submission

    val maybeDelayedProcessingMessage =
      if (lsuTrafficInitialized.isCompleted || skipLsuChecks)
        ""
      else " Traffic is not yet initialized. Handling of submission request might be delayed."

    logger.debug(
      s"Request to send submission with id ${submission.messageId} with max sequencing time $maxSequencingTime from $sender to ${batch.allRecipients}. $maybeDelayedProcessingMessage"
    )

    for {
      _ <-
        if (!skipLsuChecks) EitherT.right(lsuTrafficInitialized.futureUS)
        else EitherTUtil.unitUS
      _ <-
        if (!skipLsuChecks) rejectSubmissionsBeforeOrAtSequencingTimeLowerBound()
        else EitherTUtil.unitUS
      _ <- enforceThroughputCap(submission)
      _ <- rejectSubmissionsIfOverloaded(submission)
      // TODO(i17584): revisit the consequences of no longer enforcing that
      //  aggregated submissions with signed envelopes define a topology snapshot
      _ <- validateMaxSequencingTime(submission)
      // TODO(#19476): Why we don't check group recipients here?
      approximateSnapshot <- EitherT.liftF(
        cryptoApi.currentSnapshotApproximation
      )
      _ <- SubmissionRequestValidations
        .checkSenderAndRecipientsAreRegistered(
          submission,
          // Using currentSnapshotApproximation due to members registration date
          // expected to be before submission sequencing time
          approximateSnapshot.ipsSnapshot,
        )
        .leftMap(_.toSequencerDeliverError)
      _ = if (logEventDetails)
        logger.debug(
          s"Invoking send operation on the ledger with the following protobuf message serialized to bytes ${prettyPrinter
              .printAdHoc(submission.toProtoVersioned)}"
        )
      _ <- enforceRateLimiting(signedSubmission)
      _ <- EitherT(
        futureSupervisor.supervised(
          s"Sending submission request with id ${submission.messageId} from $sender to ${batch.allRecipients}"
        )(blockOrderer.send(signedSubmission).value)
      ).mapK(FutureUnlessShutdown.outcomeK).leftWiden[CantonBaseError]
    } yield ()
  }

  override protected def localSequencerMember: Member = sequencerId

  override protected def acknowledgeSignedInternal(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val req = signedAcknowledgeRequest.content
    logger.debug(s"Request for member ${req.member} to acknowledge timestamp ${req.timestamp}")
    for {
      _ <- EitherTUtil.toFutureUnlessShutdown(
        rejectAcknowledgementIfOverloaded().leftMap(_.asGrpcError)
      )

      _ = signedAcknowledgeRequest.content.member match {
        case _: ParticipantId =>
          // Participants should not ack before upgrade time because the events are on the old synchronizer.
          EitherTUtil.toFutureUnlessShutdown(
            rejectSubmissionsBeforeOrAtSequencingTimeLowerBound().leftMap(_.asGrpcError)
          )

        case _: SequencerId | _: MediatorId =>
          // Synchronizer nodes can receive messages on the new synchronizer before upgrade time so they can ack
          FutureUnlessShutdown.unit
      }
      waitForAcknowledgementF = stateManager.waitForAcknowledgementToComplete(
        req.member,
        req.timestamp,
      )
      _ <- FutureUnlessShutdown.outcomeF(blockOrderer.acknowledge(signedAcknowledgeRequest))
      _ <- FutureUnlessShutdown.outcomeF(waitForAcknowledgementF)
    } yield ()
  }

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] = {
    val delay = 1.second
    val waitForBlockContainingTimestamp = EitherT.right(
      Pause(
        logger,
        this,
        maxRetries = (timeouts.default.duration / delay).toInt,
        delay,
        s"$functionFullName($timestamp)",
      )
        .unlessShutdown(
          FutureUnlessShutdown.pure(timestamp <= stateManager.getHeadState.block.lastTs),
          DbExceptionRetryPolicy,
        )
    )

    waitForBlockContainingTimestamp.flatMap { foundBlock =>
      if (foundBlock) {
        // if we found a block, now also await for the underlying DatabaseSequencer's snapshot to be ready.
        // since it uses a different, watermark-based mechanism to determine how far in time it has progressed.
        super[DatabaseSequencer].awaitSnapshot(timestamp).flatMap(_ => snapshot(timestamp))
      } else
        EitherT.leftT[FutureUnlessShutdown, SequencerSnapshot](
          BlockNotFound.InvalidTimestamp(timestamp): SequencerError
        )
    }
  }

  override protected def readPayloadsFromTimestampsInternal(timestamps: Seq[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Map[PayloadId, Batch[ClosedEnvelope]]] =
    // In the block sequencer, payload Ids are sequencing timestamps so we can use that to look up payloads directly
    // We don't load them in the cache though to avoid interfering with optimizations on event delivery to members
    reader.readPayloadsByIdWithoutCacheLoading(timestamps.map(PayloadId(_)))

  private val eventCostCalculator = new EventCostCalculator(loggerFactory)

  override def getTrafficSummaries(timestamps: Seq[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficControlError, Seq[TrafficSummary]] = {

    val timestampsSet = SortedSet.from(timestamps)
    val headBlock = stateManager.getHeadState.block
    val latestSequencedTimestamp = headBlock.lastTs
    val latestSequencerEventTimestamp = headBlock.latestSequencerEventTimestamp

    // Get the latest snapshot available by making use of the latestSequencerEventTimestamp in the head state
    // This ensures we can immediately get topology snapshots for events that have been sequenced without
    // waiting for the head topolog snapshot to get updated
    def getSnapshot(timestamp: CantonTimestamp) = SyncCryptoClient
      .getSnapshotForTimestamp(
        cryptoApi,
        timestamp,
        latestSequencerEventTimestamp,
      )
      .map(_.ipsSnapshot)

    // Computes the detailed event cost for a batch at the sequencing timestamp
    def computeDetailedEventCostForBatch(
        batch: Batch[ClosedEnvelope],
        sequencingTime: CantonTimestamp,
    ): EitherT[FutureUnlessShutdown, TrafficControlError, EventCostCalculator.EventCostDetails] = {

      // TODO(i29505): remove once flat fees for broadcasts is implemented
      val groups =
        batch.envelopes.flatMap(_.recipients.allRecipients).collect { case g: GroupRecipient => g }

      for {
        topologySnapshot <- OptionT.liftF(getSnapshot(sequencingTime))
        trafficParams <- OptionT(
          topologySnapshot.trafficControlParameters(protocolVersion)
        )
        groupToMembers <- OptionT.liftF(
          GroupAddressResolver.resolveGroupsToMembers(groups.toSet, topologySnapshot)
        )
        eventCost = eventCostCalculator.computeEventCost(
          batch,
          trafficParams.readVsWriteScalingFactor,
          groupToMembers,
          protocolVersion,
          trafficParams.baseEventCost,
        )
      } yield eventCost
    }
      .toRight(TrafficControlErrors.TrafficControlDisabled.Error())
      .leftWiden[TrafficControlError]

    def buildEnvelopeTrafficSummary(
        eventCostDetails: EventCostCalculator.EventCostDetails
    ): EitherT[FutureUnlessShutdown, TrafficControlError, Seq[EnvelopeTrafficSummary]] =
      EitherT
        .fromEither[FutureUnlessShutdown](
          eventCostDetails.envelopes.toSeq.traverse { case (closedEnvelope, costDetails) =>
            closedEnvelope
              .toOpenEnvelope(cryptoApi.pureCrypto, protocolVersion)
              .leftMap(err => TrafficControlErrors.EnvelopeTrafficSummaryError.Error(err))
              .map { openEnvelope =>
                val viewHashes = openEnvelope.protocolMessage match {
                  // For encrypted view messages, extract the view hash
                  case message: EncryptedViewMessage[?] =>
                    List(message.viewHash.unwrap.getCryptographicEvidence)
                  case _ => List.empty
                }
                EnvelopeTrafficSummary(
                  envelopeTrafficCost = costDetails.finalCost,
                  viewHashes = viewHashes,
                )
              }
          }
        )
        .leftWiden[TrafficControlError]

    def computeSummary(
        batch: Batch[ClosedEnvelope],
        sequencingTimestamp: CantonTimestamp,
    ): EitherT[FutureUnlessShutdown, TrafficControlError, TrafficSummary] = for {
      eventCostDetails <- computeDetailedEventCostForBatch(batch, sequencingTimestamp)
      envelopeSummaries <- buildEnvelopeTrafficSummary(eventCostDetails)
    } yield TrafficSummary(
      sequencingTime = Some(sequencingTimestamp.toProtoTimestamp),
      totalTrafficCost = eventCostDetails.eventCost.value,
      envelopes = envelopeSummaries,
    )

    for {
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        lsuTrafficInitialized.futureUS.isCompleted,
        TrafficStateNotFound.Error(),
      )

      // Check that the latest timestamp is not above the latest sequenced timestamp
      _ <- timestampsSet.lastOption
        .find(_ > latestSequencedTimestamp)
        .fold(EitherTUtil.unitUS[TrafficControlError])(inTheFuture =>
          EitherT.leftT[FutureUnlessShutdown, Unit](
            TrafficControlErrors.RequestedTimestampInTheFuture.Error(inTheFuture)
          )
        )
        .leftWiden[TrafficControlError]
      eventsMap <- EitherT.liftF(readPayloadsFromTimestampsInternal(timestamps))
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        eventsMap.sizeIs == timestampsSet.size,
        TrafficControlErrors.NoEventAtTimestamps.Error(
          timestampsSet.diff(eventsMap.keySet.map(_.unwrap))
        ),
      )
      trafficSummaries <- MonadUtil
        .parTraverseWithLimit(batchingConfig.parallelism)(
          eventsMap.toSeq
        ) { case (payloadId, batch) =>
          val sequencingTime = payloadId.unwrap
          computeSummary(batch, sequencingTime)
        }
    } yield trafficSummaries
  }

  override def awaitContainingBlockLastTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, CantonTimestamp] = {
    val delay = 1.second
    EitherT(
      Pause(
        logger,
        this,
        maxRetries = (timeouts.default.duration / delay).toInt,
        delay,
        s"$functionFullName($timestamp)",
      ).unlessShutdown(
        store
          .findBlockContainingTimestamp(timestamp)
          .map(_.lastTs)
          .value,
        DbExceptionRetryPolicy,
      )
    )
  }

  @nowarn("cat=deprecation")
  override def snapshot(
      timestamp: CantonTimestamp
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerError, SequencerSnapshot] =
    // TODO(#12676) Make sure that we don't request a snapshot for a state that was already pruned

    for {
      additionalInfo <- blockOrderer
        .sequencerSnapshotAdditionalInfo(timestamp)
        .mapK(FutureUnlessShutdown.outcomeK)

      implementationSpecificInfo = additionalInfo.map(info =>
        SequencerSnapshot.ImplementationSpecificInfo(
          implementationName = "BlockSequencer",
          info.checkedToByteString,
        )
      )

      topologySnapshot <- EitherT.right(cryptoApi.awaitSnapshot(timestamp))
      parameterChanges <- EitherT.right(
        topologySnapshot.ipsSnapshot.listDynamicSynchronizerParametersChanges()
      )
      maxSequencingTimeBound = SequencerUtils.maxSequencingTimeUpperBoundAt(
        timestamp,
        parameterChanges,
      )
      blockState <- store
        .readStateForBlockContainingTimestamp(timestamp, maxSequencingTimeBound)
      // Look up traffic info at the latest timestamp from the block,
      // because that's where the onboarded sequencer will start reading
      trafficPurchased <- EitherT
        .right[SequencerError](
          trafficPurchasedStore
            .lookupLatestBeforeInclusive(blockState.latestBlock.lastTs)
        )
      trafficConsumed <- EitherT
        .right[SequencerError](
          blockRateLimitManager.trafficConsumedStore
            .lookupLatestBeforeInclusive(blockState.latestBlock.lastTs)
        )

      _ = if (logger.underlying.isDebugEnabled()) {
        logger.debug(
          s"""BlockSequencer data for snapshot for timestamp $timestamp:
             |blockState: $blockState
             |trafficPurchased: $trafficPurchased
             |trafficConsumed: $trafficConsumed
             |implementationSpecificInfo: $implementationSpecificInfo""".stripMargin
        )
      }
      finalSnapshot <- {
        super.snapshot(blockState.latestBlock.lastTs).map { dbsSnapshot =>
          val finalSnapshot = dbsSnapshot.copy(
            latestBlockHeight = blockState.latestBlock.height,
            inFlightAggregations = blockState.inFlightAggregations,
            additional = implementationSpecificInfo,
            trafficPurchased = trafficPurchased,
            trafficConsumed = trafficConsumed,
          )(dbsSnapshot.representativeProtocolVersion)
          finalSnapshot
        }
      }
    } yield {
      logger.trace(
        s"Resulting snapshot for timestamp $timestamp:\n$finalSnapshot"
      )
      finalSnapshot
    }

  private def waitForPruningToComplete(timestamp: CantonTimestamp): (Boolean, Future[Unit]) = {
    val newPromise = Promise[Unit]()
    val (isNew, promise) = sequencerPruningPromises
      .putIfAbsent(timestamp, newPromise)
      .fold((true, newPromise))(oldPromise => (false, oldPromise))
    (isNew, promise.future)
  }

  private def resolveSequencerPruning(timestamp: CantonTimestamp): Unit =
    sequencerPruningPromises.remove(timestamp) foreach { promise => promise.success(()) }

  /** Important: currently both the disable member and the prune functionality on the block
    * sequencer are purely local operations that do not affect other block sequencers that share the
    * same source of events.
    */
  override def prune(requestedTimestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PruningError, String] = {

    val (isNewRequest, pruningF) = waitForPruningToComplete(requestedTimestamp)
    val supervisedPruningF = futureSupervisor.supervised(
      description = s"Waiting for local pruning operation at $requestedTimestamp to complete",
      logLevel = Level.INFO,
    )(pruningF)

    if (isNewRequest)
      for {
        status <- EitherT.right[PruningError](this.pruningStatus)
        _ <- condUnitET[FutureUnlessShutdown](
          requestedTimestamp <= status.safePruningTimestamp,
          UnsafePruningPoint(requestedTimestamp, status.safePruningTimestamp): PruningError,
        )
        msg <- EitherT(
          pruningQueue
            .executeUS(
              for {
                eventsMsg <- store.prune(requestedTimestamp)
                trafficMsg <- blockRateLimitManager.prune(requestedTimestamp)
                msgEither <-
                  super[DatabaseSequencer]
                    .prune(requestedTimestamp)
                    .map(dbsMsg =>
                      s"${eventsMsg.replace("0 events and ", "")}\n$dbsMsg\n$trafficMsg"
                    )
                    .value
              } yield msgEither,
              s"pruning sequencer at $requestedTimestamp",
            )
            .unwrap
            .map(
              _.onShutdown(
                Right(s"pruning at $requestedTimestamp canceled because we're shutting down")
              )
            )
        ).mapK(FutureUnlessShutdown.outcomeK)
        _ = resolveSequencerPruning(requestedTimestamp)
        _ <- EitherT.right(supervisedPruningF).mapK(FutureUnlessShutdown.outcomeK)
      } yield msg
    else
      EitherT.right(
        FutureUnlessShutdown
          .outcomeF(supervisedPruningF)
          .map(_ =>
            s"Pruning at $requestedTimestamp is already happening due to an earlier request"
          )
      )
  }

  override def findPruningTimestamp(index: PositiveInt)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, PruningSupportError, Option[CantonTimestamp]] =
    EitherT.leftT[FutureUnlessShutdown, Option[CantonTimestamp]](PruningError.NotSupported)

  override def reportMaxEventAgeMetric(
      oldestEventTimestamp: Option[CantonTimestamp]
  ): Either[PruningSupportError, Unit] = Either.left(PruningError.NotSupported)

  override protected def healthInternal(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerHealthStatus] =
    for {
      ledgerStatus <- FutureUnlessShutdown.outcomeF(blockOrderer.health)
      isStorageActive = storage.isActive
      _ = logger.trace(s"Storage active: ${storage.isActive}")
    } yield {
      if (!ledgerStatus.isActive) SequencerHealthStatus(isActive = false, ledgerStatus.description)
      else
        SequencerHealthStatus(
          isStorageActive,
          if (isStorageActive) None else Some("Can't connect to database"),
        )
    }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    logger.debug(s"$name sequencer shutting down")
    Seq[AsyncOrSyncCloseable](
      SyncCloseable("lsu-traffic-initialized", lsuTrafficInitialized.shutdown_()),
      SyncCloseable("pruningQueue", pruningQueue.close()),
      SyncCloseable("stateManager.close()", stateManager.close()),
      // The kill switch ensures that we don't process the remaining contents of the queue buffer
      AsyncCloseable(
        "killSwitchF(_.shutdown())",
        killSwitchF.map(_.shutdown()),
        timeouts.shutdownProcessing,
      ),
      SyncCloseable(
        "DatabaseSequencer.onClose()",
        super[DatabaseSequencer].onClosed(),
      ),
      AsyncCloseable("done", done, timeouts.shutdownProcessing), // Close the consumer first
      SyncCloseable("blockOrderer.close()", blockOrderer.close()),
      SyncCloseable("cryptoApi.close()", cryptoApi.close()),
      SyncCloseable("circuitBreaker.close()", circuitBreaker.close()),
      SyncCloseable("throughputCap.close()", throughputCap.close()),
    )
  }

  /** Compute traffic states for the specified members at the provided timestamp.
    * @param requestedMembers
    *   members for which to compute traffic states
    * @param selector
    *   timestamp selector determining at what time the traffic states will be computed
    */
  private def trafficStatesForMembers(
      requestedMembers: Set[Member],
      selector: TimestampSelector,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[Member, Either[String, TrafficState]]] =
    if (requestedMembers.isEmpty) {
      // getStates interprets an empty list of members as "return all members"
      // so we handle it here.
      FutureUnlessShutdown.pure(Map.empty)
    } else {
      val timestamp = selector match {
        case ExactTimestamp(timestamp) => Some(timestamp)
        case LastUpdatePerMember => None
        // For the latest safe timestamp, we use the last timestamp of the latest processed block.
        // Even though it may be more recent than the TrafficConsumed timestamp of individual members,
        // we are sure that nothing has been consumed since then, because by the time we update getHeadState.block.lastTs
        // all traffic has been consumed for that block. This means we can use this timestamp to compute an updated
        // base traffic that will be correct. More precisely, we take the immediate successor such that we include
        // all the changes of that last block.
        case LatestSafe => Some(stateManager.getHeadState.block.lastTs.immediateSuccessor)
        case LatestApproximate =>
          Some(clock.now.max(stateManager.getHeadState.block.lastTs.immediateSuccessor))
      }

      blockRateLimitManager.getStates(
        requestedMembers,
        timestamp,
        stateManager.getHeadState.block.latestSequencerEventTimestamp.orElse(
          lsuSequencingBounds.map(_.upgradeTime)
        ),
        // TODO(#18401) set warnIfApproximate to true and check that we don't get warnings
        // Warn on approximate topology or traffic purchased when getting exact traffic states only (so when selector is not LatestApproximate)
        // selector != LatestApproximate

        // Also don't warn until the sequencer has at least received one event
        // This used to check the ephemeral state for headCounter(sequencerId).exists(_ > Genesis),
        // but because the ephemeral state for the block sequencer didn't actually contain
        // any sequencer counter data anymore, this condition was always false, which made the overall expression
        // for warnIfApproximate false
        warnIfApproximate = false,
      )
    }

  override def setTrafficPurchased(
      member: Member,
      serial: PositiveInt,
      totalTrafficPurchased: NonNegativeLong,
      sequencerClient: SequencerClientSend,
      synchronizerTimeTracker: SynchronizerTimeTracker,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficControlError, Unit] =
    for {
      latestBalanceO <- EitherT.right(blockRateLimitManager.lastKnownBalanceFor(member))
      maxSerialO = latestBalanceO.map(_.serial)
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        maxSerialO.forall(_ < serial),
        TrafficControlErrors.TrafficControlSerialTooLow.Error(
          s"The provided serial value $serial is too low. Latest serial used by this member is $maxSerialO"
        ),
      )
      _ <- trafficPurchasedSubmissionHandler.sendTrafficPurchasedRequest(
        member,
        serial,
        totalTrafficPurchased,
        sequencerClient,
        synchronizerTimeTracker,
        cryptoApi,
      )
    } yield ()

  @nowarn("cat=deprecation")
  override def trafficStatus(requestedMembers: Seq[Member], selector: TimestampSelector)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficControlError, SequencerTrafficStatus] =
    for {
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        lsuTrafficInitialized.futureUS.isCompleted,
        TrafficControlErrors.LsuTrafficNotInitialized.Error(),
      )
      topologySnapshot <- EitherT.right(cryptoApi.currentSnapshotApproximation)
      members <- EitherT.right(
        if (requestedMembers.isEmpty) {
          // If requestedMembers is not set get the traffic states of all known members
          topologySnapshot.ipsSnapshot.knownMembers()
        } else {
          topologySnapshot.ipsSnapshot
            .knownMembers()
            .map { registered =>
              requestedMembers.toSet.intersect(registered)
            }
        }
      )
      trafficState <- EitherT.right(
        trafficStatesForMembers(
          members,
          selector,
        )
      )
    } yield SequencerTrafficStatus(trafficState)

  override def getTrafficStateAt(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerRateLimitError.TrafficNotFound, Option[
    TrafficState
  ]] = for {
    _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
      lsuTrafficInitialized.futureUS.isCompleted,
      SequencerRateLimitError.TrafficNotFound(
        member
      ),
    )
    latestSequencerEventTimestamp =
      stateManager.getHeadState.block.latestSequencerEventTimestamp.orElse(
        lsuSequencingBounds.map(_.upgradeTime)
      )
    result <- blockRateLimitManager.getTrafficStateForMemberAt(
      member,
      timestamp,
      latestSequencerEventTimestamp,
    )
  } yield result

  override def sequencingTime(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    blockOrderer.sequencingTime

  override private[canton] def orderer: Some[BlockOrderer] = Some(blockOrderer)

  override private[sequencer] def updateLsuSuccessor(
      successorO: Option[SynchronizerSuccessor],
      announcementEffectiveTime: EffectiveTime,
  )(implicit traceContext: TraceContext): Unit = {
    successorO match {
      case Some(successor) =>
        announcedLsu.set(
          Some(AnnouncedLsu(successor, announcementEffectiveTime, loggerFactory))
        )
      case None => announcedLsu.set(None)
    }
    super.updateLsuSuccessor(successorO, announcementEffectiveTime)
  }

  override def getLsuTrafficControlState(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, LsuTrafficState] =
    for {
      // - Check that LSU is ongoing
      upgrade <- EitherT
        .fromOption[FutureUnlessShutdown](
          announcedLsu.get(),
          SequencerError.NoOngoingLsu.Error(cryptoApi.psid, cryptoApi.topologyKnownUntilTimestamp),
        )
        .leftWiden[CantonBaseError]
      // - Traffic control is enabled
      _ <- EitherT(
        cryptoApi.ips.currentSnapshotApproximation
          .flatMap(
            _.trafficControlParameters(protocolVersion)
          )
          .map(_.toRight[CantonBaseError](TrafficControlDisabled.Error()))
      )

      // - Basic time check against the wall clock
      _ <- {
        val now = clock.now
        EitherTUtil.condUnitET[FutureUnlessShutdown](
          now >= upgrade.successor.upgradeTime,
          SequencerError.NotAtUpgradeTimeOrBeyond.Error(
            upgrade.successor.upgradeTime,
            Some(now),
          ),
        )
      }

      // - Check that sequencer has reached the upgrade time
      latestPersistedBlockTimeO <- EitherT.right[LsuSequencerError](
        // - Traffic states have been persisted (the block completion is written last, after traffic)
        // TODO(##29986): When traffic accounting is made async, we need a different way to determine that
        //  it has become consistent (accounting reached the upgrade time)
        store.readHeadBlockInfo().map(_.map(_.lastTs))
      )
      latestSequencerEventTimestamp = stateManager.getHeadState.block.latestSequencerEventTimestamp
        .orElse(
          lsuSequencingBounds.map(_.upgradeTime)
        )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        latestPersistedBlockTimeO.exists(_ >= upgrade.successor.upgradeTime),
        SequencerError.NotAtUpgradeTimeOrBeyond.Error(
          upgrade.successor.upgradeTime,
          latestPersistedBlockTimeO,
        ),
      )
      // We use the topology snapshot at upgrade time to await the processing of member registrations,
      // so that traffic state doesn't miss any members.
      // Technically this should not be necessary due to the topology freeze around LSU.
      _ <- EitherT.right[LsuSequencerError](
        SyncCryptoClient.getSnapshotForTimestamp(
          cryptoApi,
          upgrade.successor.upgradeTime,
          latestSequencerEventTimestamp,
        )
      )
      // - All checks passed
      // - Get all members known at the upgrade time
      allMembers <- EitherT.right[LsuSequencerError](
        dbSequencerStore.allRegisteredMembers(registeredAtBeforeInclusive =
          upgrade.successor.upgradeTime
        )
      )
      // - Get traffic states at the upgrade time for all members
      consumedRecordsPerMember <- EitherT.right[LsuSequencerError](
        blockRateLimitManager.trafficConsumedStore
          .lookupLatestBeforeInclusive(upgrade.successor.upgradeTime.immediatePredecessor)
          .map(_.map(tc => tc.member -> tc).toMap)
      )
      // TODO(#29997): This doesn't scale to millions of traffic accounts, need a batch method or a different approach
      trafficStates <-
        MonadUtil
          .parTraverseWithLimit(batchingConfig.parallelism)(allMembers.toList) { member =>
            val trafficConsumedO = consumedRecordsPerMember.get(member)
            val trafficPurchasedET =
              blockRateLimitManager.trafficPurchasedManager.getTrafficPurchasedAt(
                member,
                upgrade.successor.upgradeTime,
                latestSequencerEventTimestamp.orElse(lsuSequencingBounds.map(_.upgradeTime)),
              )
            trafficPurchasedET.map { trafficPurchasedO =>
              member -> trafficConsumedO
                .getOrElse(TrafficConsumed.init(member))
                .toTrafficState(trafficPurchasedO)
            }
          }
          .leftMap(error =>
            SequencerError.LsuTrafficNotFound.Error(
              s"Failed to get traffic states for all members at upgrade time: $error"
            ): CantonBaseError
          )
          .map(_.toMap)
      _ = {
        if (logger.underlying.isDebugEnabled) {
          logger.debug(
            "Traffic states at LSU time for all members:"
          )
          trafficStates.foreach { case (member, trafficState) =>
            logger.debug(s"\t- $member: $trafficState")
          }
        }
      }

    } yield {
      LsuTrafficState(trafficStates)(
        LsuTrafficState.protocolVersionRepresentativeFor(protocolVersion)
      )
    }

  private val runningSetLsuTraffic =
    new AtomicReference[Option[PromiseUnlessShutdown[Either[CantonBaseError, Unit]]]](None)
  override def setLsuTrafficControlState(
      state: LsuTrafficState
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] = {
    val promise = PromiseUnlessShutdown.unsupervised[Either[CantonBaseError, Unit]]()
    val currentPromiseO = runningSetLsuTraffic.compareAndExchange(None, Some(promise))
    currentPromiseO match {
      case Some(otherPromise) => EitherT(otherPromise.futureUS)
      case None =>
        val resultET: EitherT[FutureUnlessShutdown, CantonBaseError, Unit] = for {
          // - Check that there's an LSU predecessor
          upgradeTime <- EitherT
            .fromOption[FutureUnlessShutdown](
              lsuSequencingBounds.map(_.upgradeTime),
              MissingSynchronizerPredecessor.Error(cryptoApi.psid, sequencerId),
            )
            .leftWiden[CantonBaseError]
          _ <- EitherT(
            cryptoApi.ips.currentSnapshotApproximation
              .flatMap(
                _.trafficControlParameters(protocolVersion)
              )
              .map(_.toRight(TrafficControlDisabled.Error()))
          )
          // Check if the initialization has already been completed
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            !lsuTrafficInitialized.futureUS.isCompleted,
            LsuTrafficAlreadyInitialized.Error(cryptoApi.psid),
          )
          // - Check that the node has not progressed beyond the upgrade time
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            stateManager.getHeadState.block.lastTs <= upgradeTime,
            SequencerPastUpgradeTime.Error(
              cryptoApi.psid,
              stateManager.getHeadState.block.lastTs,
              upgradeTime,
            ),
          )

          // Check that all members registered via topology are present in the provided traffic state
          allMembers <- EitherT.right[LsuSequencerError](
            dbSequencerStore.allRegisteredMembers(registeredAtBeforeInclusive = upgradeTime)
          )
          missingMembers = allMembers.diff(state.membersTraffic.keySet)
          _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
            // We expect all members mentioned in topology to have a traffic record.
            // Some may have been offboarded or disabled, so the provided state may contain more members.
            missingMembers.isEmpty,
            InvalidTrafficState.Error(
              s"The provided traffic states must contain traffic state for all members known in topology at the upgrade time $upgradeTime, missing members: $missingMembers"
            ),
          )

          // - Clean up the stores
          _ = logger.debug(s"Clearing existing LSU traffic control state")
          _ <- EitherT.right(blockRateLimitManager.trafficConsumedStore.truncate())
          _ <- EitherT.right(trafficPurchasedStore.truncate())

          // - Set the traffic states for all members provided in 'state'
          _ = logger.debug(s"Setting LSU traffic consumed")
          membersTraffic = state.membersTraffic.toList.sortBy { case (_, state) => state.timestamp }
          _ <- EitherT.right(
            blockRateLimitManager.trafficConsumedStore.store(
              membersTraffic.map { case (member, trafficState) =>
                val trafficConsumed = trafficState.toTrafficConsumed(member)
                logger.debug(
                  s"Setting LSU traffic consumed for member $member to $trafficConsumed"
                )
                trafficConsumed
              }
            )
          )
          _ <- EitherT.right(
            MonadUtil.parTraverseWithLimit_(batchingConfig.parallelism)(
              membersTraffic.flatMap { case (member, trafficState) =>
                trafficState.toTrafficPurchased(member).toList
              }
            ) { trafficPurchasedRecord =>
              logger.debug(
                s"Setting LSU traffic purchased for member ${trafficPurchasedRecord.member} to $trafficPurchasedRecord"
              )
              val updateTrafficPurchasedRecord =
                trafficPurchasedRecord.copy(sequencingTimestamp = upgradeTime.immediatePredecessor)
              blockRateLimitManager.trafficPurchasedManager.addTrafficPurchased(
                updateTrafficPurchasedRecord
              )
            }
          )
          // The method below normally deletes the data > the passed argument for crash recovery purposes.
          // With CantonTimestamp.MaxValue this will only reset the TrafficConsumedManager's cache.
          _ <- EitherT.right(blockRateLimitManager.resetStateTo(CantonTimestamp.MaxValue))
          _ <- EitherT.right(trafficPurchasedStore.setInitialTimestamp(upgradeTime))
        } yield {
          blockRateLimitManager.trafficPurchasedManager.tick(upgradeTime)
          logger.info(s"LSU traffic control state has been initialized")
          lsuTrafficInitialized.success(UnlessShutdown.unit)
          ()
        }
        resultET.value.onComplete { result =>
          runningSetLsuTraffic.set(None)
          promise.complete(result)
        }
        resultET
    }
  }

  override def getThroughputCap(
      requestType: SubmissionRequestType
  ): Option[BlockSequencerConfig.IndividualThroughputCapConfig] =
    throughputCap.getCap(requestType)

  override def setThroughputCap(
      requestType: SubmissionRequestType,
      config: Option[BlockSequencerConfig.IndividualThroughputCapConfig],
  )(implicit traceContext: TraceContext): Unit = throughputCap.replaceCap(requestType, config)

  override def performLsuSequencingTest(mediatorGroupRecipient: MediatorGroupRecipient)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CantonBaseError, Unit] = {

    def sign[A <: HasCryptographicEvidence](
        syncCryptoApi: SynchronizerSnapshotSyncCryptoApi,
        hashPurpose: HashPurpose,
        request: A,
    ): EitherT[FutureUnlessShutdown, ErrorCode.Wrap, SignedContent[A]] =
      RequestSigner(cryptoApi, loggerFactory)
        .signRequest(
          request = request,
          hashPurpose = hashPurpose,
          snapshot = syncCryptoApi,
          approximateTimestampOverride = None,
        )
        .leftMap { err =>
          val message = s"Error signing submission request $err"
          logger.error(message)
          SendAsyncClientError.ErrorCode
            .Wrap(SendAsyncClientError.RequestFailed(message))
        }

    for {
      syncCryptoApi <- EitherT.liftF(cryptoApi.currentSnapshotApproximation)

      _ <- OptionT(syncCryptoApi.ipsSnapshot.mediatorGroup(mediatorGroupRecipient.group)).toRight(
        SendAsyncClientError.ErrorCode
          .Wrap(
            SendAsyncClientError.RequestInvalid(
              s"Mediator group ${mediatorGroupRecipient.group} does not exist at timestamp ${syncCryptoApi.ipsSnapshot.timestamp}"
            )
          )
      )

      content = LsuSequencingTestMessageContent.create(psid = cryptoApi.psid, sender = sequencerId)
      signedContent <- sign(syncCryptoApi, HashPurpose.LsuSequencingTestMessageContent, content)

      batch = Batch.of(
        protocolVersion,
        (
          LsuSequencingTestMessage(signedContent.content, signedContent.signature),
          Recipients.cc(mediatorGroupRecipient),
        ),
      )
      messageId = MessageId.randomMessageId()

      submissionRequest <- SubmissionRequest
        .create(
          sequencerId,
          messageId,
          Batch.closeEnvelopes(batch),
          maxSequencingTime = CantonTimestamp.MaxValue,
          topologyTimestamp = None,
          aggregationRule = None,
          submissionCost = None, // sequencer is the sender
          SubmissionRequest.protocolVersionRepresentativeFor(protocolVersion),
        )
        .leftMap(err =>
          SendAsyncClientError.ErrorCode
            .Wrap(SendAsyncClientError.RequestInvalid(s"Unable to get submission request: $err"))
        )
        .toEitherT[FutureUnlessShutdown]

      signedSubmissionRequest <- sign(
        syncCryptoApi,
        HashPurpose.SubmissionRequestSignature,
        submissionRequest,
      )

      _ = logger.info(
        s"Creation of LsuSequencingTestMessage with mediator group $mediatorGroupRecipient and message id $messageId succeeded"
      )

      _ <- sendAsyncSignedInternal(signedSubmissionRequest, skipLsuChecks = true)
    } yield ()
  }
}
