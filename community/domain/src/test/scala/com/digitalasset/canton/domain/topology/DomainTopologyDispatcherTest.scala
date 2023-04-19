// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble}
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.crypto.DomainSnapshotSyncCryptoApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.DomainNodeParameters
import com.digitalasset.canton.domain.topology.DomainTopologySender.{
  TopologyDispatchingDegradation,
  TopologyDispatchingInternalError,
}
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.protocol.messages.DomainTopologyTransactionMessage
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.{
  RequestInvalid,
  RequestRefused,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendResult,
  SequencerClient,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  Deliver,
  DeliverError,
  DeliverErrorReason,
  Envelope,
  MessageId,
  OpenEnvelope,
  SendAsyncError,
}
import com.digitalasset.canton.store.memory.InMemorySequencerCounterTrackerStore
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{DomainId, Member, TestingOwnerWithKeys}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  MockedNodeParameters,
  SequencerCounter,
}
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise, blocking}
import scala.util.{Failure, Success}

class DomainTopologyDispatcherTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with MockClock { self =>

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  private def domainNodeParameters(
      processingTimeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing,
      cachingConfigs: CachingConfigs = CachingConfigs.testing,
  ) = DomainNodeParameters(
    general = MockedNodeParameters.cantonNodeParameters(
      processingTimeouts,
      cachingConfigs,
    ),
    protocol = CantonNodeParameters.Protocol.Impl(
      devVersionSupport = false,
      dontWarnOnDeprecatedPV = false,
      initialProtocolVersion = testedProtocolVersion,
    ),
    maxBurstFactor = PositiveDouble.tryCreate(1.0),
  )

  case class Awaiter(
      atLeast: Int,
      observation: Seq[Long],
      current: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      recipients: Set[Member],
      promise: Promise[Awaiter] = Promise(),
  ) extends PrettyPrinting
      with Product
      with Serializable {

    override def pretty: Pretty[Awaiter] = prettyOfClass(
      param("atLeast", _.atLeast),
      param("observation", _.atLeast),
      param("current", _.current),
      param("recipients", _.recipients),
    )

    def add(
        append: Seq[SignedTopologyTransaction[TopologyChangeOp]],
        recps: Set[Member],
    ): Awaiter = {
      val updated = {
        copy(
          current = current ++ append,
          recipients = recipients ++ recps,
          observation = observation :+ System.nanoTime(),
        )
      }
      updated.updateAtLeast(updated.atLeast)
    }

    def updateAtLeast(atLeast: Int): Awaiter =
      if (current.length >= atLeast && atLeast > 0) {
        // try, as might be called several times (with same result) during contention
        promise.trySuccess(this)
        Awaiter(0, Seq(), Seq(), Set())
      } else copy(atLeast = atLeast)

    def compare(expected: SignedTopologyTransaction[TopologyChangeOp]*): Assertion = {
      current.map(_.transaction.element.mapping) shouldBe expected.map(
        _.transaction.element.mapping
      )
    }

  }

  private def toValidated(
      tx: SignedTopologyTransaction[TopologyChangeOp]
  ): ValidatedTopologyTransaction =
    ValidatedTopologyTransaction(tx, None)

  final class Fixture
      extends TestingOwnerWithKeys(domainManager, loggerFactory, parallelExecutionContext) {

    val ts0 = CantonTimestamp.Epoch
    val ts1 = ts0.plusSeconds(1)
    val ts2 = ts1.plusSeconds(1)

    val sourceStore = new InMemoryTopologyStore(
      TopologyStoreId.AuthorizedStore,
      loggerFactory,
      timeouts,
      futureSupervisor,
    )
    val targetStore =
      new InMemoryTopologyStore(
        TopologyStoreId.DomainStore(domainId),
        loggerFactory,
        timeouts,
        futureSupervisor,
      )

    val manager = mock[DomainTopologyManager]
    when(manager.store).thenReturn(sourceStore)

    val topologyClient = mock[DomainTopologyClientWithInit]

    val clock = mockClock

    val parameters = domainNodeParameters(DefaultProcessingTimeouts.testing, CachingConfigs.testing)

    val lock = new Object()
    val awaiter = new AtomicReference[Awaiter](Awaiter(0, Seq(), Seq(), Set()))
    def expect(atLeast: Int): Future[Awaiter] = {
      logger.debug(s"Expecting $atLeast")
      blocking(lock.synchronized {
        awaiter
          .getAndUpdate(cur => cur.updateAtLeast(atLeast))
          .promise
          .future
          .thereafter {
            case Success(cur) => logger.debug(s"Awaiter returned $cur")
            case Failure(ex) => logger.debug("Awaiter failed with an exception", ex)
          }
      })
    }

    val sendDelay = new AtomicReference[Future[Unit]](Future.unit)
    val senderFailure =
      new AtomicReference[Option[EitherT[FutureUnlessShutdown, String, Unit]]](None)

    val sender = new DomainTopologySender() {
      override def sendTransactions(
          snapshot: DomainSnapshotSyncCryptoApi,
          transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
          recipients: Set[Member],
      )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
        logger.debug(
          s"Observed ${transactions.map(_.transaction.element.mapping)}"
        )
        blocking(lock.synchronized {
          awaiter.updateAndGet(_.add(transactions, recipients))
        })
        val ret = EitherT.right[String](
          FutureUnlessShutdown.outcomeF(sendDelay.get())
        )
        senderFailure.get().fold(ret)(failure => ret.flatMap(_ => failure))
      }

      override def onClosed(): Unit = {}
      override protected val timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing
      override protected val initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
      override val name: String = "domain-topology-sender"
      override protected val loggerFactory: NamedLoggerFactory = self.loggerFactory
    }
    val processor = mock[TopologyTransactionProcessor]

    def mkDispatcher = new DomainTopologyDispatcher(
      domainId,
      testedProtocolVersion,
      sourceStore,
      processor,
      Map(),
      targetStore,
      this.cryptoApi.crypto,
      clock,
      false,
      parameters,
      FutureSupervisor.Noop,
      sender,
      loggerFactory,
      new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts),
    )
    val dispatcher = mkDispatcher

    def init(flusher: Future[Unit] = Future.unit): Future[Unit] = {
      initDispatcher(dispatcher, flusher)
    }

    def initDispatcher(
        dispatcher: DomainTopologyDispatcher,
        flusher: Future[Unit],
    ): Future[Unit] = {
      dispatcher
        .init(FutureUnlessShutdown.outcomeF(flusher))
        .failOnShutdown("dispatcher initialization")
    }

    def close(): Unit = {}

    def submit(
        ts: CantonTimestamp,
        tx: SignedTopologyTransaction[TopologyChangeOp]*
    ): Future[Unit] = submit(dispatcher, ts, tx: _*)
    def submit(
        d: DomainTopologyDispatcher,
        ts: CantonTimestamp,
        tx: SignedTopologyTransaction[TopologyChangeOp]*
    ): Future[Unit] = {
      logger.debug(
        s"Submitting at $ts ${tx.map(x => (x.operation, x.transaction.element.mapping))}"
      )
      append(sourceStore, ts, tx).map { _ =>
        d.addedSignedTopologyTransaction(ts, tx)
      }
    }

    def append(
        store: TopologyStore[TopologyStoreId],
        ts: CantonTimestamp,
        txs: Seq[SignedTopologyTransaction[TopologyChangeOp]],
    ): Future[Unit] = {
      store.append(SequencedTime(ts), EffectiveTime(ts), txs.map(toValidated))
    }

    def txs = this.TestingTransactions

    def genPs(state: ParticipantPermission) = mkAdd(
      ParticipantState(
        RequestSide.Both,
        domainId,
        participant1,
        state,
        TrustLevel.Ordinary,
      )
    )
    val mpsS = genPs(ParticipantPermission.Submission)
    val mpsO = genPs(ParticipantPermission.Observation)
    val mpsC = genPs(ParticipantPermission.Confirmation)
    val mpsD = genPs(ParticipantPermission.Disabled)

  }

  type FixtureParam = Fixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Fixture
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  "domain topology dispatcher" should {

    "dispatch" when {
      "transactions in sequence" in { f =>
        import f.*
        val grabF = expect(3)
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, txs.id1k1)
          _ <- submit(ts1, txs.id2k2)
          res <- grabF
        } yield {
          res.compare(txs.ns1k1, txs.id1k1, txs.id2k2)
        }
      }
      "end batch if we have a domain parameters change or a participant state change" in { f =>
        import f.*
        val grabF = expect(2)
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, txs.ps1, txs.id1k1)
          res <- grabF
          grab2 = expect(3)
          - <- submit(ts1, txs.okm1, txs.dpc1, txs.ns1k2)
          res2 <- grab2
          res3 <- expect(1)
        } yield {
          res.compare(txs.ns1k1, txs.ps1)
          res2.compare(txs.id1k1, txs.okm1, txs.dpc1)
          res3.compare(txs.ns1k2)
        }
      }
      "delay second dispatch on effective time update" in { f =>
        import f.*
        val tdp = TestDomainParameters.defaultDynamic
        def dpc(factor: Int) =
          mkDmGov(
            DomainParametersChange(
              DomainId(uid),
              tdp.tryUpdate(topologyChangeDelay =
                tdp.topologyChangeDelay * NonNegativeInt.tryCreate(factor)
              ),
            ),
            SigningKeys.key2,
          )
        val dpc1 = dpc(1)
        val dpc2 = dpc(2)
        for {
          _ <- f.init()
          _ <- submit(ts0, dpc2)
          res <- expect(1)
          _ <- submit(ts1, dpc1)
          res2 <- expect(1)
          _ <- submit(ts2, txs.ns1k1)
          res3 <- expect(1)
        } yield {
          res.compare(dpc2)
          res2.compare(dpc1)
          res3.compare(txs.ns1k1)
          val first = res2.observation.headOption.valueOrFail("should have observation")
          val snd = res3.observation.headOption.valueOrFail("should have observation")
          val delta = tdp.topologyChangeDelay * NonNegativeInt.tryCreate(2)
          (snd - first) should be > delta.duration.toNanos
        }
      }
    }

    "abort" when {

      def shouldHalt(f: Fixture): Future[Assertion] = {
        import f.*
        loggerFactory.assertLogs(
          for {
            _ <- f.init()
            _ <- submit(ts0, txs.ns1k1)
          } yield { assert(true) },
          _.errorMessage should include("Halting topology dispatching"),
        )
      }

      "on fatal submission exceptions" in { f =>
        f.senderFailure.set(Some(EitherT.right(FutureUnlessShutdown.failed(new Exception("booh")))))
        shouldHalt(f)
      }
      "on fatal submission failures failures" in { f =>
        f.senderFailure.set(Some(EitherT.leftT("booh")))
        shouldHalt(f)
      }
    }

    "resume" when {
      "restarting idle" in { f =>
        import f.*
        logger.debug("restarting when idle")
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, txs.id1k1)
          _ <- expect(2)
          d2 = f.mkDispatcher
          _ <- f.initDispatcher(d2, Future.unit)
          _ <- submit(d2, ts1, txs.okm1, txs.ns1k2)
          res <- expect(2)
        } yield {
          d2.close()
          res.compare(txs.okm1, txs.ns1k2)
        }
      }
      "restarting with somewhat pending txs" in { f =>
        import f.*
        trait TestFlusher {
          def foo: Future[Unit]
        }
        val flusher = mock[TestFlusher]
        when(flusher.foo).thenReturn(Future.unit)
        for {
          _ <- f.append(sourceStore, ts0, Seq(txs.ns1k2))
          _ <- f.append(sourceStore, ts1, Seq(txs.ns1k1, txs.okm1))
          _ <- f.targetStore.updateDispatchingWatermark(ts0)
          _ <- f.append(targetStore, ts1, Seq(txs.ns1k2, txs.ns1k1))
          _ <- f.init(flusher.foo)
          res <- f.expect(1)
        } yield {
          // ensure we've flushed the system
          verify(flusher, times(1)).foo
          // first tx should not be submitted due to watermark, and second should be filtered out
          res.compare(txs.okm1)
        }
      }
      "racy start" in { f =>
        import f.*
        for {
          // add one into the store
          _ <- f.append(sourceStore, ts0, Seq(txs.ns1k2))
          // now add one into the store and into the queue
          _ <- submit(ts1, txs.ns1k1)
          // now add one just into the queue (so won't be picked up during init when we read from the store)
          _ = f.dispatcher.addedSignedTopologyTransaction(ts2, Seq(txs.okm1))
          _ <- f.init()
          res <- expect(3)
        } yield {
          res.compare(txs.ns1k2, txs.ns1k1, txs.okm1)
        }
      }
    }

    "bootstrapping participants" when {
      "send snapshot to new participant" in { f =>
        import f.*
        val grabF = expect(1)
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.dpc1)
          res1 <- grabF
          grab2F = expect(4)
          _ <- submit(ts1, txs.ns1k1, txs.okm1, txs.ps1)
          _ = submit(ts2, txs.okm1)
          res2 <- grab2F
          res3 <- expect(1)
        } yield {
          res1.compare(txs.dpc1)
          res1.recipients should not contain (participant1)
          res2.compare(txs.dpc1, txs.ns1k1, txs.okm1, txs.ps1)
          res2.recipients should contain(participant1)
          res3.compare(txs.okm1)
          res3.recipients should contain(participant1)
        }
      }

      "keep distributing on non-deactivation changes" in { f =>
        import f.*
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, mpsO)
          _ <- expect(2)
          _ <- submit(ts1, txs.okm1)
          res1 <- expect(1)
          _ <- submit(ts2, mpsC)
          _ <- submit(ts2.plusMillis(1), revert(mpsO))
          res2 <- expect(2)
          _ <- submit(ts2.plusMillis(2), mpsS)
          res3a <- expect(1)
          _ <- submit(ts2.plusMillis(3), revert(mpsC))
          res3b <- expect(1)
          grabF = expect(1)
          _ <- submit(ts2.plusMillis(4), mpsD)
          _ <- submit(ts2.plusMillis(5), revert(mpsS))
          res4a <- grabF
          res4b <- expect(1)
          _ <- submit(ts2.plusMillis(6), txs.dpc1)
          res5 <- expect(1)
        } yield {
          res1.recipients should contain(participant1)
          res2.recipients should contain(participant1)
          res3a.recipients should contain(participant1)
          res3b.recipients should contain(participant1)
          res4a.recipients should contain(participant1)
          res4b.recipients should not contain (participant1)
          res5.recipients should not contain (participant1)
        }
      }

      "resume distribution to re-activated participants" in { f =>
        import f.*
        val mpsS2 = genPs(ParticipantPermission.Observation)
        val rmpsS = revert(mpsS)
        val rmpsD = revert(mpsD)
        for {
          _ <- f.init()
          _ <- submit(ts0, txs.ns1k1, mpsS)
          _ <- submit(ts0.immediateSuccessor, mpsD)
          _ <- expect(3)
          _ <- submit(ts1.immediatePredecessor, rmpsS, txs.ns1k1)
          res1 <- expect(2)
          _ <- submit(ts1, mpsS2)
          res2 <- expect(1)
          _ <- submit(ts1.immediateSuccessor, rmpsD)
          res3 <- expect(4)
          _ <- submit(ts2, txs.okm1)
          res4 <- expect(1)
        } yield {
          res1.recipients should not contain (participant1)
          res2.recipients should not contain (participant1)
          res3.recipients should contain(participant1)
          res3.compare(rmpsS, txs.ns1k1, mpsS2, rmpsD) // includes catchup
          // catchup and dispatch
          res3.observation should have length (2)
          res4.recipients should contain(participant1)
        }
      }

    }

  }

}

trait MockClock {

  this: BaseTest =>

  private[topology] def mockClock: Clock = {
    val clock = mock[Clock]
    when(clock.scheduleAfter(any[CantonTimestamp => Unit], any[java.time.Duration])).thenAnswer {
      (task: CantonTimestamp => Unit, duration: java.time.Duration) =>
        Threading.sleep(duration.toMillis)
        logger.debug("Done waiting")
        task(CantonTimestamp.Epoch)
        FutureUnlessShutdown.unit
    }
  }
}

class DomainTopologySenderTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with MockClock {

  import com.digitalasset.canton.topology.DefaultTestIdentities.*

  case class Response(
      sync: Either[SendAsyncClientError, Unit],
      async: Option[SendResult] = Some(SendResult.Success(mock[Deliver[Envelope[_]]])),
      await: Future[Unit] = Future.unit,
  ) {
    val sendNotification: Promise[Batch[OpenEnvelope[DomainTopologyTransactionMessage]]] = Promise()
  }

  class Fixture
      extends TestingOwnerWithKeys(domainManager, loggerFactory, parallelExecutionContext) {

    val client = mock[SequencerClient]
    val timeTracker = mock[DomainTimeTracker]
    val clock = mockClock

    val responses = new AtomicReference[List[Response]](List.empty)
    val sender = new DomainTopologySender.Impl(
      domainId,
      testedProtocolVersion,
      client,
      timeTracker,
      clock,
      maxBatchSize = 1,
      retryInterval = 100.millis,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
      FutureSupervisor.Noop,
    ) {
      override def send(
          batch: Batch[OpenEnvelope[DomainTopologyTransactionMessage]],
          callback: SendCallback,
      )(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Either[SendAsyncClientError, Unit]] = {
        logger.debug(s"Send invoked with ${batch}")
        responses.getAndUpdate(_.drop(1)) match {
          case Nil =>
            logger.error("Unexpected send!!!")
            FutureUnlessShutdown.pure(Left(RequestInvalid("unexpected send")))
          case one :: _ =>
            one.sendNotification.success(batch)
            one.await.foreach { _ =>
              one.async.map(UnlessShutdown.Outcome(_)).foreach(callback)
            }
            FutureUnlessShutdown.pure(one.sync)
        }
      }
    }
    val snapshot = cryptoApi.currentSnapshotApproximation

    def submit(
        recipients: Set[Member],
        transactions: SignedTopologyTransaction[TopologyChangeOp]*
    ) = {
      sender
        .sendTransactions(snapshot, transactions, recipients)
        .value
        .onShutdown(Left(("shutdown")))
    }

    def respond(
        response: Response
    ): Future[Batch[OpenEnvelope[DomainTopologyTransactionMessage]]] = {
      responses.updateAndGet(_ :+ response)
      response.sendNotification.future
    }

    val txs = TestingTransactions

  }

  type FixtureParam = Fixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Fixture
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {}
  }

  "domain topology sender" should {
    "split batch" when {
      "getting multiple transactions" in { f =>
        import f.*
        val resp1F = respond(Response(sync = Right(())))
        val resp2F = respond(Response(sync = Right(())))
        val subF = submit(Set(participant1), txs.id1k1, txs.okm1)

        for {
          res <- subF
          one <- resp1F
          two <- resp2F
        } yield {
          assert(res.isRight)
          one.envelopes.flatMap(_.protocolMessage.transactions) shouldBe Seq(txs.id1k1)
          two.envelopes.flatMap(_.protocolMessage.transactions) shouldBe Seq(txs.okm1)
        }
      }
    }

    "abort" when {
      "on fatal submission failures" in { f =>
        import f.*
        val respondF = respond(
          Response(sync = Left(RequestRefused(SendAsyncError.RequestInvalid("booh"))), async = None)
        )
        loggerFactory.assertLogs(
          {
            val submF = submit(Set(participant1), txs.id1k1)
            for {
              res <- submF
              _ <- respondF
            } yield {
              assert(res.isLeft, res)
            }
          },
          _.shouldBeCantonErrorCode(TopologyDispatchingInternalError),
        )
      }
      "on fatal send tracker failures" in { f =>
        import f.*
        val respondF = respond(
          Response(
            sync = Right(()),
            async = Some(
              SendResult.Error(
                DeliverError.create(
                  counter = SequencerCounter(1),
                  timestamp = CantonTimestamp.Epoch,
                  domainId,
                  messageId = MessageId.tryCreate("booh"),
                  reason = DeliverErrorReason.BatchInvalid("booh"),
                  protocolVersion = testedProtocolVersion,
                )
              )
            ),
          )
        )
        loggerFactory.assertLogs(
          submit(Set(participant1), txs.id1k1).flatMap(res =>
            respondF.map { _ =>
              assert(res.isLeft, res)
            }
          ),
          _.shouldBeCantonErrorCode(TopologyDispatchingInternalError),
        )
      }
    }

    "retry" when {

      def checkDegradation(f: Fixture, failure: Response): Future[Assertion] = {
        import f.*
        val resp1F = respond(failure)
        val resp2F = respond(Response(sync = Right(())))
        val stage1F = loggerFactory.assertLogs(
          {
            val submF = submit(Set(participant1), txs.id1k1)
            for {
              res <- resp1F
            } yield (res, submF)
          },
          _.shouldBeCantonErrorCode(TopologyDispatchingDegradation),
        )
        for {
          stage1 <- stage1F
          (res1, submF) = stage1
          res2 <- resp2F
          sub <- submF
        } yield {
          // we retried
          res1 shouldBe res2
          assert(sub.isRight)
        }

      }

      "on retryable submission failures" in { f =>
        checkDegradation(
          f,
          Response(sync = Left(RequestRefused(SendAsyncError.Overloaded("boooh"))), async = None),
        )
      }
      "on send tracker timeouts" in { f =>
        checkDegradation(
          f,
          Response(sync = Right(()), async = Some(SendResult.Timeout(CantonTimestamp.Epoch))),
        )
      }
    }
  }

}
