// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.QueueOfferResult
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.data.{EitherT, NonEmptyList}
import cats.syntax.functor._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerHighAvailabilityConfig.SingleSequencerTotalNodeCount
import com.digitalasset.canton.domain.sequencing.sequencer.store._
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member, ParticipantId}
import com.digitalasset.canton.lifecycle.{AsyncCloseable, AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, TestDomainParameters}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.AkkaUtil
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
class SequencerWriterSourceTest extends AsyncWordSpec with BaseTest with HasExecutorService {

  class MockEventSignaller extends EventSignaller {
    override val timeouts = SequencerWriterSourceTest.this.timeouts
    private val listenerRef =
      new AtomicReference[Option[WriteNotification => Unit]](None)

    def attachWriteListener(listener: WriteNotification => Unit): AutoCloseable = {
      if (!listenerRef.compareAndSet(None, Some(listener))) {
        // suggests something is screwed up with the test
        fail("There is already an active listener subscribed")
      }

      () => listenerRef.set(None)
    }

    override def notifyOfLocalWrite(
        notification: WriteNotification
    )(implicit traceContext: TraceContext): Future[Unit] =
      Future.successful {
        listenerRef.get().foreach(listener => listener(notification))
      }

    override def readSignalsForMember(
        member: Member,
        memberId: SequencerMemberId,
    ): Source[ReadSignal, NotUsed] =
      ???

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq()
    override protected def logger: TracedLogger =
      SequencerWriterSourceTest.this.logger
  }

  class Env(highAvailabilityConfig: SequencerHighAvailabilityConfig) extends FlagCloseableAsync {
    override val timeouts = SequencerWriterSourceTest.this.timeouts
    protected val logger = SequencerWriterSourceTest.this.logger
    implicit val actorSystem = ActorSystem()
    val instanceIndex: Int = 0
    val keepAliveFrequency = NonNegativeFiniteDuration.ofMillis(100)
    val testWriterConfig =
      SequencerWriterConfig.LowLatency(eventWriteBatchMaxSize = 1, payloadWriteBatchMaxSize = 1)

    class InMemoryStoreWithTimeAdvancement(lFactory: NamedLoggerFactory)(implicit
        ec: ExecutionContext
    ) extends InMemorySequencerStore(lFactory)(ec) {
      private val timeAdvancement = new AtomicReference[java.time.Duration](java.time.Duration.ZERO)

      def setClockAdvanceBeforeSavePayloads(duration: java.time.Duration): Unit =
        timeAdvancement.set(duration)

      override def savePayloads(
          payloadsToInsert: NonEmptyList[Payload],
          instanceDiscriminator: UUID,
      )(implicit traceContext: TraceContext): EitherT[Future, SavePayloadsError, Unit] = {
        clock.advance(timeAdvancement.get())
        super.savePayloads(payloadsToInsert, instanceDiscriminator)
      }
    }

    val store =
      new InMemoryStoreWithTimeAdvancement(loggerFactory) // allows setting time advancements
    val writerStore = SequencerWriterStore.singleInstance(store)
    val clock = new SimClock(loggerFactory = loggerFactory)

    val domainParameters = DynamicDomainParameters.initialValues(clock)
    val eventSignaller = new MockEventSignaller

    // explicitly pass a real execution context so shutdowns don't deadlock while Await'ing completion of the done
    // future while still finishing up running tasks that require an execution context
    val (writer, doneF) = AkkaUtil.runSupervised(
      logger.error("Writer flow failed", _), {
        SequencerWriterSource(
          testWriterConfig,
          highAvailabilityConfig,
          TestDomainParameters.domainSyncCryptoApi(
            DefaultTestIdentities.domainId,
            loggerFactory,
            clock,
          ),
          writerStore,
          clock,
          eventSignaller,
          loggerFactory,
        )(executorService, implicitly[TraceContext])
          .toMat(Sink.ignore)(Keep.both)
      },
    )

    def completeFlow(): Future[Unit] = {
      writer.complete()
      doneF.void
    }

    def offerDeliverOrFail(event: Presequenced[DeliverStoreEvent[Payload]]): Unit =
      writer.deliverEventQueue.offer(event) shouldBe QueueOfferResult.Enqueued

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
      Seq(
        AsyncCloseable("sequencer", writer.complete(), 10.seconds),
        AsyncCloseable("done", doneF, 10.seconds),
        AsyncCloseable("actorSystem", actorSystem.terminate(), 10.seconds),
      )
  }

  def withEnv(
      highAvailabilityConfig: SequencerHighAvailabilityConfig = SequencerHighAvailabilityConfig()
  )(testCode: Env => Future[Assertion]): Future[Assertion] = {
    val env = new Env(highAvailabilityConfig)
    val result = testCode(env)
    result.onComplete(_ => env.close())
    result
  }

  private val alice = ParticipantId("alice")
  private val bob = ParticipantId("bob")
  private val messageId1 = MessageId.tryCreate("1")
  private val messageId2 = MessageId.tryCreate("2")
  private val nextPayload = new AtomicLong(1)
  def generatePayload(): Payload = {
    val n = nextPayload.getAndIncrement()
    Payload(PayloadId(CantonTimestamp.assertFromLong(n)), ByteString.copyFromUtf8(n.toString))
  }
  private val payload1 = generatePayload()
  private val payload2 = generatePayload()

  "payload to event time bound" should {

    "prevent sequencing deliver events if their payloads are too old" in withEnv() { env =>
      import env._

      // setup advancing the clock past the payload to event bound immediately after the payload is persisted
      // (but before the event time is generated which will then be beyond a valid bound).
      store.setClockAdvanceBeforeSavePayloads(
        testWriterConfig.payloadToEventBound.duration.plusSeconds(1)
      )

      val nowish = clock.now.plusSeconds(10)
      clock.advanceTo(nowish)

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch)
        deliver1 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId1,
          Set.empty,
          payload1,
          None,
        )
        _ <- loggerFactory.assertLogs(
          {
            offerDeliverOrFail(Presequenced.alwaysValid(deliver1))
            completeFlow()
          },
          _.warningMessage shouldBe "The payload to event time bound [PT1M] has been been exceeded by payload time [1970-01-01T00:00:00.000001Z] and sequenced event time [1970-01-01T00:01:11Z]",
        )
        events <- store.readEvents(aliceId)
      } yield events should have size (0)
    }
  }

  "max sequencing time" should {
    "drop sends above the max sequencing time" in withEnv() { env =>
      import env._
      val nowish = clock.now.plusSeconds(10)
      clock.advanceTo(nowish)

      val beforeNow = nowish.minusSeconds(5)
      val longAfterNow = nowish.plusSeconds(5)

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch)
        deliver1 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId1,
          Set.empty,
          payload1,
          None,
        )
        deliver2 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId2,
          Set.empty,
          payload2,
          None,
        )
        _ <- loggerFactory.assertLogs(
          {
            offerDeliverOrFail(Presequenced.withMaxSequencingTime(deliver1, beforeNow))
            offerDeliverOrFail(Presequenced.withMaxSequencingTime(deliver2, longAfterNow))
            completeFlow()
          },
          _.warningMessage should include regex "The sequencer time \\[.*\\] has exceeded the max-sequencing-time of the send request \\[1970-01-01T00:00:05Z\\]: deliver\\[message-id:1\\]",
        )
        events <- store.readEvents(aliceId)
      } yield {
        events should have size (1)
        events.headOption.map(_.event).value should matchPattern {
          case DeliverStoreEvent(_, `messageId2`, _, _, _, _) =>
        }
      }
    }
  }

  "signing tolerance" should {
    def sendWithSigningTimestamp(
        nowish: CantonTimestamp,
        validSigningTimestamp: CantonTimestamp,
        invalidSigningTimestamp: CantonTimestamp,
    )(implicit
        env: Env
    ): Future[Seq[StoreEvent[Payload]]] = {
      import env._

      clock.advanceTo(nowish)

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch)
        deliver1 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId1,
          Set.empty,
          payload1,
          Some(validSigningTimestamp),
        )
        deliver2 = DeliverStoreEvent.ensureSenderReceivesEvent(
          aliceId,
          messageId2,
          Set.empty,
          payload1,
          Some(invalidSigningTimestamp),
        )
        _ = offerDeliverOrFail(Presequenced.alwaysValid(deliver1))
        _ = offerDeliverOrFail(Presequenced.alwaysValid(deliver2))
        _ <- completeFlow()
        events <- store.readEvents(aliceId)
      } yield {
        events should have size (2)
        events.map(_.event)
      }
    }

    /*
      TODO(#8637)
      For the following two test suites, we want to check that the first event is a success
      and the second is a failure. However, these two tests were flaky.
      For now, we check that exactly one event is a success and the other is a failure and
      we issue a warning if the second is a success. The goal is to be able to distinguish
      between the following two issues:
      - ordering (failure, success) instead of (success, failure)
      - content (failure, failure) instead of (success, failure)
     */
    def checkResult(
        test: String,
        events: Seq[StoreEvent[Payload]],
        valid: PartialFunction[StoreEvent[Payload], Assertion],
        invalid: PartialFunction[StoreEvent[Payload], Assertion],
    ): Assertion = {
      val eventsWithIdx = events.zipWithIndex

      events.size shouldBe 2

      val validIdxO = eventsWithIdx.collectFirst { case (_: DeliverStoreEvent[_], id) => id }
      val invalidIdxO = eventsWithIdx.collectFirst { case (_: DeliverErrorStoreEvent, id) => id }

      val validIdx = withClue("Exactly one event should be a success")(validIdxO.value)
      val invalidIdx = withClue("Exactly one event should be an error")(invalidIdxO.value)

      if (validIdx == 1) logger.warn(s"$test: Valid event is expected to be at index 0, found 1")

      inside(events(validIdx))(valid)
      inside(events(invalidIdx))(invalid)
    }

    "cause errors if way ahead of valid signing window" in withEnv() { implicit env =>
      val nowish = CantonTimestamp.Epoch.plusSeconds(10)

      // upper bound is inclusive
      val margin = NonNegativeFiniteDuration.ofMillis(1)
      val validSigningTimestamp = nowish

      for {
        events <- sendWithSigningTimestamp(
          nowish,
          validSigningTimestamp = validSigningTimestamp,
          invalidSigningTimestamp = validSigningTimestamp + margin,
        )
      } yield {
        checkResult(
          "cause errors if way ahead of valid signing window",
          events,
          valid = { case event: DeliverStoreEvent[Payload] =>
            event.messageId shouldBe messageId1
          },
          invalid = { case DeliverErrorStoreEvent(_, `messageId2`, message, _) =>
            message should (include("Invalid signing timestamp")
              and include("The signing timestamp must be before or at "))
          },
        )
      }
    }

    "cause errors if way behind the valid signing window" in withEnv() { implicit env =>
      import env._
      val nowish = CantonTimestamp.Epoch.plusSeconds(60)

      // lower bound is exclusive
      val margin = NonNegativeFiniteDuration.ofMillis(1)
      val invalidSigningTimestamp = nowish - domainParameters.sequencerSigningTolerance

      for {
        events <- sendWithSigningTimestamp(
          nowish,
          validSigningTimestamp = invalidSigningTimestamp + margin,
          invalidSigningTimestamp = invalidSigningTimestamp,
        )
      } yield {
        checkResult(
          "cause errors if way behind the valid signing window",
          events,
          { case event: DeliverStoreEvent[Payload] =>
            event.messageId shouldBe messageId1
          },
          { case DeliverErrorStoreEvent(_, `messageId2`, message, _) =>
            message should (include("Invalid signing timestamp")
              and include("The signing timestamp must be strictly after "))
          },
        )
      }
    }
  }

  "deliver with unknown recipients" should {
    "instead write an error" in withEnv() { implicit env =>
      import env._
      val messageId = MessageId.tryCreate("test-unknown-recipients")

      for {
        aliceId <- store.registerMember(alice, CantonTimestamp.Epoch)
        _ <- valueOrFail(
          writer.send(
            SubmissionRequest(
              alice,
              MessageId.tryCreate("test-unknown-recipients"),
              isRequest = true,
              batch = Batch.fromClosed(ClosedEnvelope(ByteString.EMPTY, Recipients.cc(bob))),
              maxSequencingTime = CantonTimestamp.MaxValue,
              timestampOfSigningKey = None,
            )
          )
        )("send to unknown recipient")
        _ <- eventuallyF(10.seconds) {
          for {
            events <- env.store.readEvents(aliceId)
            error = events.collectFirst {
              case Sequenced(
                    _,
                    deliverError @ DeliverErrorStoreEvent(`aliceId`, `messageId`, _, _),
                  ) =>
                deliverError
            }.value
          } yield error.message shouldBe s"Unknown recipients: $bob"
        }
      } yield succeed
    }
  }

  "notifies the event signaller of writes" in withEnv() { implicit env =>
    import env._

    def deliverEvent(memberId: SequencerMemberId): Unit =
      offerDeliverOrFail(
        Presequenced.alwaysValid(
          DeliverStoreEvent.ensureSenderReceivesEvent(
            memberId,
            messageId1,
            Set.empty,
            generatePayload(),
            None,
          )
        )
      )

    // get the next notification that is hopefully caused by whatever happens in the passed sequencing block
    // we've set the event batch count to 1, so we should know ahead of time how many writes this will cause
    // which allows us to collect all notifications that should be emitted during this batch
    def expectNotification(writeCount: Int)(members: SequencerMemberId*)(
        sequencingBlock: () => Unit
    ): Future[WriteNotification] = {
      val combinedNotificationsF = {
        val promise = Promise[WriteNotification]()
        val items = new AtomicReference[Seq[WriteNotification]](Seq.empty)

        val removeListener = eventSignaller.attachWriteListener { notification =>
          // ignore notifications for no-one as that's just our keep alives
          if (notification != WriteNotification.None) {
            val newItems = items.updateAndGet(_ :+ notification)

            if (newItems.size == writeCount) {
              val combined = NonEmptyList
                .fromListUnsafe(newItems.toList)
                .reduceLeft(_ union _)

              promise.success(combined)
            }
          }
        }

        // try to not leak a callback
        promise.future transform { result =>
          removeListener.close()
          result
        }
      }

      sequencingBlock()

      combinedNotificationsF map { notification =>
        forAll(members) { member =>
          withClue(s"expecting notification for $member") {
            notification.includes(member) shouldBe true
          }
        }

        notification
      }
    }

    for {
      aliceId <- store.registerMember(alice, CantonTimestamp.Epoch)
      bobId <- store.registerMember(bob, CantonTimestamp.Epoch)
      // check single members
      _ <- expectNotification(writeCount = 1)(aliceId) { () =>
        deliverEvent(aliceId)
      }
      _ <- expectNotification(writeCount = 1)(bobId) { () =>
        deliverEvent(bobId)
      }
      // if multiple deliver events are written in the same batch it should union the members
      _ <- expectNotification(writeCount = 2)(aliceId, bobId) { () =>
        deliverEvent(aliceId)
        deliverEvent(bobId)
      }
    } yield succeed
  }

  "an idle writer still updates its watermark to demonstrate that its online" in withEnv(
    SequencerHighAvailabilityConfig(
      enabled = true,
      totalNodeCount = SingleSequencerTotalNodeCount,
      keepAliveInterval = NonNegativeFiniteDuration.ofSeconds(1L),
    )
  ) { implicit env =>
    import env._
    // the specified keepAliveInterval of 1s ensures the watermark gets updated
    for {
      initialWatermark <- eventuallyF(5.seconds) {
        store.fetchWatermark(instanceIndex).map(_.value)
      }
      _ <- eventuallyF(5.seconds) {
        for {
          updated <- store.fetchWatermark(instanceIndex)
        } yield updated.value.timestamp shouldBe >=(initialWatermark.timestamp)
      }
    } yield succeed
  }

  def eventuallyF[A](timeout: FiniteDuration, checkInterval: FiniteDuration = 100.millis)(
      testCode: => Future[A]
  )(implicit env: Env): Future[A] = {
    val giveUpAt = Instant.now().plus(timeout.toMicros, ChronoUnit.MICROS)
    val resultP = Promise[A]()

    def check(): Unit = testCode.onComplete {
      case Success(value) => resultP.success(value)
      case Failure(testFailedEx: TestFailedException) =>
        // see if we can still retry
        if (Instant.now().isBefore(giveUpAt)) {
          // schedule a check later
          env.actorSystem.scheduler.scheduleOnce(checkInterval)(check())
        } else {
          // fail immediately
          resultP.failure(testFailedEx)
        }
      case Failure(otherReason) => resultP.failure(otherReason)
    }

    check()

    resultP.future
  }
}
