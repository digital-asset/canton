// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, SinkQueueWithCancel, Source}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.DomainSequencingTestUtils
import com.digitalasset.canton.domain.sequencing.sequencer.DomainSequencingTestUtils._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.domain.sequencing.sequencer.store._
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  Member,
  ParticipantId,
  SequencerId,
  TestingTopology,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, DiscardOps, SequencerCounter}
import org.mockito.Mockito
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

class SequencerReaderTest extends FixtureAsyncWordSpec with BaseTest {

  private val alice: Member = ParticipantId("alice")
  private val ts0 = CantonTimestamp.ofEpochSecond(0)
  private val domainId = DefaultTestIdentities.domainId
  private val crypto = TestingTopology().build(loggerFactory).forOwner(SequencerId(domainId))
  private val cryptoD =
    valueOrFail(crypto.forDomain(domainId).toRight("no crypto api"))("domain crypto")
  private val instanceDiscriminator = new UUID(1L, 2L)

  class ManualEventSignaller(implicit materializer: Materializer)
      extends EventSignaller
      with FlagCloseableAsync {
    private val (queue, source) = Source
      .queue[ReadSignal](1)
      .buffer(1, OverflowStrategy.dropHead)
      .preMaterialize()

    override protected def timeouts: ProcessingTimeout = SequencerReaderTest.this.timeouts

    def signalRead(): Unit = queue.offer(ReadSignal).discard[QueueOfferResult]

    override def readSignalsForMember(
        member: Member,
        memberId: SequencerMemberId,
    ): Source[ReadSignal, NotUsed] =
      source

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      SyncCloseable("queue", queue.complete())
    )

    override protected def logger: TracedLogger = SequencerReaderTest.this.logger

    override def notifyOfLocalWrite(notification: WriteNotification)(implicit
        traceContext: TraceContext
    ): Future[Unit] = Future.unit
  }

  class Env extends FlagCloseableAsync {
    protected val timeouts = SequencerReaderTest.this.timeouts
    protected val logger = SequencerReaderTest.this.logger
    val autoPushLatestTimestamps =
      new AtomicBoolean(true) // should the latest timestamp be added to the signaller when stored
    val actorSystem = ActorSystem(classOf[SequencerReaderTest].getSimpleName)
    implicit val materializer = Materializer(actorSystem)
    val store = new InMemorySequencerStore(loggerFactory)
    val instanceIndex: Int = 0
    // create a spy so we can add verifications on how many times methods were called
    val storeSpy = spy[InMemorySequencerStore](store)
    val testConfig =
      CommunitySequencerReaderConfig(
        readBatchSize = 10,
        checkpointInterval = NonNegativeFiniteDuration.ofMillis(200),
      )
    val eventSignaller = new ManualEventSignaller()
    val reader = new SequencerReader(
      testConfig,
      domainId,
      storeSpy,
      cryptoD,
      eventSignaller,
      timeouts,
      loggerFactory,
    )
    val defaultTimeout = 20.seconds

    def ts(epochSeconds: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(epochSeconds.toLong)

    def readAsSeq(
        member: Member,
        counter: SequencerCounter,
        take: Int,
    ): Future[Seq[OrdinarySerializedEvent]] =
      valueOrFail(reader.read(member, counter))(s"Events source for $member") flatMap {
        _.take(take.toLong)
          .idleTimeout(defaultTimeout)
          .runWith(Sink.seq)
      }

    def readWithQueue(
        member: Member,
        counter: SequencerCounter,
    ): SinkQueueWithCancel[OrdinarySerializedEvent] =
      Source
        .future(valueOrFail(reader.read(member, counter))(s"Events source for $member"))
        .flatMapConcat(identity)
        .idleTimeout(defaultTimeout)
        .runWith(Sink.queue())

    def waitFor(duration: FiniteDuration): Future[Unit] = {
      val promise = Promise[Unit]()

      actorSystem.scheduler.scheduleOnce(duration)(promise.success(()))

      promise.future
    }

    def storeAndWatermark(events: Seq[Sequenced[PayloadId]]): Future[Unit] = {
      val eventsNE = NonEmptyUtil.fromUnsafe(events)
      val payloads = DomainSequencingTestUtils.payloadsForEvents(events)

      for {
        _ <- store
          .savePayloads(NonEmptyUtil.fromUnsafe(payloads), instanceDiscriminator)
          .valueOrFail(s"Save payloads")
        _ <- store.saveEvents(instanceIndex, eventsNE)
        _ <- store
          .saveWatermark(instanceIndex, eventsNE.last1.timestamp)
          .valueOrFail("saveWatermark")
      } yield {
        // update the event signaller if auto signalling is enabled
        if (autoPushLatestTimestamps.get()) eventSignaller.signalRead()
      }
    }

    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq(
      AsyncCloseable("actorSystem", actorSystem.terminate(), 10.seconds),
      SyncCloseable("materializer", materializer.shutdown()),
    )
  }

  override type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  "Reader" should {
    "read a stream of events" in { env =>
      import env._

      for {
        aliceId <- store.registerMember(alice, ts0)
        // generate 20 delivers starting at ts0+1s
        events = (1L to 20L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
        _ <- storeAndWatermark(events)
        events <- readAsSeq(alice, 0, 20)
      } yield {
        forAll(events.zipWithIndex) { case (event, n) => event.counter shouldBe n }
      }
    }

    "read a stream of events from a non-zero offset" in { env =>
      import env._

      for {
        aliceId <- store.registerMember(alice, ts0)
        delivers = (1L to 20L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
          .toList
        _ <- storeAndWatermark(delivers)
        events <- readAsSeq(alice, 5, 15)
      } yield {
        events.headOption.value.counter shouldBe 5L
        events.headOption.value.timestamp shouldBe ts0.plusSeconds(6)
        events.lastOption.value.counter shouldBe 19L
        events.lastOption.value.timestamp shouldBe ts0.plusSeconds(20)
      }
    }

    "read stream of events while new events are being added" in { env =>
      import env._

      for {
        aliceId <- store.registerMember(alice, ts0)
        delivers = (1L to 5L)
          .map(ts0.plusSeconds)
          .map(Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)()))
          .toList
        _ <- storeAndWatermark(delivers)
        queue = readWithQueue(alice, 0)
        // read off all of the initial delivers
        _ <- MonadUtil.sequentialTraverse_(delivers.zipWithIndex.map(_._2)) { expectedCounter =>
          for {
            eventO <- queue.pull()
          } yield eventO.value.counter shouldBe expectedCounter
        }
        // start reading the next event
        nextEventF = queue.pull()
        // add another
        _ <- storeAndWatermark(
          Seq(
            Sequenced(
              ts0.plusSeconds(6L),
              mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)(),
            )
          )
        )
        // wait for the next event
        nextEventO <- nextEventF
        _ = queue.cancel() // cancel the queue now we're done with it
      } yield nextEventO.value.counter shouldBe 5L // it'll be alices fifth event
    }

    "attempting to read an unregistered member returns error" in { env =>
      import env._

      for {
        // we haven't registered alice
        error <- leftOrFail(reader.read(alice, 0L))("read unknown member")
      } yield error shouldBe CreateSubscriptionError.UnknownMember(alice)
    }

    "attempting to read for a disabled member returns error" in { env =>
      import env._

      for {
        aliceId <- store.registerMember(alice, ts0)
        _ <- store.disableMember(aliceId)
        error <- leftOrFail(reader.read(alice, 0L))("read disabled member")
      } yield error shouldBe CreateSubscriptionError.MemberDisabled(alice)
    }

    "waits for a signal that new events are available" in { env =>
      import env._

      val waitP = Promise[Unit]()

      for {
        aliceId <- store.registerMember(alice, ts0)
        // start reading for an event but don't wait for it
        eventsF = readAsSeq(alice, 0, 1)
        // set a timer to wait for a little
        _ = actorSystem.scheduler.scheduleOnce(500.millis)(waitP.success(()))
        // still shouldn't have read anything
        _ = eventsF.isCompleted shouldBe false
        // now signal that events are available which should cause the future read to move ahead
        _ = env.eventSignaller.signalRead()
        _ <- waitP.future
        // add an event
        _ <- storeAndWatermark(
          Seq(
            Sequenced(
              ts0 plusSeconds 1,
              mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)(),
            )
          )
        )
        _ = env.eventSignaller.signalRead() // signal that something is there
        events <- eventsF
      } yield {
        events should have size 1 // should have got our single deliver event
      }
    }

    "reading all immediately available events" should {
      "use returned events before filtering based what has actually been requested" in { env =>
        import env._

        // disable auto signalling
        autoPushLatestTimestamps.set(false)

        for {
          aliceId <- store.registerMember(alice, ts0)
          // generate 20 delivers starting at ts0+1s
          delivers = (1L to 25L)
            .map(ts0.plusSeconds)
            .map(
              Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
            )
          _ <- storeAndWatermark(delivers)
          // store a counter check point at 5s
          _ <- store.saveCounterCheckpoint(aliceId, 5, ts(6)).valueOrFail("saveCounterCheckpoint")
          events <- readAsSeq(alice, 10, 15)
        } yield {
          // this assertion is a bit redundant as we're actually just looking for the prior fetch to complete rather than get stuck
          events should have size (15)
        }
      }
    }

    "counter checkpoint" should {
      "issue counter checkpoints occasionally" in { env =>
        import env._

        import scala.jdk.CollectionConverters._

        def saveCounterCheckpointCallCount =
          Mockito
            .mockingDetails(storeSpy)
            .getInvocations
            .asScala
            .count(_.getMethod.getName == "saveCounterCheckpoint")

        for {
          aliceId <- store.registerMember(alice, ts0)
          // generate 20 delivers starting at ts0+1s
          delivers = (1L to 20L)
            .map(ts0.plusSeconds)
            .map(
              Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
            )
          _ <- storeAndWatermark(delivers)
          // take some events
          queue = readWithQueue(alice, 0)
          // read a bunch of items
          readEvents <- MonadUtil.sequentialTraverse(1L to 20L)(_ => queue.pull())
          // wait for a bit over the checkpoint interval (although I would expect because these actions are using the same scheduler the actions may be correctly ordered regardless)
          _ <- waitFor(testConfig.checkpointInterval.toScala * 6)
          checkpointsWritten = saveCounterCheckpointCallCount
          // close the queue before we make any assertions
          _ = queue.cancel()
          lastEventRead = readEvents.lastOption.value.value
          checkpointForLastEventO <- store.fetchClosestCheckpointBefore(
            aliceId,
            lastEventRead.counter + 1,
          )
        } yield {
          // check it created a checkpoint for the last event we read
          checkpointForLastEventO.value.counter shouldBe lastEventRead.counter
          checkpointForLastEventO.value.timestamp shouldBe lastEventRead.timestamp

          // make sure we didn't write a checkpoint for every event (in practice this should be <3)
          checkpointsWritten should (be > 0 and be < 20)
        }
      }

      "start subscriptions from the closest counter checkpoint if available" in { env =>
        import env._

        for {
          aliceId <- store.registerMember(alice, ts0)
          // write a bunch of events
          delivers = (1L to 20L)
            .map(ts0.plusSeconds)
            .map(
              Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
            )
          _ <- storeAndWatermark(delivers)
          checkpointTimestamp = ts0.plusSeconds(11)
          _ <- valueOrFail(store.saveCounterCheckpoint(aliceId, 10L, checkpointTimestamp))(
            "saveCounterCheckpoint"
          )
          // read from a point ahead of this checkpoint
          events <- readAsSeq(alice, 15L, 3)
        } yield {
          // it should have started reading from the closest counter checkpoint timestamp
          verify(storeSpy).readEvents(eqTo(aliceId), eqTo(Some(checkpointTimestamp)), anyInt)(
            anyTraceContext
          )
          // but only emitted events starting from 15
          events.headOption.value.counter shouldBe 15L
          // our deliver events start at ts0+1s and as alice is registered before the first deliver event their first
          // event (0) is for ts0+1s.
          // event 15 should then have ts ts0+16s
          events.headOption.value.timestamp shouldBe ts0.plusSeconds(16)
        }
      }
    }

    "lower bound checks" should {
      "error if subscription would need to start before the lower bound due to no checkpoints" in {
        env =>
          import env._

          val expectedMessage =
            "Subscription for PAR::alice::default@0 would require reading data from 1970-01-01T00:00:00Z but our lower bound is 1970-01-01T00:00:10Z."

          for {
            aliceId <- store.registerMember(alice, ts0)
            // write a bunch of events
            delivers = (1L to 20L)
              .map(ts0.plusSeconds)
              .map(
                Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
              )
            _ <- storeAndWatermark(delivers)
            _ <- store.saveLowerBound(ts(10)).valueOrFail("saveLowerBound")
            error <- loggerFactory.assertLogs(
              leftOrFail(reader.read(alice, 0L))("read"),
              _.errorMessage shouldBe expectedMessage,
            )
          } yield inside(error) { case CreateSubscriptionError.EventsUnavailable(0L, message) =>
            message should include(expectedMessage)
          }
      }

      "error if subscription would need to start before the lower bound due to checkpoints" in {
        env =>
          import env._

          val expectedMessage =
            "Subscription for PAR::alice::default@9 would require reading data from 1970-01-01T00:00:00Z but our lower bound is 1970-01-01T00:00:10Z."

          for {
            aliceId <- store.registerMember(alice, ts0)
            // write a bunch of events
            delivers = (1L to 20L)
              .map(ts0.plusSeconds)
              .map(
                Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
              )
            _ <- storeAndWatermark(delivers)
            _ <- store
              .saveCounterCheckpoint(aliceId, 9L, ts(10))
              .valueOrFail("saveCounterCheckpoint")
            _ <- store.saveLowerBound(ts(10)).valueOrFail("saveLowerBound")
            error <- loggerFactory.assertLogs(
              leftOrFail(reader.read(alice, 9L))("read"),
              _.errorMessage shouldBe expectedMessage,
            )
          } yield inside(error) { case CreateSubscriptionError.EventsUnavailable(9L, message) =>
            message shouldBe expectedMessage
          }
      }

      "not error if there is a counter checkpoint above lower bound" in { env =>
        import env._

        for {
          aliceId <- store.registerMember(alice, ts0)
          // write a bunch of events
          delivers = (1L to 20L)
            .map(ts0.plusSeconds)
            .map(
              Sequenced(_, mockDeliverStoreEvent(sender = aliceId, traceContext = traceContext)())
            )
          _ <- storeAndWatermark(delivers)
          _ <- store
            .saveCounterCheckpoint(aliceId, 11L, ts(10))
            .valueOrFail("saveCounterCheckpoint")
          _ <- store.saveLowerBound(ts(10)).valueOrFail("saveLowerBound")
          _ <- reader.read(alice, 12L).valueOrFail("read")
        } yield succeed // the above not failing is enough of an assertion
      }
    }
  }
}
