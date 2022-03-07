// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.data.Validated.Valid
import cats.syntax.foldable._
import cats.syntax.traverse._
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Crypto, Fingerprint, Signature, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, SequencerTestUtils}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.store.SequencedEventStore._
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, DomainId, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

trait SequencedEventStoreTest extends PrunableByTimeTest {
  this: AsyncWordSpec with BaseTest =>

  val sequencerKey: Fingerprint = Fingerprint.tryCreate("sequencer")
  val crypto: Crypto =
    SymbolicCrypto.tryCreate(Seq(sequencerKey), Seq(), None, timeouts, loggerFactory)

  def sign(str: String): Signature =
    DefaultProcessingTimeouts.default
      .await("event signing")(crypto.privateCrypto.sign(TestHash.digest(str), sequencerKey).value)
      .valueOrFail("failed to create signature")

  val domainId: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("da::default"))

  def mkBatch(envelopes: ClosedEnvelope*): Batch[ClosedEnvelope] =
    Batch(envelopes.toList)

  def signDeliver(event: Deliver[ClosedEnvelope]): SignedContent[Deliver[ClosedEnvelope]] =
    SignedContent(event, sign(s"deliver signature ${event.counter}"), None)

  lazy val closedEnvelope =
    ClosedEnvelope(ByteString.copyFromUtf8("message"), RecipientsTest.testInstance)

  def mkDeliver(counter: SequencerCounter, ts: CantonTimestamp): OrdinarySerializedEvent =
    mkOrdinaryEvent(
      SignedContent(
        Deliver.create(
          counter,
          ts,
          domainId,
          Some(MessageId.tryCreate("deliver")),
          mkBatch(closedEnvelope),
        ),
        sign("deliver signature"),
        None,
      ),
      nonEmptyTraceContext2,
    )

  lazy val singleDeliver: OrdinarySerializedEvent =
    mkDeliver(99, CantonTimestamp.ofEpochMilli(-1))

  lazy val singleMaxDeliverPositive: OrdinarySerializedEvent =
    mkOrdinaryEvent(
      SignedContent(
        Deliver.create(
          Long.MaxValue,
          CantonTimestamp.MaxValue,
          domainId,
          Some(MessageId.tryCreate("single-max-positive-deliver")),
          mkBatch(closedEnvelope),
        ),
        sign("single deliver signature"),
        Some(CantonTimestamp.MaxValue),
      ),
      nonEmptyTraceContext2,
    )

  val singleMinDeliver: OrdinarySerializedEvent =
    mkOrdinaryEvent(
      SignedContent(
        Deliver.create(
          Long.MinValue,
          CantonTimestamp.MinValue,
          domainId,
          Some(MessageId.tryCreate("single-min-deliver")),
          mkBatch(closedEnvelope),
        ),
        sign("single deliver signature"),
        Some(CantonTimestamp.MinValue),
      ),
      nonEmptyTraceContext2,
    )

  val modifiedSingleDeliver: OrdinarySerializedEvent =
    mkOrdinaryEvent(
      SignedContent(
        Deliver.create(
          99L,
          CantonTimestamp.ofEpochMilli(-1),
          domainId,
          Some(MessageId.tryCreate("single-deliver")),
          mkBatch(closedEnvelope),
        ),
        singleDeliver.signedEvent.signature,
        None,
      ),
      nonEmptyTraceContext2,
    )

  def mkDeliverEventTc1(counter: SequencerCounter, ts: CantonTimestamp): OrdinarySerializedEvent =
    mkOrdinaryEvent(
      SignedContent(
        SequencerTestUtils.mockDeliver(counter, ts, domainId),
        sign("Mock deliver signature"),
        None,
      ),
      nonEmptyTraceContext1,
    )

  val event: OrdinarySerializedEvent = mkDeliverEventTc1(100L, CantonTimestamp.Epoch)

  val emptyDeliver: OrdinarySerializedEvent =
    mkOrdinaryEvent(
      SignedContent(
        Deliver.create(
          101L,
          CantonTimestamp.ofEpochMilli(1),
          domainId,
          Some(MessageId.tryCreate("empty-deliver")),
          mkBatch(),
        ),
        sign("Deliver signature"),
        None,
      )
    )

  def mkDeliverError(counter: SequencerCounter, ts: CantonTimestamp): OrdinarySerializedEvent =
    mkOrdinaryEvent(
      SignedContent(
        DeliverError.create(
          counter,
          ts,
          domainId,
          MessageId.tryCreate("deliver-error"),
          DeliverErrorReason.BatchRefused("paniertes schnitzel"),
        ),
        sign("Deliver error signature"),
        None,
      )
    )

  def ts(counter: SequencerCounter): CantonTimestamp =
    CantonTimestamp.Epoch.addMicros(counter)

  def mkOrdinaryEvent(
      event: SignedContent[SequencedEvent[ClosedEnvelope]],
      traceContext: TraceContext = TraceContext.empty,
  ): OrdinarySerializedEvent =
    OrdinarySequencedEvent(event)(traceContext)

  def mkEmptyIgnoredEvent(
      counter: SequencerCounter,
      microsSinceMin: Long = -1,
  ): IgnoredSequencedEvent[Nothing] = {
    val t =
      if (microsSinceMin < 0) ts(counter) else CantonTimestamp.MinValue.addMicros(microsSinceMin)
    IgnoredSequencedEvent(t, counter, None)(traceContext)
  }

  def sequencedEventStore(mkSes: ExecutionContext => SequencedEventStore): Unit = {
    def mk(): SequencedEventStore = mkSes(executionContext)

    behave like prunableByTime(mkSes)

    "not find sequenced events in empty store" in {
      val store = mk()
      val criteria = List(ByTimestamp(CantonTimestamp.Epoch), LatestUpto(CantonTimestamp.MaxValue))
      criteria
        .traverse_ { criterion =>
          store
            .find(criterion)
            .value
            .map(res => res shouldBe Left(SequencedEventNotFoundError(criterion)))
        }
        .map(_ => succeed)
    }

    "should find stored sequenced events" in {
      val store = mk()

      val events = List[OrdinarySerializedEvent](
        event,
        emptyDeliver,
        singleDeliver,
      )
      val criteria = List(
        ByTimestamp(CantonTimestamp.Epoch),
        ByTimestamp(CantonTimestamp.ofEpochMilli(1)),
        ByTimestamp(CantonTimestamp.ofEpochMilli(-1)),
      )

      for {
        _stored <- store.store(events)
        found <- criteria.traverse(store.find).toValidatedNec
      } yield {
        assert(found.isValid, "finding deliver events succeeds")
        assert(found.map(_.toSeq) == Valid(events), "found the right deliver events")
      }
    }

    "store is idempotent" in {
      val store = mk()

      val events = List[OrdinarySerializedEvent](
        event,
        event,
        singleDeliver,
        singleDeliver,
        modifiedSingleDeliver,
      )

      store.store(events).map(_ => succeed)
    }

    "store works for no events" in {
      val store = mk()
      store.store(Seq.empty).map(_ => succeed)
    }

    "find works for many events" in {
      val store = mk()

      val events = (1L to 100L).toList.map { i =>
        mkOrdinaryEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(2 * i + 1000L, CantonTimestamp.ofEpochMilli(i * 2), domainId),
            sign(s"signature $i"),
            None,
          )
        )
      }

      for {
        _ <- store.store(events)
        found <- (1L to 200L).toList
          .traverse { i =>
            store.find(ByTimestamp(CantonTimestamp.ofEpochMilli(i))).value
          }
      } yield {
        assert(found.collect { case Right(event) => event } == events)
        assert(
          found.collect { case Left(error) => error } == (1L to 100L).map(i =>
            SequencedEventNotFoundError(ByTimestamp(CantonTimestamp.ofEpochMilli(2 * i - 1)))
          )
        )
      }
    }

    "get a range by timestamp" in {
      val store = mk()
      val eventCount = 100L
      val firstIndex = 10
      val lastIndex = 90
      val events = (1L to eventCount).toList.map { i =>
        mkOrdinaryEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(2 * i + 1000L, CantonTimestamp.Epoch.plusMillis(i * 2), domainId),
            sign(s"signature $i"),
            None,
          )
        )
      }

      for {
        _ <- store.store(events)
        found <- store.findRange(
          ByTimestampRange(events(firstIndex).timestamp, events(lastIndex).timestamp),
          None,
        )
      } yield {
        assert(found.toList == events.slice(firstIndex, lastIndex + 1))
      }
    }

    "get a range with a limit" in {
      val store = mk()
      val eventCount = 100L
      val firstIndex = 10
      val limit = 90
      val events = (1L to eventCount).toList.map { i =>
        mkOrdinaryEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(2 * i + 1000L, CantonTimestamp.Epoch.plusMillis(i * 2), domainId),
            sign(s"signature $i"),
            None,
          )
        )
      }

      for {
        _ <- store.store(events)
        foundByTs <- store.findRange(
          ByTimestampRange(events(firstIndex).timestamp, events.lastOption.value.timestamp),
          Some(limit),
        )
      } yield {
        assert(foundByTs.toList == events.slice(firstIndex, firstIndex + limit))
      }
    }

    "returns all values within a range when range bounds are not in the store" in {
      val store = mk()
      val eventCount = 100L
      val firstIndex = 10
      val lastIndex = 90
      val delta = 10
      val events = (1L to eventCount).toList.map { i =>
        mkOrdinaryEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(delta * i, CantonTimestamp.Epoch.plusMillis(i * delta), domainId),
            sign(s"signature $i"),
            None,
          )
        )
      }

      for {
        _ <- store.store(events)
        foundByTs1 <- store.findRange(
          ByTimestampRange(
            events(firstIndex).timestamp.minusMillis(delta / 2L),
            events(lastIndex).timestamp.plusMillis(delta / 2L),
          ),
          None,
        )
        foundByTs2 <- store.findRange(
          ByTimestampRange(
            events.headOption.value.timestamp.minusMillis(delta / 2L),
            events.lastOption.value.timestamp.plusMillis(delta / 2L),
          ),
          None,
        )
      } yield {
        assert(foundByTs1.toList == events.slice(firstIndex, lastIndex + 1))
        assert(foundByTs2.toList == events)
      }
    }

    "find range returns no values for empty store" in {
      val store = mk()
      for {
        foundByTs <- store.findRange(
          ByTimestampRange(CantonTimestamp.Epoch, CantonTimestamp.Epoch.plusMillis(100)),
          None,
        )
      } yield {
        assert(foundByTs.toList == List.empty)
      }
    }

    "find range returns no values when range outside store values" in {
      val store = mk()
      val min = 50L
      val max = 100L
      val getSc = { i: Long =>
        i * 2 + 100
      }
      val getTs = { i: Long =>
        CantonTimestamp.Epoch.plusMillis(i * 2 + 200)
      }
      val events = (min to max).toList.map { i =>
        mkOrdinaryEvent(
          SignedContent(
            SequencerTestUtils.mockDeliver(getSc(i), getTs(i), domainId),
            sign(s"signature $i"),
            None,
          )
        )
      }

      for {
        _ <- store.store(events)
        foundByTsAbove <- store.findRange(ByTimestampRange(getTs(max + 5), getTs(max + 10)), None)

        foundByTsBelow <- store.findRange(ByTimestampRange(getTs(min - 10), getTs(min - 5)), None)
      } yield {
        assert(foundByTsAbove.toList == List.empty)
        assert(foundByTsBelow.toList == List.empty)
      }
    }

    "find range requires that the start of the range is not after the end" in {
      val store = mk()
      val events = (1L to 100L).toList.map { i =>
        mkOrdinaryEvent(
          SignedContent(
            SequencerTestUtils
              .mockDeliver(2 * i + 1000L, CantonTimestamp.Epoch.plusMillis(i * 2), domainId),
            sign(s"signature $i"),
            None,
          )
        )
      }

      for {
        _ <- store.store(events)
      } yield {
        assertThrows[IllegalArgumentException](
          store.findRange(
            ByTimestampRange(events.lastOption.value.timestamp, events.headOption.value.timestamp),
            None,
          )
        )
      }
    }

    "find range checks overlap with pruning" in {
      val store = mk()
      val events = (1L to 5L).toList.map { i =>
        mkOrdinaryEvent(
          SignedContent(
            SequencerTestUtils.mockDeliver(i, CantonTimestamp.ofEpochSecond(i), domainId),
            sign(s"signature $i"),
            None,
          )
        )
      }
      val tsPrune = CantonTimestamp.ofEpochSecond(2)
      val ts4 = CantonTimestamp.ofEpochSecond(4)
      val criterionAt = ByTimestampRange(tsPrune, CantonTimestamp.MaxValue)
      val criterionBelow = ByTimestampRange(CantonTimestamp.MinValue, CantonTimestamp.Epoch)
      for {
        _ <- store.store(events)
        _ <- store.prune(tsPrune).merge
        succeed <- store
          .findRange(ByTimestampRange(tsPrune.immediateSuccessor, ts4), None)
          .valueOrFail("successful range query")
        fail2 <- leftOrFail(store.findRange(criterionAt, None))("at pruning point")
        failBelow <- leftOrFail(store.findRange(criterionBelow, None))("before pruning point")
      } yield {
        val pruningStatus = PruningStatus(PruningPhase.Completed, tsPrune)
        fail2 shouldBe SequencedEventRangeOverlapsWithPruning(
          criterionAt,
          pruningStatus,
          events.filter(_.timestamp > tsPrune),
        )
        failBelow shouldBe SequencedEventRangeOverlapsWithPruning(
          criterionBelow,
          pruningStatus,
          Seq.empty,
        )
      }
    }

    "find returns the latest event" in {
      val store = mk()

      val firstDeliver =
        mkOrdinaryEvent(
          signDeliver(SequencerTestUtils.mockDeliver(100L, CantonTimestamp.Epoch, domainId)),
          nonEmptyTraceContext1,
        )
      val secondDeliver =
        mkOrdinaryEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(101L, CantonTimestamp.ofEpochSecond(1), domainId)
          ),
          nonEmptyTraceContext2,
        )
      val thirdDeliver =
        mkOrdinaryEvent(
          signDeliver(
            SequencerTestUtils.mockDeliver(103L, CantonTimestamp.ofEpochSecond(100000), domainId)
          )
        )
      val emptyBatch = mkBatch()
      val deliver1 =
        mkOrdinaryEvent(
          signDeliver(
            Deliver
              .create(
                102L,
                CantonTimestamp.ofEpochSecond(2),
                domainId,
                Some(MessageId.tryCreate("deliver1")),
                emptyBatch,
              )
          )
        )
      val deliver2 = mkOrdinaryEvent(
        signDeliver(
          Deliver.create(
            104L,
            CantonTimestamp.ofEpochSecond(200000),
            domainId,
            Some(MessageId.tryCreate("deliver2")),
            emptyBatch,
          )
        )
      )

      for {
        _ <- store.store(Seq(firstDeliver))
        findDeliver <- store
          .find(LatestUpto(CantonTimestamp.MaxValue))
          .valueOrFail("find first deliver")
        _ <- store.store(Seq(secondDeliver, deliver1, thirdDeliver))
        findLatestDeliver <- store
          .find(LatestUpto(CantonTimestamp.MaxValue))
          .valueOrFail("find third deliver")
        _ <- store.store(Seq(deliver2))
        findDeliver2 <- store.find(LatestUpto(deliver2.timestamp)).valueOrFail("find deliver")
        findDeliver1 <- store
          .find(LatestUpto(thirdDeliver.timestamp.immediatePredecessor))
          .valueOrFail("find deliver")
      } yield {
        findDeliver shouldBe firstDeliver
        findLatestDeliver shouldBe thirdDeliver
        findDeliver2 shouldBe deliver2
        findDeliver1 shouldBe deliver1
      }
    }

    "delete old sequenced events when pruned" in {
      val store = mk()

      val ts0 = CantonTimestamp.Epoch
      val ts1 = ts0.plusSeconds(1)
      val ts2 = ts0.plusSeconds(2)
      val ts3 = ts0.plusSeconds(10)
      val ts4 = ts0.plusSeconds(20)

      val firstDeliver =
        mkOrdinaryEvent(signDeliver(SequencerTestUtils.mockDeliver(100L, ts0, domainId)))
      val secondDeliver =
        mkOrdinaryEvent(signDeliver(SequencerTestUtils.mockDeliver(101L, ts1, domainId)))
      val thirdDeliver =
        mkOrdinaryEvent(signDeliver(SequencerTestUtils.mockDeliver(103L, ts3, domainId)))
      val emptyBatch = mkBatch()
      val deliver1 =
        mkOrdinaryEvent(
          signDeliver(
            Deliver.create(102L, ts2, domainId, Some(MessageId.tryCreate("deliver1")), emptyBatch)
          )
        )
      val deliver2 =
        mkOrdinaryEvent(
          signDeliver(
            Deliver.create(104L, ts4, domainId, Some(MessageId.tryCreate("deliver2")), emptyBatch)
          )
        )

      for {
        _ <- store.store(Seq(firstDeliver, secondDeliver, deliver1, thirdDeliver, deliver2))
        _ <- store.prune(ts2).merge
        eventsAfterPruning <- store.sequencedEvents()
      } yield {
        assert(
          eventsAfterPruning.toSet === Set(thirdDeliver, deliver2),
          "only events with a later timestamp left",
        )
      }
    }

    "store events up to Long max limit" in {
      val store = mk()

      val events = List[OrdinarySerializedEvent](
        event,
        singleMaxDeliverPositive,
        singleMinDeliver,
      )
      val criteria = List(
        ByTimestamp(CantonTimestamp.Epoch),
        ByTimestamp(CantonTimestamp.MaxValue),
        ByTimestamp(CantonTimestamp.MinValue),
      )

      for {
        _stored <- store.store(events)
        found <- criteria.traverse(store.find).toValidatedNec
      } yield {
        assert(found.isValid, "finding deliver events succeeds")
        assert(found.map(_.toSeq) == Valid(events), "found the right deliver events")
      }
    }

    {
      lazy val deliver = mkDeliver(10, ts(10))
      lazy val secondDeliver = mkDeliverEventTc1(11, ts(11))
      lazy val deliverError = mkDeliverError(12, ts(12))

      "ignore existing events" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(11, 11))("ignoreEvents")
          events <- store.sequencedEvents()
          range <- valueOrFail(store.findRange(ByTimestampRange(ts(11), ts(12)), limit = None))(
            "findRange"
          )
          byTimestamp <- valueOrFail(store.find(ByTimestamp(ts(11))))("find by timestamp")
          latestUpTo <- valueOrFail(store.find(LatestUpto(ts(11))))("find latest up to")
        } yield {
          events shouldBe Seq(deliver, secondDeliver.asIgnoredEvent, deliverError)
          range shouldBe Seq(secondDeliver.asIgnoredEvent, deliverError)
          byTimestamp shouldBe secondDeliver.asIgnoredEvent
          latestUpTo shouldBe secondDeliver.asIgnoredEvent
        }
      }

      "ignore non-existing events" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(13, 14))("ignoreEvents")
          events <- store.sequencedEvents()
          range <- valueOrFail(store.findRange(ByTimestampRange(ts(12), ts(14)), limit = None))(
            "findRange"
          )
          ignoredEventByTimestamp <- valueOrFail(store.find(ByTimestamp(ts(13))))(
            "find by timestamp"
          )
          ignoredEventLatestUpTo <- valueOrFail(store.find(LatestUpto(ts(13))))("find latest up to")
        } yield {
          events shouldBe Seq(
            deliver,
            secondDeliver,
            deliverError,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
          range shouldBe Seq(deliverError, mkEmptyIgnoredEvent(13), mkEmptyIgnoredEvent(14))
          ignoredEventByTimestamp shouldBe mkEmptyIgnoredEvent(13)
          ignoredEventLatestUpTo shouldBe mkEmptyIgnoredEvent(13)
        }
      }

      "ignore existing and non-existing events" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(11, 14))("ignoreEvents")
          events <- store.sequencedEvents()
          range <- valueOrFail(store.findRange(ByTimestampRange(ts(11), ts(13)), limit = None))(
            "findRange"
          )
          deliverByTimestamp <- valueOrFail(store.find(ByTimestamp(ts(10))))("find by timestamp")
          deliverLatestUpTo <- valueOrFail(store.find(LatestUpto(ts(10))))("find latest up to")
        } yield {
          events shouldBe Seq(
            deliver,
            secondDeliver.asIgnoredEvent,
            deliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
          range shouldBe Seq(
            secondDeliver.asIgnoredEvent,
            deliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
          )
          deliverByTimestamp shouldBe deliver
          deliverLatestUpTo shouldBe deliver
        }
      }

      "add ignored events when empty" in {
        val store = mk()

        for {
          _ <- valueOrFail(store.ignoreEvents(10, 12))("ignoreEvents")
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(
            mkEmptyIgnoredEvent(10, 1),
            mkEmptyIgnoredEvent(11, 2),
            mkEmptyIgnoredEvent(12, 3),
          )
        }
      }

      "ignore beyond first event" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(0, 14))("ignoreEvents")
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(
            deliver.asIgnoredEvent,
            secondDeliver.asIgnoredEvent,
            deliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
        }
      }

      "ignore no events" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(1, 0))("ignoreEvents1")
          _ <- valueOrFail(store.ignoreEvents(11, 10))("ignoreEvents2")
          _ <- valueOrFail(store.ignoreEvents(21, 20))("ignoreEvents3")
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(deliver, secondDeliver, deliverError)
        }
      }

      "ignore ignored events" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(12, 13))("ignoreEvents1")
          _ <- valueOrFail(store.ignoreEvents(11, 14))("ignoreEvents2")
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(
            deliver,
            secondDeliver.asIgnoredEvent,
            deliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
        }
      }

      "prevent sequencer counter gaps" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          err <- store.ignoreEvents(20, 21).value
          events <- store.sequencedEvents()
        } yield {
          events shouldBe Seq(deliver, secondDeliver, deliverError)
          err shouldBe Left(ChangeWouldResultInGap(13, 19))
        }
      }

      "unignore events" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(11, 14))("ignoreEvents")

          _ <- valueOrFail(store.unignoreEvents(20, 0))("unignoreEvents20-0")
          events1 <- store.sequencedEvents()

          _ <- valueOrFail(store.unignoreEvents(12, 12))("unignoreEvents12")
          events2 <- store.sequencedEvents()

          err3 <- store.unignoreEvents(13, 13).value
          events3 <- store.sequencedEvents()

          _ <- valueOrFail(store.unignoreEvents(14, 14))("unignoreEvents14")
          events4 <- store.sequencedEvents()

          _ <- valueOrFail(store.unignoreEvents(0, 20))("unignoreEvents0-20")
          events5 <- store.sequencedEvents()
        } yield {
          events1 shouldBe Seq(
            deliver,
            secondDeliver.asIgnoredEvent,
            deliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )

          events2 shouldBe Seq(
            deliver,
            secondDeliver.asIgnoredEvent,
            deliverError,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )

          err3 shouldBe Left(ChangeWouldResultInGap(13, 13))
          events3 shouldBe Seq(
            deliver,
            secondDeliver.asIgnoredEvent,
            deliverError,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )

          events4 shouldBe Seq(
            deliver,
            secondDeliver.asIgnoredEvent,
            deliverError,
            mkEmptyIgnoredEvent(13),
          )

          events5 shouldBe Seq(deliver, secondDeliver, deliverError)
        }
      }

      "delete events" in {
        val store = mk()

        for {
          _ <- store.store(Seq(deliver, secondDeliver, deliverError))
          _ <- valueOrFail(store.ignoreEvents(11, 14))("ignoreEvents")
          _ <- store.delete(15)
          events1 <- store.sequencedEvents()
          _ <- store.delete(14)
          events2 <- store.sequencedEvents()
          _ <- store.delete(12)
          events3 <- store.sequencedEvents()
          _ <- store.delete(0)
          events4 <- store.sequencedEvents()
        } yield {
          events1 shouldBe Seq(
            deliver,
            secondDeliver.asIgnoredEvent,
            deliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
            mkEmptyIgnoredEvent(14),
          )
          events2 shouldBe Seq(
            deliver,
            secondDeliver.asIgnoredEvent,
            deliverError.asIgnoredEvent,
            mkEmptyIgnoredEvent(13),
          )
          events3 shouldBe Seq(deliver, secondDeliver.asIgnoredEvent)
          events4 shouldBe Seq.empty
        }
      }
    }
  }
}
