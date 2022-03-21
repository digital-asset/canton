// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.option._
import com.daml.ledger.participant.state.v2.TransactionMeta
import com.daml.lf.CantonOnly
import com.daml.lf.data.{ImmArray, Time}
import com.digitalasset.canton._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.LedgerSyncEvent.PublicPackageUploadRejected
import com.digitalasset.canton.participant.protocol.TransactionUpdate
import com.digitalasset.canton.participant.store.db.DbEventLogTestResources
import com.digitalasset.canton.participant.sync.TimestampedEvent.TimelyRejectionEventId
import com.digitalasset.canton.participant.sync.{
  LedgerEvent,
  TimestampedEvent,
  TimestampedEventAndCausalChange,
}
import com.digitalasset.canton.participant.{
  LedgerSyncEvent,
  LedgerSyncRecordTime,
  LocalOffset,
  RequestCounter,
}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.util.UUID
import scala.collection.immutable.HashMap
import scala.concurrent.Future

trait SingleDimensionEventLogTest extends BeforeAndAfterAll with BaseTest {
  this: AsyncWordSpec =>
  import SingleDimensionEventLogTest._

  lazy val id: EventLogId = DbEventLogTestResources.dbSingleDimensionEventLogEventLogId

  def generateEventWithTransactionId(
      localOffset: LocalOffset,
      idString: String,
  ): TimestampedEvent = {

    val transactionMeta =
      TransactionMeta(
        ledgerEffectiveTime = Time.Timestamp.Epoch,
        workflowId = None,
        submissionTime = Time.Timestamp.Epoch,
        submissionSeed = LedgerEvent.noOpSeed,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      )
    val transactionId = LedgerTransactionId.assertFromString(idString)

    val committedTransaction = LfCommittedTransaction(
      CantonOnly
        .lfVersionedTransaction(
          version = LfTransactionVersion.V10,
          nodes = HashMap.empty,
          roots = ImmArray.empty,
        )
    )

    val transactionAccepted = LedgerSyncEvent.TransactionAccepted(
      None,
      transactionMeta,
      committedTransaction,
      transactionId,
      LfTimestamp.Epoch,
      List.empty,
      None,
    )

    TimestampedEvent(transactionAccepted, localOffset, None)
  }

  lazy val domain1: DomainId = DomainId.tryFromString("domain::one")
  lazy val update: TransactionUpdate =
    TransactionUpdate(Set.empty, CantonTimestamp.MinValue, domain1, 0L)

  def singleDimensionEventLog(mk: () => SingleDimensionEventLog[EventLogId]): Unit = {

    def withEventLog[A](body: SingleDimensionEventLog[EventLogId] => Future[A]): Future[A] = {
      val eventLog = mk()
      for {
        _ <- eventLog.prune(Long.MaxValue)
        result <- body(eventLog)
      } yield result
    }

    def assertEvents(
        eventLog: SingleDimensionEventLog[EventLogId],
        expectedEvents: Seq[TimestampedEvent],
    ): Future[Assertion] =
      for {
        events <- eventLog.lookupEventRange(None, None, None, None, None)
        lastOffset <- eventLog.lastOffset.value
      } yield {
        val normalizedEvents = events
          .map({ case (offset, TimestampedEventAndCausalChange(event, clock)) =>
            offset -> event.normalized
          })
          .toMap
        normalizedEvents shouldBe expectedEvents
          .map(event => event.localOffset -> event.normalized)
          .toMap
        lastOffset shouldBe expectedEvents.lastOption.map(_.localOffset)
      }

    "empty" should {
      "have an empty last offset and no events" in withEventLog { eventLog =>
        assertEvents(eventLog, Seq.empty)
      }

      "publish a single event and read from ledger beginning" in withEventLog { eventLog =>
        val ts = LedgerSyncRecordTime.Epoch
        val event = generateEvent(ts, 1L)
        logger.debug("Starting 1")
        for {
          _ <- eventLog.insert(event, None)
          _ <- assertEvents(eventLog, Seq(event))
        } yield {
          logger.debug("Finished 1")
          succeed
        }
      }

      "store dummy events for transfers" in withEventLog { eventLog =>
        logger.debug("Starting 2")
        for {
          _ <- eventLog.storeTransferUpdate(update)
        } yield {
          logger.debug("Finished 2")
          succeed
        }
      }

      "support deleteSince" in withEventLog { eventLog =>
        logger.debug("Starting 3")
        for {
          _ <- eventLog.deleteSince(3L)
          _ <- assertEvents(eventLog, Seq.empty)
        } yield {
          logger.debug("Finished 3")
          succeed
        }
      }

      "publish an event by ID several times" in withEventLog { eventLog =>
        logger.debug("Starting 4")
        val domainId = DomainId.tryFromString("domain::id")
        val eventId1 = TimelyRejectionEventId(domainId, new UUID(0L, 1L))
        val eventId2 = TimelyRejectionEventId(domainId, new UUID(0L, 2L))

        val event1 =
          generateEventWithTransactionId(1L, "transaction-id").copy(eventId = eventId1.some)
        val event2 =
          generateEventWithTransactionId(2L, "transaction-id").copy(eventId = eventId2.some)
        val event3 = generateEventWithTransactionId(3L, "transaction-id")

        for {
          () <- eventLog.insertUnlessEventIdClash(event1, None).valueOrFail("First insert")
          () <- eventLog.insertUnlessEventIdClash(event2, None).valueOrFail("Second insert")
          () <- eventLog.insert(event3, None)
          _ <- assertEvents(eventLog, Seq(event1, event2, event3))
        } yield {
          logger.debug("Finished 4")
          succeed
        }
      }
    }

    "contains events" should {
      "publish an event and read from the ledger end" in withEventLog { eventLog =>
        logger.debug("Starting 5")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)

        val ts2 = LedgerSyncRecordTime.Epoch.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(
            Seq(event1, event2).map(e => TimestampedEventAndCausalChange(e, None))
          )
          _ <- assertEvents(eventLog, Seq(event1, event2))
        } yield {
          logger.debug("Finished 5")
          inserts shouldBe Seq(Right(()), Right(()))
        }
      }

      "publish events with the same timestamp" in withEventLog { eventLog =>
        logger.debug("Starting 6")
        val ts = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts, 1)
        val event2 = generateEvent(ts, 2)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(
            Seq(event1, event2).map(e => TimestampedEventAndCausalChange(e, None))
          )
          _ <- assertEvents(eventLog, Seq(event1, event2))
        } yield {
          logger.debug("Finished 6")
          inserts shouldBe Seq(Right(()), Right(()))
        }
      }

      "publish events out of order" in withEventLog { eventLog =>
        logger.debug("Starting 7")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)

        val ts2 = LedgerSyncRecordTime.Epoch.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2)

        for {
          _ <- eventLog.insert(event2, None)
          _ <- eventLog.insert(event1, None)
          _ <- assertEvents(eventLog, Seq(event1, event2))
        } yield {
          logger.debug("Finished 7")
          succeed
        }
      }

      "be idempotent on duplicate publications" in withEventLog { eventLog =>
        logger.debug("Starting 8")
        val event = generateEvent(LedgerSyncRecordTime.Epoch, 1)

        for {
          _ <- eventLog.insert(event, None)
          _ <- eventLog.insert(event, None)
          _ <- assertEvents(eventLog, Seq(event))
        } yield {
          logger.debug("Finished 8")
          succeed
        }
      }

      "publish events with extreme counters and timestamps" in withEventLog { eventLog =>
        logger.debug("Starting 9")
        val event1 =
          generateEvent(LedgerSyncRecordTime.MinValue, Long.MinValue, Some(Long.MinValue))
        val event2 =
          generateEvent(LedgerSyncRecordTime.MaxValue, Long.MaxValue, Some(Long.MaxValue))

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(
            Seq(event1, event2).map(e => TimestampedEventAndCausalChange(e, None))
          )
          _ <- assertEvents(eventLog, Seq(event1, event2))
        } yield {
          logger.debug("Finished 9")
          inserts shouldBe Seq(Right(()), Right(()))
        }
      }

      "reject publication of conflicting events" in withEventLog { eventLog =>
        logger.debug("Starting 10")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)

        val ts2 = LedgerSyncRecordTime.Epoch.addMicros(5000000L)
        val event2 = generateEvent(ts2, 1)

        val domainId = DomainId.tryFromString("domain::id")
        val eventId = TimelyRejectionEventId(domainId, new UUID(0L, 1L))

        for {
          _ <- eventLog.insert(event1, None)
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            eventLog.insert(event2, None),
            _.getMessage should startWith("Unable to overwrite an existing event. Aborting."),
          )
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            eventLog.insertUnlessEventIdClash(event2.copy(eventId = eventId.some), None).value,
            _.getMessage should startWith("Unable to overwrite an existing event. Aborting."),
          )
          _ <- assertEvents(eventLog, Seq(event1))
        } yield {
          logger.debug("Finished 10")
          succeed
        }
      }

      "reject publication of conflicting event ids" in withEventLog { eventLog =>
        logger.debug("Starting 11")
        val domainId = DomainId.tryFromString("domain::id")
        val eventId = TimelyRejectionEventId(domainId, new UUID(0L, 1L))

        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1L).copy(eventId = eventId.some)

        val ts2 = LedgerSyncRecordTime.Epoch.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2L).copy(eventId = eventId.some)

        for {
          () <- eventLog.insertUnlessEventIdClash(event1, None).valueOrFail("first insert")
          clash <- leftOrFail(eventLog.insertUnlessEventIdClash(event2, None))("second insert")
          _ <- assertEvents(eventLog, Seq(event1))
          _ <- eventLog.insert(
            event2.copy(eventId = None),
            None,
          ) // We can still insert it normally without generating an ID
          _ <- assertEvents(eventLog, Seq(event1, event2.copy(eventId = None)))
        } yield {
          logger.debug("Finished 11")
          clash shouldBe event1
        }
      }

      "reject duplicate transaction ids" in withEventLog { eventLog =>
        logger.debug("Starting 12")
        val event1 = generateEventWithTransactionId(1, "id1")
        val event2 = generateEventWithTransactionId(2, "id2")
        val event3 = generateEventWithTransactionId(3, "id1")

        for {
          _ <- eventLog.insert(event1, None)
          _ <- eventLog.insert(event2, None)
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            eventLog.insert(event3, None),
            _.getMessage should fullyMatch regex "Unable to insert event, as the eventId id .* has already been inserted with offset 1\\.",
          )
        } yield {
          logger.debug("Finished 12")
          succeed
        }
      }

      "lookup events by transaction id" in withEventLog { eventLog =>
        logger.debug("Starting 13")
        val event1 = generateEventWithTransactionId(1, "id1")
        val event2 = generateEventWithTransactionId(2, "id2")
        val event3 = generateEvent(LedgerSyncRecordTime.Epoch, 3)

        for {
          _ <- eventLog.insert(event1, None)
          _ <- eventLog.insert(event2, None)
          _ <- eventLog.insert(event3, None)
          et1 <- eventLog.eventByTransactionId(LedgerTransactionId.assertFromString("id1")).value
          et2 <- eventLog.eventByTransactionId(LedgerTransactionId.assertFromString("id2")).value
          et3 <- eventLog.eventByTransactionId(LedgerTransactionId.assertFromString("id3")).value
        } yield {
          logger.debug("Finished 13")
          et1.value.tse.normalized shouldBe event1.normalized
          et2.value.tse.normalized shouldBe event2.normalized
          et3 shouldBe None
        }
      }
      "correctly prune specified events" in withEventLog { eventLog =>
        logger.debug("Starting 14")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)
        val ts2 = ts1.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2)
        val ts3 = ts2.addMicros(5000000L)
        val event3 = generateEvent(ts3, 3)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(
            Seq(event1, event2, event3).map(e => TimestampedEventAndCausalChange(e, None))
          )
          _ <- eventLog.prune(2)
          _ <- assertEvents(eventLog, Seq(event3))
        } yield {
          logger.debug("Finished 14")
          inserts shouldBe Seq(Right(()), Right(()), Right(()))
        }
      }

      "existsBetween finds time jumps" in withEventLog { eventLog =>
        logger.debug("Starting 15")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1L)
        val ts2 = ts1.addMicros(-5000000L)
        val event2 = generateEvent(ts2, 2L)
        val ts3 = ts1.addMicros(5000000L)
        val event3 = generateEvent(ts3, 3L)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(
            Seq(event1, event2, event3).map(e => TimestampedEventAndCausalChange(e, None))
          )
          q1 <- eventLog.existsBetween(CantonTimestamp(ts1), 2L)
          q2 <- eventLog.existsBetween(CantonTimestamp(ts1).immediateSuccessor, 2L)
          q3 <- eventLog.existsBetween(CantonTimestamp(ts2), 0L)
          q4 <- eventLog.existsBetween(CantonTimestamp.MaxValue, RequestCounter.MaxValue)
          q5 <- eventLog.existsBetween(CantonTimestamp(ts3), 3L)
        } yield {
          logger.debug("Finished 15")
          inserts shouldBe Seq(Right(()), Right(()), Right(()))
          q1 shouldBe true
          q2 shouldBe false
          q3 shouldBe false
          q4 shouldBe false
          q5 shouldBe true
        }
      }

      "delete events from the given request counter" in withEventLog { eventLog =>
        logger.debug("Starting 16")
        val ts1 = LedgerSyncRecordTime.Epoch
        val event1 = generateEvent(ts1, 1)
        val ts2 = ts1.addMicros(5000000L)
        val event2 = generateEvent(ts2, 2)
        val ts4 = ts2.addMicros(5000000L)
        val event4 = generateEvent(ts4, 4)
        val ts4a = ts4.addMicros(1L)
        val event4a = generateEvent(ts4a, 4)

        for {
          inserts <- eventLog.insertsUnlessEventIdClash(
            Seq(event1, event2, event4).map(e => TimestampedEventAndCausalChange(e, None))
          )
          () <- eventLog.deleteSince(3L)
          _ <- assertEvents(eventLog, Seq(event1, event2))
          () <- eventLog.insert(event4a, None) // can insert deleted event with different timestamp
          _ <- assertEvents(eventLog, Seq(event1, event2, event4a))
          () <- eventLog.deleteSince(2L)
          _ <- assertEvents(eventLog, Seq(event1))
        } yield {
          logger.debug("Finished 16")
          inserts shouldBe Seq(Right(()), Right(()), Right(()))
        }
      }
    }
  }
}

object SingleDimensionEventLogTest {
  def generateEvent(
      recordTime: LedgerSyncRecordTime,
      localOffset: LocalOffset,
      requestSequencerCounter: Option[SequencerCounter] = Some(42L),
  )(implicit traceContext: TraceContext): TimestampedEvent =
    TimestampedEvent(
      PublicPackageUploadRejected(
        LedgerSubmissionId.assertFromString("submission"),
        recordTime,
        "event",
      ),
      localOffset,
      requestSequencerCounter,
    )
}
