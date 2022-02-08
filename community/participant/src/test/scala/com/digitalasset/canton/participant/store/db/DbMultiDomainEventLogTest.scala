// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.{EventLogId, MultiDomainEventLogTest}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.resource.{DbStorage, IdempotentInsert}
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.time.Clock
import io.functionmeta.functionFullName
import slick.dbio.DBIOAction

import java.util.concurrent.Semaphore
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, blocking}

trait DbMultiDomainEventLogTest extends MultiDomainEventLogTest with DbTest {

  override def beforeAll(): Unit = {
    DbMultiDomainEventLogTest.acquireLinearizedEventLogLock(
      ErrorLoggingContext.fromTracedLogger(logger)
    )
    super.beforeAll()
  }

  override def afterAll() = {
    super.afterAll()
    DbMultiDomainEventLogTest.releaseLinearizedEventLogLock(
      ErrorLoggingContext.fromTracedLogger(logger)
    )
  }

  // If this test is run multiple times against a local persisted Postgres DB,
  // then the second run would find the requests from the first run and fail.
  override protected def cleanUpEventLogs(): Unit = {
    val theStorage = storage
    import theStorage.api._

    val cleanupF = theStorage.update(
      DBIO.seq(
        sqlu"truncate table linearized_event_log", // table guarded by DbMultiDomainEventLogTest.acquireLinearizedEventLogLock
        sqlu"delete from event_log where ${indexedStringStore.minIndex} <= log_id and log_id <= ${indexedStringStore.maxIndex}", // table shared with other tests
        sqlu"delete from event_log where log_id = $participantEventLogId", // table shared with other tests
      ),
      functionFullName,
    )
    Await.result(cleanupF, 10.seconds)
  }

  override def cleanDb(storage: DbStorage): Future[_] =
    Future.unit // Don't delete anything between tests, as tests depend on each other.

  override def storeEventsToSingleDimensionEventLogs(
      events: Seq[(EventLogId, TimestampedEvent)]
  ): Future[Unit] = {
    val theStorage = storage
    import theStorage.api._
    import theStorage.converters._
    import ParticipantStorageImplicits._

    val queries = events.map {
      case (id, tsEvent @ TimestampedEvent(event, localOffset, requestSequencerCounter, eventId)) =>
        IdempotentInsert.insertIgnoringConflicts(
          storage,
          "event_log pk_event_log",
          sql"""event_log (log_id, local_offset, ts, request_sequencer_counter, event_id, content, trace_context)
               values (${id.index}, $localOffset, ${tsEvent.timestamp}, $requestSequencerCounter, 
                 $eventId, $event, ${tsEvent.traceContext})""",
        )
    }

    theStorage.update_(DBIOAction.sequence(queries), functionFullName)
  }

  private def createDbMultiDomainEventLog(
      storage: DbStorage,
      clock: Clock,
  ): Future[DbMultiDomainEventLog] = {
    DbMultiDomainEventLog.apply(
      storage,
      clock,
      ParticipantTestMetrics,
      DefaultProcessingTimeouts.testing,
      indexedStringStore,
      loggerFactory,
      maxBatchSize = 3,
    )
  }

  "DbMultiDomainEventLog" should {

    behave like multiDomainEventLog { clock =>
      Await.result(createDbMultiDomainEventLog(storage, clock), 10.seconds)
    }
  }
}

object DbMultiDomainEventLogTest {

  /** Synchronize access to the linearized_event_log so that tests do not interfere */
  private val accessLinearizedEventLog: Semaphore = new Semaphore(1)

  def acquireLinearizedEventLogLock(elc: ErrorLoggingContext): Unit = {
    elc.logger.info(s"Acquiring linearized event log test lock")(elc.traceContext)
    blocking(accessLinearizedEventLog.acquire())
  }

  def releaseLinearizedEventLogLock(elc: ErrorLoggingContext): Unit = {
    elc.logger.info(s"Releasing linearized event log test lock")(elc.traceContext)
    accessLinearizedEventLog.release()
  }
}

class MultiDomainEventLogTestH2 extends DbMultiDomainEventLogTest with H2Test

class MultiDomainEventLogTestPostgres extends DbMultiDomainEventLogTest with PostgresTest
