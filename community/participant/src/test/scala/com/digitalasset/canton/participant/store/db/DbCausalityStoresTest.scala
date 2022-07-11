// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.store.{CausalityStoresTest, EventLogId}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.participant.{GlobalOffset, LedgerSyncEvent}
import com.digitalasset.canton.resource.{DbStorage, IdempotentInsert}
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import io.functionmeta.functionFullName
import slick.dbio.DBIOAction
import slick.jdbc.SetParameter

import scala.annotation.nowarn
import scala.concurrent.Future

trait DbCausalityStoresTest extends CausalityStoresTest with DbTest {

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

  override def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api._

    storage
      .update(
        DBIO.seq(
          sqlu"truncate table per_party_causal_dependencies", // table exclusively accessed by this test
          sqlu"truncate table linearized_event_log", // table guarded by DbMultiDomainEventLogTest.acquireLinearizedEventLogLock
          sqlu"delete from transfer_causality_updates where ${indexedStringStore.minIndex} <= log_id and log_id <= ${indexedStringStore.maxIndex}", // table shared with other tests
          sqlu"delete from event_log where ${indexedStringStore.minIndex} <= log_id and log_id <= ${indexedStringStore.maxIndex}", // table shared with other tests
        ),
        s"Cleaned DB for ${this.getClass}",
      )
      .map { case () => logger.info(s"Cleaned DB for ${this.getClass}") }
  }

  def persistEvents(
      events: Seq[(EventLogId, TimestampedEvent, Option[GlobalOffset])]
  ): Future[Unit] = {
    val theStorage = storage
    import theStorage.api._
    import theStorage.converters._

    @nowarn("cat=unused") implicit val setParameterTraceContext: SetParameter[TraceContext] =
      TraceContext.getVersionedSetParameter(testedProtocolVersion)
    @nowarn("cat=unused") implicit val setParameterLedgerSyncEvent: SetParameter[LedgerSyncEvent] =
      ParticipantStorageImplicits.setLedgerSyncEvent(testedProtocolVersion)

    val queries = events.flatMap {
      case (
            id,
            tsEvent @ TimestampedEvent(event, localOffset, requestSequencerCounter, eventId),
            globalOffsetO,
          ) =>
        val writeEventLog = IdempotentInsert.insertIgnoringConflicts(
          storage,
          "event_log pk_event_log",
          sql"""
                            event_log (log_id, local_offset, ts, request_sequencer_counter, event_id, content, trace_context)
                            values ($id, $localOffset, ${tsEvent.timestamp}, $requestSequencerCounter,
                            $eventId, $event, ${tsEvent.traceContext})""",
        )

        val writeMultiDomainEventLog = globalOffsetO.map { globalOffset =>
          IdempotentInsert.insertIgnoringConflicts(
            storage,
            "linearized_event_log ( global_offset )",
            sql""" 
                            linearized_event_log (global_offset, log_id, local_offset, publication_time)
                            values (${globalOffset}, ${id.index}, $localOffset, ${CantonTimestamp
              .now()})
            """,
          )
        }
        writeEventLog :: writeMultiDomainEventLog.toList
    }

    theStorage.update_(DBIOAction.sequence(queries), functionFullName)
  }

  "DbCausalityStoresTest" should {

    behave like causalityStores(
      () => {

        for {
          lookup <- DbMultiDomainCausalityStore(
            storage,
            indexedStringStore,
            timeouts,
            loggerFactory,
          )
          single = new DbSingleDomainCausalDependencyStore(
            writeToDomain,
            storage,
            timeouts,
            loggerFactory,
          )
          _unit <- single.initialize(None)
        } yield TestedStores(lookup, single)
      },
      persistence = true,
    )
  }
}

class CausalityStoresTestH2 extends DbCausalityStoresTest with H2Test

class CausalityStoresTestPostgres extends DbCausalityStoresTest with PostgresTest
