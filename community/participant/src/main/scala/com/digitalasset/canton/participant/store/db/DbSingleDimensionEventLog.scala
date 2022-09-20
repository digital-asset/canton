// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.OptionT
import cats.syntax.either._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.protocol.CausalityUpdate
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.participant.sync.{TimestampedEvent, TimestampedEventAndCausalChange}
import com.digitalasset.canton.participant.{LocalOffset, RequestCounter}
import com.digitalasset.canton.resource.{DbStorage, DbStore, IdempotentInsert}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.version.ReleaseProtocolVersion
import io.functionmeta.functionFullName
import slick.jdbc._

import scala.collection.{SortedMap, mutable}
import scala.concurrent.{ExecutionContext, Future}

class DbSingleDimensionEventLog[+Id <: EventLogId](
    override val id: Id,
    override protected val storage: DbStorage,
    indexedStringStore: IndexedStringStore,
    releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends SingleDimensionEventLog[Id]
    with DbStore {

  import ParticipantStorageImplicits._
  import storage.api._
  import storage.converters._

  protected val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("single-dimension-event-log")

  private def log_id: Int = id.index

  private implicit val setParameterTraceContext: SetParameter[TraceContext] =
    TraceContext.getVersionedSetParameter(releaseProtocolVersion.v)
  private implicit val setParameterCausalityUpdate: SetParameter[CausalityUpdate] =
    CausalityUpdate.getVersionedSetParameter
  private implicit val setParameterCausalityUpdateO: SetParameter[Option[CausalityUpdate]] =
    CausalityUpdate.getVersionedSetParameterO
  private implicit val setParameterSerializableLedgerSyncEvent
      : SetParameter[SerializableLedgerSyncEvent] =
    SerializableLedgerSyncEvent.getVersionedSetParameter

  override def insertsUnlessEventIdClash(
      events: Seq[TimestampedEventAndCausalChange]
  )(implicit traceContext: TraceContext): Future[Seq[Either[TimestampedEvent, Unit]]] = {
    idempotentInserts(events).flatMap { insertResult =>
      insertResult.traverse {
        case right @ Right(()) => Future.successful(right)
        case Left(event) =>
          event.eventId match {
            case None => cannotInsertEvent(event)
            case Some(eventId) =>
              val e = eventById(eventId).map(_.tse)
              e.toLeft(cannotInsertEvent(event)).value
          }
      }
    }
  }

  private def idempotentInserts(
      events: Seq[TimestampedEventAndCausalChange]
  )(implicit traceContext: TraceContext): Future[List[Either[TimestampedEvent, Unit]]] =
    processingTime.metric.event {
      for {
        rowCounts <- rawInserts(events)
        _ = ErrorUtil.requireState(
          rowCounts.length == events.size,
          "Event insertion did not produce a result for each event",
        )
        insertionResults = rowCounts.toList.zip(events).map { case (rowCount, event) =>
          Either.cond(rowCount == 1, (), event)
        }
        checkResults <- insertionResults.traverse {
          case Right(()) =>
            Future.successful(Right(()))
          case Left(TimestampedEventAndCausalChange(event, causalityUpdate)) =>
            logger.info(
              show"Insertion into event log at offset ${event.localOffset} skipped. Checking the reason..."
            )
            eventAt(event.localOffset).fold(Either.left[TimestampedEvent, Unit](event)) {
              case TimestampedEventAndCausalChange(existingEvent, existingUpdate) =>
                if (existingEvent.normalized == event.normalized) {
                  ErrorUtil.requireState(
                    existingUpdate == causalityUpdate,
                    show"The event at offset" +
                      show" ${event.localOffset} has been re-inserted with a different causality update." +
                      show"Old value: ${existingUpdate}, new value: ${causalityUpdate}.",
                  )
                  logger.info(
                    show"The event at offset ${event.localOffset} has already been inserted. Nothing to do."
                  )
                  Right(())
                } else
                  ErrorUtil.internalError(
                    new IllegalArgumentException(
                      show"""Unable to overwrite an existing event. Aborting.
                            |Existing event: ${existingEvent}

                            |New event: $event""".stripMargin
                    )
                  )
            }
        }
      } yield checkResults
    }

  def storeTransferUpdate(
      causalityUpdate: CausalityUpdate
  )(implicit tc: TraceContext): Future[Unit] = {
    val insertAction = IdempotentInsert.insertIgnoringConflicts(
      storage,
      "transfer_causality_updates pk_transfer_causality_updates",
      sql"""transfer_causality_updates
                   values (${log_id}, ${causalityUpdate.rc}, ${causalityUpdate.ts}, ${causalityUpdate})""",
    )

    storage.update_(insertAction, functionFullName)
  }

  private def rawInserts(
      events: Seq[TimestampedEventAndCausalChange]
  )(implicit traceContext: TraceContext): Future[Array[Int]] = {
    // resolve associated domain-id
    val eventsWithAssociatedDomainIdF = events.traverse { event =>
      event.tse.eventId.flatMap(_.associatedDomain) match {
        case Some(domainId) =>
          IndexedDomain.indexed(indexedStringStore)(domainId).map(indexed => (event, Some(indexed)))
        case None => Future.successful((event, None))
      }
    }
    eventsWithAssociatedDomainIdF.flatMap { eventsWithAssociatedDomainId =>
      processingTime.metric.event {
        val dbio = storage.profile match {
          case _: DbStorage.Profile.Oracle =>
            val query =
              """merge into event_log e
                 using dual on ( (e.event_id = ?) or (e.log_id = ? and e.local_offset = ?))
                 when not matched then
                 insert (log_id, local_offset, ts, request_sequencer_counter, event_id, associated_domain, content, trace_context, causality_update)
                 values (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            DbStorage.bulkOperation(
              query,
              eventsWithAssociatedDomainId,
              storage.profile,
            ) { pp => eventWithClock =>
              val (TimestampedEventAndCausalChange(event, clock), associatedDomainIdO) =
                eventWithClock

              pp >> event.eventId
              pp >> log_id
              pp >> event.localOffset
              pp >> log_id
              pp >> event.localOffset
              pp >> event.timestamp
              pp >> event.requestSequencerCounter
              pp >> event.eventId
              pp >> associatedDomainIdO.map(_.index)
              pp >> SerializableLedgerSyncEvent(event.event, releaseProtocolVersion.v)
              pp >> event.traceContext
              pp >> clock
            }
          case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
            val query =
              """insert into event_log (log_id, local_offset, ts, request_sequencer_counter, event_id, associated_domain, content, trace_context, causality_update)
               values (?, ?, ?, ?, ?, ?, ?, ?, ?)
               on conflict do nothing"""
            DbStorage.bulkOperation(query, eventsWithAssociatedDomainId, storage.profile) {
              pp => eventAndClock =>
                val (TimestampedEventAndCausalChange(event, clock), associatedDomainIdO) =
                  eventAndClock

                pp >> log_id
                pp >> event.localOffset
                pp >> event.timestamp
                pp >> event.requestSequencerCounter
                pp >> event.eventId
                pp >> associatedDomainIdO.map(_.index)
                pp >> SerializableLedgerSyncEvent(event.event, releaseProtocolVersion.v)
                pp >> event.traceContext
                pp >> clock
            }
        }
        storage.queryAndUpdate(dbio, functionFullName)
      }
    }
  }

  private def cannotInsertEvent(event: TimestampedEvent): Nothing = {
    implicit val traceContext: TraceContext = event.traceContext
    val withId = event.eventId.fold("")(id => s" with id $id")
    ErrorUtil.internalError(
      new IllegalStateException(
        show"Unable to insert event at offset ${event.localOffset}${withId.unquoted}.\n$event"
      )
    )
  }

  override def prune(
      beforeAndIncluding: LocalOffset
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      storage.update_(
        sqlu"""delete from event_log where log_id = $log_id and local_offset <= $beforeAndIncluding""",
        functionFullName,
      )
    }

  override def lookupEventRange(
      fromInclusive: Option[LocalOffset],
      toInclusive: Option[LocalOffset],
      fromTimestampInclusive: Option[CantonTimestamp],
      toTimestampInclusive: Option[CantonTimestamp],
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LocalOffset, TimestampedEventAndCausalChange]] = {

    processingTime.metric.event {
      DbSingleDimensionEventLog.lookupEventRange(
        storage,
        id,
        fromInclusive,
        toInclusive,
        fromTimestampInclusive,
        toTimestampInclusive,
        limit,
      )
    }
  }

  override def eventAt(
      offset: LocalOffset
  )(implicit traceContext: TraceContext): OptionT[Future, TimestampedEventAndCausalChange] = {
    implicit val getResultCU: GetResult[Option[CausalityUpdate]] =
      CausalityUpdate.hasVersionedWrapperGetResultO
    import TimestampedEventAndCausalChange.getResultTimestampedEventAndCausalChange

    processingTime.metric.optionTEvent {
      storage
        .querySingle(
          sql"""select /*+ INDEX (event_log pk_event_log) */ 
       local_offset, request_sequencer_counter, event_id, content, trace_context, causality_update
              from event_log
              where log_id = $log_id and local_offset = $offset"""
            .as[TimestampedEventAndCausalChange]
            .headOption,
          functionFullName,
        )
    }

  }

  override def lastOffset(implicit traceContext: TraceContext): OptionT[Future, LocalOffset] =
    processingTime.metric.optionTEvent {
      storage.querySingle(
        sql"""select local_offset from event_log where log_id = $log_id order by local_offset desc #${storage
          .limit(1)}"""
          .as[LocalOffset]
          .headOption,
        functionFullName,
      )
    }

  override def eventById(
      eventId: EventId
  )(implicit traceContext: TraceContext): OptionT[Future, TimestampedEventAndCausalChange] = {
    implicit val getResultCU: GetResult[Option[CausalityUpdate]] =
      CausalityUpdate.hasVersionedWrapperGetResultO
    import TimestampedEventAndCausalChange.getResultTimestampedEventAndCausalChange

    processingTime.metric.optionTEvent {
      storage
        .querySingle(
          sql"""select local_offset, request_sequencer_counter, event_id, content, trace_context, causality_update
              from event_log 
              where log_id = $log_id and event_id = $eventId"""
            .as[TimestampedEventAndCausalChange]
            .headOption,
          functionFullName,
        )
    }
  }

  override def existsBetween(
      timestampInclusive: CantonTimestamp,
      localOffsetInclusive: LocalOffset,
  )(implicit traceContext: TraceContext): Future[Boolean] = processingTime.metric.event {
    val query =
      sql"""
        select 1 from event_log where log_id = $log_id and local_offset <= $localOffsetInclusive and ts >= $timestampInclusive
        #${storage.limit(1)}
      """
        .as[Int]
        .headOption
    storage.query(query, "exists between").map(_.isDefined)
  }

  override def deleteSince(
      inclusive: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      storage.update_(
        sqlu"""delete from event_log where log_id = $log_id and local_offset >= $inclusive""",
        functionFullName,
      )
    }

}

object DbSingleDimensionEventLog {
  private[store] def lookupEventRange(
      storage: DbStorage,
      eventLogId: EventLogId,
      fromInclusive: Option[LocalOffset],
      toInclusive: Option[LocalOffset],
      fromTimestampInclusive: Option[CantonTimestamp],
      toTimestampInclusive: Option[CantonTimestamp],
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
      closeContext: CloseContext,
  ): Future[SortedMap[LocalOffset, TimestampedEventAndCausalChange]] = {
    import DbStorage.Implicits.BuilderChain._
    import ParticipantStorageImplicits._
    import TimestampedEventAndCausalChange.getResultTimestampedEventAndCausalChange
    import storage.api._
    import storage.converters._

    val filters = List(
      fromInclusive.map(n => sql" and local_offset >= $n"),
      toInclusive.map(n => sql" and local_offset <= $n"),
      fromTimestampInclusive.map(n => sql" and ts >= $n"),
      toTimestampInclusive.map(n => sql" and ts <= $n"),
    ).flattenOption.intercalate(sql"")

    for {
      eventsVector <- storage.query(
        (sql"""select local_offset, request_sequencer_counter, event_id, content, trace_context, causality_update
                 from event_log
                 where log_id = ${eventLogId.index}""" ++ filters ++
          sql""" order by local_offset asc #${storage.limit(limit.getOrElse(Int.MaxValue))}""")
          .as[TimestampedEventAndCausalChange]
          .map(_.map { case t @ TimestampedEventAndCausalChange(event, cu) =>
            event.localOffset -> t
          }),
        functionFullName,
      )
    } yield {
      val result = new mutable.TreeMap[LocalOffset, TimestampedEventAndCausalChange]()
      result ++= eventsVector
      result
    }
  }
}
