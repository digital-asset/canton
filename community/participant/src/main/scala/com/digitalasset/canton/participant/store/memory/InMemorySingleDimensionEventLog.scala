// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.OptionT
import cats.syntax.option._
import cats.syntax.traverse._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.protocol.CausalityUpdate
import com.digitalasset.canton.participant.store.{EventLogId, SingleDimensionEventLog}
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.participant.sync.{TimestampedEvent, TimestampedEventAndCausalChange}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil._

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class InMemorySingleDimensionEventLog[+Id <: EventLogId](
    override val id: Id,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends SingleDimensionEventLog[Id]
    with NamedLogging {

  protected case class Entries(
      eventsByOffset: SortedMap[LocalOffset, TimestampedEventAndCausalChange],
      eventsByEventId: Map[EventId, TimestampedEventAndCausalChange],
  )

  protected val state = new AtomicReference(Entries(TreeMap.empty, Map.empty))

  override def insertsUnlessEventIdClash(events: Seq[TimestampedEventAndCausalChange])(implicit
      traceContext: TraceContext
  ): Future[Seq[Either[TimestampedEvent, Unit]]] = Future.fromTry {
    events.traverse(insertUnlessEventIdClashInternal)
  }

  private def insertUnlessEventIdClashInternal(
      eventAndClock: TimestampedEventAndCausalChange
  ): Try[Either[TimestampedEvent, Unit]] = {
    val TimestampedEventAndCausalChange(event, causalityUpdate) = eventAndClock
    implicit val traceContext: TraceContext = event.traceContext

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var eventIdClash: Option[TimestampedEvent] = None
    val eventId = event.eventId

    def insertInternal(oldLedger: Entries): Entries = {
      val Entries(ledger, byEventId) = oldLedger
      val offset = event.localOffset
      ledger.get(offset) match {
        case None =>
          eventId.flatMap(byEventId.get) match {
            case Some(TimestampedEventAndCausalChange(existingEvent, existingUpdate)) =>
              eventIdClash = existingEvent.some
              oldLedger
            case None =>
              logger.trace(show"Inserted event at offset $offset.")
              Entries(
                ledger + (offset -> eventAndClock),
                eventId.fold(byEventId)(id => byEventId + (id -> eventAndClock)),
              )
          }
        case Some(TimestampedEventAndCausalChange(existingEvent, existingUpdate))
            if existingEvent.normalized == event.normalized =>
          ErrorUtil.requireState(
            existingUpdate == causalityUpdate,
            show"The event at offset" +
              show" ${event.localOffset} has been re-inserted with a different causality update." +
              show"Old value: ${existingUpdate}, new value: ${causalityUpdate}.",
          )
          logger.info(
            s"The event to insert at offset $offset already exists in the event log. Nothing to do."
          )
          oldLedger
        case Some(TimestampedEventAndCausalChange(existingEvent, causalityUpdate)) =>
          ErrorUtil.internalError(
            new IllegalArgumentException(show"""Unable to overwrite an existing event. Aborting.
                                               |Existing event: ${existingEvent}
                                               |New event: $event""".stripMargin)
          )
      }
    }

    Try {
      val newLedger = state.updateAndGet(insertInternal)
      // We get here only if the new event was inserted or it was already there
      // So this suffices as a check for whether the event is now there
      Either.cond(
        newLedger.eventsByOffset.contains(event.localOffset),
        (),
        eventIdClash.getOrElse(
          ErrorUtil.internalError(
            new IllegalStateException("Event is not there despite no event clash having been found")
          )
        ),
      )
    }
  }

  def storeTransferUpdate(causalityUpdate: CausalityUpdate)(implicit
      tc: TraceContext
  ): Future[Unit] = Future.unit

  override def prune(
      beforeAndIncluding: LocalOffset
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.info(s"Pruning event log at offset $beforeAndIncluding...")
    val _ = state.updateAndGet { case Entries(ledger, _byEventId) =>
      val newLedger = ledger.filter { case (off, _) => off > beforeAndIncluding }
      val newEventIds = byEventIds(newLedger)
      Entries(newLedger, newEventIds)
    }
    Future.unit
  }

  override def lookupEventRange(
      fromInclusive: Option[LocalOffset],
      toInclusive: Option[LocalOffset],
      fromTimestampInclusive: Option[CantonTimestamp],
      toTimestampInclusive: Option[CantonTimestamp],
      limit: Option[Int],
  )(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LocalOffset, TimestampedEventAndCausalChange]] =
    Future.successful {
      val allEvents = state.get().eventsByOffset
      val filteredEvents = allEvents
        .rangeFrom(fromInclusive.getOrElse(Long.MinValue))
        .rangeTo(toInclusive.getOrElse(Long.MaxValue))
        .filter { case (_, TimestampedEventAndCausalChange(event, update)) =>
          fromTimestampInclusive.forall(_ <= event.timestamp) && toTimestampInclusive.forall(
            event.timestamp <= _
          )
        }
      limit match {
        case Some(n) => filteredEvents.take(n)
        case None => filteredEvents
      }
    }

  override def eventAt(
      offset: LocalOffset
  )(implicit traceContext: TraceContext): OptionT[Future, TimestampedEventAndCausalChange] =
    OptionT(Future.successful {
      state.get().eventsByOffset.get(offset)
    })

  override def lastOffset(implicit traceContext: TraceContext): OptionT[Future, LocalOffset] =
    OptionT(Future.successful {
      state.get().eventsByOffset.lastOption.map { case (offset, _) => offset }
    })

  override def eventById(eventId: EventId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, TimestampedEventAndCausalChange] =
    OptionT(Future.successful(state.get().eventsByEventId.get(eventId)))

  override def existsBetween(
      timestampInclusive: CantonTimestamp,
      localOffsetInclusive: LocalOffset,
  )(implicit traceContext: TraceContext): Future[Boolean] = Future.successful {
    state.get().eventsByOffset.rangeTo(localOffsetInclusive).exists {
      case (localOffset, TimestampedEventAndCausalChange(tse, _)) =>
        tse.timestamp >= timestampInclusive
    }
  }

  override def deleteSince(
      inclusive: LocalOffset
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      val _ = state.updateAndGet { case Entries(ledger, _) =>
        val newLedger = ledger.filter { case (offset, _) => offset < inclusive }
        val newTransactionIds = byEventIds(newLedger)
        Entries(newLedger, newTransactionIds)
      }
    }

  private[this] def byEventIds(
      byOffset: SortedMap[LocalOffset, TimestampedEventAndCausalChange]
  ): Map[EventId, TimestampedEventAndCausalChange] =
    byOffset
      .map { case (_, eventAndCausalChange) =>
        eventAndCausalChange.tse.eventId.map(_ -> eventAndCausalChange)
      }
      .collect { case Some(entry) => entry }
      .toMap
}
