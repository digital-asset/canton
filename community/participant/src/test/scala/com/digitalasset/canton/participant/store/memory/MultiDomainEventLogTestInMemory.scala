// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.data.OptionT
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.{EventLogId, MultiDomainEventLogTest}
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.participant.sync.{TimestampedEvent, TimestampedEventAndCausalChange}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class MultiDomainEventLogTestInMemory extends MultiDomainEventLogTest {

  val eventsRef: AtomicReference[Map[(EventLogId, LocalOffset), TimestampedEventAndCausalChange]] =
    new AtomicReference(Map.empty)

  override def storeEventsToSingleDimensionEventLogs(
      events: Seq[(EventLogId, TimestampedEvent)]
  ): Future[Unit] = {
    val rawEventsMap: Map[(EventLogId, LocalOffset), Seq[(EventLogId, TimestampedEvent)]] =
      events.groupBy { case (id, tsEvent) =>
        (id, tsEvent.localOffset)
      }
    val eventsMap: Map[(EventLogId, LocalOffset), TimestampedEventAndCausalChange] =
      rawEventsMap.map { case (key, events) =>
        val (_, event) = events.loneElement
        key -> TimestampedEventAndCausalChange(event, None)
      }
    eventsRef.set(eventsMap)
    Future.unit
  }

  protected override def cleanUpEventLogs(): Unit = ()

  def lookupEvent(
      id: EventLogId,
      localOffset: LocalOffset,
  ): Future[TimestampedEventAndCausalChange] = {
    Future.successful(eventsRef.get()(id -> localOffset))
  }

  def lookupOffsetsBetween(
      id: EventLogId
  )(fromInclusive: LocalOffset, upToInclusive: LocalOffset): Future[Seq[LocalOffset]] =
    Future.successful {
      eventsRef
        .get()
        .collect {
          case ((`id`, offset), _) if fromInclusive <= offset && offset <= upToInclusive => offset
        }
        .toSeq
        .sorted
    }

  def domainIdOfEventId(eventId: EventId): OptionT[Future, (EventLogId, LocalOffset)] = {
    val resultO = eventsRef.get().collectFirst {
      case (eventLogIdAndLocalOffset, event) if event.tse.eventId.contains(eventId) =>
        eventLogIdAndLocalOffset
    }
    OptionT(Future.successful(resultO))
  }

  "MultiDomainEventLogTestInMemory" should {
    behave like multiDomainEventLog {
      new InMemoryMultiDomainEventLog(
        _ => lookupEvent,
        _ => lookupOffsetsBetween,
        _ => domainIdOfEventId,
        participantEventLogId,
        _,
        ParticipantTestMetrics,
        indexedStringStore,
        timeouts,
        loggerFactory,
      )
    }
  }
}
