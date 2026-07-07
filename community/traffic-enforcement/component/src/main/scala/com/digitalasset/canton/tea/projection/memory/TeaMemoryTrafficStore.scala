// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.memory

import cats.data.OptionT
import cats.implicits.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tea.projection.memory.TeaMemoryTrafficStore.EventKey
import com.digitalasset.canton.tea.projection.{
  AccountId,
  AccountState,
  DeltaEvent,
  EventId,
  EventSource,
  EventType,
  TeaTrafficStore,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable.VectorMap
import scala.concurrent.{ExecutionContext, Future}

object TeaMemoryTrafficStore {
  private final case class EventKey(eventSource: EventSource, eventId: EventId)
}

class TeaMemoryTrafficStore(implicit ec: ExecutionContext) extends TeaTrafficStore {

  private val lock = new Mutex()
  private val balances = scala.collection.mutable.Map.empty[AccountId, AccountState]
  // Mapping Account -> Map[EventId -> Event]
  // use a VectorMap to maintain insertion order
  private val events =
    scala.collection.mutable.Map.empty[AccountId, VectorMap[EventKey, DeltaEvent]]

  override def getBalance(accountId: AccountId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, AccountState] =
    OptionT.fromOption[FutureUnlessShutdown](balances.get(accountId))

  override def persistDelta(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      eventType: EventType,
      delta: Long,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, AccountState] =
    OptionT[Future, AccountState](
      persistDeltaInternal(accountId, eventId, DeltaEvent(delta, timestamp, eventType, eventSource))
    )
      .mapK(FutureUnlessShutdown.outcomeK)

  private[memory] def persistDeltaInternal(
      account: AccountId,
      eventId: EventId,
      deltaEvent: DeltaEvent,
  ): Future[Option[AccountState]] = Future.successful {
    lock.exclusive {
      val eventPersisted = new AtomicBoolean(false)

      val debitAmount = if (deltaEvent.delta < 0) -deltaEvent.delta else 0L
      val creditAmount = if (deltaEvent.delta > 0) deltaEvent.delta else 0L

      val key = EventKey(deltaEvent.eventSource, eventId)
      events
        .updateWith(account) {
          case None =>
            eventPersisted.set(true)
            Some(VectorMap(key -> deltaEvent))
          case Some(existingEvents) if !existingEvents.contains(key) =>
            eventPersisted.set(true)
            Some(existingEvents.updated(key, deltaEvent))
          case Some(existingEvents) =>
            Some(existingEvents)
        }
        .discard

      if (eventPersisted.get()) {
        balances
          .updateWith(account) {
            case None =>
              Some(
                AccountState(
                  account,
                  debitAmount,
                  creditAmount,
                  deltaEvent.timestamp,
                )
              )
            case Some(state) =>
              Some(
                state.copy(
                  totalDebits = state.totalDebits + debitAmount,
                  totalCredits = state.totalCredits + creditAmount,
                  updatedAt = deltaEvent.timestamp.max(state.updatedAt),
                )
              )
          }
      } else {
        balances.get(account)
      }
    }
  }

  override def getEvents(accountId: AccountId, fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DeltaEvent]] = FutureUnlessShutdown.pure {
    events
      .get(accountId)
      .toList
      .flatMap(_.values)
      .sortBy(_.timestamp)
      .dropWhile(_.timestamp.isBefore(fromInclusive))
  }
}
