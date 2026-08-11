// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.memory

import cats.data.{EitherT, OptionT}
import cats.implicits.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficEnforcementError
import com.digitalasset.canton.tea.projection.memory.TeaMemoryTrafficStore.{
  BalanceKey,
  EventKey,
  TrafficUpdateOutOfBoundException,
}
import com.digitalasset.canton.tea.projection.{
  AccountId,
  AccountState,
  DeltaEvent,
  EventId,
  EventSource,
  TeaTrafficStore,
  TrafficDelta,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex

import scala.collection.immutable.VectorMap
import scala.concurrent.{ExecutionContext, Future}

object TeaMemoryTrafficStore {
  private final case class EventKey(eventSource: EventSource, eventId: EventId)
  private final case class BalanceKey(accountId: AccountId)

  /** Internal signal raised when applying a delta would drive a running total below zero. It is
    * caught and mapped to a
    * [[com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficUpdateOutOfBound]] error.
    */
  private final class TrafficUpdateOutOfBoundException
      extends RuntimeException
      with scala.util.control.NoStackTrace

  /** Classifies the failures that mean "this delta cannot be applied", mirroring
    * [[com.digitalasset.canton.tea.projection.db.TeaDbTrafficStore.rejection]] for the DB backends.
    */
  private[memory] def rejection(accountId: AccountId, trafficDelta: TrafficDelta)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): PartialFunction[Throwable, TrafficEnforcementError] = {
    case _: ArithmeticException | _: TrafficUpdateOutOfBoundException =>
      TrafficEnforcementErrors.TrafficUpdateOutOfBound
        .Reject(accountId.toString, trafficDelta.toString)
  }
}

class TeaMemoryTrafficStore(override val loggerFactory: NamedLoggerFactory)(implicit
    ec: ExecutionContext
) extends TeaTrafficStore
    with NamedLogging {

  private val lock = new Mutex()
  private val balances = scala.collection.mutable.Map.empty[BalanceKey, AccountState]
  // Mapping Account -> Map[EventKey -> Event]
  // use a VectorMap to maintain insertion order
  private val events =
    scala.collection.mutable.Map.empty[AccountId, VectorMap[EventKey, DeltaEvent]]
  // Deduplication is global on (event_source, event_id), independent of the account, so we track
  // the set of keys seen across all accounts.
  private val seenKeys = scala.collection.mutable.Set.empty[EventKey]

  override def getBalance(accountId: AccountId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, AccountState] =
    OptionT.fromOption[FutureUnlessShutdown](balances.get(BalanceKey(accountId)))

  override def persistTrafficDelta(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      trafficDelta: TrafficDelta,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, Option[AccountState]] =
    EitherT(
      persistDeltaInternal(
        accountId,
        eventId,
        eventSource,
        trafficDelta,
        timestamp = timestamp,
      )
        .map(Either.right[TrafficEnforcementError, Option[AccountState]])
        .recover(TeaMemoryTrafficStore.rejection(accountId, trafficDelta).andThen(Left(_)))
    ).mapK(FutureUnlessShutdown.outcomeK)

  private[memory] def persistDeltaInternal(
      account: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      trafficDelta: TrafficDelta,
      timestamp: CantonTimestamp,
  ): Future[Option[AccountState]] = Future {
    lock.exclusive {
      val deltaEvent = DeltaEvent(trafficDelta, timestamp, eventSource)
      val key = EventKey(deltaEvent.eventSource, eventId)
      val balanceKey = BalanceKey(account)

      // Deduplication is global on (event_source, event_id): if the key was already seen for any
      // account, this is a duplicate and must be a no-op.
      val eventPersisted = !seenKeys.contains(key)

      if (eventPersisted) {
        val (debitUpdate, creditUpdate) = trafficDelta.debitAndCreditDeltas

        // Compute the resulting totals as raw Longs: they may transiently be negative (or overflow),
        // which we detect and reject rather than let the NonNegativeLong conversion throw.
        val (newTotalDebits, newTotalCredits, newUpdatedAt) = balances.get(balanceKey) match {
          case None =>
            (debitUpdate, creditUpdate, deltaEvent.timestamp)
          case Some(state) =>
            (
              Math.addExact(state.totalDebits.value, debitUpdate),
              Math.addExact(state.totalCredits.value, creditUpdate),
              deltaEvent.timestamp.max(state.updatedAt),
            )
        }

        // Reject the update if applying the delta would drive either running total below zero.
        // Validate before mutating so the in-memory state is left untouched on failure.
        val newState =
          (NonNegativeLong.create(newTotalDebits), NonNegativeLong.create(newTotalCredits)) match {
            case (Right(debits), Right(credits)) =>
              AccountState(account, debits, credits, newUpdatedAt)
            case _ =>
              throw new TrafficUpdateOutOfBoundException
          }

        seenKeys.add(key).discard
        events
          .updateWith(account) {
            case None => Some(VectorMap(key -> deltaEvent))
            case Some(existingEvents) => Some(existingEvents.updated(key, deltaEvent))
          }
          .discard
        balances.update(balanceKey, newState)
        Some(newState)
      } else {
        balances.get(balanceKey)
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
