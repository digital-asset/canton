// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, DbStorage, DbStore}
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficEnforcementError
import com.digitalasset.canton.tea.projection.db.TeaDbTrafficStore.TrafficUpdateOutOfBoundException
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
import com.digitalasset.canton.util.retry.ErrorKind
import org.postgresql.util.PSQLState

import java.sql.SQLException
import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

import AccountState.*

/** Store for DB operations on traffic persistence.
  */
class TeaDbTrafficStore(
    override val storage: DbStorage,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    databaseQueryTimeout: PositiveFiniteDuration,
)(implicit ec: ExecutionContext)
    extends DbStore
    with TeaTrafficStore {
  import storage.api.*

  override def persistTrafficDelta(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      trafficDelta: TrafficDelta,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, Option[AccountState]] = {
    val classify = TeaDbTrafficStore.classifyPersistFailure(accountId, trafficDelta)
    EitherT(
      storage
        .queryAndUpdate(
          persistDeltaDBIO(
            accountId,
            eventId,
            eventSource,
            trafficDelta,
            timestamp = timestamp,
          ).transactionally,
          "persist traffic delta",
          // Single attempt per call: the RPC client retries within its own deadline, reusing the
          // same deduplication id.
          maxRetries = 0,
        )
        .transformWithHandledAborted {
          case Success(state) => FutureUnlessShutdown.pure(Right(state))
          case Failure(ex) => FutureUnlessShutdown.pure(Left(classify(ex)))
        }
    )
  }

  private def insertEventDBIO(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      trafficDelta: TrafficDelta,
      timestamp: CantonTimestamp,
  ): DBIOAction[Option[Long], NoStream, Effect.Write & Effect.Read] =
    storage.profile match {
      case _: Profile.Postgres =>
        for {
          insertedRows <- sql"""insert into par_traffic_enforcement_event
                 (account_id, event_id, event_source, event_type, amount, timestamp) values ($accountId, $eventId, $eventSource, ${trafficDelta.eventType}, ${trafficDelta.value}, $timestamp)
                 on conflict (event_source, event_id) do nothing
                 returning sequence_nb""".as[Long]
          singleEvent <- insertedRows.toList match {
            case Nil => DBIO.successful(None)
            case singleton :: Nil => DBIO.successful(Some(singleton))
            case moreThanOne =>
              DBIO.failed(
                new RuntimeException(s"Inserted $moreThanOne row in the traffic event table")
              )
          }
        } yield singleEvent
      // H2 doesn't handle on conflict do nothing so use the merge into syntax and then fetch the event to retrieve
      // the generated sequencer_nb
      case _: Profile.H2 =>
        for {
          insertedRows <- sqlu"""
            merge into par_traffic_enforcement_event t
            using (values ($accountId, $eventId, $eventSource, ${trafficDelta.eventType}, ${trafficDelta.value}, $timestamp)) as v(account_id, event_id, event_source, event_type, amount, timestamp)
            on t.event_source = v.event_source and t.event_id = v.event_id
            when not matched then
              insert (event_id, event_source, event_type, account_id, amount, timestamp)
              values (v.event_id, v.event_source, v.event_type, v.account_id, v.amount, v.timestamp)
            when matched and 1 = 0 then -- This ensures it NEVER updates existing rows
              update set t.event_id = v.event_id
          """
          event <-
            if (insertedRows == 0) DBIO.successful(None)
            else if (insertedRows == 1) {
              sql"""
                    select sequence_nb
                    from par_traffic_enforcement_event
                    where event_source = $eventSource and event_id = $eventId
                  """.as[Long].map(_.headOption)
            } else
              DBIO.failed(
                new RuntimeException("Inserted more than one row in the traffic event table")
              )
        } yield event
    }

  private def updateBalanceDBIO(
      accountId: AccountId,
      trafficDelta: TrafficDelta,
      sequenceNb: Long,
      timestamp: CantonTimestamp,
  ) = {
    val (debitUpdate, creditUpdate) = trafficDelta.debitAndCreditDeltas

    // Read the resulting totals as raw Longs: after applying the delta they may transiently be
    // negative, which we must detect and reject rather than let the NonNegativeLong conversion throw.
    val updatedBalance = storage.profile match {
      // We update the balance by trying an insert
      // then on conflict we update the total debit / credit values by adding the inserted delta with the existing one.
      // Timestamp is updated as greatest of existing + inserted
      case _: Profile.Postgres =>
        sql"""insert into par_traffic_enforcement_balance
               (account_id, event_sequence_nb, total_debits, total_credits, updated_at) values($accountId, $sequenceNb, $debitUpdate, $creditUpdate, $timestamp)
               on conflict (account_id) do update set
                  event_sequence_nb = excluded.event_sequence_nb,
                  total_debits = par_traffic_enforcement_balance.total_debits + excluded.total_debits,
                  total_credits = par_traffic_enforcement_balance.total_credits + excluded.total_credits,
                  updated_at = greatest(excluded.updated_at, par_traffic_enforcement_balance.updated_at)
               returning account_id, total_debits, total_credits, updated_at"""
          .as[(AccountId, Long, Long, CantonTimestamp)]
          .map(_.headOption)
      case _: Profile.H2 =>
        for {
          _ <- sqlu"""
              merge into par_traffic_enforcement_balance t
              using (values ($accountId, $sequenceNb, $debitUpdate, $creditUpdate, $timestamp)) as v(account_id, event_sequence_nb, debit_amt, credit_amt, updated_at)
              on t.account_id = v.account_id
              when matched then
                update set
                  event_sequence_nb = v.event_sequence_nb,
                  total_debits = t.total_debits + v.debit_amt,
                  total_credits = t.total_credits + v.credit_amt,
                  updated_at = greatest(v.updated_at, t.updated_at)
              when not matched then
                insert (account_id, event_sequence_nb, total_debits, total_credits, updated_at)
                values (v.account_id, v.event_sequence_nb, v.debit_amt, v.credit_amt, v.updated_at)
      """
          balance <- getRawBalanceDBIO(accountId)
        } yield balance
    }

    // Reject the update if applying the delta would drive either running total below zero.
    // Failing here rolls back the enclosing transaction (including the inserted event).
    updatedBalance.flatMap {
      case Some((account, totalDebits, totalCredits, updatedAt)) =>
        (NonNegativeLong.create(totalDebits), NonNegativeLong.create(totalCredits)) match {
          case (Right(debits), Right(credits)) =>
            DBIO.successful(Some(AccountState(account, debits, credits, updatedAt)))
          case _ =>
            DBIO.failed(new TrafficUpdateOutOfBoundException)
        }
      case None => DBIO.successful(None)
    }
  }

  private[db] def persistDeltaDBIO(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      trafficDelta: TrafficDelta,
      timestamp: CantonTimestamp,
  ): DBIOAction[Option[AccountState], NoStream, Effect.Write & Effect.Read] =
    for {
      // Start by inserting the event in the event table
      // Deduplicate using the event id, this will tell us whether we need to update the balance table
      insertedEvent <- insertEventDBIO(
        accountId,
        eventId,
        eventSource,
        trafficDelta,
        timestamp,
      )
      balanceUpdate <-
        // If the event was inserted (not a duplicate), update the balance table
        insertedEvent match {
          case Some(sequenceNb) =>
            updateBalanceDBIO(
              accountId,
              trafficDelta,
              sequenceNb,
              timestamp,
            )
          case None => getBalanceDBIO(accountId)
        }
    } yield balanceUpdate

  override def getBalance(accountId: AccountId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, Option[AccountState]] = EitherT(
    getBalanceAttempt(accountId).transformWithHandledAborted {
      case Success(balance) => FutureUnlessShutdown.pure(Right(balance))
      case Failure(exception) =>
        FutureUnlessShutdown.pure(
          Left(TeaDbTrafficStore.classifyFailure(exception))
        )
    }
  )

  /** Only one attempt per call (`maxRetries = 0`); retries are left to the RPC client, not the
    * store.
    */
  private def getBalanceAttempt(
      accountId: AccountId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[AccountState]] =
    storage.queryWithStatementTimeout(
      getBalanceDBIO(accountId),
      databaseQueryTimeout,
      "get_traffic_balance",
      maxRetries = 0,
    )

  private def getBalanceDBIO(accountId: AccountId) =
    sql"select account_id, total_debits, total_credits, updated_at from par_traffic_enforcement_balance where account_id = $accountId"
      .as[AccountState]
      .map(_.headOption)

  /** Reads the balance totals as raw Longs so a transiently negative running total can be detected
    * instead of failing the NonNegativeLong conversion.
    */
  private def getRawBalanceDBIO(accountId: AccountId) =
    sql"select account_id, total_debits, total_credits, updated_at from par_traffic_enforcement_balance where account_id = $accountId"
      .as[(AccountId, Long, Long, CantonTimestamp)]
      .map(_.headOption)

  override def getEvents(accountId: AccountId, fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[DeltaEvent]] =
    storage.query(
      sql"select amount, timestamp, event_type, event_source from par_traffic_enforcement_event where account_id = $accountId and timestamp >= $fromInclusive order by timestamp"
        .as[DeltaEvent],
      "get_events",
    )
}

object TeaDbTrafficStore {

  // Rolls back the transaction; `rejection` below converts it to a TrafficUpdateOutOfBound error.
  private[db] final class TrafficUpdateOutOfBoundException
      extends RuntimeException
      with NoStackTrace

  /** The delta cannot be applied: a negative running total detected in Scala, or a numeric overflow
    * reported differently by H2 and Postgres.
    */
  private[db] def rejection(accountId: AccountId, trafficDelta: TrafficDelta)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): PartialFunction[Throwable, TrafficEnforcementError] = {
    case ex if isOutOfBound(ex) =>
      TrafficEnforcementErrors.TrafficUpdateOutOfBound
        .Reject(accountId.toString, trafficDelta.toString)
  }

  private def isOutOfBound(ex: Throwable): Boolean = ex match {
    case _: TrafficUpdateOutOfBoundException => true
    // Both H2 and Postgres report a bigint overflow as the SQL standard numeric_value_out_of_range.
    case sqlEx: SQLException => sqlEx.getSQLState == PSQLState.NUMERIC_VALUE_OUT_OF_RANGE.getState
    case _ => false
  }

  private[db] def classifyPersistFailure(accountId: AccountId, trafficDelta: TrafficDelta)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Throwable => TrafficEnforcementError = {
    val reject = rejection(accountId, trafficDelta)
    ex => reject.applyOrElse(ex, classifyFailure(_: Throwable))
  }

  /** `query_canceled` is how PostgreSQL reports our own `statement_timeout` firing, which the
    * generic policy would otherwise call fatal.
    */
  private[db] def classifyFailure(exception: Throwable)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): TrafficEnforcementError =
    exception match {
      case sqlEx: SQLException if sqlEx.getSQLState == PSQLState.QUERY_CANCELED.getState =>
        TrafficEnforcementErrors.TransientFailure.Reject(exception)
      case _ =>
        DbExceptionRetryPolicy.determineExceptionErrorKind(
          exception,
          errorLoggingContext.logger,
        )(errorLoggingContext.traceContext) match {
          case _: ErrorKind.TransientErrorKind =>
            TrafficEnforcementErrors.TransientFailure.Reject(exception)
          case _ =>
            TrafficEnforcementErrors.FatalFailure.Reject(exception)
        }
    }
}
