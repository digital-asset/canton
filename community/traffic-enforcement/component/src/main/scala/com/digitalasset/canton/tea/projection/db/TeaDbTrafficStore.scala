// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import cats.data.EitherT
import cats.syntax.applicativeError.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, DbStorage, DbStore}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficEnforcementError
import com.digitalasset.canton.tea.projection.db.TeaDbTrafficStore.TrafficUpdateOutOfBoundException
import com.digitalasset.canton.tea.projection.{
  AccountDelta,
  AccountId,
  AccountState,
  CompletionAccounting,
  CompletionAccountingPolicy,
  CompletionDecision,
  CorrelationAccount,
  CorrelationKey,
  DeltaEvent,
  EventId,
  EventSource,
  ProjectionEvent,
  PruneCounts,
  SubmissionCorrelation,
  TeaTrafficStore,
  TrafficDelta,
  UnchargedCompletion,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.ErrorKind
import org.postgresql.util.PSQLState
import slick.jdbc.GetResult

import java.sql.SQLException
import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success}

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

  override def insertCorrelation(correlation: SubmissionCorrelation)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, Unit] =
    // A retry after a lost commit ack would report a false CorrelationConflict.
    singleAttemptUpdate(insertCorrelationDBIO(correlation), "insert_correlation").flatMap {
      insertedRows =>
        EitherT.cond[FutureUnlessShutdown](insertedRows == 1, (), correlation.key.conflictError)
    }

  private def insertCorrelationDBIO(correlation: SubmissionCorrelation) = {
    val changeIdHash = correlation.key.changeIdHash.toHexString
    val submissionId = correlation.key.submissionId
    val accountId = correlation.account.toOption
    val insertedAt = correlation.insertedAt
    storage.profile match {
      case _: Profile.Postgres =>
        sqlu"""insert into par_traffic_enforcement_correlations
               (change_id_hash, submission_id, account_id, timestamp)
               values ($changeIdHash, $submissionId, $accountId, $insertedAt)
               on conflict (change_id_hash, submission_id) do nothing"""
      case _: Profile.H2 =>
        sqlu"""merge into par_traffic_enforcement_correlations t
               using (values ($changeIdHash, $submissionId, $accountId, $insertedAt)) as v(change_id_hash, submission_id, account_id, timestamp)
               on t.change_id_hash = v.change_id_hash and t.submission_id = v.submission_id
               when not matched then
                 insert (change_id_hash, submission_id, account_id, timestamp)
                 values (v.change_id_hash, v.submission_id, v.account_id, v.timestamp)"""
    }
  }

  override def lookupCorrelation(key: CorrelationKey)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, Option[SubmissionCorrelation]] =
    storage
      .queryWithStatementTimeout(
        lookupCorrelationDBIO(key),
        databaseQueryTimeout,
        "lookup_correlation",
        maxRetries = 0,
      )
      .attemptT
      .leftMap(TeaDbTrafficStore.classifyFailure)

  private def lookupCorrelationDBIO(key: CorrelationKey) = {
    val changeIdHash = key.changeIdHash.toHexString
    sql"""select account_id, timestamp from par_traffic_enforcement_correlations
          where change_id_hash = $changeIdHash and submission_id = ${key.submissionId}"""
      .as[(Option[AccountId], CantonTimestamp)]
      .map(_.headOption.map { case (accountId, insertedAt) =>
        SubmissionCorrelation(key, CorrelationAccount.fromOption(accountId), insertedAt)
      })
  }

  // Match the timestamp so we don't delete a newer row with the same key.
  private[db] def deleteCorrelationDBIO(correlation: SubmissionCorrelation) = {
    val changeIdHash = correlation.key.changeIdHash.toHexString
    sqlu"""delete from par_traffic_enforcement_correlations
            where change_id_hash = $changeIdHash and submission_id = ${correlation.key.submissionId}
              and timestamp = ${correlation.insertedAt}"""
  }

  /** Returns `None` without writing if the completion is at or below the stored last applied
    * offset. Not transactional on its own, so the projection calls it in the transaction that
    * stores the offset.
    *
    * We need the offset check because the projection might replay an already charged completion
    * after a restart if the projection offset wasn't saved yet. The original correlation is gone,
    * so if the client retried the command in the meantime, the replay could use the retry's
    * correlation instead. That would leave the retry's completion uncharged, giving us both a
    * charged and an uncharged result even though the command was charged correctly.
    */
  private[db] def applyCompletionDBIO(
      event: ProjectionEvent.Completion,
      projectionId: String,
      eventSource: EventSource,
      policy: CompletionAccountingPolicy,
  )(implicit
      traceContext: TraceContext
  ): DBIOAction[Option[CompletionDecision], NoStream, Effect.Write & Effect.Read] = {
    val stateKey = TeaDbTrafficStore.LastCompletionOffsetKey
    for {
      lastApplied <- readStateDBIO(stateKey)(_.toLongOption)
      result <-
        if (lastApplied.exists(_ >= event.offset)) DBIO.successful(None)
        else
          for {
            correlation <- lookupCorrelationDBIO(event.record.correlationKey)
            decision = CompletionAccounting.decide(event, correlation, policy)
            _ <- decision match {
              case CompletionDecision.Debit(delta) =>
                persistProjectedDeltaDBIO(
                  delta,
                  event.debitEventId(projectionId),
                  eventSource,
                  event.timestamp,
                )
              case CompletionDecision.RecordUncharged(completion) =>
                insertUnchargedDBIO(completion).map(_ => ())
              case CompletionDecision.NoEvent => DBIO.unit
            }
            _ <- DBIO.sequenceOption(correlation.map(deleteCorrelationDBIO))
            _ <- upsertStateDBIO(stateKey, event.offset.toString)
          } yield Some(decision)
    } yield result
  }

  private def readStateDBIO[A](name: String)(
      parse: String => Option[A]
  ): DBIOAction[Option[A], NoStream, Effect.Read] =
    sql"select state_value from par_traffic_enforcement_state where name = $name"
      .as[String](GetResult(_.nextString()))
      .headOption
      .flatMap {
        case None => DBIO.successful(None)
        case Some(value) =>
          parse(value) match {
            case Some(parsed) => DBIO.successful(Some(parsed))
            case None =>
              DBIO.failed(
                new DbDeserializationException(
                  s"Invalid value '$value' for traffic enforcement state '$name'"
                )
              )
          }
      }

  private def upsertStateDBIO(name: String, value: String) =
    storage.profile match {
      case _: Profile.Postgres =>
        sqlu"""insert into par_traffic_enforcement_state (name, state_value)
               values ($name, $value)
               on conflict (name) do update set state_value = excluded.state_value"""
      case _: Profile.H2 =>
        sqlu"""merge into par_traffic_enforcement_state t
               using (values ($name, $value)) as v(name, state_value)
               on t.name = v.name
               when matched then
                 update set state_value = v.state_value
               when not matched then
                 insert (name, state_value) values (v.name, v.state_value)"""
    }

  private def insertUnchargedDBIO(completion: UnchargedCompletion) = {
    val changeIdHash = completion.changeIdHash.toHexString
    storage.profile match {
      case _: Profile.Postgres =>
        sqlu"""insert into par_traffic_enforcement_uncharged_completions
               (completion_offset, change_id_hash, submission_id, paid_traffic_cost, reason, act_as_category, timestamp)
               values (${completion.offset}, $changeIdHash, ${completion.submissionId}, ${completion.paidTrafficCost},
                 ${completion.reason}, ${completion.actAsCategory}, ${completion.timestamp})
               on conflict (completion_offset) do nothing"""
      case _: Profile.H2 =>
        sqlu"""merge into par_traffic_enforcement_uncharged_completions t
               using (values (${completion.offset}, $changeIdHash, ${completion.submissionId}, ${completion.paidTrafficCost},
                 ${completion.reason}, ${completion.actAsCategory}, ${completion.timestamp}))
                 as v(completion_offset, change_id_hash, submission_id, paid_traffic_cost, reason, act_as_category, timestamp)
               on t.completion_offset = v.completion_offset
               when not matched then
                 insert (completion_offset, change_id_hash, submission_id, paid_traffic_cost, reason, act_as_category, timestamp)
                 values (v.completion_offset, v.change_id_hash, v.submission_id, v.paid_traffic_cost, v.reason, v.act_as_category, v.timestamp)"""
    }
  }

  override def getUnchargedCompletions(fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[UnchargedCompletion]] =
    storage.query(
      sql"""select completion_offset, change_id_hash, submission_id, paid_traffic_cost, reason, act_as_category, timestamp
            from par_traffic_enforcement_uncharged_completions
            where timestamp >= $fromInclusive order by completion_offset"""
        .as[UnchargedCompletion],
      "get_uncharged_completions",
    )

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
      .attemptT
      .leftMap(classify)
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

  /** The projection's write, which classifies failures the same way as `persistTrafficDelta`. */
  private[db] def persistProjectedDeltaDBIO(
      accountDelta: AccountDelta,
      eventId: EventId,
      eventSource: EventSource,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): DBIOAction[Unit, NoStream, Effect.Write & Effect.Read] = {
    val classify =
      TeaDbTrafficStore.classifyPersistFailure(accountDelta.account, accountDelta.delta)
    persistDeltaDBIO(
      accountDelta.account,
      eventId,
      eventSource,
      accountDelta.delta,
      timestamp,
    ).asTry
      .flatMap {
        case Success(_) => DBIO.unit
        // TODO(#35342): decide the failure policy for a rejected delta. Failing stalls the
        //  projection, skipping the event drops the delta and needs a different transaction shape,
        //  since the event row inserted above has to roll back while the offset bump commits.
        case Failure(ex) => DBIO.failed(classify(ex).asGrpcError)
      }
  }

  override def getBalance(accountId: AccountId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, Option[AccountState]] =
    getBalanceAttempt(accountId).attemptT.leftMap(TeaDbTrafficStore.classifyFailure)

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

  override def prune(beforeInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, PruneCounts] = {
    val prune = for {
      events <- sqlu"delete from par_traffic_enforcement_event where timestamp <= $beforeInclusive"
      correlations <-
        sqlu"delete from par_traffic_enforcement_correlations where timestamp <= $beforeInclusive"
      unchargedCompletions <-
        sqlu"delete from par_traffic_enforcement_uncharged_completions where timestamp <= $beforeInclusive"
    } yield PruneCounts(
      events = events,
      correlations = correlations,
      unchargedCompletions = unchargedCompletions,
    )

    singleAttemptUpdate(prune.transactionally, "prune")
  }

  /** Let the caller handle retries, like the other request-path operations. */
  private def singleAttemptUpdate[A](
      action: DBIOAction[A, NoStream, Effect.Write & Effect.Transactional],
      operationName: String,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TrafficEnforcementError, A] =
    storage
      .update(action, operationName, maxRetries = 0)
      .attemptT
      .leftMap(TeaDbTrafficStore.classifyFailure)

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

  private val LastCompletionOffsetKey: String = "debit_projection.last_completion_offset"

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
        .Reject(accountId.toString, trafficDelta.value, trafficDelta.eventType.toString)
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
