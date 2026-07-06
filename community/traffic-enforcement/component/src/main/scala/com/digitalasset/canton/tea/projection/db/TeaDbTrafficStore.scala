// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import cats.data.OptionT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorage, DbStore}
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

import scala.concurrent.ExecutionContext

import AccountState.*

/** Store for DB operations on traffic persistence.
  */
class TeaDbTrafficStore(
    override val storage: DbStorage,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext)
    extends DbStore
    with TeaTrafficStore {
  import storage.api.*

  implicit val rowsAlteredAccountState: DbStorage.RowsAltered[Option[AccountState]] = _.isDefined

  override def persistDelta(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      eventType: EventType,
      delta: Long,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, AccountState] = OptionT(
    storage.queryAndUpdate(
      persistDeltaDBIO(accountId, eventId, eventSource, eventType, delta, timestamp),
      "persist traffic delta",
    )
  )

  private def insertEventDBIO(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      eventType: EventType,
      delta: Long,
      timestamp: CantonTimestamp,
  ): DBIOAction[Option[Long], NoStream, Effect.Write & Effect.Read] =
    storage.profile match {
      case _: Profile.Postgres =>
        for {
          insertedRows <- sql"""insert into par_traffic_enforcement_event
                 (account_id, event_id, event_source, event_type, amount, timestamp) values ($accountId, $eventId, $eventSource, $eventType, $delta, $timestamp)
                 on conflict (event_source, event_id) do nothing
                 returning sequence_nb""".as[Long]
          singleEvent <- insertedRows.toList match {
            case Nil => DBIO.successful(None)
            case singleton :: Nil => DBIO.successful(Some(singleton))
            case moreThanOne =>
              DBIO.failed(
                new RuntimeException("Inserted more than one row in the traffic event table")
              )
          }
        } yield singleEvent
      // H2 doesn't handle on conflict do nothing so use the merge into syntax and then fetch the event to retrieve
      // the generated sequencer_nb
      case _: Profile.H2 =>
        for {
          insertedRows <- sqlu"""
            merge into par_traffic_enforcement_event t
            using (values ($accountId, $eventId, $eventSource, $eventType, $delta, $timestamp)) as v(account_id, event_id, event_source, event_type, amount, timestamp)
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
                    where event_id = $eventId
                  """.as[Long].map(_.headOption)
            } else
              DBIO.failed(
                new RuntimeException("Inserted more than one row in the traffic event table")
              )
        } yield event
    }

  private def updateBalanceDBIO(
      accountId: AccountId,
      eventType: EventType,
      debitAmount: Long,
      creditAmount: Long,
      sequenceNb: Long,
      timestamp: CantonTimestamp,
  ) =
    storage.profile match {
      // We update the balance by trying an insert
      // then on conflict we update the total debit / credit values by adding the inserted delta with the existing one.
      // Timestamp is updated as greatest of existing + inserted
      case _: Profile.Postgres =>
        sql"""insert into par_traffic_enforcement_balance
               (account_id, event_sequence_nb, event_type, total_debits, total_credits, updated_at) values($accountId, $sequenceNb, $eventType, $debitAmount, $creditAmount, $timestamp)
               on conflict (account_id, event_type) do update set
                  event_sequence_nb = excluded.event_sequence_nb,
                  total_debits = par_traffic_enforcement_balance.total_debits + excluded.total_debits,
                  total_credits = par_traffic_enforcement_balance.total_credits + excluded.total_credits,
                  updated_at = greatest(excluded.updated_at, par_traffic_enforcement_balance.updated_at)
               returning account_id, total_debits, total_credits, updated_at"""
          .as[AccountState]
          .map(_.headOption)
      case _: Profile.H2 =>
        for {
          _ <- sqlu"""
              merge into par_traffic_enforcement_balance t
              using (values ($accountId, $sequenceNb, $eventType, $debitAmount, $creditAmount, $timestamp)) as v(account_id, event_sequence_nb, event_type, debit_amt, credit_amt, updated_at)
              on t.account_id = v.account_id and t.event_type = v.event_type
              when matched then
                update set
                  event_sequence_nb = v.event_sequence_nb,
                  total_debits = t.total_debits + v.debit_amt,
                  total_credits = t.total_credits + v.credit_amt,
                  updated_at = greatest(v.updated_at, t.updated_at)
              when not matched then
                insert (account_id, event_sequence_nb, event_type, total_debits, total_credits, updated_at)
                values (v.account_id, v.event_sequence_nb, v.event_type, v.debit_amt, v.credit_amt, v.updated_at)
      """
          balance <- getBalanceDBIO(accountId)
        } yield balance
    }

  private[db] def persistDeltaDBIO(
      accountId: AccountId,
      eventId: EventId,
      eventSource: EventSource,
      eventType: EventType,
      delta: Long,
      timestamp: CantonTimestamp,
  ): DBIOAction[Option[AccountState], NoStream, Effect.Write & Effect.Read] = {
    val debitAmount = if (delta < 0) -delta else 0L
    val creditAmount = if (delta > 0) delta else 0L

    for {
      // Start by inserting the event in the event table
      // Deduplicate using the event id, this will tell us whether we need to update the balance table
      insertedEvent <- insertEventDBIO(accountId, eventId, eventSource, eventType, delta, timestamp)
      balanceUpdate <-
        // If the event was inserted (not a duplicate), update the balance table
        insertedEvent match {
          case Some(sequenceNb) =>
            updateBalanceDBIO(
              accountId,
              eventType,
              debitAmount,
              creditAmount,
              sequenceNb,
              timestamp,
            )
          case None => getBalanceDBIO(accountId)
        }
    } yield balanceUpdate
  }

  def getBalance(accountId: AccountId)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, AccountState] = storage.querySingle(
    getBalanceDBIO(accountId),
    "get_traffic_balance",
  )

  private def getBalanceDBIO(accountId: AccountId) =
    sql"select account_id, total_debits, total_credits, updated_at from par_traffic_enforcement_balance where account_id = $accountId"
      .as[AccountState]
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
