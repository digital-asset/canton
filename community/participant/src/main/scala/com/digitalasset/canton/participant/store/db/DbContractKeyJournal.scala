// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Monad
import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.functorFilter._
import cats.syntax.list._
import cats.syntax.traverse._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.participant.store.ContractKeyJournal.ContractKeyJournalError
import com.digitalasset.canton.participant.store.db.DbContractKeyJournal.DbContractKeyJournalError
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfGlobalKey, LfHash}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.DbPrunableByTimeDomain
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import io.functionmeta.functionFullName

import scala.concurrent.{ExecutionContext, Future}

class DbContractKeyJournal(
    override protected[this] val storage: DbStorage,
    override val domainId: IndexedDomain,
    maxContractIdSqlInListSize: PositiveNumeric[Int],
    override protected[this] val loggerFactory: NamedLoggerFactory,
)(override protected[this] implicit val ec: ExecutionContext)
    extends ContractKeyJournal
    with NamedLogging
    with DbPrunableByTimeDomain[ContractKeyJournalError] {

  import ContractKeyJournal._
  import DbStorage.Implicits._
  import storage.api._

  override protected val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("contract-key-journal")

  override protected[this] def pruning_status_table: String = "contract_key_pruning"

  override def fetchStates(
      keys: Iterable[LfGlobalKey]
  )(implicit traceContext: TraceContext): Future[Map[LfGlobalKey, ContractKeyState]] =
    if (keys.isEmpty) Future.successful(Map.empty)
    else {
      processingTime.metric.event {
        import DbStorage.Implicits.BuilderChain._
        keys.toList.toNel match {
          case None => Future.successful(Map.empty)
          case Some(keysNel) =>
            val inClauses = DbStorage.toInClauses_(
              "contract_key_hash",
              keysNel,
              maxContractIdSqlInListSize,
            )
            val queries = inClauses.map { inClause =>
              val query =
                storage.profile match {
                  case _: DbStorage.Profile.Oracle =>
                    sql"""
              select contract_key_hash, status, ts, request_counter from
              (
                select contract_key_hash, status, ts, request_counter, 
                   ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc) as row_num
                 from contract_key_journal
                 where domain_id = $domainId and """ ++ inClause ++ sql"""
              ) ordered_changes
              where row_num = 1
              """
                  case _ =>
                    sql"""
              with ordered_changes(contract_key_hash, status, ts, request_counter, row_num) as (
                select contract_key_hash, status, ts, request_counter, 
                   ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc)
                 from contract_key_journal
                 where domain_id = $domainId and """ ++ inClause ++ sql"""
              )
              select contract_key_hash, status, ts, request_counter
              from ordered_changes
              where row_num = 1;
              """

                }
              query.as[(LfHash, ContractKeyState)]
            }
            for {
              foundKeysVector <- storage.sequentialQueryAndCombine(
                queries,
                functionFullName,
              )
            } yield {
              val foundKeys = foundKeysVector.toMap
              keys.to(LazyList).mapFilter(key => foundKeys.get(key.hash).map(key -> _)).toMap
            }
        }
      }
    }

  override def addKeyStateUpdates(updates: Map[LfGlobalKey, Status], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    processingTime.metric.eitherTEvent {
      import DbStorage.Implicits.BuilderChain._

      // Keep trying to insert the updates until all updates are in the DB or an exception occurs or we've found an inconsistency.
      val fut =
        Monad[Future].tailRecM[LazyList[LfGlobalKey], Either[ContractKeyJournalError, Unit]](
          updates.keySet.to(LazyList)
        ) { remainingKeys =>
          if (remainingKeys.isEmpty) { Future.successful(Either.right(Either.right(()))) }
          else {
            val values =
              storage.profile match {
                case _: DbStorage.Profile.Oracle =>
                  remainingKeys
                    .map(key =>
                      sql"select $domainId domain_id, $key contract_key_hash, ${checked(
                        updates(key)
                      )} status, ${toc.timestamp} ts, ${toc.rc} request_counter from dual"
                    )
                    .intercalate(sql" union all ")
                case _ =>
                  remainingKeys
                    .map(key =>
                      sql"""($domainId, $key, CAST(${checked(
                        updates(key)
                      )} as key_status), ${toc.timestamp}, ${toc.rc})"""
                    )
                    .intercalate(sql", ")
              }

            val updCount = remainingKeys.length
            val query = storage.profile match {
              case _: DbStorage.Profile.H2 =>
                sql"""
                  merge into contract_key_journal as journal
                  using (select * from (values """ ++ values ++
                  sql""") as upds(domain_id, contract_key_hash, status, ts, request_counter)) as source
                  on journal.domain_id = source.domain_id and
                     journal.contract_key_hash = source.contract_key_hash and
                     journal.ts = source.ts and
                     journal.request_counter = source.request_counter
                  when not matched then
                    insert (domain_id, contract_key_hash, status, ts, request_counter)
                     values (source.domain_id, source.contract_key_hash, source.status, source.ts, source.request_counter);
                  """
              case _: DbStorage.Profile.Postgres =>
                sql"""
                  insert into contract_key_journal(domain_id, contract_key_hash, status, ts, request_counter)
                  values """ ++ values ++
                  sql"""
                  on conflict (domain_id, contract_key_hash, ts, request_counter) do nothing;
                  """
              case _: DbStorage.Profile.Oracle =>
                sql"""
                      merge into contract_key_journal ckj USING (with UPDATES as(""" ++ values ++
                  sql""") select * from UPDATES) S on ( ckj.domain_id=S.domain_id and ckj.contract_key_hash=S.contract_key_hash and ckj.ts=S.ts and ckj.request_counter=S.request_counter)
                       when not matched then
                       insert (domain_id, contract_key_hash, status, ts, request_counter)
                       values (S.domain_id, S.contract_key_hash, S.status, S.ts, S.request_counter)
                     """
            }

            storage.update(query.asUpdate, functionFullName).flatMap { rowCount =>
              if (rowCount == updCount) Future.successful(Either.right(Either.right(())))
              else {
                val keysQ = remainingKeys.map(key => sql"$key").intercalate(sql", ")
                // We read all keys to be written rather than those keys mapped to a different value
                // so that we find out if some key is still missing in the DB.
                val keyStatesQ =
                  sql"""
                select contract_key_hash, status
                from contract_key_journal
                where domain_id = $domainId and contract_key_hash in (""" ++ keysQ ++
                    sql""") and ts = ${toc.timestamp} and request_counter = ${toc.rc}
                """
                storage.query(keyStatesQ.as[(LfHash, Status)], functionFullName).map {
                  keysWithStatus =>
                    val found = keysWithStatus.map { case (keyHash, status) =>
                      keyHash -> status
                    }.toMap
                    val wrongOrMissing = remainingKeys.traverse { key =>
                      found.get(key.hash) match {
                        case None => Either.right(Some(key))
                        case Some(status) =>
                          val newStatus = checked(updates(key))
                          Either.cond(
                            status == newStatus,
                            None,
                            InconsistentKeyAllocationStatus(key, toc, status, newStatus),
                          )
                      }
                    }
                    wrongOrMissing match {
                      case Left(wrong) => Either.right(Either.left(wrong))
                      case Right(missingO) =>
                        val missing = missingO.flattenOption
                        Either.cond(missing.isEmpty, Either.right(()), missing)
                    }
                }
              }
            }
          }
        }
      EitherTUtil.fromFuture(fut, DbContractKeyJournalError).subflatMap(Predef.identity)
    }

  override def doPrune(
      beforeAndIncluding: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, ContractKeyJournalError, Unit] =
    processingTime.metric.eitherTEvent {
      val query = storage.profile match {
        case _: DbStorage.Profile.H2 =>
          sqlu"""
          with ordered_changes(domain_id, contract_key_hash, status, ts, request_counter, row_num) as ( 
             select domain_id, contract_key_hash, status, ts, request_counter,
               ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc)
             from contract_key_journal
             where domain_id = $domainId and ts <= $beforeAndIncluding
          )
          delete from contract_key_journal
          where
            (domain_id, contract_key_hash, ts, request_counter) in (
              select domain_id, contract_key_hash, ts, request_counter
              from ordered_changes
              where ordered_changes.row_num > 1 or ordered_changes.status = ${ContractKeyJournal.Unassigned}
            );
          """
        case _: DbStorage.Profile.Postgres =>
          // If Unique Contract Keys become widely used, we may consider applying a partial-index based optimization
          // similar to how we prune the DbActiveContractStore. Contract keys are a bit more involved because we cannot
          // rely purely on the presence of "unassigned" entries (the DbActiveContractStore can depend on "deactivation"s
          // to narrow down pruning entries).
          sqlu"""
          with ordered_changes(domain_id, contract_key_hash, status, ts, request_counter, row_num) as (
            select domain_id, contract_key_hash, status, ts, request_counter,
               ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc)
             from contract_key_journal
             where domain_id = $domainId and ts <= $beforeAndIncluding
          ),
          latest_change(domain_id, contract_key_hash, ts, request_counter) as (
            select domain_id, contract_key_hash, ts, request_counter
            from ordered_changes
            where row_num = 1
          )
          delete from contract_key_journal as ckj
          using latest_change as lc
          where
            ckj.domain_id = lc.domain_id and
            ckj.contract_key_hash = lc.contract_key_hash and
            (ckj.ts, ckj.request_counter, ckj.status) <= (lc.ts, lc.request_counter, CAST(${ContractKeyJournal.Unassigned} as key_status));
          """
        case _: DbStorage.Profile.Oracle =>
          sqlu"""
          delete from contract_key_journal
          where
            (domain_id, contract_key_hash, ts, request_counter) in (
              select domain_id, contract_key_hash, ts, request_counter
              from (
                  select domain_id, contract_key_hash, status, ts, request_counter,
                   ROW_NUMBER() OVER (partition by domain_id, contract_key_hash order by ts desc, request_counter desc) as row_num
                 from contract_key_journal
                 where domain_id = $domainId and ts <= $beforeAndIncluding) ordered_changes
              where ordered_changes.row_num > 1 or ordered_changes.status = ${ContractKeyJournal.Unassigned}
            )
          """
      }
      EitherT.right(storage.update_(query, functionFullName))
    }

  override def deleteSince(
      inclusive: TimeOfChange
  )(implicit traceContext: TraceContext): EitherT[Future, ContractKeyJournalError, Unit] =
    processingTime.metric.eitherTEvent {
      EitherT.right(
        storage.update_(
          sqlu"""delete from contract_key_journal
               where domain_id = $domainId and ts > ${inclusive.timestamp} or (ts = ${inclusive.timestamp} and request_counter >= ${inclusive.rc})""",
          functionFullName,
        )
      )
    }

  override def countUpdates(key: LfGlobalKey)(implicit traceContext: TraceContext): Future[Int] =
    processingTime.metric.event {
      storage
        .query(
          sql"select count(*) from contract_key_journal where domain_id = $domainId and contract_key_hash = $key"
            .as[Int]
            .headOption,
          functionFullName,
        )
        .map(_.getOrElse(0))
    }

}

object DbContractKeyJournal {
  case class DbContractKeyJournalError(err: Throwable) extends ContractKeyJournalError {
    override def asThrowable: Throwable = err
  }
}
