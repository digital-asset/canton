// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Monad
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.participant.store.ContractKeyJournal.ContractKeyJournalError
import com.digitalasset.canton.participant.store.db.DbContractKeyJournal.DbContractKeyJournalError
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfGlobalKey, LfHash}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.DbPrunableByTimeDomain
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

class DbContractKeyJournal(
    override protected val storage: DbStorage,
    override val domainId: IndexedDomain,
    maxContractIdSqlInListSize: PositiveNumeric[Int],
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override protected[this] implicit val ec: ExecutionContext)
    extends ContractKeyJournal
    with DbStore
    with DbPrunableByTimeDomain {

  import ContractKeyJournal.*
  import DbStorage.Implicits.*
  import storage.api.*

  override protected val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("contract-key-journal")

  override protected[this] def pruning_status_table: String = "contract_key_pruning"

  override def fetchStates(
      keys: Iterable[LfGlobalKey]
  )(implicit traceContext: TraceContext): Future[Map[LfGlobalKey, ContractKeyState]] =
    if (keys.isEmpty) Future.successful(Map.empty)
    else {
      processingTime.event {
        import DbStorage.Implicits.BuilderChain.*
        NonEmpty.from(keys.toSeq) match {
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

  override def addKeyStateUpdates(updates: Map[LfGlobalKey, (Status, TimeOfChange)])(implicit
      traceContext: TraceContext
  ): EitherT[Future, ContractKeyJournalError, Unit] =
    processingTime.eitherTEvent {
      import DbStorage.Implicits.BuilderChain.*

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
                    .map(key => {
                      val (status, toc) = updates(key)
                      sql"select $domainId domain_id, $key contract_key_hash, ${checked(
                          status
                        )} status, ${toc.timestamp} ts, ${toc.rc} request_counter from dual"
                    })
                    .intercalate(sql" union all ")
                case _ =>
                  remainingKeys
                    .map(key => {
                      val (status, toc) = updates(key)
                      sql"""($domainId, $key, CAST(${checked(
                          status
                        )} as key_status), ${toc.timestamp}, ${toc.rc})"""
                    })
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
                val (tss, rcs) =
                  remainingKeys.collect(updates).map(_._2).map(toc => (toc.timestamp, toc.rc)).unzip
                val tsQ = tss.map(ts => sql"$ts").intercalate(sql", ")
                val rcQ = rcs.map(rc => sql"$rc").intercalate(sql", ")
                // We read all keys to be written rather than those keys mapped to a different value
                // so that we find out if some key is still missing in the DB.

                val keyStatesQ =
                  sql"""
                select contract_key_hash, status
                from contract_key_journal
                where domain_id = $domainId and contract_key_hash in (""" ++ keysQ ++
                    sql""") and ts in (""" ++ tsQ ++ sql""") and request_counter in (""" ++ rcQ ++ sql""")
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
                          val (newStatus, toc) = checked(updates(key))
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
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
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
          val deleteAssignedSql = sqlu"""
            delete from contract_key_journal ckj1
            where domain_id = $domainId
            and ts <=  $beforeAndIncluding
            and status = CAST(${ContractKeyJournal.Assigned} as key_status)
            and exists (
                select 1 from contract_key_journal ckj2
                where ckj2.domain_id = ckj1.domain_id
                and ckj2.contract_key_hash = ckj1.contract_key_hash
                and (ckj2.ts, ckj2.request_counter) > (ckj1.ts, ckj1.request_counter)
                and ckj2.ts <= $beforeAndIncluding
            );
          """

          val deleteUnassignedSql = sqlu"""
            delete from contract_key_journal ckj1
            where domain_id = $domainId
            and ts <=  $beforeAndIncluding
            and status = CAST(${ContractKeyJournal.Unassigned} as key_status)
            and not exists (
              select 1 from contract_key_journal ckj2
              where ckj2.domain_id = ckj1.domain_id
              and ckj2.contract_key_hash = ckj1.contract_key_hash
              and (ckj2.ts, ckj2.request_counter) < (ckj1.ts, ckj1.request_counter)
              and ckj2.status = CAST(${ContractKeyJournal.Assigned} as key_status)
            );
          """

          deleteAssignedSql.andThen(deleteUnassignedSql)

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
      storage.update_(query, functionFullName)
    }

  override def deleteSince(
      inclusive: TimeOfChange
  )(implicit traceContext: TraceContext): EitherT[Future, ContractKeyJournalError, Unit] =
    processingTime.eitherTEvent {
      EitherT.right(
        storage.update_(
          sqlu"""delete from contract_key_journal
               where domain_id = $domainId and ts > ${inclusive.timestamp} or (ts = ${inclusive.timestamp} and request_counter >= ${inclusive.rc})""",
          functionFullName,
        )
      )
    }

  override def countUpdates(key: LfGlobalKey)(implicit traceContext: TraceContext): Future[Int] =
    processingTime.event {
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
  final case class DbContractKeyJournalError(err: Throwable) extends ContractKeyJournalError {
    override def asThrowable: Throwable = err
  }
}
