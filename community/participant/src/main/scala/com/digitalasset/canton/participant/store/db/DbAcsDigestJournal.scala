// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.event.RecordTime.*
import com.digitalasset.canton.participant.store.data.AcsDigestJournalData.JournalTable
import com.digitalasset.canton.participant.store.data.DbAcsDigestJournalImplicits
import com.digitalasset.canton.participant.store.{
  AcsDigestJournal,
  AcsDigestStore,
  PaginationTokenDone,
}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{PositionedParameters, SetParameter}

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.math.Ordering.Implicits.*

import DbStorage.Implicits.BuilderChain.*
import SetParameter.*

class DbAcsDigestJournal[K, V](
    override protected val storage: DbStorage,
    override val indexedSynchronizer: IndexedSynchronizer,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val timeouts: ProcessingTimeout,
    prettyKey: K => String,
    journalTable: JournalTable,
    createJournalImplicitsF: DbStorage => DbAcsDigestJournalImplicits[K, V],
)(implicit ec: ExecutionContext)
    extends AcsDigestJournal[K, V]
    with DbStore {

  private val journalImplicits = createJournalImplicitsF(storage)
  import journalImplicits.*
  import DbAcsDigestJournal.*
  import storage.api.*
  import journalTable.*

  private val synchronizerIdx = indexedSynchronizer.index

  private val digestColNamesInSelect = digestColumnNames.mkString(", ")

  override def deleteAfter(fromExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val RecordTime(fromTs, fromTieBreaker) = fromExclusive

    val deleteAction =
      sqlu"""
      delete from #$tableName
      where synchronizer_idx = $synchronizerIdx
        and (ts > $fromTs or (ts = $fromTs and tie_breaker > $fromTieBreaker))
    """

    logger.trace(s"Deleting entries from $tableName after $fromExclusive...")

    storage.update_(
      action = deleteAction,
      operationName = functionFullName,
    )
  }

  override def deleteUpTo(toExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {

    val RecordTime(toTs, toTieBreaker) = toExclusive

    val pruneActionForNonNull = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        // H2 doesn't support 'with' on 'delete from' so we use 'exists' instead
        sqlu"""
        delete from #$tableName old
        where old.synchronizer_idx = $synchronizerIdx
          and (old.ts < $toTs or (old.ts = $toTs and old.tie_breaker < $toTieBreaker))
          and exists (
            select 1
            from #$tableName newer
            where newer.synchronizer_idx = $synchronizerIdx
              and newer.#$keyColumnName = old.#$keyColumnName
              and newer.replaces_ts = old.ts
              and newer.replaces_tie_breaker = old.tie_breaker
              and (newer.ts < $toTs or (newer.ts = $toTs and newer.tie_breaker <= $toTieBreaker))
          )
        """

      case _: DbStorage.Profile.Postgres =>
        sqlu"""
        with replaced_entries as (
          select #$keyColumnName as key_id, replaces_ts, replaces_tie_breaker
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and (ts < $toTs or (ts = $toTs and tie_breaker <= $toTieBreaker))
        )
        delete from #$tableName
        where synchronizer_idx = $synchronizerIdx
          and (#$keyColumnName, ts, tie_breaker) in (
            select key_id, replaces_ts, replaces_tie_breaker from replaced_entries
          )
        """
    }

    val nullDigestCondition = digestColumnNames.map(col => s"$col is null").mkString(" and ")

    val pruneActionOnNull = sqlu"""
      delete from #$tableName
      where synchronizer_idx = $synchronizerIdx
        and (ts < $toTs or (ts = $toTs and tie_breaker < $toTieBreaker))
        and #$nullDigestCondition
    """

    logger.trace(s"Pruning historical ACS journal entries in $tableName up to $toExclusive...")

    for {
      nonNullRowsDeleted <- storage.update(pruneActionForNonNull, functionFullName)
      nullRowsDeleted <- storage.update(pruneActionOnNull, functionFullName)
    } yield logger.trace(
      s"Prune (deleteUpTo), deleted ${nonNullRowsDeleted + nullRowsDeleted} number of ACS journal digest in $tableName!"
    )
  }

  override def upsertDigestUpdates(
      digests: immutable.Iterable[AcsDigestStore.AcsDigestUpdate[K, V]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def setParamsPerUpdate(
        pp: PositionedParameters
    )(update: AcsDigestStore.AcsDigestUpdate[K, V]): Unit = {
      val AcsDigestStore.AcsDigestUpdate(
        AcsDigestStore.AcsDigest(key, digest, recordTime),
        replacesRecordTime,
      ) = update
      pp >> indexedSynchronizer
      pp >> key
      pp >> recordTime.timestamp
      pp >> recordTime.tieBreaker
      pp >> digest
      pp >> replacesRecordTime.map(_.timestamp)
      pp >> replacesRecordTime.map(_.tieBreaker)
    }

    val digestColPlaceHolders = Seq.fill(digestColumnNames.length)("?").mkString(", ")

    val bulkQuery = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        s"""merge into $tableName (
             synchronizer_idx, $keyColumnName,
             ts, tie_breaker, $digestColNamesInSelect, replaces_ts, replaces_tie_breaker
           )
           key (synchronizer_idx, $keyColumnName, ts, tie_breaker)
           values (?, ?, ?, ?, $digestColPlaceHolders, ?, ?)
         """

      case _: DbStorage.Profile.Postgres =>
        s"""insert into $tableName (
               synchronizer_idx, $keyColumnName,
               ts, tie_breaker, $digestColNamesInSelect, replaces_ts, replaces_tie_breaker
             )
             values (?, ?, ?, ?, $digestColPlaceHolders, ?, ?)
             on conflict (synchronizer_idx, $keyColumnName, ts, tie_breaker)
             do update set
               ${digestColumnNames.map(col => s"$col = excluded.$col").mkString(", ")},
               replaces_ts = excluded.replaces_ts,
               replaces_tie_breaker = excluded.replaces_tie_breaker
         """
    }

    logger.trace(s"Batch upserting ACS journal updates into $tableName...")

    val bulkUpsert =
      DbStorage.bulkOperation(bulkQuery, digests, storage.profile)(setParamsPerUpdate)

    storage
      .queryAndUpdate(
        action = bulkUpsert,
        operationName = functionFullName,
      )
      .map { rowsAltered =>
        logger.trace(
          s"Batch upserted ${rowsAltered.sum} number of ACS journal digests into $tableName!"
        )
      }
  }

  override def lookup(key: K, toInclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AcsDigestStore.AcsDigestUpdate[K, V]]] = {
    val RecordTime(toTs, toTieBreaker) = toInclusive

    val lookupQuery =
      (sql"""
        select #$keyColumnName, ts, tie_breaker, #$digestColNamesInSelect, replaces_ts, replaces_tie_breaker from #$tableName
        where
        synchronizer_idx = $synchronizerIdx
        and #$keyColumnName = $key
        and (ts < $toTs or (ts = $toTs and tie_breaker <= $toTieBreaker))
        order by synchronizer_idx, #$keyColumnName, ts desc, tie_breaker desc
        """ ++ storage.limitSql(1))
        .as[AcsDigestStore.AcsDigestUpdate[K, V]]
        .headOption

    logger.trace(
      s"ACS running digest lookup for key ${prettyKey(key)} to inclusive $toInclusive in $tableName..."
    )

    storage.query(
      action = lookupQuery,
      operationName = functionFullName,
    )
  }

  override def bulkLookup(keys: immutable.Iterable[K], toInclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[K, AcsDigestStore.AcsDigestUpdate[K, V]]] =
    NonEmpty.from(keys) match {
      case None => FutureUnlessShutdown.pure(Map.empty)
      case Some(nonEmptyIterable) =>
        // uses the ClassTag[K] - and SetParameters[Array[K]] implicits
        val keysArray = toKeysArray(keys)
        val RecordTime(toTs, toTieBreaker) = toInclusive

        val bulkLookupQuery: SQLActionBuilder = storage.profile match {
          case _: DbStorage.Profile.H2 =>
            val inClause = DbStorage.toInClause(keyColumnName, nonEmptyIterable)
            sql"""
            select r.#$keyColumnName, r.ts, r.tie_breaker, r.#$digestColNamesInSelect, r.replaces_ts, r.replaces_tie_breaker
            from (
              select
                #$keyColumnName, ts, tie_breaker, #$digestColNamesInSelect, replaces_ts, replaces_tie_breaker,
                ROW_NUMBER() OVER (
                  PARTITION BY #$keyColumnName
                  ORDER BY ts DESC, tie_breaker DESC
                ) as rn
              from #$tableName
              where """ ++ inClause ++ sql"""
                and synchronizer_idx = $synchronizerIdx
                and (ts < $toTs or (ts = $toTs and tie_breaker <= $toTieBreaker))
            ) as r
            where r.rn = 1
            """

          case _: DbStorage.Profile.Postgres =>
            sql"""
            select k.target_key_id, j.ts, j.tie_breaker, j.#$digestColNamesInSelect, j.replaces_ts, j.replaces_tie_breaker
              from UNNEST($keysArray) as k(target_key_id)
              cross join lateral (
                 select ts, tie_breaker, #$digestColNamesInSelect, replaces_ts, replaces_tie_breaker
                 from #$tableName j
                 where j.synchronizer_idx = $synchronizerIdx
                   and j.#$keyColumnName = (k.target_key_id)::int
                   and (j.ts < $toTs or (j.ts = $toTs and j.tie_breaker <= $toTieBreaker))
                 order by j.synchronizer_idx, j.#$keyColumnName, j.ts desc, j.tie_breaker desc
                 limit 1
              ) as j
            """
        }

        logger.trace(
          s"ACS running digest bulk lookup for ${keys.size} keys to inclusive $toInclusive in $tableName..."
        )

        storage
          .query(
            action = bulkLookupQuery.as[AcsDigestStore.AcsDigestUpdate[K, V]],
            operationName = functionFullName,
          )
          .map { resultSequence =>
            resultSequence.iterator.map { update =>
              update.digestUpdate.key -> update
            }.toMap
          }
    }

  override type SnapshotPaginationToken = SnapshotToken[K]

  override def snapshot(
      tokenOrStart: Either[SnapshotPaginationToken, AtInclusive],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    (
        immutable.Iterable[AcsDigestStore.AcsDigest[K, V]],
        Either[PaginationTokenDone, SnapshotPaginationToken],
    )
  ] = {
    val (targetTime, startKeyO) = tokenOrStart match {
      case Right(atInclusive) => (atInclusive, Option.empty[K])
      case Left(token) => (token.atInclusive, Some(token.lastKey))
    }

    val RecordTime(toTs, toTieBreaker) = targetTime

    val keyCondition = startKeyO match {
      case Some(startKeyVal) => sql"and #$keyColumnName > $startKeyVal"
      case None => sql""
    }

    // For forward lookup to check if we have more pages
    val fetchLimit = limit + 1

    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sql"""
          select r.#$keyColumnName, r.ts, r.tie_breaker, r.#$digestColNamesInSelect
          from (
            select #$keyColumnName, ts, tie_breaker, #$digestColNamesInSelect,
                   ROW_NUMBER() OVER (PARTITION BY #$keyColumnName ORDER BY ts DESC, tie_breaker DESC) as rn
            from #$tableName
            where synchronizer_idx = $synchronizerIdx
              and (ts < $toTs or (ts = $toTs and tie_breaker <= $toTieBreaker))
              """ ++ keyCondition ++
          sql"""
          ) as r
          where r.rn = 1
          order by r.#$keyColumnName asc
        """ ++ storage.limitSql(fetchLimit)

      case _: DbStorage.Profile.Postgres =>
        sql"""
        with recursive key_batch as (
          (
            select #$keyColumnName as key_id
            from #$tableName
            where synchronizer_idx = $synchronizerIdx
              and (ts < $toTs or (ts = $toTs and tie_breaker <= $toTieBreaker))
              """ ++ keyCondition ++
          sql"""
            order by #$keyColumnName asc, ts desc, tie_breaker desc
            limit 1
          )
          union all
          select (
            select j.#$keyColumnName
            from #$tableName j
            where j.synchronizer_idx = $synchronizerIdx
              and j.#$keyColumnName > kb.key_id
              and (j.ts < $toTs or (j.ts = $toTs and j.tie_breaker <= $toTieBreaker))
            order by j.#$keyColumnName asc, j.ts desc, j.tie_breaker desc
            limit 1
          )
          from key_batch kb
          -- recursive query's termination case
          where kb.key_id is not null
        )
        select j.#$keyColumnName, j.ts, j.tie_breaker, j.#$digestColNamesInSelect
        from key_batch b
        cross join lateral (
          select #$keyColumnName, ts, tie_breaker, #$digestColNamesInSelect
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and #$keyColumnName = b.key_id
            and (ts < $toTs or (ts = $toTs and tie_breaker <= $toTieBreaker))
          order by ts desc, tie_breaker desc
          limit 1
        ) as j
      """ ++ storage.limitSql(fetchLimit)
    }

    storage
      .query(
        action = query.as[AcsDigestStore.AcsDigest[K, V]],
        operationName = functionFullName,
      )
      .map {
        case acsDigests if acsDigests.sizeIs <= limit => (acsDigests, Left(PaginationTokenDone))
        case acsDigests =>
          val limitNumOfAcsDigests = acsDigests.dropRight(1) // O(log(N)) on Vector
          val lastQueriedKey = limitNumOfAcsDigests.lastOption
            .map(_.key)
            .getOrElse(
              throw new IllegalStateException(
                s"Unexpected error in snapshot query in $tableName, to $targetTime!"
              )
            )
          (limitNumOfAcsDigests, Right(SnapshotToken(targetTime, lastQueriedKey)))
      }
  }

  override type ChangesBetweenPaginationToken = ChangesBetweenToken[K]

  override def changesBetween(
      tokenOrStart: Either[ChangesBetweenPaginationToken, AcsDigestStore.ChangesBetweenTimeRange],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    (
        immutable.Iterable[AcsDigestStore.AcsDigest[K, V]],
        Either[PaginationTokenDone, ChangesBetweenPaginationToken],
    )
  ] = {
    val (range, startKeyO) = tokenOrStart match {
      case Right(timeRange) => (timeRange, Option.empty[K])
      case Left(token) => (token.timeRange, Some(token.lastKey))
    }

    val RecordTime(fromTs, fromTieBreaker) = range.fromInclusive
    val RecordTime(toTs, toTieBreaker) = range.toExclusive

    val keyCondition = startKeyO match {
      case Some(startKeyVal) => sql"and #$keyColumnName > $startKeyVal"
      case None => sql""
    }

    // For forward lookup to check if we have more pages
    val fetchLimit = limit + 1

    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sql"""
        select r.#$keyColumnName, r.ts, r.tie_breaker, r.#$digestColNamesInSelect
        from (
          select #$keyColumnName, ts, tie_breaker, #$digestColNamesInSelect,
                 ROW_NUMBER() OVER (PARTITION BY #$keyColumnName ORDER BY ts DESC, tie_breaker DESC) as rn
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and (ts > $fromTs or (ts = $fromTs and tie_breaker >= $fromTieBreaker))
            and (ts < $toTs or (ts = $toTs and tie_breaker < $toTieBreaker))
            """ ++ keyCondition ++ sql"""
        ) as r
        where r.rn = 1
        order by r.#$keyColumnName asc
      """ ++ storage.limitSql(fetchLimit)

      case _: DbStorage.Profile.Postgres =>
        sql"""
        with recursive key_batch as (
          (
            select #$keyColumnName as key_id
            from #$tableName
            where synchronizer_idx = $synchronizerIdx
              and (ts > $fromTs or (ts = $fromTs and tie_breaker >= $fromTieBreaker))
              and (ts < $toTs or (ts = $toTs and tie_breaker < $toTieBreaker))
              """ ++ keyCondition ++ sql"""
            order by #$keyColumnName asc, ts desc, tie_breaker desc
            limit 1
          )
          union all
          select (
            select j.#$keyColumnName
            from #$tableName j
            where j.synchronizer_idx = $synchronizerIdx
              and j.#$keyColumnName > kb.key_id
              and (j.ts > $fromTs or (j.ts = $fromTs and j.tie_breaker >= $fromTieBreaker))
              and (j.ts < $toTs or (j.ts = $toTs and j.tie_breaker < $toTieBreaker))
            order by j.#$keyColumnName asc, j.ts desc, j.tie_breaker desc
            limit 1
          )
          from key_batch kb
          -- recursive query's termination case
          where kb.key_id is not null
        )
        select j.#$keyColumnName, j.ts, j.tie_breaker, j.#$digestColNamesInSelect
        from key_batch b
        cross join lateral (
          select #$keyColumnName, ts, tie_breaker, #$digestColNamesInSelect
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and #$keyColumnName = b.key_id
            and (ts > $fromTs or (ts = $fromTs and tie_breaker >= $fromTieBreaker))
            and (ts < $toTs or (ts = $toTs and tie_breaker < $toTieBreaker))
          order by ts desc, tie_breaker desc
          limit 1
        ) as j
      """ ++ storage.limitSql(fetchLimit)
    }

    storage
      .query(
        action = query.as[AcsDigestStore.AcsDigest[K, V]],
        operationName = functionFullName,
      )
      .map {
        case acsDigests if acsDigests.sizeIs <= limit => (acsDigests, Left(PaginationTokenDone))
        case acsDigests =>
          val limitNumOfAcsDigests = acsDigests.dropRight(1) // O(log(N)) on Vector
          val lastQueriedKey = limitNumOfAcsDigests.lastOption
            .map(_.key)
            .getOrElse(
              throw new IllegalStateException(
                s"Unexpected error in changesBetween query, in $tableName between ${range.fromInclusive} and ${range.toExclusive}!"
              )
            )
          (limitNumOfAcsDigests, Right(ChangesBetweenToken(range, lastQueriedKey)))
      }
  }

  override def checkReplacesInvariant(upToInclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {

    val RecordTime(toTs, toTieBreaker) = upToInclusive

    val checkQuery =
      (sql"""
        select #$keyColumnName, ts, tie_breaker, replaces_ts, replaces_tie_breaker, prev_ts, prev_tie_breaker
        from (
          select
            #$keyColumnName,
            ts,
            tie_breaker,
            replaces_ts,
            replaces_tie_breaker,
            LAG(ts, 1) OVER (PARTITION BY #$keyColumnName ORDER BY ts, tie_breaker) as prev_ts,
            LAG(tie_breaker, 1) OVER (PARTITION BY #$keyColumnName ORDER BY ts, tie_breaker) as prev_tie_breaker
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and (ts < $toTs or (ts = $toTs and tie_breaker <= $toTieBreaker))
        ) as lagged
        where
          (prev_ts is null and replaces_ts is not null and (replaces_ts > ts or (replaces_ts = ts and replaces_tie_breaker >= tie_breaker)))
          or
          (prev_ts is not null and (replaces_ts is null or replaces_ts <> prev_ts or replaces_tie_breaker <> prev_tie_breaker))
        order by ts, tie_breaker
      """ ++ storage.limitSql(1))
        .as[(K, RecordTime, Option[RecordTime], Option[RecordTime])]
        .headOption

    storage.query(checkQuery, functionFullName).map {
      case Some((key, recordTime, Some(replacesRecordTime), _))
          if replacesRecordTime >= recordTime =>
        ErrorUtil.invalidState(
          s"ReplacesRecordTime check error for Key ${prettyKey(key)} - " +
            s"We cannot have replacesTime=$replacesRecordTime which is gte to recordTime $recordTime"
        )
      case Some((key, recordTime, None, Some(precedentRecordTime))) =>
        ErrorUtil.invalidState(
          s"ReplacesRecordTime check error for key ${prettyKey(key)} at $recordTime - " +
            s"Replaces time should point to $precedentRecordTime but it is empty!"
        )
      case Some(
            (key, currentRecordTime, Some(currentReplacesRecordTime), Some(precedingRecordTime))
          ) if currentReplacesRecordTime != precedingRecordTime =>
        ErrorUtil.invalidState(
          s"ReplacesRecordTime check error for key ${prettyKey(key)} - " +
            s"We cannot have an entry with (recordTime=$currentRecordTime, replacesTime=$currentReplacesRecordTime) " +
            s"which is not pointing to the preceding recordTime=$precedingRecordTime"
        )
      case Some((key, recordTime, replacesRecordTime, precedentRecordTime)) =>
        ErrorUtil.invalidState(
          s"ReplacesRecordTime check error for key ${prettyKey(key)} - " +
            s"found a case which shouldn't exist for recordTime $recordTime, replacesRecordTime: $replacesRecordTime " +
            s"and precedentRecordTime $precedentRecordTime"
        )
      case None => () // All is good. No violation found!
    }
  }
}

object DbAcsDigestJournal {
  final case class SnapshotToken[K](atInclusive: RecordTime, lastKey: K)
  final case class ChangesBetweenToken[K](
      timeRange: AcsDigestStore.ChangesBetweenTimeRange,
      lastKey: K,
  )
}
