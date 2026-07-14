// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.commitment.AcsDigestTrace
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
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import slick.jdbc.canton.SQLActionBuilder
import slick.jdbc.{PositionedParameters, SetParameter}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

import DbStorage.Implicits.BuilderChain.*

class DbAcsDigestJournal[K, V](
    override protected val storage: DbStorage,
    indexedSynchronizer: IndexedSynchronizer,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val timeouts: ProcessingTimeout,
    prettyKey: K => String,
    journalTable: JournalTable,
    createJournalImplicitsF: DbStorage => DbAcsDigestJournalImplicits[K, V],
    releaseProtocolVersion: ReleaseProtocolVersion,
)(implicit ec: ExecutionContext)
    extends AcsDigestJournal[K, V]
    with DbStore {

  private val journalImplicits = createJournalImplicitsF(storage)
  import journalImplicits.*
  import DbAcsDigestJournal.*
  import storage.api.*
  import journalTable.*

  implicit val setParameterAcsDigestTrace: SetParameter[AcsDigestTrace] =
    AcsDigestTrace.getVersionedSetParameter(releaseProtocolVersion.v)
  implicit val setParameterAcsDigestTraceO: SetParameter[Option[AcsDigestTrace]] =
    AcsDigestTrace.getVersionedSetParameterO(releaseProtocolVersion.v)

  private val synchronizerIdx = indexedSynchronizer.index

  private val digestColNamesInSelect = digestColumnNames.mkString(", ")

  override def deleteAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val deleteAction =
      sqlu"""
      delete from #$tableName
      where synchronizer_idx = $synchronizerIdx
        and change_offset > $fromExclusive
    """

    logger.trace(s"Deleting entries from $tableName after $fromExclusive...")

    storage.update_(
      action = deleteAction,
      operationName = functionFullName,
    )
  }

  override def deleteUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val pruneActionForNonNull = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        // H2 doesn't support 'with' on 'delete from' so we use 'exists' instead
        sqlu"""
        delete from #$tableName
        where synchronizer_idx = $synchronizerIdx
          and change_offset < $toExclusive
          and (#$keyColumnName, change_offset) in (
            select #$keyColumnName, replaces_offset
            from #$tableName
            where synchronizer_idx = $synchronizerIdx
              and change_offset <= $toExclusive
              and replaces_offset is not null
          )
        """

      case _: DbStorage.Profile.Postgres =>
        sqlu"""
        with replaced_entries as (
          select #$keyColumnName as key_id, replaces_offset
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and change_offset <= $toExclusive
            and replaces_offset is not null
        )
        delete from #$tableName
        where synchronizer_idx = $synchronizerIdx
          and (#$keyColumnName, change_offset) in (
            select key_id, replaces_offset from replaced_entries
          )
        """
    }

    val nullDigestCondition = digestColumnNames.map(col => s"$col is null").mkString(" and ")

    val pruneActionOnNull = sqlu"""
      delete from #$tableName
      where synchronizer_idx = $synchronizerIdx
        and change_offset < $toExclusive
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
        AcsDigestStore.AcsDigest(key, offset, timestamp, digest, tracedChanges),
        replacesOffset,
      ) = update
      pp >> indexedSynchronizer
      pp >> key
      pp >> offset
      pp >> timestamp
      pp >> digest
      pp >> tracedChanges
      pp >> replacesOffset
    }

    val digestColPlaceHolders = Seq.fill(digestColumnNames.length)("?").mkString(", ")

    val bulkQuery = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        s"""merge into $tableName (
             synchronizer_idx, $keyColumnName,
             change_offset, ts, $digestColNamesInSelect, trace_data, replaces_offset
           )
           key (synchronizer_idx, $keyColumnName, change_offset)
           values (?, ?, ?, ?, $digestColPlaceHolders, ?, ?)
         """

      case _: DbStorage.Profile.Postgres =>
        s"""insert into $tableName (
               synchronizer_idx, $keyColumnName,
               change_offset, ts, $digestColNamesInSelect, trace_data, replaces_offset             )
             values (?, ?, ?, ?, $digestColPlaceHolders, ?, ?)
             on conflict (synchronizer_idx, $keyColumnName, change_offset)
             do update set
               ${digestColumnNames.map(col => s"$col = excluded.$col").mkString(", ")},
               trace_data = excluded.trace_data,
               replaces_offset = excluded.replaces_offset
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

  override def lookup(key: K, toInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AcsDigestStore.AcsDigestUpdate[K, V]]] = {
    val lookupQuery =
      (sql"""
        select #$keyColumnName, change_offset, ts, #$digestColNamesInSelect, trace_data, replaces_offset from #$tableName
        where
        synchronizer_idx = $synchronizerIdx
        and #$keyColumnName = $key
        and change_offset <= $toInclusive
        order by synchronizer_idx, #$keyColumnName, change_offset desc
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

  override def bulkLookup(keys: immutable.Iterable[K], toInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[K, AcsDigestStore.AcsDigestUpdate[K, V]]] =
    NonEmpty.from(keys) match {
      case None => FutureUnlessShutdown.pure(Map.empty)
      case Some(nonEmptyIterable) =>
        // uses the ClassTag[K] - and SetParameters[Array[K]] implicits
        val keysArray = toKeysArray(keys)

        val bulkLookupQuery: SQLActionBuilder = storage.profile match {
          case _: DbStorage.Profile.H2 =>
            val inClause = DbStorage.toInClause(keyColumnName, nonEmptyIterable)
            sql"""
            select r.#$keyColumnName, r.change_offset, r.ts, r.#$digestColNamesInSelect, r.trace_data, r.replaces_offset
            from (
              select
                #$keyColumnName, change_offset, ts, #$digestColNamesInSelect, trace_data, replaces_offset,
                row_number() over (
                  partition by #$keyColumnName
                  order by change_offset desc
                ) as rn
              from #$tableName
              where """ ++ inClause ++ sql"""
                and synchronizer_idx = $synchronizerIdx
                and change_offset <= $toInclusive
            ) as r
            where r.rn = 1
            """

          case _: DbStorage.Profile.Postgres =>
            sql"""
            select k.target_key_id, j.change_offset, j.ts, j.#$digestColNamesInSelect, j.trace_data, j.replaces_offset
              from UNNEST($keysArray) as k(target_key_id)
              cross join lateral (
                 select change_offset, ts, #$digestColNamesInSelect, trace_data, replaces_offset
                 from #$tableName j
                 where j.synchronizer_idx = $synchronizerIdx
                   and j.#$keyColumnName = (k.target_key_id)::int
                   and j.change_offset <= $toInclusive
                 order by j.synchronizer_idx, j.#$keyColumnName, j.change_offset desc
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
    val (atInclusive, startKeyO) = tokenOrStart match {
      case Right(atInclusive) => (atInclusive, Option.empty[K])
      case Left(token) => (token.atInclusive, Some(token.lastKey))
    }

    val keyCondition = startKeyO match {
      case Some(startKeyVal) => sql"and #$keyColumnName > $startKeyVal"
      case None => sql""
    }

    // For forward lookup to check if we have more pages
    val fetchLimit = limit + 1

    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sql"""
          select r.#$keyColumnName, r.change_offset, r.ts, r.#$digestColNamesInSelect, r.trace_data
          from (
            select #$keyColumnName, change_offset, ts, #$digestColNamesInSelect, trace_data,
                   row_number() over (partition by #$keyColumnName order by change_offset desc) as rn
            from #$tableName
            where synchronizer_idx = $synchronizerIdx
              and change_offset <= $atInclusive
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
              and change_offset <= $atInclusive
              """ ++ keyCondition ++
          sql"""
            order by #$keyColumnName asc, change_offset desc
            limit 1
          )
          union all
          select (
            select j.#$keyColumnName
            from #$tableName j
            where j.synchronizer_idx = $synchronizerIdx
              and j.#$keyColumnName > kb.key_id
              and j.change_offset <= $atInclusive
            order by j.#$keyColumnName asc, j.change_offset desc
            limit 1
          )
          from key_batch kb
          -- recursive query's termination case
          where kb.key_id is not null
        )
        select j.#$keyColumnName, j.change_offset, j.ts, j.#$digestColNamesInSelect, j.trace_data
        from key_batch b
        cross join lateral (
          select #$keyColumnName, change_offset, ts, #$digestColNamesInSelect, trace_data
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and #$keyColumnName = b.key_id
            and change_offset <= $atInclusive
          order by change_offset desc
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
                s"Unexpected error in snapshot query in $tableName, to $atInclusive!"
              )
            )
          (limitNumOfAcsDigests, Right(SnapshotToken(atInclusive, lastQueriedKey)))
      }
  }

  override type ChangesBetweenPaginationToken = ChangesBetweenToken[K]

  override def changesBetween(
      tokenOrStart: Either[ChangesBetweenPaginationToken, AcsDigestStore.ChangesBetweenOffsetRange],
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

    val fromInclusive = range.fromInclusive
    val toExclusive = range.toExclusive

    val keyCondition = startKeyO match {
      case Some(startKeyVal) => sql"and #$keyColumnName > $startKeyVal"
      case None => sql""
    }

    // For forward lookup to check if we have more pages
    val fetchLimit = limit + 1

    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sql"""
        select r.#$keyColumnName, r.change_offset, r.ts, r.#$digestColNamesInSelect, r.trace_data
        from (
          select #$keyColumnName, change_offset, ts, #$digestColNamesInSelect, trace_data,
                 row_number() over (partition by #$keyColumnName order by change_offset desc) as rn
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and change_offset >= $fromInclusive
            and change_offset < $toExclusive
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
              and change_offset >= $fromInclusive
              and change_offset < $toExclusive
              """ ++ keyCondition ++ sql"""
            order by #$keyColumnName asc, change_offset desc
            limit 1
          )
          union all
          select (
            select j.#$keyColumnName
            from #$tableName j
            where j.synchronizer_idx = $synchronizerIdx
              and j.#$keyColumnName > kb.key_id
              and j.change_offset >= $fromInclusive
              and j.change_offset < $toExclusive
            order by j.#$keyColumnName asc, j.change_offset desc
            limit 1
          )
          from key_batch kb
          -- recursive query's termination case
          where kb.key_id is not null
        )
        select j.#$keyColumnName, j.change_offset, j.ts, j.#$digestColNamesInSelect, j.trace_data
        from key_batch b
        cross join lateral (
          select #$keyColumnName, change_offset, ts, #$digestColNamesInSelect, trace_data
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and #$keyColumnName = b.key_id
            and change_offset >= $fromInclusive
            and change_offset < $toExclusive
          order by change_offset desc
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

  override def checkReplacesInvariant(upToInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {

    val checkQuery =
      (sql"""
        select #$keyColumnName, change_offset, replaces_offset, prev_offset
        from (
          select
            #$keyColumnName,
            change_offset,
            replaces_offset,
            lag(change_offset, 1) over (partition by #$keyColumnName order by change_offset) as prev_offset
          from #$tableName
          where synchronizer_idx = $synchronizerIdx
            and change_offset <= $upToInclusive
        ) as lagged
        where
          (prev_offset is null and replaces_offset is not null and (replaces_offset >= change_offset))
          or
          (prev_offset is not null and (replaces_offset is null or replaces_offset <> prev_offset))
        order by change_offset
      """ ++ storage.limitSql(1))
        .as[(K, Offset, Option[Offset], Option[Offset])]
        .headOption

    storage.query(checkQuery, functionFullName).map {
      case Some((key, offset, Some(replacesOffset), _)) if replacesOffset >= offset =>
        ErrorUtil.invalidState(
          s"ReplacesOffset check error for Key ${prettyKey(key)} - " +
            s"We cannot have replacesOffset=$replacesOffset which is gte to change offset $offset"
        )
      case Some((key, offset, None, Some(precedentOffset))) =>
        ErrorUtil.invalidState(
          s"ReplacesOffset check error for key ${prettyKey(key)} at $offset - " +
            s"Replaces offset should point to $precedentOffset but it is empty!"
        )
      case Some(
            (key, offset, Some(currentReplacesOffset), Some(precedingOffset))
          ) if currentReplacesOffset != precedingOffset =>
        ErrorUtil.invalidState(
          s"ReplacesOffset check error for key ${prettyKey(key)} - " +
            s"We cannot have an entry with (offset=$offset, replacesOffset=$currentReplacesOffset) " +
            s"which is not pointing to the preceding offset=$precedingOffset"
        )
      case Some((key, offset, replacesOffset, precedentOffset)) =>
        ErrorUtil.invalidState(
          s"ReplacesOffset check error for key ${prettyKey(key)} - " +
            s"found a case which shouldn't exist for offset $offset, replacesOffset: $replacesOffset " +
            s"and precedentOffset $precedentOffset"
        )
      case None => () // All is good. No violation found!
    }
  }
}

object DbAcsDigestJournal {
  final case class SnapshotToken[K](atInclusive: Offset, lastKey: K)
  final case class ChangesBetweenToken[K](
      timeRange: AcsDigestStore.ChangesBetweenOffsetRange,
      lastKey: K,
  )
}
