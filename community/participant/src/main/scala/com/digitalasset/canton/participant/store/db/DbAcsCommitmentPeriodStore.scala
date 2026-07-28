// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Eval
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.{
  CommitmentMatchPeriod,
  MatchedCommitmentMatchPeriod,
  MatchingWatermark,
  MismatchedCommitmentMatchPeriod,
  MismatchedOrUnexpectedCommitmentMatchPeriod,
  OutstandingCommitmentMatchPeriod,
}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  HashedDigest,
  InternedParticipantId,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.resource.DbStorage.RowsAltered
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.{DbDeserializationException, DbPrunableByTimeSynchronizer}
import com.digitalasset.canton.store.{IndexedString, IndexedSynchronizer}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, HexString}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, PositionedParameters, SetParameter, SimpleJdbcAction}

import java.sql.ResultSet
import scala.collection.immutable
import scala.concurrent.ExecutionContext

class DbAcsCommitmentPeriodStore(
    override protected val storage: DbStorage,
    override protected[this] val indexedSynchronizer: IndexedSynchronizer,
    stringInterningEval: Eval[StringInterning],
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    enableConsistencyChecks: Boolean,
)(override implicit protected val ec: ExecutionContext)
    extends AcsCommitmentPeriodStore
    with DbStore
    with DbPrunableByTimeSynchronizer[IndexedSynchronizer]
    with NamedLogging {

  import DbAcsCommitmentPeriodStore.*
  import storage.api.*
  import storage.converters.*

  override protected[this] def pruning_status_table: String & Singleton =
    "par_acs_commitment_period_pruning"
  override protected[this] def partitionColumn: String & Singleton = "synchronizer_idx"
  override protected[this] implicit def setParameterIndexedSynchronizer
      : SetParameter[IndexedSynchronizer] = IndexedString.setParameterIndexedString

  override protected def stringInterning: StringInterning = stringInterningEval.value

  override def lookupOutstanding(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[OutstandingCommitmentMatchPeriod]] =
    lookupInTable(
      periods,
      outstandingTable,
      hashedDigestColumn,
      unitNotAColumn,
      None,
    )

  override def lookupMismatchedOrUnexpected(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod]] =
    lookupInTable(periods, mismatchTable, hashedDigestOptionColumn, offsetColumn, None)

  override def lookupMatched(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MatchedCommitmentMatchPeriod]] =
    lookupInTable(periods, matchTable, unitNotAColumn, offsetColumn, None)

  override def lookupMismatchedByHash(
      periods: immutable.Iterable[(InternedParticipantId, HashedDigest, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MismatchedCommitmentMatchPeriod]] = {
    val plainPeriods = periods.map { case (participant, _, period) => (participant, period) }
    val expectedHashes = periods.map { case (_, hash, _) => hash }
    lookupInTable(
      plainPeriods,
      mismatchTable,
      hashedDigestSomeColumn,
      offsetColumn,
      Some(expectedHashes),
    )
  }

  private def lookupInTable[Digest, Off](
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)],
      tableName: String & Singleton,
      hashedDigestColumn: OptionalColumn[Digest],
      offsetColumn: OptionalColumn[Off],
      hashes: Option[immutable.Iterable[HashedDigest]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[CommitmentMatchPeriod[Digest, Off]]] = {
    if (enableConsistencyChecks)
      hashes.foreach { hs =>
        ErrorUtil.requireArgument(
          hs.sizeCompare(periods) == 0,
          s"periods and hashes must have the same size, but got ${periods.size} vs. ${hs.size}",
        )
      }

    if (periods.isEmpty) FutureUnlessShutdown.pure(immutable.Iterable.empty)
    else {
      val participants = toIntArrayLiteral(periods.map { case (participant, _) => participant })
      val fromExclusives = toLongArrayLiteral(periods.map { case (_, period) =>
        period.fromExclusive.toMicros
      })
      val toInclusives = toLongArrayLiteral(periods.map { case (_, period) =>
        period.toInclusive.toMicros
      })

      val hashesLiteral = hashes.fold("")(hs => ", " + toByteaArrayLiteral(hs))
      val hashesColumn = if (hashes.isEmpty) "" else ", expected_hashed_digest"
      val hashesFilter =
        if (hashes.isEmpty) ""
        else "and stored.expected_hashed_digest = k.expected_hashed_digest"

      val indexedSynchronizerLiteral = s"${indexedSynchronizer.index}::int"
      def selectColumnOverlap(column: OptionalColumn[?]): String =
        column.withName(name => s", overlap.$name as $name")
      def selectColumnStored(column: OptionalColumn[?]): String =
        column.withName(name => s", stored.$name")

      // Use a plain JDBC query because Postgres' query planner sometimes struggles with
      // cross join lateral queries on UNNESTed arrays in a prepared statement
      // as it cannot judge the size of the array and thus defaults to a sequential scan
      val plainQuery =
        storage.profile match {
          case _: DbStorage.Profile.Postgres =>
            s"""
              select
                k.participant_id as participant_id,
                overlap.from_exclusive as from_exclusive,
                overlap.to_inclusive as to_inclusive
                ${selectColumnOverlap(hashedDigestColumn)}
                ${selectColumnOverlap(offsetColumn)}
              from
                UNNEST($participants, $fromExclusives, $toInclusives $hashesLiteral)
                  as k(participant_id, from_exclusive, to_inclusive $hashesColumn)
                cross join lateral (
                (
                  -- Branch 1: Intervals that end within the target window
                  select
                    stored.from_exclusive,
                    stored.to_inclusive
                    ${selectColumnStored(hashedDigestColumn)}
                    ${selectColumnStored(offsetColumn)}
                  from $tableName stored
                  where stored.synchronizer_idx = $indexedSynchronizerLiteral
                    and stored.participant_id = k.participant_id
                    $hashesFilter
                    and stored.to_inclusive > k.from_exclusive
                    and stored.to_inclusive <= k.to_inclusive
                  order by stored.to_inclusive
                ) union all (
                  -- Branch 2: The single interval that crosses the upper bound
                  select
                    stored.from_exclusive,
                    stored.to_inclusive
                    ${selectColumnStored(hashedDigestColumn)}
                    ${selectColumnStored(offsetColumn)}
                  from $tableName stored
                  where stored.synchronizer_idx = $indexedSynchronizerLiteral
                    and stored.participant_id = k.participant_id
                    $hashesFilter
                    and stored.from_exclusive < k.to_inclusive
                    and stored.to_inclusive > k.to_inclusive
                  order by stored.to_inclusive
                  limit 1
                )
              ) as overlap
              """
          case _: DbStorage.Profile.H2 =>
            s"""
            with k as (
              select *
              from UNNEST($participants, $fromExclusives, $toInclusives $hashesLiteral)
                as unnested_t(participant_id, from_exclusive, to_inclusive $hashesColumn)
            )
            select
              overlap.participant_id as participant_id,
              overlap.from_exclusive as from_exclusive,
              overlap.to_inclusive as to_inclusive
              ${selectColumnOverlap(hashedDigestColumn)}
              ${selectColumnOverlap(offsetColumn)}
            from (
              -- Branch 1: Intervals that end within the target window
              select
                k.participant_id,
                stored.from_exclusive,
                stored.to_inclusive
                ${selectColumnStored(hashedDigestColumn)}
                ${selectColumnStored(offsetColumn)}
              from k
                inner join $tableName stored
                on stored.synchronizer_idx = $indexedSynchronizerLiteral
                  and stored.participant_id = k.participant_id
                  $hashesFilter
                  and stored.to_inclusive > k.from_exclusive
                  and stored.to_inclusive <= k.to_inclusive
            union all
              -- Branch 2: The single interval that crosses the upper bound
              select
                k.participant_id,
                stored.from_exclusive,
                stored.to_inclusive
                ${selectColumnStored(hashedDigestColumn)}
                ${selectColumnStored(offsetColumn)}
              from k
                inner join $tableName stored
                on stored.synchronizer_idx = $indexedSynchronizerLiteral
                  and stored.participant_id = k.participant_id
                  $hashesFilter
                  and stored.to_inclusive = (
                    select min(s2.to_inclusive)
                    from $tableName s2
                    where s2.synchronizer_idx = $indexedSynchronizerLiteral
                      and s2.participant_id = k.participant_id
                      $hashesFilter
                      and s2.from_exclusive < k.to_inclusive
                      and s2.to_inclusive > k.to_inclusive
                  )
          ) as overlap"""
        }
      def parse(result: ResultSet): CommitmentMatchPeriod[Digest, Off] = {
        val participantId = result.getInt("participant_id")
        val fromExclusive = CantonTimestamp.assertFromLong(result.getLong("from_exclusive"))
        val toInclusive = CantonTimestamp.assertFromLong(result.getLong("to_inclusive"))
        val hashedDigest = hashedDigestColumn.parseFrom(result)
        val offset = offsetColumn.parseFrom(result)
        CommitmentMatchPeriod(participantId, fromExclusive, toInclusive, hashedDigest, offset)
      }

      val action = SimpleJdbcAction { connection =>
        QueryStrategy.plainJdbcQuery(plainQuery)(parse)(connection.connection)
      }
      implicit val rowsAltered: RowsAltered[Vector[CommitmentMatchPeriod[Digest, Off]]] =
        new RowsAltered[Vector[CommitmentMatchPeriod[Digest, Off]]] {
          override def apply(a: Vector[CommitmentMatchPeriod[Digest, Off]]): Boolean = false
        }
      storage.queryAndUpdate(action, functionFullName)
    }
  }

  override def watermarks()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[AcsCommitmentPeriodStore.MatchingWatermark] = {
    val query =
      sql"select watermark_reconciliation, watermark_affirmation, watermark_matching from par_acs_commitment_period_watermark where synchronizer_idx = $indexedSynchronizer"
        .as[MatchingWatermark]
        .headOption
        .map(_.getOrElse(MatchingWatermark.initial))
    storage.query(query, functionFullName)
  }

  override def increaseInsertionWatermark(watermark: CantonTimestamp, affirmationOnly: Boolean)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = maybeCheckInvariant {
    // When `affirmationOnly` = true, reconciliation doesn't advance.
    // Use `MinValue` as a safe fallback if there has not been a row previously.
    val reconciliationWatermark = if (affirmationOnly) CantonTimestamp.MinValue else watermark
    val upsert = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
          insert into par_acs_commitment_period_watermark (synchronizer_idx, watermark_reconciliation, watermark_affirmation, watermark_matching)
          values ($indexedSynchronizer, $reconciliationWatermark, $watermark, null)
          on conflict (synchronizer_idx) do update
            set watermark_reconciliation =
                  (case when $affirmationOnly then par_acs_commitment_period_watermark.watermark_reconciliation
                        else greatest(par_acs_commitment_period_watermark.watermark_reconciliation, excluded.watermark_reconciliation)
                   end),
                watermark_affirmation = greatest(par_acs_commitment_period_watermark.watermark_affirmation, excluded.watermark_affirmation)
          """
      case _: DbStorage.Profile.H2 =>
        sqlu"""
          merge into par_acs_commitment_period_watermark
          using values ($indexedSynchronizer, $reconciliationWatermark, $watermark, null)
            as excluded (synchronizer_idx, watermark_reconciliation, watermark_affirmation, watermark_matching)
          on (par_acs_commitment_period_watermark.synchronizer_idx = excluded.synchronizer_idx)
          when matched then
            update set
                watermark_reconciliation =
                  (case when $affirmationOnly then par_acs_commitment_period_watermark.watermark_reconciliation
                        else greatest(par_acs_commitment_period_watermark.watermark_reconciliation, excluded.watermark_reconciliation)
                   end),
                watermark_affirmation = greatest(par_acs_commitment_period_watermark.watermark_affirmation, excluded.watermark_affirmation)
          when not matched then
            insert (synchronizer_idx, watermark_reconciliation, watermark_affirmation, watermark_matching)
            values (excluded.synchronizer_idx, excluded.watermark_reconciliation, excluded.watermark_affirmation, excluded.watermark_matching)
        """
    }
    storage.update_(upsert, functionFullName)
  }

  override def increaseMatcherWatermark(offset: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    // Timestamp to use for tick watermarks when there has not been any row previously for the synchronizer.
    val defaultTickWatermark = CantonTimestamp.MinValue
    val upsert = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
          insert into par_acs_commitment_period_watermark (synchronizer_idx, watermark_reconciliation, watermark_affirmation, watermark_matching)
          values ($indexedSynchronizer, $defaultTickWatermark, $defaultTickWatermark, $offset)
          on conflict (synchronizer_idx) do update
            set watermark_matching = greatest(par_acs_commitment_period_watermark.watermark_matching, excluded.watermark_matching)
        """
      case _: DbStorage.Profile.H2 =>
        sqlu"""
          merge into par_acs_commitment_period_watermark
          using values ($indexedSynchronizer, $defaultTickWatermark, $defaultTickWatermark, $offset)
            as excluded (synchronizer_idx, watermark_reconciliation, watermark_affirmation, watermark_matching)
          on (par_acs_commitment_period_watermark.synchronizer_idx = excluded.synchronizer_idx)
          when matched then
            update set
                watermark_matching = greatest(par_acs_commitment_period_watermark.watermark_matching, excluded.watermark_matching)
          when not matched then
            insert (synchronizer_idx, watermark_reconciliation, watermark_affirmation, watermark_matching)
            values (excluded.synchronizer_idx, excluded.watermark_reconciliation, excluded.watermark_affirmation, excluded.watermark_matching)
        """
    }
    storage.update_(upsert, functionFullName)
  }

  override def markOutstanding(digests: immutable.Iterable[OutstandingCommitmentMatchPeriod])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    if (digests.isEmpty) FutureUnlessShutdown.unit
    else
      maybeCheckInvariant {
        val participantsArray = digests.map(_.participant).toArray
        val fromExclusivesArray = digests.map(_.fromExclusive).toArray
        val toInclusivesArray = digests.map(_.toInclusive).toArray
        val hashedDigestsArray = digests.map(_.hashedDigest.toByteArray).toArray
        val query = sqlu"""
            insert into par_acs_commitment_period_outstanding (synchronizer_idx, participant_id, from_exclusive, to_inclusive, expected_hashed_digest)
            (
              select
                 $indexedSynchronizer as synchronizer_idx,
                 k.participant_id as participant_id,
                 greatest(k.from_exclusive, watermark.watermark_reconciliation) as from_exclusive,
                 k.to_inclusive as to_inclusive,
                 k.expected_hashed_digest as expected_hashed_digest
              from
                 UNNEST($participantsArray, $fromExclusivesArray, $toInclusivesArray, $hashedDigestsArray)
                   as k(participant_id, from_exclusive, to_inclusive, expected_hashed_digest)
                 cross join (
                   select
                     coalesce(wm.watermark_reconciliation, ${CantonTimestamp.MinValue}) as watermark_reconciliation,
                     coalesce(wm.watermark_affirmation, ${CantonTimestamp.MinValue}) as watermark_affirmation
                   from
                     -- ensure that a row with default values is returned even if the watermark table is empty for the synchronizer
                     (select 1 as dummy)
                     left join par_acs_commitment_period_watermark wm
                     on wm.synchronizer_idx = $indexedSynchronizer
                 ) as watermark
              where k.to_inclusive > watermark.watermark_affirmation
            )
            on conflict do nothing
          """
        storage.update_(query, functionFullName)
      }

  override def persistMatchingOutcome(
      deleteOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      deleteMismatched: immutable.Iterable[MismatchedCommitmentMatchPeriod],
      insertOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      insertMismatchedOrUnexpected: immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod],
      insertMatched: immutable.Iterable[MatchedCommitmentMatchPeriod],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    maybeCheckInvariant {
      for {
        _ <-
          if (enableConsistencyChecks) {
            for {
              _ <- ensurePresentForDeletion(
                outstandingTable,
                deleteOutstanding,
                hashedDigestColumn,
                unitNotAColumn,
                "outstanding",
              )
              _ <- ensurePresentForDeletion(
                mismatchTable,
                deleteMismatched,
                hashedDigestSomeColumn,
                offsetColumn,
                "mismatched",
              )
            } yield {
              checkPersistMatchingOutcomeArgumentConsistency(
                deleteOutstanding,
                deleteMismatched,
                insertOutstanding,
                insertMismatchedOrUnexpected,
                insertMatched,
              )
            }
          } else FutureUnlessShutdown.unit
        updates = DBIO
          .seq(
            deleteFromTableQuery(deleteOutstanding, outstandingTable),
            deleteFromTableQuery(deleteMismatched, mismatchTable),
            insertIntoTableQuery(
              insertOutstanding,
              outstandingTable,
              hashedDigestColumn,
              unitNotAColumn,
            ),
            insertIntoTableQuery(
              insertMismatchedOrUnexpected,
              mismatchTable,
              hashedDigestOptionColumn,
              offsetColumn,
            ),
            insertIntoTableQuery(
              insertMatched,
              matchTable,
              unitNotAColumn,
              offsetColumn,
            ),
          )
          .transactionally
        result <- storage.queryAndUpdate(updates, functionFullName)
      } yield result
    }

  private def ensurePresentForDeletion[Digest, Off](
      tableName: String & Singleton,
      periods: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]],
      hashedDigestColumn: OptionalColumn[Digest],
      offsetColumn: OptionalColumn[Off],
      kind: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (periods.isEmpty) FutureUnlessShutdown.unit
    else {
      // We retrieve the periods and check that they are equal at the Scala level,
      // instead of checking inside the DB whether they are the same.
      // This ensures that we are not prone to serialization idiosyncrasies
      // that could render Scala-equal object unequal at the DB level.
      val participants = periods.map(_.participant).toArray
      val toInclusives = periods.map(_.toInclusive).toArray
      val hashedDigestSelect = hashedDigestColumn.withName(name => s", $name")
      val offsetSelect = offsetColumn.withName(name => s", $name")
      implicit val getResultDigest: GetResult[Digest] = hashedDigestColumn.getResult
      implicit val getResultOff: GetResult[Off] = offsetColumn.getResult
      val query =
        sql"""
        select
          participant_id,
          from_exclusive,
          to_inclusive
          #$hashedDigestSelect
          #$offsetSelect
        from #$tableName
        where synchronizer_idx = $indexedSynchronizer
          and (participant_id, to_inclusive) in (
            select participant_id, to_inclusive
            from UNNEST($participants, $toInclusives) as k(participant_id, to_inclusive)
          )
        """.as[(InternedParticipantId, CantonTimestamp, CantonTimestamp, Digest, Off)]
      for {
        found <- storage.query(query, functionFullName)
      } yield {
        if (found.isEmpty) {
          ErrorUtil.invalidArgument(s"Expected to find $kind periods to be deleted, but found none")
        }
        val foundMap = found.map { case (participant, from, to, digest, offset) =>
          (participant, to) -> CommitmentMatchPeriod(participant, from, to, digest, offset)
        }.toMap
        periods.foreach { period =>
          val foundPeriod = foundMap.get((period.participant, period.toInclusive))
          ErrorUtil.requireArgument(
            foundPeriod.contains(period),
            s"Expected to find $kind period $period to be deleted, but found $foundPeriod",
          )
        }
      }
    }

  private def deleteFromTableQuery[Digest, Off](
      periods: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]],
      tableName: String & Singleton,
  ): DBIOAction[?, NoStream, Effect.Write] =
    if (periods.isEmpty) DBIO.unit
    else {
      val participants = periods.map(_.participant).toArray
      val toInclusives = periods.map(_.toInclusive).toArray
      storage.profile match {
        case _: DbStorage.Profile.Postgres =>
          sqlu"""
            with to_delete as (
              select participant_id, to_inclusive
              from UNNEST($participants, $toInclusives) as k(participant_id, to_inclusive)
            )
            delete from #$tableName stored
            using to_delete
              where stored.synchronizer_idx = $indexedSynchronizer
                and stored.participant_id = to_delete.participant_id
                and stored.to_inclusive = to_delete.to_inclusive
          """
        case _: DbStorage.Profile.H2 =>
          sqlu"""
             delete from #$tableName
             where synchronizer_idx = $indexedSynchronizer
               and (participant_id, to_inclusive) in (
                  select participant_id, to_inclusive
                  from UNNEST($participants, $toInclusives) as k(participant_id, to_inclusive)
                )
          """
      }
    }

  private def insertIntoTableQuery[Digest, Off](
      periods: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]],
      tableName: String & Singleton,
      hashedDigestColumn: OptionalColumn[Digest],
      offsetColumn: OptionalColumn[Off],
  )(implicit traceContext: TraceContext): DBIO[Unit] =
    NonEmpty.from(periods).traverse_ { periodsNE =>
      def insertColumnName(column: OptionalColumn[?]): String = column.withName(name => s", $name")
      def insertValuePlaceholder(column: OptionalColumn[?]): String =
        column.withName(_ => s", ?")
      val bulkQuery =
        s"""
          insert into $tableName (
            synchronizer_idx, participant_id, from_exclusive, to_inclusive
            ${insertColumnName(hashedDigestColumn)}
            ${insertColumnName(offsetColumn)}
          ) values (
            ?, ?, ?, ?
            ${insertValuePlaceholder(hashedDigestColumn)}
            ${insertValuePlaceholder(offsetColumn)}
          ) on conflict do nothing
        """
      DbStorage.bulkOperation_(bulkQuery, periodsNE, storage.profile) { pp => period =>
        pp >> indexedSynchronizer
        pp >> period.participant
        pp >> period.fromExclusive
        pp >> period.toInclusive
        hashedDigestColumn.set(pp, period.hashedDigest)
        offsetColumn.set(pp, period.offset)
      }
    }

  override def checkInvariant()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val allTables = Seq[String & Singleton](outstandingTable, mismatchTable, matchTable)
    def checkGoodPeriods(): FutureUnlessShutdown[Unit] = {
      def checkTable(tableName: String & Singleton): FutureUnlessShutdown[Unit] = {
        val query =
          sql"""
            select participant_id, from_exclusive, to_inclusive
            from #$tableName
            where synchronizer_idx = $indexedSynchronizer
              and from_exclusive >= to_inclusive
            #${storage.limit(1)}
          """.as[(InternedParticipantId, CantonTimestamp, CantonTimestamp)].headOption
        storage
          .query(query, functionFullName + "-goodPeriods")
          .map(_.foreach { case (participant, from, to) =>
            ErrorUtil.invalidState(s"Invalid period for participant ${stringInterning.participantId
                .externalize(participant)}: fromExclusive $from must be less than toInclusive $to")
          })
      }
      allTables.parTraverse_(checkTable)
    }

    def checkWatermarks(): FutureUnlessShutdown[Unit] = {
      val query =
        sql"""
          select watermark_reconciliation, watermark_affirmation
          from par_acs_commitment_period_watermark
          where synchronizer_idx = $indexedSynchronizer
            and watermark_reconciliation > watermark_affirmation
        """.as[(CantonTimestamp, CantonTimestamp)].headOption
      storage.query(query, functionFullName + "-watermarks").map {
        case Some((reconciliation, affirmation)) =>
          ErrorUtil.invalidState(
            s"Affirmation watermark $affirmation is below reconciliation watermark $reconciliation"
          )
        case None =>
      }
    }

    def checkDisjoint(): FutureUnlessShutdown[Unit] = {
      def checkDisjoint(
          table1: String & Singleton,
          table2: String & Singleton,
      ): FutureUnlessShutdown[Unit] = {
        val query =
          sql"""
            select tbl1.participant_id, tbl1.from_exclusive, tbl1.to_inclusive, tbl2.from_exclusive, tbl2.to_inclusive
            from #$table1 tbl1
              inner join #$table2 tbl2
                on tbl1.synchronizer_idx = $indexedSynchronizer
               and tbl2.synchronizer_idx = $indexedSynchronizer
               and tbl1.participant_id = tbl2.participant_id
               and tbl1.from_exclusive < tbl2.to_inclusive
               and tbl2.from_exclusive < tbl1.to_inclusive
               -- In a self-join, avoid comparing a row to itself
               and (case when $table1 = $table2 then tbl1.to_inclusive != tbl2.to_inclusive else true end)
            #${storage.limit(1)}
          """
            .as[
              (
                  InternedParticipantId,
                  CantonTimestamp,
                  CantonTimestamp,
                  CantonTimestamp,
                  CantonTimestamp,
              )
            ]
            .headOption
        storage.query(query, functionFullName + "-disjoint").map {
          case Some((participant, from1, to1, from2, to2)) =>
            ErrorUtil.invalidState(
              s"Overlapping periods for participant ${stringInterning.participantId
                  .externalize(participant)}: ($from1, $to1] and ($from2, $to2]"
            )
          case None =>
        }
      }

      val tablePairs = for {
        table1 <- allTables
        table2 <- allTables
        if table1 <= table2 // avoid checking the same pair twice
      } yield (table1, table2)
      // We only have 6 table pairs, so we can run them in parallel without worrying about overwhelming the database
      tablePairs.parTraverse_ { case (first, second) => checkDisjoint(first, second) }
    }

    def checkWatermarkBounds(): FutureUnlessShutdown[Unit] = {
      def checkTable(table: String & Singleton, name: String): FutureUnlessShutdown[Unit] = {
        val query =
          sql"""
            (
              select participant_id, from_exclusive, to_inclusive, watermark_affirmation as watermark
              from #$table as stored
                inner join par_acs_commitment_period_watermark
                  on stored.synchronizer_idx = $indexedSynchronizer
                 and par_acs_commitment_period_watermark.synchronizer_idx = $indexedSynchronizer
                 and to_inclusive > watermark_affirmation
              #${storage.limit(1)}
            ) union all (
              select participant_id, from_exclusive, to_inclusive, ${CantonTimestamp.MinValue} as watermark
              from #$table as stored
                where stored.synchronizer_idx = $indexedSynchronizer
                  and not exists(select 1 from par_acs_commitment_period_watermark where synchronizer_idx = $indexedSynchronizer)
              #${storage.limit(1)}
            )
          """
            .as[(InternedParticipantId, CantonTimestamp, CantonTimestamp, CantonTimestamp)]
            .headOption
        storage.query(query, functionFullName + "-watermarkBounds").map {
          case Some((participant, from, to, affirmation)) =>
            ErrorUtil.invalidState(
              s"$name period ($from, $to] for participant ${stringInterning.participantId
                  .externalize(participant)} exceeds affirmation watermark $affirmation"
            )
          case None =>
        }
      }
      for {
        _ <- checkTable(mismatchTable, "Mismatched")
        _ <- checkTable(matchTable, "Matched")
      } yield ()
    }

    for {
      _ <- checkGoodPeriods()
      _ <- checkWatermarks()
      _ <- checkDisjoint()
      _ <- checkWatermarkBounds()
    } yield ()
  }

  private def maybeCheckInvariant[A](
      f: => FutureUnlessShutdown[A]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[A] =
    if (enableConsistencyChecks) {
      for {
        _ <- checkInvariant()
        result <- f
        _ <- checkInvariant()
      } yield result
    } else f

  override def deleteOutstandingAfter(fromExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = maybeCheckInvariant {
    val query =
      sqlu"""
             delete from par_acs_commitment_period_outstanding
             where synchronizer_idx = $indexedSynchronizer
               and to_inclusive > $fromExclusive
        """
    storage.update_(query, functionFullName)
  }

  override protected def doPrune(limit: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Int] = maybeCheckInvariant {
    def query(tableName: String & Singleton) =
      sqlu"""
        delete from #$tableName
        where synchronizer_idx = $indexedSynchronizer
          and to_inclusive < $limit
            """
    val tableNames = Seq[String & Singleton](
      outstandingTable,
      mismatchTable,
      matchTable,
    )
    val deletes = DBIO.sequence(tableNames.map(query)).map(_.sum)
    storage.update(deletes, functionFullName)
  }

  private def toIntArrayLiteral(values: IterableOnce[Int]): String =
    storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        values.iterator.mkString("ARRAY[", ", ", "]::integer[]")
      case _: DbStorage.Profile.H2 =>
        values.iterator.mkString("CAST(ARRAY[", ", ", "] AS INTEGER ARRAY)")
    }

  private def toLongArrayLiteral(values: IterableOnce[Long]): String =
    storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        values.iterator.mkString("ARRAY[", ", ", "]::bigint[]")
      case _: DbStorage.Profile.H2 =>
        values.iterator.mkString("CAST(ARRAY[", ", ", "] AS BIGINT ARRAY)")
    }

  private def toByteaArrayLiteral(values: IterableOnce[ByteString]): String =
    storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        values.iterator
          .map(bytes => s"'\\x${HexString.toHexString(bytes)}'")
          .mkString("ARRAY[", ", ", "]::bytea[]")
      case _: DbStorage.Profile.H2 =>
        values.iterator
          .map(bytes => s"X'${HexString.toHexString(bytes)}'")
          .mkString("CAST(ARRAY[", ", ", "] AS VARBINARY ARRAY)")
    }
}

object DbAcsCommitmentPeriodStore {
  private val outstandingTable: String & Singleton = "par_acs_commitment_period_outstanding"
  private val mismatchTable: String & Singleton = "par_acs_commitment_period_mismatch"
  private val matchTable: String & Singleton = "par_acs_commitment_period_match"

  private sealed trait OptionalColumn[A] extends Product with Serializable {
    def withName(f: String & Singleton => String): String
    def set(pp: PositionedParameters, value: A): Unit
    def parseFrom(rs: ResultSet): A
    def getResult: GetResult[A]
  }
  private final case class Column[A](
      name: String & Singleton,
      set: SetParameter[A],
      parse: ResultSet => A,
  ) extends OptionalColumn[A] {
    override def parseFrom(rs: ResultSet): A = parse(rs)
    override def withName(f: String with Singleton => String): String = f(name)
    override def set(pp: PositionedParameters, value: A): Unit = pp.>>(value)(set)
    override def getResult: GetResult[A] = GetResult { rs =>
      val x = parseFrom(rs.rs)
      rs.skip.discard
      x
    }
  }

  private val expectedHashedDigestName: String & Singleton = "expected_hashed_digest"
  private val hashedDigestColumn: Column[HashedDigest] = {
    // We use varbinary for digests in H2, so this should work for both Postgres and H2
    val setParameter = SetParameter[HashedDigest]((bytes, pp) => pp.setBytes(bytes.toByteArray))
    Column(
      expectedHashedDigestName,
      setParameter,
      rs =>
        ByteString.copyFrom(
          Option(rs.getBytes(expectedHashedDigestName))
            .getOrElse(throw new DbDeserializationException(s"$expectedHashedDigestName is null"))
        ),
    )
  }

  private val hashedDigestOptionColumn: Column[Option[HashedDigest]] = {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    val setParameter =
      SetParameter[Option[HashedDigest]]((bytes, pp) =>
        // Postgres profile will fail with setBytesOption if given None because this sets the type to BLOB.
        // Easy fix is to use setBytes with null instead.
        pp.setBytes(bytes.map(_.toByteArray).orNull)
      )
    Column(
      expectedHashedDigestName,
      setParameter,
      rs => Option(rs.getBytes(expectedHashedDigestName)).map(ByteString.copyFrom),
    )
  }

  private val hashedDigestSomeColumn: Column[Some[HashedDigest]] = {
    val setParameter =
      SetParameter[Some[HashedDigest]]((bytes, pp) => pp.setBytes(bytes.value.toByteArray))
    Column(
      expectedHashedDigestName,
      setParameter,
      rs => {
        val byteArray = Option(rs.getBytes(expectedHashedDigestName))
          .getOrElse(throw new DbDeserializationException(s"$expectedHashedDigestName is null"))
        Some(ByteString.copyFrom(byteArray))
      },
    )
  }

  private val offsetColumnName: String & Singleton = "update_offset"
  private val offsetColumn: Column[Offset] =
    Column(
      offsetColumnName,
      SetParameter[Offset],
      rs => Offset.tryFromLong(rs.getLong(offsetColumnName)),
    )

  private final case class NotAColumn[A](default: A) extends OptionalColumn[A] {
    override def parseFrom(rs: ResultSet): A = default
    override def withName(f: String with Singleton => String): String = ""
    override def set(pp: PositionedParameters, value: A): Unit = ()
    override def getResult: GetResult[A] = GetResult(_ => default)
  }
  private val unitNotAColumn: NotAColumn[Unit] = NotAColumn(())
}
