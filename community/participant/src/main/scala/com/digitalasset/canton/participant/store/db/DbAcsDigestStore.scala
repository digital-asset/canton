// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Eval
import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  Checkpoint,
  CheckpointType,
  InternedParticipantId,
}
import com.digitalasset.canton.participant.store.data.AcsDigestJournalData.JournalTable.{
  ParticipantJournalTable,
  PartyJournalTable,
}
import com.digitalasset.canton.participant.store.data.DbAcsDigestJournalImplicits.{
  ParticipantJournalImplicits,
  PartyJournalImplicits,
}
import com.digitalasset.canton.participant.store.{AcsDigestJournal, AcsDigestStore}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

import DbStorage.Implicits.BuilderChain.*

class DbAcsDigestStore(
    indexedSynchronizer: IndexedSynchronizer,
    stringInterningEval: Eval[StringInterning],
    override protected val storage: DbStorage,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val timeouts: ProcessingTimeout,
)(override implicit val executionContext: ExecutionContext)
    extends AcsDigestStore
    with DbStore {
  import storage.api.*

  private val synchronizerIdx = indexedSynchronizer.index

  @inline
  private def stringInterning = stringInterningEval.value

  override protected val party_ : AcsDigestJournal[InternedPartyId] =
    new DbAcsDigestJournal[InternedPartyId](
      storage,
      indexedSynchronizer,
      loggerFactory,
      timeouts,
      prettyKey = stringInterning.party.externalize,
      journalTable = PartyJournalTable,
      createJournalImplicitsF = PartyJournalImplicits(_),
    )
  override protected val participant_ : AcsDigestJournal[InternedParticipantId] =
    new DbAcsDigestJournal[InternedParticipantId](
      storage,
      indexedSynchronizer,
      loggerFactory,
      timeouts,
      prettyKey = stringInterning.participantId.externalize,
      journalTable = ParticipantJournalTable,
      createJournalImplicitsF = ParticipantJournalImplicits(_),
    )

  override def insertCheckpointTime(
      checkpoint: Checkpoint
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val insertCheckpoint = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_acs_running_digests_checkpoint (synchronizer_idx, change_offset, ts, checkpoint_type)
               values ($synchronizerIdx, ${checkpoint.offset}, ${checkpoint.recordTime}, ${checkpoint.checkpointType})"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_acs_running_digests_checkpoint(synchronizer_idx, change_offset, ts, checkpoint_type)
               values  ($synchronizerIdx, ${checkpoint.offset}, ${checkpoint.recordTime}, ${checkpoint.checkpointType})
               on conflict (synchronizer_idx, change_offset) do
                 update set ts = excluded.ts, checkpoint_type = excluded.checkpoint_type
                 where par_acs_running_digests_checkpoint.ts <> excluded.ts or
                       par_acs_running_digests_checkpoint.checkpoint_type <> excluded.checkpoint_type
          """
    }

    logger.trace(s"insertCheckpointTime at ${checkpoint.offset}: $insertCheckpoint")

    storage
      .update(
        action = insertCheckpoint,
        operationName = "insert into par_acs_running_digests_checkpoint",
      )
      .map(alteredRowCount =>
        logger.trace(
          s"$alteredRowCount row inserted into par_acs_running_digests_checkpoint"
        )
      )
  }

  override def latestCheckpointUpTo(
      toInclusive: Offset,
      checkpointTypes: Option[NonEmpty[Set[CheckpointType]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] = {
    val query = checkpointTypes match {
      case None =>
        sql"""
           select change_offset, ts, checkpoint_type
           from par_acs_running_digests_checkpoint
           where synchronizer_idx = $synchronizerIdx
             and change_offset <= $toInclusive
           order by change_offset desc
           #${storage.limit(1)}
        """.as[Checkpoint]
      case Some(types) =>
        import storage.DbStorageConverters.setParameterArrayInt
        storage.profile match {
          case _: DbStorage.Profile.Postgres =>
            val typesArray = types.toArray
            // Explicitly use a cross lateral join to induce Postgres' query planner to execute a separate subquery for
            // each checkpoint type of interest.
            sql"""
              with latest_checkpoints as (
                select cp.change_offset, cp.ts, k.checkpoint_type
                from UNNEST($typesArray) as k(checkpoint_type)
                  cross join lateral (
                    select sub.change_offset, sub.ts
                    from par_acs_running_digests_checkpoint sub
                    where sub.synchronizer_idx = $synchronizerIdx
                      and sub.checkpoint_type = k.checkpoint_type
                      and sub.change_offset <= $toInclusive
                    order by sub.synchronizer_idx, sub.checkpoint_type, sub.change_offset desc
                    #${storage.limit(1)}
                  ) cp
              )
              select cps.change_offset, cps.ts, cps.checkpoint_type
              from latest_checkpoints cps
              order by cps.change_offset desc
              #${storage.limit(1)}
            """.as[Checkpoint]
          case _: DbStorage.Profile.H2 =>
            val inClause = DbStorage.toInClause("checkpoint_type", types)
            (sql"""
              select change_offset, ts, checkpoint_type
              from par_acs_running_digests_checkpoint
              where synchronizer_idx = $synchronizerIdx
                and change_offset <= $toInclusive
                and """ ++ inClause ++ sql"""
              order by change_offset desc
              #${storage.limit(1)}
            """).as[Checkpoint]
        }
    }
    logger.trace(
      s"querying latest checkpoint up to $toInclusive for types ${checkpointTypes.getOrElse("all")}"
    )
    storage.query(
      query.headOption,
      "read the latest ACS running digest journal checkpoint",
    )
  }

  override def firstCheckpointAfter(
      fromExclusive: Offset,
      checkpointTypes: Option[NonEmpty[Set[CheckpointType]]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[Checkpoint]] = {
    val query = checkpointTypes match {
      case None =>
        sql"""
           select change_offset, ts, checkpoint_type
           from par_acs_running_digests_checkpoint
           where synchronizer_idx = $synchronizerIdx
             and change_offset > $fromExclusive
           order by change_offset
           #${storage.limit(1)}
        """.as[Checkpoint]
      case Some(types) =>
        import storage.DbStorageConverters.setParameterArrayInt
        storage.profile match {
          case _: DbStorage.Profile.Postgres =>
            val typesArray = types.toArray
            // Explicitly use a cross lateral join to induce Postgres' query planner to execute a separate subquery for
            // each checkpoint type of interest.
            sql"""
              with first_checkpoints as (
                select cp.change_offset, cp.ts, k.checkpoint_type
                from UNNEST($typesArray) as k(checkpoint_type)
                cross join lateral (
                  select sub.change_offset, sub.ts
                  from par_acs_running_digests_checkpoint sub
                  where sub.synchronizer_idx = $synchronizerIdx
                    and sub.checkpoint_type = k.checkpoint_type
                    and sub.change_offset > $fromExclusive
                  order by sub.synchronizer_idx, sub.checkpoint_type, sub.change_offset
                  #${storage.limit(1)}
                ) cp
              )
              select cps.change_offset, cps.ts, cps.checkpoint_type
              from first_checkpoints cps
              order by cps.change_offset
              #${storage.limit(1)}
            """.as[Checkpoint]
          case _: DbStorage.Profile.H2 =>
            val inClause = DbStorage.toInClause("checkpoint_type", types)
            (sql"""
              select change_offset, ts, checkpoint_type
              from par_acs_running_digests_checkpoint
              where synchronizer_idx = $synchronizerIdx
                and change_offset > $fromExclusive
                and """ ++ inClause ++ sql"""
              order by change_offset
              #${storage.limit(1)}
            """).as[Checkpoint]
        }
    }
    logger.trace(
      s"querying first checkpoint after $fromExclusive for types ${checkpointTypes.getOrElse("all")}"
    )
    storage.query(
      query.headOption,
      "read the first ACS running digest journal checkpoint",
    )
  }

  override protected def deleteCheckpointsAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val deleteFromCheckpointJournal = sqlu"""delete from par_acs_running_digests_checkpoint
          where synchronizer_idx = $synchronizerIdx
            and change_offset > $fromExclusive
    """

    logger.trace(s"delete from checkpoint journal, starting from $fromExclusive (exclusive)")

    storage
      .update(
        action = deleteFromCheckpointJournal,
        operationName = "delete from par_acs_running_digests_checkpoint",
      )
      .map { alteredRowCount =>
        logger.trace(
          s"Deleted $alteredRowCount checkpoint rows from ACS running digest checkpoints from $fromExclusive (exclusive)..."
        )
      }
  }

  override protected def deleteCheckpointsUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val deleteUpToCheckpointJournal = sqlu"""delete from par_acs_running_digests_checkpoint
          where synchronizer_idx = $synchronizerIdx
            and change_offset < $toExclusive
    """

    logger.trace(s"delete from checkpoint journal, up to $toExclusive (exclusive)...")

    storage
      .update(
        action = deleteUpToCheckpointJournal,
        operationName = "delete up to, from par_acs_running_digests_checkpoint",
      )
      .map { alteredRowCount =>
        logger.trace(
          s"Deleted $alteredRowCount checkpoint rows from ACS running digest checkpoints, up to $toExclusive (exclusive)"
        )
      }
  }
}
