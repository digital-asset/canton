// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.Eval
import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  HashedDigest,
  InternedParticipantId,
  RawDigest,
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

  override protected val party_
      : AcsDigestJournal[AcsDigestStore.PartyAndOrder[InternedPartyId], RawDigest] =
    new DbAcsDigestJournal[AcsDigestStore.PartyAndOrder[InternedPartyId], RawDigest](
      storage,
      indexedSynchronizer,
      loggerFactory,
      timeouts,
      prettyKey = _.map(stringInterning.party.externalize).toString,
      journalTable = PartyJournalTable,
      createJournalImplicitsF = PartyJournalImplicits(_),
    )
  override protected val participant_
      : AcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)] =
    new DbAcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)](
      storage,
      indexedSynchronizer,
      loggerFactory,
      timeouts,
      prettyKey = stringInterning.participantId.externalize,
      journalTable = ParticipantJournalTable,
      createJournalImplicitsF = ParticipantJournalImplicits(_),
    )

  override def insertCheckpointTime(offset: Offset, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val insertCheckpoint = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_acs_running_digests_checkpoint (synchronizer_idx, change_offset, ts)
               values ($synchronizerIdx, $offset, $timestamp)"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_acs_running_digests_checkpoint(synchronizer_idx, change_offset, ts)
               values  ($synchronizerIdx, $offset, $timestamp)
               on conflict (synchronizer_idx, change_offset) do nothing
          """
    }

    logger.trace(s"insertCheckpointTime at $offset: $insertCheckpoint")

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

  override def latestCheckpointUpTo(toInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] = {
    val queryLatestCheckpoint =
      (sql"""
        select change_offset, ts from par_acs_running_digests_checkpoint where
          synchronizer_idx = $synchronizerIdx and change_offset <= $toInclusive
        order by change_offset desc
        """ ++ storage.limitSql(1)).as[Checkpoint].headOption

    logger.trace(s"querying latest checkpoint up to $toInclusive")

    storage
      .query(
        action = queryLatestCheckpoint,
        operationName = s"read the latest ACS running digest journal checkpoint",
      )
  }

  override def firstCheckpointAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] = {
    val queryFirstCheckpointAfter =
      (sql"""
        select change_offset, ts from par_acs_running_digests_checkpoint where
          synchronizer_idx = $synchronizerIdx
          and change_offset > $fromExclusive
        order by change_offset
        """ ++ storage.limitSql(1)).as[Checkpoint].headOption

    logger.trace(s"querying first checkpoint after $fromExclusive")

    storage
      .query(
        action = queryFirstCheckpointAfter,
        operationName = s"read the first ACS running digest journal checkpoint",
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
