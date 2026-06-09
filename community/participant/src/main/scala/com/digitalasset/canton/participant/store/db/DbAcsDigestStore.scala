// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.event.RecordTime
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
    stringInterning: StringInterning,
    override protected val storage: DbStorage,
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val timeouts: ProcessingTimeout,
)(override implicit val executionContext: ExecutionContext)
    extends AcsDigestStore
    with DbStore {
  import storage.api.*

  private val synchronizerIdx = indexedSynchronizer.index

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

  override def insertCheckpointTime(recordTime: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val insertCheckpoint = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_acs_running_digests_checkpoint (synchronizer_idx, ts, tie_breaker)
               values ($synchronizerIdx, ${recordTime.timestamp}, ${recordTime.tieBreaker})"""
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_acs_running_digests_checkpoint(synchronizer_idx, ts, tie_breaker)
               values  ($synchronizerIdx, ${recordTime.timestamp}, ${recordTime.tieBreaker})
               on conflict (synchronizer_idx, ts, tie_breaker) do nothing
          """
    }

    logger.trace(s"insertCheckpointTime at $recordTime: $insertCheckpoint")

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

  override def latestCheckpointUpTo(toInclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RecordTime]] = {
    val queryLatestCheckpoint =
      (sql"""
        select ts, tie_breaker from par_acs_running_digests_checkpoint where synchronizer_idx = $synchronizerIdx
          and (ts < ${toInclusive.timestamp} or (ts = ${toInclusive.timestamp} and tie_breaker <= ${toInclusive.tieBreaker}))
        order by ts desc, tie_breaker desc
        """ ++ storage.limitSql(1)).as[RecordTime].headOption

    logger.trace(s"querying latest checkpoint up to $toInclusive")

    storage
      .query(
        action = queryLatestCheckpoint,
        operationName = s"read the latest ACS running digest journal checkpoint",
      )
  }

  override def firstCheckpointAfter(fromExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RecordTime]] = {
    val queryFirstCheckpointAfter =
      (sql"""
        select ts, tie_breaker from par_acs_running_digests_checkpoint where synchronizer_idx = $synchronizerIdx
          and (ts > ${fromExclusive.timestamp} or (ts = ${fromExclusive.timestamp} and tie_breaker > ${fromExclusive.tieBreaker}))
        order by ts, tie_breaker
        """ ++ storage.limitSql(1)).as[RecordTime].headOption

    logger.trace(s"querying first checkpoint after $fromExclusive")

    storage
      .query(
        action = queryFirstCheckpointAfter,
        operationName = s"read the first ACS running digest journal checkpoint",
      )
  }

  override protected def deleteCheckpointsAfter(fromExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val deleteFromCheckpointJournal = sqlu"""delete from par_acs_running_digests_checkpoint
          where synchronizer_idx = $synchronizerIdx
            and (ts > ${fromExclusive.timestamp}
                 or (ts = ${fromExclusive.timestamp} and tie_breaker > ${fromExclusive.tieBreaker}))"""

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

  override protected def deleteCheckpointsUpTo(toExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val deleteUpToCheckpointJournal = sqlu"""delete from par_acs_running_digests_checkpoint
          where synchronizer_idx = $synchronizerIdx
            and (ts < ${toExclusive.timestamp}
                 or (ts = ${toExclusive.timestamp} and tie_breaker < ${toExclusive.tieBreaker}))"""

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
