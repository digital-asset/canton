// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.syntax.either.*
import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.*
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ConcurrentSkipListSet
import scala.concurrent.ExecutionContext
import scala.util.Either

class InMemoryAcsDigestStore(
    indexedSynchronizer: IndexedSynchronizer,
    stringInterning: StringInterning,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends AcsDigestStore
    with NamedLogging {
  // Note: shardId=indexedSynchronizer.index is fixed so we don't have to store it in the journals

  private val checkpointJournal =
    new ConcurrentSkipListSet[RecordTime](RecordTime.recordTimeOrdering)
  private val participant_ =
    new InMemoryAcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)](
      indexedSynchronizer,
      loggerFactory,
      prettyKey = stringInterning.participantId.externalize,
    )
  private val party_ = new InMemoryAcsDigestJournal[PartyAndOrder[InternedPartyId], RawDigest](
    indexedSynchronizer,
    loggerFactory,
    prettyKey = _.map(stringInterning.party.externalize).party,
  )

  override def participant
      : AcsDigestStore.DigestJournal[InternedParticipantId, (RawDigest, HashedDigest)] =
    participant_

  override def party
      : AcsDigestStore.DigestJournal[AcsDigestStore.PartyAndOrder[InternedPartyId], RawDigest] =
    party_

  override def insertCheckpointTime(recordTime: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(checkpointJournal.add(recordTime).discard)

  override def deleteAfter(fromExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    for {
      _ <- FutureUnlessShutdown.pure {
        val isInclusive = false
        checkpointJournal.tailSet(fromExclusive, isInclusive).clear()
      }
      _ <- party_.deleteAfter(fromExclusive)
      _ <- participant_.deleteAfter(fromExclusive)
    } yield ()

  override def deleteUpTo(toExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = for {
    _ <- FutureUnlessShutdown.pure {
      val isInclusive = false
      checkpointJournal.headSet(toExclusive, isInclusive).clear()
    }
    _ <- party_.deleteUpTo(toExclusive)
    _ <- participant_.deleteUpTo(toExclusive)
  } yield ()

  override def latestCheckpointUpTo(toInclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RecordTime]] =
    FutureUnlessShutdown.pure {
      Option(checkpointJournal.floor(toInclusive))
    }

  override def firstCheckpointAfter(fromExclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[RecordTime]] =
    FutureUnlessShutdown.pure {
      Option(checkpointJournal.higher(fromExclusive))
    }

  override def checkReplacesInvariant()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    Either.catchOnly[NoSuchElementException](checkpointJournal.last()).toOption match {
      case Some(lastCheckpoint) =>
        for {
          _ <- party_.checkReplacesInvariant(lastCheckpoint)
          _ <- participant_.checkReplacesInvariant(lastCheckpoint)
        } yield ()
      case None =>
        logger.debug(s"checkReplacesInvariant: no latest checkpoint!")
        FutureUnlessShutdown.unit
    }
}
