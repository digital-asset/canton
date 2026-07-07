// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.*
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ConcurrentSkipListMap
import scala.concurrent.ExecutionContext

class InMemoryAcsDigestStore(
    indexedSynchronizer: IndexedSynchronizer,
    stringInterning: StringInterning,
    override val loggerFactory: NamedLoggerFactory,
)(override implicit val executionContext: ExecutionContext)
    extends AcsDigestStore
    with NamedLogging {
  // Note: shardId=indexedSynchronizer.index is fixed so we don't have to store it in the journals

  private val checkpointJournal = new ConcurrentSkipListMap[Offset, CantonTimestamp]()
  override protected val participant_ =
    new InMemoryAcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)](
      indexedSynchronizer,
      loggerFactory,
      prettyKey = stringInterning.participantId.externalize,
    )
  override protected val party_ =
    new InMemoryAcsDigestJournal[PartyAndOrder[InternedPartyId], RawDigest](
      indexedSynchronizer,
      loggerFactory,
      prettyKey = _.map(stringInterning.party.externalize).party,
    )

  override def insertCheckpointTime(offset: Offset, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(checkpointJournal.put(offset, timestamp).discard)

  override protected def deleteCheckpointsAfter(
      fromExclusive: Offset
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      val isInclusive = false
      checkpointJournal.tailMap(fromExclusive, isInclusive).clear()
    }

  override protected def deleteCheckpointsUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    val isInclusive = false
    checkpointJournal.headMap(toExclusive, isInclusive).clear()
  }

  override def latestCheckpointUpTo(toInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    FutureUnlessShutdown.pure {
      Option(checkpointJournal.floorEntry(toInclusive))
        .map(entry => (entry.getKey, entry.getValue))
    }

  override def firstCheckpointAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    FutureUnlessShutdown.pure {
      Option(checkpointJournal.higherEntry(fromExclusive))
        .map(entry => (entry.getKey, entry.getValue))
    }
}
