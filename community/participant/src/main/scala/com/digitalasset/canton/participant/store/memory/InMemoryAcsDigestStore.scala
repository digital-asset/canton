// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsDigestStore.*
import com.digitalasset.canton.participant.store.{AcsDigestJournal, AcsDigestStore}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.ConcurrentSkipListMap
import scala.concurrent.ExecutionContext

class InMemoryAcsDigestStore @VisibleForTesting private[store] (
    override val loggerFactory: NamedLoggerFactory,
    override protected val party_ : AcsDigestJournal[PartyAndOrder[InternedPartyId], RawDigest],
    override protected val participant_ : AcsDigestJournal[
      InternedParticipantId,
      (RawDigest, HashedDigest),
    ],
)(override implicit val executionContext: ExecutionContext)
    extends AcsDigestStore
    with NamedLogging {

  private val checkpointJournal = new ConcurrentSkipListMap[Offset, Checkpoint]()

  override def insertCheckpointTime(
      checkpoint: Checkpoint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure(
      checkpointJournal
        .put(checkpoint.offset, checkpoint)
        .discard
    )

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
        .map(_.getValue)
    }

  override def firstCheckpointAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    FutureUnlessShutdown.pure {
      Option(checkpointJournal.higherEntry(fromExclusive))
        .map(_.getValue)
    }
}

object InMemoryAcsDigestStore {
  def create(stringInterning: Eval[StringInterning], loggerFactory: NamedLoggerFactory)(implicit
      executionContext: ExecutionContext
  ): InMemoryAcsDigestStore = {
    val party = new InMemoryAcsDigestJournal[PartyAndOrder[InternedPartyId], RawDigest](
      loggerFactory,
      prettyKey = _.map(stringInterning.value.party.externalize).party,
    )
    val participant =
      new InMemoryAcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)](
        loggerFactory,
        prettyKey = stringInterning.value.participantId.externalize,
      )
    new InMemoryAcsDigestStore(loggerFactory, party, participant)
  }
}
