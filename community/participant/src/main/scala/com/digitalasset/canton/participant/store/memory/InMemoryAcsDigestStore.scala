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
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.ConcurrentSkipListMap
import scala.concurrent.ExecutionContext

class InMemoryAcsDigestStore @VisibleForTesting private[store] (
    override val loggerFactory: NamedLoggerFactory,
    override protected val party_ : AcsDigestJournal[PartyAndOrder[InternedPartyId]],
    override protected val participant_ : AcsDigestJournal[InternedParticipantId],
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

  override def latestCheckpointUpTo(
      toInclusive: Offset,
      checkpointTypes: Option[NonEmpty[Set[CheckpointType]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    FutureUnlessShutdown.pure {
      import scala.jdk.OptionConverters.*
      checkpointJournal
        .headMap(toInclusive, true)
        .descendingMap()
        .values()
        .stream
        .filter(checkpoint => checkpointTypes.forall(_.contains(checkpoint.checkpointType)))
        .findFirst()
        .toScala
    }

  override def firstCheckpointAfter(
      fromExclusive: Offset,
      checkpointTypes: Option[NonEmpty[Set[CheckpointType]]],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    FutureUnlessShutdown.pure {
      import scala.jdk.OptionConverters.*
      checkpointJournal
        .tailMap(fromExclusive, false)
        .values()
        .stream
        .filter(checkpoint => checkpointTypes.forall(_.contains(checkpoint.checkpointType)))
        .findFirst()
        .toScala
    }

  override def close(): Unit = ()
}

object InMemoryAcsDigestStore {
  def create(stringInterning: Eval[StringInterning], loggerFactory: NamedLoggerFactory)(implicit
      executionContext: ExecutionContext
  ): InMemoryAcsDigestStore = {
    val party = new InMemoryAcsDigestJournal[PartyAndOrder[InternedPartyId]](
      loggerFactory,
      prettyKey = _.map(stringInterning.value.party.externalize).party,
    )
    val participant =
      new InMemoryAcsDigestJournal[InternedParticipantId](
        loggerFactory,
        prettyKey = stringInterning.value.participantId.externalize,
      )
    new InMemoryAcsDigestStore(loggerFactory, party, participant)
  }
}
