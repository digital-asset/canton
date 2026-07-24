// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.{
  MatchedCommitmentMatchPeriod,
  MismatchedCommitmentMatchPeriod,
  MismatchedOrUnexpectedCommitmentMatchPeriod,
  OutstandingCommitmentMatchPeriod,
}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  HashedDigest,
  InternedParticipantId,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.immutable
import scala.concurrent.ExecutionContext

class DelegatingAcsCommitmentPeriodStore(delegate: AcsCommitmentPeriodStore & NamedLogging)
    extends AcsCommitmentPeriodStore
    with NamedLogging {

  override protected def loggerFactory: NamedLoggerFactory = delegate.loggerFactoryInternal

  override protected def stringInterning: StringInterning = delegate.stringInterningInternal

  override def lookupOutstanding(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[OutstandingCommitmentMatchPeriod]] =
    delegate.lookupOutstanding(periods)

  override def lookupMismatchedOrUnexpected(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod]] =
    delegate.lookupMismatchedOrUnexpected(periods)

  override def lookupMismatchedByHash(
      periods: immutable.Iterable[(InternedParticipantId, HashedDigest, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MismatchedCommitmentMatchPeriod]] =
    delegate.lookupMismatchedByHash(periods)

  override def lookupMatched(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MatchedCommitmentMatchPeriod]] =
    delegate.lookupMatched(periods)

  override def watermarks()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[AcsCommitmentPeriodStore.MatchingWatermark] =
    delegate.watermarks()

  override def increaseInsertionWatermark(watermark: CantonTimestamp, affirmationOnly: Boolean)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    delegate.increaseInsertionWatermark(watermark, affirmationOnly)

  override def increaseMatcherWatermark(offset: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    delegate.increaseMatcherWatermark(offset)

  override def markOutstanding(digests: immutable.Iterable[OutstandingCommitmentMatchPeriod])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    delegate.markOutstanding(digests)

  override def persistMatchingOutcome(
      deleteOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      deleteMismatched: immutable.Iterable[MismatchedCommitmentMatchPeriod],
      insertOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      insertMismatchedOrUnexpected: immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod],
      insertMatched: immutable.Iterable[MatchedCommitmentMatchPeriod],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    delegate.persistMatchingOutcome(
      deleteOutstanding,
      deleteMismatched,
      insertOutstanding,
      insertMismatchedOrUnexpected,
      insertMatched,
    )

  override def checkInvariant()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    delegate.checkInvariant()

  override def deleteOutstandingAfter(fromExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    delegate.deleteOutstandingAfter(fromExclusive)

  override protected def doPrune(limit: CantonTimestamp, lastPruning: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Int] =
    delegate.doPruneInternal(limit, lastPruning)

  override protected implicit val ec: ExecutionContext = delegate.ecInternal

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[PruningStatus]] =
    delegate.pruningStatus

  override protected def advancePruningTimestamp(phase: PruningPhase, timestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    delegate.advancePruningTimestampInternal(phase, timestamp)
}
