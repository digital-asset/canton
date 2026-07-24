// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.Eval
import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  Checkpoint,
  HashedDigest,
  InternedParticipantId,
  RawDigest,
}
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable
import scala.concurrent.ExecutionContext

class BlockingAcsDigestStore(
    stringInterning: StringInterning,
    loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends AcsDigestStore {

  private[this] val blockingRef: AtomicReference[FutureUnlessShutdown[Unit]] =
    new AtomicReference[FutureUnlessShutdown[Unit]]

  def setBlocking(releaseAfter: FutureUnlessShutdown[Unit]): Unit =
    blockingRef.set(releaseAfter)

  private val delegate: InMemoryAcsDigestStore =
    InMemoryAcsDigestStore.create(Eval.now(stringInterning), loggerFactory)

  private def blockAndThen[A](f: => FutureUnlessShutdown[A]): FutureUnlessShutdown[A] =
    blockingRef.get.flatMap(_ => f)

  override protected val party_
      : AcsDigestJournal[AcsDigestStore.PartyAndOrder[InternedPartyId], RawDigest] =
    new BlockingAcsDigestJournal[AcsDigestStore.PartyAndOrder[InternedPartyId], RawDigest](
      () => blockingRef.get,
      delegate.partyInternal,
    )

  override protected val participant_
      : AcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)] =
    new BlockingAcsDigestJournal[InternedParticipantId, (RawDigest, HashedDigest)](
      () => blockingRef.get,
      delegate.participantInternal,
    )

  override def insertCheckpointTime(checkpoint: Checkpoint)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    blockAndThen(delegate.insertCheckpointTime(checkpoint))

  override protected def deleteCheckpointsAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    blockAndThen(delegate.deleteCheckpointsAfterInternal(fromExclusive))

  override protected def deleteCheckpointsUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    blockAndThen(delegate.deleteCheckpointsUpToInternal(toExclusive))

  override def latestCheckpointUpTo(toInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    blockAndThen(delegate.latestCheckpointUpTo(toInclusive))

  override def firstCheckpointAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[Checkpoint]] =
    blockAndThen(delegate.firstCheckpointAfter(fromExclusive))
}

class BlockingAcsDigestJournal[K, V](
    getBlocking: () => FutureUnlessShutdown[Unit],
    // The pointless `private[...]` modifier suppresses the Scala compiler's worry
    // that the Token types escapes the visibility of the private modifier.
    private[BlockingAcsDigestJournal] val delegate: AcsDigestJournal[K, V],
)(private implicit val executionContext: ExecutionContext)
    extends AcsDigestJournal[K, V] {

  private def blockAndThen[A](f: => FutureUnlessShutdown[A]): FutureUnlessShutdown[A] =
    getBlocking().flatMap(_ => f)

  override def upsertDigestUpdates(
      digests: immutable.Iterable[AcsDigestStore.AcsDigestUpdate[K, V]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    blockAndThen(delegate.upsertDigestUpdates(digests))

  override def lookup(key: K, toInclusive: AtInclusive)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AcsDigestStore.AcsDigestUpdate[K, V]]] =
    blockAndThen(delegate.lookup(key, toInclusive))

  override def bulkLookup(keys: immutable.Iterable[K], toInclusive: AtInclusive)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[K, AcsDigestStore.AcsDigestUpdate[K, V]]] =
    blockAndThen(delegate.bulkLookup(keys, toInclusive))

  override def snapshot(
      tokenOrStart: Either[SnapshotPaginationToken, AtInclusive],
      limit: InternedParticipantId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    (
        immutable.Iterable[AcsDigestStore.AcsDigestUpdate[K, V]],
        Either[PaginationTokenDone, SnapshotPaginationToken],
    )
  ] = blockAndThen(delegate.snapshot(tokenOrStart, limit))

  override type SnapshotPaginationToken = delegate.SnapshotPaginationToken

  override def changesBetween(
      tokenOrStart: Either[
        ChangesBetweenPaginationToken,
        AcsDigestStore.ChangesBetweenOffsetRange,
      ],
      limit: InternedParticipantId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    (
        immutable.Iterable[AcsDigestStore.AcsDigest[K, V]],
        Either[PaginationTokenDone, ChangesBetweenPaginationToken],
    )
  ] = blockAndThen(delegate.changesBetween(tokenOrStart, limit))

  override type ChangesBetweenPaginationToken = delegate.ChangesBetweenPaginationToken

  override def deleteAfter(fromExclusive: AtInclusive)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = blockAndThen(delegate.deleteAfter(fromExclusive))

  override def deleteUpTo(toExclusive: AtInclusive)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = delegate.deleteUpTo(toExclusive)

  override def checkReplacesInvariant(upToInclusive: AtInclusive)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = blockAndThen(delegate.checkReplacesInvariant(upToInclusive))
}
