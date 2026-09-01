// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.config.{BatchAggregatorConfig, ProcessingTimeout}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.participant.store.AcsDigestStore.InternedParticipantId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{BatchAggregator, ErrorUtil}
import com.digitalasset.nonempty.NonEmpty
import pprint.Tree

import scala.collection.immutable
import scala.concurrent.ExecutionContext

class BatchingAcsDigestStore(
    underlying: AcsDigestStore,
    lookupAggregatorConfig: BatchAggregatorConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    override protected val executionContext: ExecutionContext
) extends AcsDigestStore
    with NamedLogging
    with FlagCloseable
    with HasCloseContext {

  override protected val party_ : AcsDigestJournal[InternedPartyId] =
    new BatchingAcsDigestJournal[InternedPartyId](
      underlying.partyInternal,
      lookupAggregatorConfig,
      loggerFactory,
    )

  override protected val participant_ : AcsDigestJournal[InternedParticipantId] =
    new BatchingAcsDigestJournal[InternedParticipantId](
      underlying.participantInternal,
      lookupAggregatorConfig,
      loggerFactory,
    )

  override def insertCheckpointTime(checkpoint: AcsDigestStore.Checkpoint)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    underlying.insertCheckpointTime(checkpoint)

  override protected def deleteCheckpointsAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    underlying.deleteCheckpointsAfterInternal(fromExclusive)

  override protected def deleteCheckpointsUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    underlying.deleteCheckpointsUpToInternal(toExclusive)

  override def latestCheckpointUpTo(
      toInclusive: Offset,
      checkpointTypes: Option[NonEmpty[Set[AcsDigestStore.CheckpointType]]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[AcsDigestStore.Checkpoint]] =
    underlying.latestCheckpointUpTo(toInclusive, checkpointTypes)

  override def firstCheckpointAfter(
      fromExclusive: Offset,
      checkpointTypes: Option[NonEmpty[Set[AcsDigestStore.CheckpointType]]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[AcsDigestStore.Checkpoint]] =
    underlying.firstCheckpointAfter(fromExclusive, checkpointTypes)

  override protected def purgeCheckpoints()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = underlying.purgeCheckpointsInternal()

  override protected def truncateCheckpoints()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = underlying.truncateCheckpointsInternal()

  override def onClosed(): Unit =
    LifeCycle.close(party_, participant_, underlying)(logger)
}

class BatchingAcsDigestJournal[K](
    // The pointless `private[...]` modifier suppresses the Scala compiler's worry
    // that the Token types escapes the visibility of the private modifier.
    private[BatchingAcsDigestJournal] val underlying: AcsDigestJournal[K],
    lookupAggregatorConfig: BatchAggregatorConfig,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    storeCloseContext: CloseContext,
    override protected val executionContext: ExecutionContext,
    prettyK: Pretty[K],
) extends AcsDigestJournal[K]
    with NamedLogging { self =>

  private object LookupAggregatorProcessor
      extends BatchAggregator.Processor[(K, Offset), Option[AcsDigestStore.AcsDigestUpdate[K]]] {
    override def kind: String = "BatchingAcsDigestJournal"
    override def logger: TracedLogger = self.logger
    override def executeBatch(items: NonEmpty[Seq[Traced[(K, Offset)]]])(implicit
        traceContext: TraceContext,
        callerCloseContext: CloseContext,
    ): FutureUnlessShutdown[immutable.Iterable[Option[AcsDigestStore.AcsDigestUpdate[K]]]] =
      underlying.bulkLookup(items.map(_.value)).map { results =>
        items.map(traced => results.get(traced.value))
      }

    override def prettyItem: Pretty[(K, Offset)] = { case (key, offset) =>
      Tree.Apply("", Iterator(prettyK.treeOf(key), Pretty[Offset].treeOf(offset)))
    }
  }

  private val lookupAggregator
      : BatchAggregator[(K, Offset), Option[AcsDigestStore.AcsDigestUpdate[K]]] =
    BatchAggregator(LookupAggregatorProcessor, lookupAggregatorConfig)

  override def upsertDigestUpdates(
      digests: immutable.Iterable[AcsDigestStore.AcsDigestUpdate[K]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    underlying.upsertDigestUpdates(digests)

  override def lookup(key: K, toInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AcsDigestStore.AcsDigestUpdate[K]]] =
    lookupAggregator.run(key -> toInclusive)

  override def bulkLookup(keysUpToInclusive: immutable.Iterable[(K, Offset)])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[(K, Offset), AcsDigestStore.AcsDigestUpdate[K]]] =
    NonEmpty.from(keysUpToInclusive) match {
      case Some(keysNE) =>
        val tracedItems = keysNE.map(Traced.apply)
        lookupAggregator.runMany(tracedItems).map { results =>
          ErrorUtil.requireState(
            results.sizeCompare(keysUpToInclusive) == 0,
            s"Batch aggregator in ACS digest journal is violating its contract: keys=${keysUpToInclusive.size}, results=${results.size}",
          )
          keysUpToInclusive
            .zip(results)
            .flatMap { case (keyUpToInclusive, result) => result.map(keyUpToInclusive -> _) }
            .toMap
        }
      case None => FutureUnlessShutdown.pure(Map.empty)
    }

  override type SnapshotPaginationToken = underlying.SnapshotPaginationToken
  override def snapshot(
      tokenOrStart: Either[SnapshotPaginationToken, AtInclusive],
      limit: InternedParticipantId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    (
        immutable.Iterable[AcsDigestStore.AcsDigestUpdate[K]],
        Either[PaginationTokenDone, SnapshotPaginationToken],
    )
  ] = underlying.snapshot(tokenOrStart, limit)

  override type ChangesBetweenPaginationToken = underlying.ChangesBetweenPaginationToken
  override def changesBetween(
      tokenOrStart: Either[ChangesBetweenPaginationToken, AcsDigestStore.ChangesBetweenOffsetRange],
      limit: InternedParticipantId,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    (
        immutable.Iterable[AcsDigestStore.AcsDigest[K]],
        Either[PaginationTokenDone, ChangesBetweenPaginationToken],
    )
  ] = underlying.changesBetween(tokenOrStart, limit)

  override def checkReplacesInvariant(upToInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    underlying.checkReplacesInvariant(upToInclusive)

  override def deleteAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    underlying.deleteAfter(fromExclusive)

  override def deleteUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    underlying.deleteUpTo(toExclusive)

  override def purge()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    underlying.purge()

  override def truncateAll()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    underlying.truncateAll()

  override def close(): Unit =
    LifeCycle.close(underlying)(logger)
}
