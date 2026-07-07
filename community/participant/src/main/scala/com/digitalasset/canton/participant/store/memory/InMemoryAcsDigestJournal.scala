// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsDigestStore.*
import com.digitalasset.canton.participant.store.{
  AcsDigestJournal,
  AcsDigestStore,
  PaginationTokenDone,
}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.util.Try

class InMemoryAcsDigestJournal[K, V](
    override val indexedSynchronizer: IndexedSynchronizer,
    override val loggerFactory: NamedLoggerFactory,
    prettyKey: K => String,
) extends AcsDigestJournal[K, V]
    with NamedLogging {
  import InMemoryAcsDigestJournal.*

  private val journal = TrieMap[K, TreeMap[Offset, AcsDigestUpdate[K, V]]]()

  override def upsertDigestUpdates(digests: immutable.Iterable[AcsDigestUpdate[K, V]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      digests.foreach { update =>
        journal
          .updateWith(update.digestUpdate.key) {
            case Some(historyMap) => Some(historyMap + (update.digestUpdate.offset -> update))
            case None => Some(TreeMap(update.digestUpdate.offset -> update))
          }
          .discard
      }
    }

  override def lookup(key: K, toInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AcsDigestUpdate[K, V]]] = FutureUnlessShutdown.pure {
    lookupInternal(key, toInclusive)
  }

  override def bulkLookup(
      keys: immutable.Iterable[K],
      toInclusive: Offset,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[K, AcsDigestUpdate[K, V]]] = FutureUnlessShutdown.pure {
    keys.flatMap { key =>
      lookupInternal(key, toInclusive)
        .map(value => key -> value)
    }.toMap
  }

  override def snapshot(
      tokenOrStart: Either[SnapshotPaginationToken, AtInclusive],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    (
        immutable.Iterable[
          AcsDigestStore.AcsDigest[K, V]
        ],
        Either[PaginationTokenDone, SnapshotPaginationToken],
    )
  ] = {
    val stream = tokenOrStart match {
      case Left(SnapshotToken(remaining)) => remaining
      case Right(atInclusive) =>
        LazyList
          .from(journal.values)
          .flatMap { history =>
            // Exact match for inclusive, fallback to maxBefore for strict less-than
            history
              .get(atInclusive)
              .map(entry => atInclusive -> entry)
              .orElse(history.maxBefore(atInclusive))
          }
          .map { case (_, update) => update.digestUpdate }
    }

    val (page, next) = stream.splitAt(limit)

    val nextToken = Either.cond(next.nonEmpty, SnapshotToken(next), PaginationTokenDone)

    FutureUnlessShutdown.pure(
      page -> nextToken
    )
  }
  override type SnapshotPaginationToken = SnapshotToken[K, V]

  override def changesBetween(
      tokenOrStart: Either[ChangesBetweenPaginationToken, ChangesBetweenOffsetRange],
      limit: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    (
        immutable.Iterable[
          AcsDigestStore.AcsDigest[K, V]
        ],
        Either[PaginationTokenDone, ChangesBetweenPaginationToken],
    )
  ] = {
    val stream = tokenOrStart match {
      case Left(SnapshotToken(remaining)) => remaining
      case Right(ChangesBetweenOffsetRange(fromInclusive, toExclusive)) =>
        LazyList
          .from(journal.values)
          .flatMap(history =>
            history
              // we need this because otherwise eg. rangeUntil(toExclusive) gives the latest change
              // that might be before fromInclusive
              .maxBefore(toExclusive)
              .filter { case (recordTime, _) => recordTime >= fromInclusive }
              .map { case (_, latestChange) => latestChange }
          )
          .map(_.digestUpdate)
    }

    val (page, next) = stream.splitAt(limit)

    val nextToken = Either.cond(next.nonEmpty, SnapshotToken(next), PaginationTokenDone)

    FutureUnlessShutdown.pure {
      page -> nextToken
    }
  }
  override type ChangesBetweenPaginationToken = SnapshotToken[K, V]

  override def checkReplacesInvariant(upToInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.fromTry {
    Try {
      journal.values
        .foreach { recordTimeDigestUpdatesMap =>
          val timeSeriesTuples = recordTimeDigestUpdatesMap
            .rangeTo(upToInclusive)
            .values
            .toList
            .reverse
            .sliding(2) // pairs of row(rt_i) row(rt_i-1)
            .map {
              _.map(window =>
                (
                  prettyKey(window.digestUpdate.key),
                  window.digestUpdate.offset,
                  window.replacesOffset,
                )
              ) // (keyString, t_i, rt_i)
            }

          timeSeriesTuples.foreach { // find one where the link is broken
            // if `t_i` < `r_ti` --> referring to the future is not possible
            case (key, currentOffset, Some(currentReplacesOffset)) :: _
                if currentReplacesOffset >= currentOffset =>
              ErrorUtil.invalidState(
                s"ReplacesOffset check error for Key $key - " +
                  s"We cannot have replacesOffset=$currentReplacesOffset which is gte to change offset $currentOffset"
              )
            // if `1 < i <= M`, `rt_i` should be Some
            case (key, currentOffset, None) :: (_, precedingOffset, _) :: _ =>
              ErrorUtil.invalidState(
                s"ReplacesOffset check error for key $key at $currentOffset - " +
                  s"Replaces offset should point to $precedingOffset but it is empty!"
              )
            // If `1 < i <= M` and `rt_i` != `t_i - 1`:
            case (key, currentOffset, Some(currentReplacesOffset)) :: (
                  _,
                  precedingOffset,
                  _,
                ) :: _ if currentReplacesOffset != precedingOffset =>
              ErrorUtil.invalidState(
                s"ReplacesOffset check error for key $key - " +
                  s"We cannot have an entry with (offset=$currentOffset, replacesOffset=$currentReplacesOffset) " +
                  s"which is not pointing to the preceding offset=$precedingOffset"
              )
            // eg. when rt_i doesn't have updates in the snapshot journal
            case _ => ()
          }
        }
    }
  }

  def deleteAfter(fromExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    journal.keys.foreach { key =>
      journal
        .updateWith(key) {
          case Some(history) =>
            val newHistory = history.rangeTo(fromExclusive)
            Option.when(newHistory.nonEmpty)(newHistory)
          case None => None
        }
        .discard
    }
  }

  def deleteUpTo(toExclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
    journal.keys.foreach { key =>
      journal
        .updateWith(key) {
          case Some(history) =>
            val latestSafeTimeO = history
              .get(toExclusive)
              .map(update => toExclusive -> update)
              .orElse(history.maxBefore(toExclusive))

            val updatedHistory = latestSafeTimeO.fold(history) {
              case (latestSafeTime, lastUpdateAtSafeTime) =>
                val isLatestUpdateNone = lastUpdateAtSafeTime.digestUpdate.digestO.isEmpty

                if (isLatestUpdateNone) history.rangeFrom(toExclusive)
                else history.rangeFrom(latestSafeTime)
            }

            Option.when(updatedHistory.nonEmpty)(updatedHistory)
          case None => None
        }
        .discard
    }
  }

  private def lookupInternal(key: K, toInclusive: Offset): Option[AcsDigestUpdate[K, V]] =
    for {
      history <- journal.get(key)
      (_, update) <- history.rangeTo(toInclusive).lastOption
    } yield update
}

object InMemoryAcsDigestJournal {
  final case class SnapshotToken[K, V](remaining: LazyList[AcsDigest[K, V]])
}
