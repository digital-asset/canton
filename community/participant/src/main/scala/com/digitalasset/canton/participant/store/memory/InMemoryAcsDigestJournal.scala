// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.event.RecordTime.recordTimeOrdering
import com.digitalasset.canton.participant.store.AcsDigestStore.*
import com.digitalasset.canton.participant.store.{
  AcsDigestJournal,
  AcsDigestStore,
  PaginationTokenDone,
}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.math.Ordering.Implicits.*
import scala.util.Try

class InMemoryAcsDigestJournal[K, V](
    override val indexedSynchronizer: IndexedSynchronizer,
    override val loggerFactory: NamedLoggerFactory,
    prettyKey: K => String,
) extends AcsDigestJournal[K, V]
    with NamedLogging {
  import InMemoryAcsDigestJournal.*

  @VisibleForTesting
  private val journal = TrieMap[K, TreeMap[RecordTime, AcsDigestUpdate[K, V]]]()

  override def upsertDigestUpdates(digests: immutable.Iterable[AcsDigestUpdate[K, V]])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.pure {
      digests.foreach { update =>
        journal
          .updateWith(update.digestUpdate.key) {
            case Some(historyMap) => Some(historyMap + (update.digestUpdate.recordTime -> update))
            case None => Some(TreeMap(update.digestUpdate.recordTime -> update))
          }
          .discard
      }
    }

  override def lookup(key: K, toInclusive: RecordTime)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[AcsDigestUpdate[K, V]]] = FutureUnlessShutdown.pure {
    lookupInternal(key, toInclusive)
  }

  override def bulkLookup(
      keys: immutable.Iterable[K],
      toInclusive: RecordTime,
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
      tokenOrStart: Either[ChangesBetweenPaginationToken, ChangesBetweenTimeRange],
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
      case Right(ChangesBetweenTimeRange(fromInclusive, toExclusive)) =>
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

  override def checkReplacesInvariant(upToInclusive: RecordTime)(implicit
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
                  window.digestUpdate.recordTime,
                  window.replacesRecordTime,
                )
              ) // (keyString, t_i, rt_i)
            }

          timeSeriesTuples.foreach { // find one where the link is broken
            // if `t_i` < `r_ti` --> referring to the future is not possible
            case (key, currentRecordTime, Some(currentReplacesRecordTime)) :: _
                if currentReplacesRecordTime >= currentRecordTime =>
              ErrorUtil.invalidState(
                s"ReplacesRecordTime check error for Key $key - " +
                  s"We cannot have replacesTime=$currentReplacesRecordTime which is gte to recordTime $currentRecordTime"
              )
            // if `1 < i <= M`, `rt_i` should be Some
            case (key, rt, None) :: (_, recordTime, _) :: _ =>
              ErrorUtil.invalidState(
                s"ReplacesRecordTime check error for key $key at $rt - " +
                  s"Replaces time should point to $recordTime but it is empty!"
              )
            // If `1 < i <= M` and `rt_i` != `t_i - 1`:
            case (key, currentRecordTime, Some(currentReplacesRecordTime)) :: (
                  _,
                  precedingRecordTime,
                  _,
                ) :: _ if currentReplacesRecordTime != precedingRecordTime =>
              ErrorUtil.invalidState(
                s"ReplacesRecordTime check error for key $key - " +
                  s"We cannot have an entry with (recordTime=$currentRecordTime, replacesTime=$currentReplacesRecordTime) " +
                  s"which is not pointing to the preceding recordTime=$precedingRecordTime"
              )
            // eg. when rt_i doesn't have updates in the snapshot journal
            case _ => ()
          }
        }
    }
  }

  def deleteAfter(fromExclusive: RecordTime)(implicit
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

  def deleteUpTo(toExclusive: RecordTime)(implicit
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

  private def lookupInternal(key: K, toInclusive: RecordTime): Option[AcsDigestUpdate[K, V]] =
    for {
      history <- journal.get(key)
      (_, update) <- history.rangeTo(toInclusive).lastOption
    } yield update
}

object InMemoryAcsDigestJournal {
  final case class SnapshotToken[K, V](remaining: LazyList[AcsDigest[K, V]])
}
