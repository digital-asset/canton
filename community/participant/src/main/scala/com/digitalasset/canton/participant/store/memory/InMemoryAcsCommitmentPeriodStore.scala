// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import cats.Eval
import cats.syntax.either.*
import com.digitalasset.canton.SynchronizedLikeMethod
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.{
  CommitmentMatchPeriod,
  MatchedCommitmentMatchPeriod,
  MatchingWatermark,
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
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{DisjointIntervals, ErrorUtil, Mutex}

import scala.collection.{immutable, mutable}
import scala.concurrent.ExecutionContext

final class InMemoryAcsCommitmentPeriodStore(
    stringInterningEval: Eval[StringInterning],
    override protected val loggerFactory: NamedLoggerFactory,
    enableConsistencyChecks: Boolean,
)(implicit
    override protected val ec: ExecutionContext
) extends AcsCommitmentPeriodStore
    with InMemoryPrunableByTime
    with NamedLogging {

  import InMemoryAcsCommitmentPeriodStore.*

  private[this] val lock = new Mutex()

  private val outstanding: Table[HashedDigest, Unit] = mutable.Map.empty
  private val mismatched: Table[Option[HashedDigest], Offset] = mutable.Map.empty
  private val matched: Table[Unit, Offset] = mutable.Map.empty

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var watermarksVar: MatchingWatermark = MatchingWatermark.initial

  override def lookupOutstanding(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[OutstandingCommitmentMatchPeriod]] =
    FutureUnlessShutdown.pure(lookupInternal(outstanding, periods))

  override def lookupMismatchedOrUnexpected(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod]] =
    FutureUnlessShutdown.pure(lookupInternal(mismatched, periods))

  override def lookupMismatchedByHash(
      periods: immutable.Iterable[(InternedParticipantId, HashedDigest, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MismatchedCommitmentMatchPeriod]] =
    FutureUnlessShutdown.pure {
      lock.exclusive {
        periods.flatMap { case (participant, expectedHash, period) =>
          overlaps(mismatched)(participant, period, withExpectedHash(expectedHash))
        }
      }
    }

  private def withExpectedHash(
      expected: HashedDigest
  ): MismatchedOrUnexpectedCommitmentMatchPeriod => Option[MismatchedCommitmentMatchPeriod] = {
    (period: MismatchedOrUnexpectedCommitmentMatchPeriod) =>
      if (period.hashedDigest.contains(expected)) Some(period.copy(hashedDigest = Some(expected)))
      else None
  }

  override def lookupMatched(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MatchedCommitmentMatchPeriod]] =
    FutureUnlessShutdown.pure(lookupInternal(matched, periods))

  override def watermarks()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[MatchingWatermark] =
    FutureUnlessShutdown.pure(lock.exclusive(watermarksVar))

  override def increaseInsertionWatermark(watermark: CantonTimestamp, affirmationOnly: Boolean)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    withLock {
      watermarksVar = watermarksVar.bump(watermark, affirmationOnly)
    }
    FutureUnlessShutdown.unit
  }

  override def increaseMatcherWatermark(offset: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    withLock {
      watermarksVar = watermarksVar.bump(offset)
    }
    FutureUnlessShutdown.unit
  }

  override def markOutstanding(digests: immutable.Iterable[OutstandingCommitmentMatchPeriod])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    withLock {
      val MatchingWatermark(reconciliation, affirmation, _) = watermarksVar
      val (tooEarly, good) = digests.partition(_.toInclusive <= affirmation)
      if (tooEarly.nonEmpty) {
        logger.debug(s"Skip inserting outdated commitment periods: ${tooEarly
            .map { matchPeriod =>
              s"${matchPeriod.participant}:${matchPeriod.fromExclusive}-${matchPeriod.toInclusive}"
            }
            .mkString(", ")}")
      }
      good.foreach { matchPeriod =>
        val periods = outstanding.getOrElseUpdate(
          matchPeriod.participant,
          mutable.SortedMap.empty[CantonTimestamp, OutstandingCommitmentMatchPeriod],
        )
        val capped = matchPeriod.capBelow(reconciliation)
        periods.getOrElseUpdate(capped.toInclusive, capped).discard
      }
    }
    FutureUnlessShutdown.unit
  }

  override def persistMatchingOutcome(
      deleteOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      deleteMismatched: immutable.Iterable[MismatchedCommitmentMatchPeriod],
      insertOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      insertMismatchedOrUnexpected: immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod],
      insertMatched: immutable.Iterable[MatchedCommitmentMatchPeriod],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    withLock {
      if (enableConsistencyChecks) {
        ensurePresentForDeletion(outstanding, deleteOutstanding, "Outstanding")
        ensurePresentForDeletion(mismatched, deleteMismatched, "Mismatched")

        checkPersistMatchingOutcomeArgumentConsistency(
          deleteOutstanding,
          deleteMismatched,
          insertOutstanding,
          insertMismatchedOrUnexpected,
          insertMatched,
        )

        // Since the insertions cover the same time range as the deletions,
        // it suffices to check the timestamps of the deletions against the watermark.
        val affirmation = watermarksVar.affirmation
        ensureAtOrBelow(deleteOutstanding, affirmation, "affirmation watermark")
        ensureAtOrBelow(deleteMismatched, affirmation, "affirmation watermark")
      }

      deleteFrom(outstanding, deleteOutstanding)
      deleteFrom(mismatched, deleteMismatched)
      insertInto(outstanding, insertOutstanding)
      insertInto(mismatched, insertMismatchedOrUnexpected)
      insertInto(matched, insertMatched)
    }
    FutureUnlessShutdown.unit
  }

  override def checkInvariant()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    lock.exclusive(checkInvariantInternal())
    FutureUnlessShutdown.unit
  }

  private def checkInvariantInternal()(implicit traceContext: TraceContext): Unit = {
    def checkGoodPeriods(): Unit = {
      val all: Seq[TableAny] = Seq(outstanding, mismatched, matched)
      all.foreach { table =>
        table.foreach { case (participant, periodMap) =>
          periodMap.foreach { case (_, period) =>
            ErrorUtil.requireState(
              period.fromExclusive < period.toInclusive,
              s"Invalid period for participant ${stringInterning.participantId
                  .externalize(participant)}: fromExclusive ${period.fromExclusive} must be less than toInclusive ${period.toInclusive}",
            )
          }
        }
      }
    }

    def checkWatermarks(): Unit = {
      val MatchingWatermark(reconciliation, affirmation, _) = watermarksVar
      ErrorUtil.requireState(
        affirmation >= reconciliation,
        s"Affirmation watermark $affirmation is below reconciliation watermark $reconciliation",
      )
    }

    def checkDisjoint(): Unit = {
      val all: Seq[TableAny] = Seq(outstanding, mismatched, matched)
      val allParticipants = all.flatMap(_.keySet).toSet
      allParticipants.foreach { participant =>
        val allPeriods = all.flatMap(_.get(participant).map(_.values)).flatten
        DisjointIntervals
          .checkDisjoint(allPeriods)
          .valueOr { err =>
            val participantId = stringInterning.participantId.externalize(participant)
            ErrorUtil.invalidState(s"Overlapping periods for participant $participantId: $err")
          }
      }
    }

    def checkWatermarkBounds[Digest, Off](table: Table[Digest, Off], name: String): Unit = {
      val affirmation = watermarksVar.affirmation
      table.foreach { case (participant, periodMap) =>
        periodMap.lastOption.foreach { case (_, period) =>
          ErrorUtil.requireState(
            period.toInclusive <= affirmation,
            s"$name period (${period.fromExclusive}, ${period.toInclusive}] for participant ${stringInterning.participantId
                .externalize(participant)} exceeds affirmation watermark $affirmation",
          )
        }
      }
    }

    checkWatermarks()
    checkGoodPeriods()
    checkDisjoint()
    checkWatermarkBounds(mismatched, "Mismatched")
    checkWatermarkBounds(matched, "Matched")
  }

  override def deleteOutstandingAfter(fromExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    withLock {
      outstanding.toList.foreach { case (participant, periodMap) =>
        val keysToDelete = periodMap.rangeFrom(fromExclusive.immediateSuccessor).keys.toList
        keysToDelete.foreach(periodMap.remove(_).discard)
        if (periodMap.isEmpty) {
          outstanding.remove(participant).discard
        }
      }
    }
    FutureUnlessShutdown.unit
  }

  override protected def doPrune(
      limit: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    val deletionCount = withLock {
      pruneTable(outstanding, limit) +
        pruneTable(mismatched, limit) +
        pruneTable(matched, limit)
    }
    FutureUnlessShutdown.pure(deletionCount)
  }

  private def pruneTable[Digest, Off](table: Table[Digest, Off], limit: CantonTimestamp): Int =
    table.toList.foldLeft(0) { case (count, (participant, periodMap)) =>
      val deleteAll = periodMap.lastOption.forall { case (end, _) => end < limit }
      if (deleteAll) {
        table.remove(participant).discard
        count + periodMap.size
      } else {
        val keysToDelete = periodMap.keysIterator.takeWhile(_ < limit).toList
        keysToDelete.foreach(periodMap.remove(_).discard)
        count + keysToDelete.size
      }
    }

  @SynchronizedLikeMethod
  private def withLock[A](f: => A)(implicit traceContext: TraceContext): A =
    lock.exclusive {
      if (enableConsistencyChecks) checkInvariantInternal()
      val result = f
      if (enableConsistencyChecks) checkInvariantInternal()
      result
    }

  private def lookupInternal[Digest, Off](
      map: Table[Digest, Off],
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)],
  ): immutable.Iterable[CommitmentMatchPeriod[Digest, Off]] =
    lock.exclusive {
      periods.flatMap { case (participant, period) =>
        overlaps(map)(participant, period)
      }
    }

  private def overlaps[Digest, Off, Digest2, Off2](map: Table[Digest, Off])(
      participant: InternedParticipantId,
      period: CommitmentPeriod,
      mapFilter: CommitmentMatchPeriod[Digest, Off] => Option[
        CommitmentMatchPeriod[Digest2, Off2]
      ] = (period: CommitmentMatchPeriod[Digest, Off]) => Some(period),
  ): immutable.Iterable[CommitmentMatchPeriod[Digest2, Off2]] =
    map.get(participant).toList.flatMap { periodMap =>
      val others = periodMap
        .range(period.fromExclusive.immediateSuccessor, period.toInclusive.immediateSuccessor)
        .valuesIterator
        .flatMap(mapFilter)
      val overlapsRight =
        periodMap.rangeFrom(period.toInclusive.immediateSuccessor).headOption.flatMap {
          case (_, matchPeriod) =>
            if (matchPeriod.fromExclusive < period.toInclusive) mapFilter(matchPeriod) else None
        }
      others ++ overlapsRight.toList
    }

  private def ensureAtOrBelow(
      periods: immutable.Iterable[CommitmentMatchPeriod[?, ?]],
      limit: CantonTimestamp,
      limitName: String,
  )(implicit
      traceContext: TraceContext
  ): Unit =
    periods.foreach { period =>
      ErrorUtil.requireState(
        period.toInclusive <= limit,
        s"Period $period exceeds $limitName $limit",
      )
    }

  private def ensurePresentForDeletion[Digest, Off](
      map: Table[Digest, Off],
      periods: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]],
      kind: String,
  )(implicit traceContext: TraceContext): Unit =
    periods.foreach { period =>
      val present = map
        .get(period.participant)
        .exists(_.get(period.toInclusive).contains(period))
      ErrorUtil.requireState(
        present,
        s"$kind period $period to be deleted does not exist in the store.",
      )
    }

  private def deleteFrom[Digest, Off](
      map: Table[Digest, Off],
      periods: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]],
  ): Unit =
    periods.foreach { period =>
      map.get(period.participant).foreach { periodMap =>
        val removed = periodMap.remove(period.toInclusive)
        if (removed.nonEmpty && periodMap.isEmpty) {
          map.remove(period.participant).discard
        }
      }
    }

  private def insertInto[Digest, Off](
      map: Table[Digest, Off],
      periods: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]],
  ): Unit =
    periods.foreach { period =>
      map
        .getOrElseUpdate(
          period.participant,
          mutable.SortedMap.empty[CantonTimestamp, CommitmentMatchPeriod[Digest, Off]],
        )
        .getOrElseUpdate(period.toInclusive, period)
        .discard
    }

  override protected def stringInterning: StringInterning = stringInterningEval.value
}

object InMemoryAcsCommitmentPeriodStore {
  private type Table[Digest, Off] = mutable.Map[
    InternedParticipantId,
    mutable.SortedMap[CantonTimestamp, CommitmentMatchPeriod[Digest, Off]],
  ]
  private type TableAny = mutable.Map[
    InternedParticipantId,
    ? <: mutable.SortedMap[CantonTimestamp, ? <: CommitmentMatchPeriod[?, ?]],
  ]
}
