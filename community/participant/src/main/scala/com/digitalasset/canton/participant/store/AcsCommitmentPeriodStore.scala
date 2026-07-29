// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.either.*
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  HashedDigest,
  InternedParticipantId,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.store.PrunableByTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.IterableUtil
import com.digitalasset.canton.util.{
  DisjointIntervals,
  ErrorUtil,
  HexString,
  IntervalOps,
  MergeableDisjointIntervals,
}
import com.digitalasset.canton.{LedgerParticipantId, checked}
import com.digitalasset.nonempty.NonEmpty
import com.google.common.annotations.VisibleForTesting
import pprint.Tree
import slick.jdbc.GetResult

import scala.collection.immutable

/** Stores the matching state for all periods for which the participant expects to receive
  * commitments from counterparticipants.
  */
trait AcsCommitmentPeriodStore extends PrunableByTime { this: NamedLogging =>
  import AcsCommitmentPeriodStore.*

  protected def stringInterning: StringInterning
  @VisibleForTesting @inline private[canton] final def stringInterningInternal: StringInterning =
    stringInterning

  /** Returns all outstanding rows whose periods overlap with the given period for the given
    * participants.
    */
  def lookupOutstanding(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[OutstandingCommitmentMatchPeriod]]

  /** Returns all mismatched or unexpected rows whose periods overlap with the given period for the
    * given participants.
    */
  def lookupMismatchedOrUnexpected(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod]]

  /** Returns all mismatched rows whose periods overlap with the given period for the given
    * counterparticipant and for the expected hashed digest.
    */
  def lookupMismatchedByHash(
      periods: immutable.Iterable[(InternedParticipantId, HashedDigest, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MismatchedCommitmentMatchPeriod]]

  /** Returns all matched rows whose periods overlap with the given period for the given
    * participants.
    */
  def lookupMatched(
      periods: immutable.Iterable[(InternedParticipantId, CommitmentPeriod)]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[immutable.Iterable[MatchedCommitmentMatchPeriod]]

  /** Returns the watermarks. */
  def watermarks()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[MatchingWatermark]

  /** Increases the watermarks that separate insertion from matching. Does nothing if the watermarks
    * are already higher.
    *
    * Must not execute concurrently with [[markOutstanding]].
    *
    * @param affirmationOnly
    *   If true, only the affirmation watermark is increased.
    */
  def increaseInsertionWatermark(watermark: CantonTimestamp, affirmationOnly: Boolean)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Increases the watermark for the
    * [[com.digitalasset.canton.participant.commitment.ReceivedAcsCommitmentMatcher]]. Does nothing
    * if the watermark is already higher.
    */
  def increaseMatcherWatermark(offset: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Inserts the hashed digests for the participants into the store. If a
    * [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.toInclusive]]
    * timestamp is at or below the
    * [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.MatchingWatermark.affirmation]],
    * the entry is skipped. Otherwise,
    * [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.fromExclusive]]
    * is modified to be at least the
    * [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.MatchingWatermark.reconciliation]]
    * watermark prior to insertion.
    *
    * If multiple updates for the same participant are given, the caller must ensure that the
    * periods do not overlap. The caller must also ensure that the shortened periods do not overlap
    * with any existing row.
    *
    * Must not execute concurrently with [[increaseInsertionWatermark]].
    */
  def markOutstanding(digests: immutable.Iterable[OutstandingCommitmentMatchPeriod])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Atomically performs the given deletions and insertions on the outstanding, mismatched, and
    * matched. All timestamps must be at or below the
    * [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.MatchingWatermark.affirmation]]
    * watermark. A period is called unexpected if it is mismatched without a hashed digest.
    *
    * The deletions and insertions must constitute a set of valid transitions. In particular, for
    * every participant ID:
    *   - The union of the deleted periods is the union of the inserted periods except for
    *     unexpected intervals.
    *   - The deleted periods are pairwise disjoint.
    *   - The inserted periods must be pairwise disjoint.
    *   - Intervals in `insertOutstanding` must be covered in `deleteOutstanding` such that all
    *     covering intervals specify the same hashed digest.
    *   - Intervals in `insertMismatchedOrUnexpected` with an expected hashed digest must be covered
    *     in `deleteOutstanding` or `deleteMismatched` such that all covering intervals specify the
    *     same hashed digest and offset, if applicable.
    *   - Intervals in `insertMismatchedOrUnexpected` without an expected hashed digest must be
    *     disjoint from all other intervals.
    *   - For intervals in `insertMatched` that are covered in `deleteMismatched`, the offset must
    *     increase non-strictly.
    *
    * Moreover, the deletions must correspond to the exact rows in the store.
    *
    * @param deleteOutstanding
    *   Deletes the rows identified by
    *   [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.participant]]
    *   and
    *   [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.toInclusive]]
    *   if present. The other fields are ignored for the purpose of deletion.
    * @param deleteMismatched
    *   Deletes the rows identified by
    *   [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.participant]]
    *   and
    *   [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.toInclusive]]
    *   if present. The other fields are ignored for the purpose of deletion.
    */
  def persistMatchingOutcome(
      deleteOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      deleteMismatched: immutable.Iterable[MismatchedCommitmentMatchPeriod],
      insertOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      insertMismatchedOrUnexpected: immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod],
      insertMatched: immutable.Iterable[MatchedCommitmentMatchPeriod],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Checks the consistency of the arguments for [[persistMatchingOutcome]]. This method is not
    * efficient and should not be run in performance-critical production deployments.
    */
  protected def checkPersistMatchingOutcomeArgumentConsistency(
      deleteOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      deleteMismatched: immutable.Iterable[MismatchedCommitmentMatchPeriod],
      insertOutstanding: immutable.Iterable[OutstandingCommitmentMatchPeriod],
      insertMismatchedOrUnexpected: immutable.Iterable[MismatchedOrUnexpectedCommitmentMatchPeriod],
      insertMatched: immutable.Iterable[MatchedCommitmentMatchPeriod],
  )(implicit traceContext: TraceContext): Unit = {

    def periodsByParticipant(
        periods: immutable.Iterable[CommitmentMatchPeriod[?, ?]],
        name: String,
    ): Map[InternedParticipantId, MergeableDisjointIntervals[CommitmentPeriod]] =
      periods
        .groupMap(_.participant)(_.commitmentPeriod)
        .map { case (participant, values) =>
          val merged = MergeableDisjointIntervals
            .from(values)
            .valueOr(err =>
              ErrorUtil.invalidArgument(
                s"$name periods are not disjoint for ${participantId(participant)}: $err"
              )
            )
            .mergeAdjacent
          participant -> merged
        }

    val deletePeriodsByParticipant =
      periodsByParticipant(deleteOutstanding.eraseType ++ deleteMismatched, "Deleted")
    val insertPeriodsByParticipant =
      periodsByParticipant(
        insertOutstanding.eraseType ++
          insertMismatchedOrUnexpected.filter(_.hashedDigest.nonEmpty) ++ insertMatched,
        "Inserted",
      )
    deletePeriodsByParticipant.foreach { case (participant, deletePeriods) =>
      val insertPeriods = insertPeriodsByParticipant.getOrElse(
        participant,
        ErrorUtil.invalidArgument(
          s"No periods to insert for ${participantId(participant)}, but periods to delete: $deletePeriods"
        ),
      )
      ErrorUtil.requireArgument(
        deletePeriods == insertPeriods,
        s"Periods to delete and insert do not match for ${participantId(participant)}. To delete = $deletePeriods, to insert = $insertPeriods",
      )
    }
    insertPeriodsByParticipant.foreach { case (participant, insertPeriods) =>
      ErrorUtil.requireArgument(
        deletePeriodsByParticipant.contains(participant),
        s"No periods to delete for ${participantId(participant)}, but periods to insert: $insertPeriods",
      )
    }

    // Explicitly name the type arguments to make scalac happy.
    def checkInsertCovered[Digest1, Off1, Digest2, Off2](
        period: CommitmentMatchPeriod[Digest1, Off1],
        overlapping: immutable.Iterable[CommitmentMatchPeriod[Digest2, Off2]],
        namePeriod: String,
        nameOverlaps: String,
    ): Unit = {
      // Overlapping fully covers the period if all of the following hold:
      // - The first interval starts no later than the period.
      // - The last interval ends no earlier than the period.
      // - All intervals in overlapping are adjacent.
      val overlappingNE = NonEmpty
        .from(overlapping)
        .getOrElse(
          ErrorUtil.invalidArgument(
            s"Inserted $namePeriod period is not covered by deleted $nameOverlaps periods for ${participantId(period.participant)}: $period"
          )
        )
      ErrorUtil.requireArgument(
        overlappingNE.head1.fromExclusive <= period.fromExclusive && overlappingNE.last1.toInclusive >= period.toInclusive,
        s"Inserted $namePeriod period is not covered by deleted $nameOverlaps periods for ${participantId(period.participant)}: $period",
      )
      overlappingNE.tail1
        .foldLeft(overlappingNE.head1) { (prev, next) =>
          ErrorUtil.requireArgument(
            prev.toInclusive == next.fromExclusive,
            s"Inserted $namePeriod period is not covered by deleted $nameOverlaps periods for ${participantId(period.participant)}: $period",
          )
          next
        }
        .discard
    }

    def byParticipant[Digest, Off](
        all: immutable.Iterable[CommitmentMatchPeriod[Digest, Off]]
    ): Map[
      InternedParticipantId,
      DisjointIntervals.Aux[CommitmentMatchPeriod[Digest, Off], CantonTimestamp],
    ] =
      all
        .groupBy(_.participant)
        .view
        .mapValues(periods =>
          // `deletePeriods` above already checked disjointness
          checked(DisjointIntervals.tryFrom(periods))
        )
        .toMap

    val deleteOutstandingByParticipant = byParticipant(deleteOutstanding)
    insertOutstanding.foreach { period =>
      val participant = period.participant
      val deleteOutstandingDI = deleteOutstandingByParticipant.getOrElse(
        participant,
        ErrorUtil.invalidArgument(
          s"No periods to delete from outstanding for ${participantId(participant)}, but period to insert into outstanding: $period"
        ),
      )
      val overlapping = deleteOutstandingDI.overlappingWith(period)
      overlapping.foreach { overlap =>
        ErrorUtil.requireArgument(
          overlap.hashedDigest == period.hashedDigest,
          s"Overlapping deleted and inserted outstanding periods have different digests for ${participantId(participant)}: $overlap vs. $period",
        )
      }
      checkInsertCovered(period, overlapping, "outstanding", "outstanding")
    }

    val deleteMismatchedByParticipant = byParticipant(deleteMismatched)
    insertMismatchedOrUnexpected.foreach { period =>
      val participant = period.participant
      val deleteMismatchedDI = deleteMismatchedByParticipant.getOrElse(
        participant,
        DisjointIntervals.empty[MismatchedOrUnexpectedCommitmentMatchPeriod],
      )
      val deleteOutstandingDI = deleteOutstandingByParticipant.getOrElse(
        participant,
        DisjointIntervals.empty[OutstandingCommitmentMatchPeriod],
      )

      val overlapOutstanding =
        deleteOutstandingDI.overlappingWith(period.fromExclusive, period.toInclusive)
      val overlapMismatched =
        deleteMismatchedDI.overlappingWith(period.fromExclusive, period.toInclusive)
      period.hashedDigest match {
        case Some(expected) =>
          overlapOutstanding.foreach { overlap =>
            ErrorUtil.requireArgument(
              overlap.hashedDigest == expected,
              s"Overlapping deleted outstanding and inserted mismatched periods have different digests for ${participantId(participant)}: $overlap vs. $period",
            )
          }
          overlapMismatched.foreach { overlap =>
            ErrorUtil.requireArgument(
              overlap.hashedDigest.contains(expected),
              s"Overlapping deleted and inserted mismatched periods have different digests for ${participantId(participant)}: $overlap vs $period",
            )
            ErrorUtil.requireArgument(
              overlap.offset == period.offset,
              s"Overlapping deleted and inserted mismatched periods have different offsets for ${participantId(participant)}: $overlap vs $period",
            )
          }
          val allOverlapping =
            IterableUtil
              .mergeSortedBy(overlapOutstanding.eraseType, overlapMismatched)(_.toInclusive)
              .toSeq
          checkInsertCovered(period, allOverlapping, "mismatched", "outstanding or mismatched")
        case None =>
          ErrorUtil.requireArgument(
            overlapOutstanding.isEmpty,
            s"Unexpected commitment overlaps with outstanding period for ${participantId(participant)}: $overlapOutstanding",
          )
          ErrorUtil.requireArgument(
            overlapMismatched.isEmpty,
            s"Unexpected commitment overlaps with mismatched period for ${participantId(participant)}: $overlapMismatched",
          )
      }
    }

    insertMatched.foreach { period =>
      val participant = period.participant
      val deleteMismatchedDI = deleteMismatchedByParticipant.getOrElse(
        participant,
        DisjointIntervals.empty[MismatchedCommitmentMatchPeriod],
      )
      val overlapMismatched =
        deleteMismatchedDI.overlappingWith(period.fromExclusive, period.toInclusive)
      overlapMismatched.foreach { overlap =>
        ErrorUtil.requireArgument(
          overlap.offset <= period.offset,
          s"Overlapping deleted mismatched and inserted matched periods have non-increasing offsets for ${participantId(
              participant
            )}: ${overlap.offset} vs ${period.offset}",
        )
      }
    }
  }

  private def participantId(interned: InternedParticipantId): LedgerParticipantId =
    stringInterning.participantId.externalize(interned)

  /** Checks the store invariant:
    *
    *   - The
    *     [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.MatchingWatermark.affirmation]]
    *     watermark is at least the
    *     [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.MatchingWatermark.reconciliation]]
    *     watermark.
    *   - Periods in outstanding, mismatched, and matched are pairwise disjoint for each
    *     participant.
    *   - [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.toInclusive]]
    *     in mismatched and matched rows is at most the
    *     [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.MatchingWatermark.affirmation]]
    *     watermark.
    *   - For all stored periods,
    *     [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.fromExclusive]]
    *     is less than
    *     [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.toInclusive]].
    */
  def checkInvariant()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Deletes all rows from outstanding whose
    * [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.toInclusive]]
    * is after `timestamp`.
    */
  def deleteOutstandingAfter(fromExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Deletes all rows whose
    * [[com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.CommitmentMatchPeriod.toInclusive]]
    * is smaller than `limit`.
    */
  override protected def doPrune(
      limit: CantonTimestamp,
      lastPruning: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int]

  override protected def kind: String = "ACS commitment matching state"
}

object AcsCommitmentPeriodStore {
  final case class MatchingWatermark(
      reconciliation: CantonTimestamp,
      affirmation: CantonTimestamp,
      matching: Option[Offset],
  ) {
    def bump(timestamp: CantonTimestamp, affirmationOnly: Boolean): MatchingWatermark =
      if (affirmationOnly) {
        copy(affirmation = affirmation.max(timestamp))
      } else {
        copy(
          reconciliation = reconciliation.max(timestamp),
          affirmation = affirmation.max(timestamp),
        )
      }

    def bump(matching: Offset): MatchingWatermark =
      if (this.matching.forall(_ < matching)) copy(matching = Some(matching)) else this
  }
  object MatchingWatermark {
    val initial: MatchingWatermark =
      MatchingWatermark(CantonTimestamp.MinValue, CantonTimestamp.MinValue, None)

    implicit val getResultMatchingWatermark: GetResult[MatchingWatermark] = GetResult { r =>
      val reconciliation = r.<<[CantonTimestamp]
      val affirmation = r.<<[CantonTimestamp]
      val matching = r.<<[Option[Offset]]
      MatchingWatermark(reconciliation, affirmation, matching)
    }
  }

  final case class CommitmentMatchPeriod[+Digest, +Off](
      participant: InternedParticipantId,
      fromExclusive: CantonTimestamp,
      toInclusive: CantonTimestamp,
      hashedDigest: Digest,
      offset: Off,
  ) extends PrettyPrintingFromCompanion {
    def capBelow(cap: CantonTimestamp): CommitmentMatchPeriod[Digest, Off] =
      if (fromExclusive < cap) copy(fromExclusive = cap) else this

    def commitmentPeriod: CommitmentPeriod = CommitmentPeriod.tryCreate(fromExclusive, toInclusive)

    override def prettyCompanion: PrettyPrintingCompanion[CommitmentMatchPeriod.this.type] =
      CommitmentMatchPeriod

    /** Returns, first, the remainders of this interval when (`startExclusive`, `endInclusive`] are
      * removed, and, second, the intersection of this interval and (`startExclusive`,
      * `endInclusive`]. Assumes that this interval overlaps with (`startExclusive`,
      * `endInclusive`].
      */
    def partition(
        startExclusive: CantonTimestamp,
        endInclusive: CantonTimestamp,
    ): (Seq[CommitmentMatchPeriod[Digest, Off]], CommitmentMatchPeriod[Digest, Off]) =
      (startExclusive > this.fromExclusive, endInclusive < this.toInclusive) match {
        case (true, true) =>
          // Both sides are cut off
          val remainders =
            Seq(this.copy(toInclusive = startExclusive), this.copy(fromExclusive = endInclusive))
          val restricted = this.copy(fromExclusive = startExclusive, toInclusive = endInclusive)
          remainders -> restricted
        case (false, true) =>
          // Right side is cut off
          val remainder = this.copy(fromExclusive = endInclusive)
          val restricted = this.copy(toInclusive = endInclusive)
          Seq(remainder) -> restricted
        case (true, false) =>
          // Left side is cut off
          val remainder = this.copy(toInclusive = startExclusive)
          val restricted = this.copy(fromExclusive = startExclusive)
          Seq(remainder) -> restricted
        case (false, false) =>
          // Nothing to cut off
          Seq.empty -> this
      }
  }

  /** An commitment period is outstanding when this participant has computed it and not yet
    * processed any counterparticipant's commitment for this period yet.
    */
  type OutstandingCommitmentMatchPeriod = CommitmentMatchPeriod[HashedDigest, Unit]

  /** A commitment period is mismatched or unexpected when the participant has processed at least
    * one commitment for the period, but none of these commitments matched what the participant
    * expected. The offset refers to the first such commitment.
    */
  type MismatchedOrUnexpectedCommitmentMatchPeriod =
    CommitmentMatchPeriod[Option[HashedDigest], Offset]

  /** A commitment period is mismatched when the participant has processed at least one commitment
    * for the period, but none of these commitments matched what the participant had computed
    * locally. The offset refers to the first such commitment.
    */
  type MismatchedCommitmentMatchPeriod = CommitmentMatchPeriod[Some[HashedDigest], Offset]

  /** A commitment period is matched when the participant has processed at least one commitment for
    * the period and this commitment matched the hashed digest that the participant had expected.
    * The offset refers to the first such commitment.
    */
  type MatchedCommitmentMatchPeriod = CommitmentMatchPeriod[Unit, Offset]

  object CommitmentMatchPeriod extends PrettyPrintingCompanion[CommitmentMatchPeriod[Any, Any]] {
    def outstanding(
        participant: InternedParticipantId,
        fromExclusive: CantonTimestamp,
        toInclusive: CantonTimestamp,
        hashedDigest: HashedDigest,
    ): OutstandingCommitmentMatchPeriod =
      CommitmentMatchPeriod(participant, fromExclusive, toInclusive, hashedDigest, ())

    def unexpected(
        participant: InternedParticipantId,
        fromExclusive: CantonTimestamp,
        toInclusive: CantonTimestamp,
        offset: Offset,
    ): MismatchedOrUnexpectedCommitmentMatchPeriod =
      CommitmentMatchPeriod(participant, fromExclusive, toInclusive, None, offset)

    def mismatched(
        participant: InternedParticipantId,
        fromExclusive: CantonTimestamp,
        toInclusive: CantonTimestamp,
        hashedDigest: HashedDigest,
        offset: Offset,
    ): MismatchedCommitmentMatchPeriod =
      CommitmentMatchPeriod(participant, fromExclusive, toInclusive, Some(hashedDigest), offset)

    def matched(
        participant: InternedParticipantId,
        fromExclusive: CantonTimestamp,
        toInclusive: CantonTimestamp,
        offset: Offset,
    ): MatchedCommitmentMatchPeriod =
      CommitmentMatchPeriod(participant, fromExclusive, toInclusive, (), offset)

    override val pretty: Pretty[CommitmentMatchPeriod[Any, Any]] = {
      def prettyHashedDigest(inst: CommitmentMatchPeriod[Any, Any]): Option[Tree] =
        inst.hashedDigest match {
          case bytes: HashedDigest =>
            val hexTruncated = HexString.toHexString(bytes, 16) + "..."
            val tree = Tree.Infix(Tree.Literal("hashed digest"), "=", Tree.Literal(hexTruncated))
            Some(tree)
          case _ => None
        }
      def treeOfOffset(offset: Offset) =
        Tree.Infix(Tree.Literal("offset"), "=", Tree.Literal(offset.toDecimalString))
      def prettyOffset(inst: CommitmentMatchPeriod[Any, Any]): Option[Tree] =
        inst.offset match {
          case offset: Offset => Some(treeOfOffset(offset))
          case optional: Option[?] =>
            optional.flatMap {
              case offset: Offset => Some(treeOfOffset(offset))
              case _ => None
            }
          case _ => None
        }

      prettyOfClass(
        // Participant should be externalized
        param("participant", _.participant),
        param("fromExclusive", _.fromExclusive),
        param("toInclusive", _.toInclusive),
        prettyHashedDigest,
        prettyOffset,
      )
    }

    implicit def intervalOpsCommitmentMatchPeriod[Digest, Off]
        : IntervalOps.Aux[CommitmentMatchPeriod[Digest, Off], CantonTimestamp] =
      new IntervalOps[CommitmentMatchPeriod[Digest, Off]] {
        override type Point = CantonTimestamp

        override def ordering: Ordering[CantonTimestamp] = implicitly

        override def startExclusive(interval: CommitmentMatchPeriod[Digest, Off]): CantonTimestamp =
          interval.fromExclusive

        override def endInclusive(interval: CommitmentMatchPeriod[Digest, Off]): CantonTimestamp =
          interval.toInclusive

        override def pretty: Pretty[CommitmentMatchPeriod[Digest, Off]] =
          CommitmentMatchPeriod.pretty

        override def split(
            interval: CommitmentMatchPeriod[Digest, Off],
            startExclusive: CantonTimestamp,
            endInclusive: CantonTimestamp,
        ): Seq[CommitmentMatchPeriod[Digest, Off]] = {
          val (outside, _) = interval.partition(startExclusive, endInclusive)
          outside
        }
      }

    implicit class EraseCommitmentMatchPeriodTypeParameters[F[+_], Digest, Off](
        private val fc: F[CommitmentMatchPeriod[Digest, Off]]
    ) extends AnyVal {

      /** Erases the type arguments to a [[CommitmentMatchPeriod]] in any covariant contexts. This
        * prevents the compiler from inferring [[scala.Any]] and [[scala.AnyVal]] when collections
        * over varying type instances are formed.
        */
      def eraseType: F[CommitmentMatchPeriod[?, ?]] = {
        val ev = implicitly[CommitmentMatchPeriod[Digest, Off] <:< CommitmentMatchPeriod[?, ?]]
        ev.substituteCo[F](fc)
      }
    }
  }
}
