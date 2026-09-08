// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.{
  CommitmentMatchPeriod,
  MatchingWatermark,
}
import com.digitalasset.canton.participant.store.AcsDigestStore.InternedParticipantId
import com.digitalasset.canton.platform.store.interning.{MockStringInterning, StringInterning}
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.store.PrunableByTimeTest
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.util.ErrorUtil.internalErrorMessage
import com.digitalasset.canton.version.HasNonImplicitTestCloseContext
import com.digitalasset.canton.{BaseTest, InUS, ProtocolVersionChecksAsyncWordSpec}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions
import scala.util.Try

trait AcsCommitmentPeriodStoreTest
    extends AsyncWordSpec
    with BaseTest
    with PrunableByTimeTest
    with ProtocolVersionChecksAsyncWordSpec
    with HasNonImplicitTestCloseContext
    with InUS {

  def acsCommitmentPeriodStore(
      mk: (StringInterning, Boolean, ExecutionContext) => AcsCommitmentPeriodStore
  ): Unit = {
    val stringInterning = new MockStringInterning
    val p1 = stringInterning.participantId.internalize(DefaultTestIdentities.participant1.toLf)
    val p2 = stringInterning.participantId.internalize(DefaultTestIdentities.participant2.toLf)
    val p3 = stringInterning.participantId.internalize(DefaultTestIdentities.participant3.toLf)

    def newStore(enableConsistencyChecks: Boolean = true) =
      mk(stringInterning, enableConsistencyChecks, executionContext)

    "increaseWatermarks" should {
      "increase the watermark" inUS {
        val store = newStore()
        for {
          empty <- store.watermark()
          _ <- store.increaseWatermark(off(10))
          off10 <- store.watermark()
        } yield {
          empty shouldBe MatchingWatermark.initial
          off10 shouldBe MatchingWatermark(Some(off(10)))
        }
      }

      "never decrease" inUS {
        val store = newStore()
        for {
          empty <- store.watermark()
          _ <- store.increaseWatermark(off(10))
          off10 <- store.watermark()
          _ <- store.increaseWatermark(off(5))
          off10a <- store.watermark()
        } yield {
          empty shouldBe MatchingWatermark.initial
          off10 shouldBe MatchingWatermark(Some(off(10)))
          off10a shouldBe off10
        }
      }
    }

    "markOutstanding" should {
      "insert periods" inUS {
        val store = newStore()
        val periods = Seq(
          CommitmentMatchPeriod.outstanding(p1, ts(1), ts(10), "P1:1-10"),
          CommitmentMatchPeriod.outstanding(p1, ts(12), ts(20), "P1:12-20"),
          CommitmentMatchPeriod.outstanding(p2, ts(4), ts(15), "P2:4-15"),
          CommitmentMatchPeriod.outstanding(p3, ts(10), ts(15), "P3:10-15"),
        )
        for {
          _ <- store.markOutstanding(periods)
          tooEarly <- store.lookupOutstanding(Seq(p1 -> cp(0, 1), p2 -> cp(0, 3)))
          hitAll <- store.lookupOutstanding(Seq(p1 -> cp(4, 14), p2 -> cp(0, 5), p3 -> cp(14, 17)))
          tooLate <- store.lookupOutstanding(Seq(p1 -> cp(20, 100), p2 -> cp(21, 22)))
        } yield {
          tooEarly.toSeq shouldBe Seq.empty
          hitAll.toSeq should contain theSameElementsAs periods
          tooLate shouldBe Seq.empty
        }
      }
    }

    "persistMatchingOutcome" should {
      "move from outstanding/mismatched to mismatched/matched" inUS {
        val store = newStore()
        val periods = Seq(
          CommitmentMatchPeriod.outstanding(p1, ts(1), ts(10), "P1:1-10"),
          CommitmentMatchPeriod.outstanding(p1, ts(12), ts(20), "P1:12-20"),
          CommitmentMatchPeriod.outstanding(p1, ts(20), ts(25), "P1:20-25"),
          CommitmentMatchPeriod.outstanding(p2, ts(1), ts(15), "P2:1-15"),
          CommitmentMatchPeriod.outstanding(p3, ts(5), ts(11), "P3:5-11"),
        )
        val insertOutstanding = Seq(
          CommitmentMatchPeriod.outstanding(p1, ts(1), ts(5), "P1:1-10"),
          CommitmentMatchPeriod.outstanding(p1, ts(15), ts(20), "P1:12-20"),
          CommitmentMatchPeriod.outstanding(p2, ts(10), ts(15), "P2:1-15"),
        )
        val insertMismatched = Seq(
          CommitmentMatchPeriod.mismatched(p1, ts(5), ts(10), "P1:1-10", off(15)),
          CommitmentMatchPeriod.mismatched(p1, ts(12), ts(15), "P1:12-20", off(15)),
        )
        val insertUnexpected = Seq(
          CommitmentMatchPeriod.unexpected(p3, ts(1), ts(4), off(123))
        )
        val insertMatched = Seq(
          CommitmentMatchPeriod.matched(p2, ts(1), ts(10), off(23))
        )
        val insertMismatched2 = Seq(
          CommitmentMatchPeriod.mismatched(p1, ts(12), ts(13), "P1:12-20", off(15)),
          CommitmentMatchPeriod.mismatched(p1, ts(14), ts(15), "P1:12-20", off(15)),
        )
        val insertMatched2 = Seq(
          CommitmentMatchPeriod.matched(p1, ts(13), ts(14), off(16))
        )
        val lookupPeriods = Seq(p1 -> cp(0, 25), p2 -> cp(0, 25), p3 -> cp(0, 25))
        for {
          _ <- store.markOutstanding(periods)
          lo <- store.lookupOutstanding(lookupPeriods)
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq(periods(0), periods(1), periods(3)),
            deleteMismatched = Seq.empty,
            insertOutstanding = insertOutstanding,
            insertMismatchedOrUnexpected = insertMismatched ++ insertUnexpected,
            insertMatched = insertMatched,
          )
          outstanding <- store.lookupOutstanding(lookupPeriods)
          mismatched <- store.lookupMismatchedOrUnexpected(lookupPeriods)
          mismatchedByHash <- store.lookupMismatchedByHash(
            Seq((p1, "P1:1-10", cp(8, 13)), (p3, "P3:5-11", cp(5, 11)))
          )
          matched <- store.lookupMatched(lookupPeriods)
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq(insertMismatched(1)),
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = insertMismatched2,
            insertMatched = insertMatched2,
          )
          outstanding2 <- store.lookupOutstanding(lookupPeriods)
          mismatched2 <- store.lookupMismatchedOrUnexpected(lookupPeriods)
          mismatchedByHash2 <- store.lookupMismatchedByHash(
            Seq((p1, "P1:12-20", cp(12, 15)))
          )
          mismatchedByHash2a <- store.lookupMismatchedByHash(
            Seq((p1, "P1:12-20", cp(13, 14)))
          )
          mismatchedByHash2b <- store.lookupMismatchedByHash(
            Seq((p1, "P1:12-20", cp(14, 15)))
          )
          matched2 <- store.lookupMatched(lookupPeriods)
        } yield {
          lo should contain theSameElementsAs periods

          outstanding should contain theSameElementsAs
            (periods(2) +: periods(4) +: insertOutstanding)
          mismatched should contain theSameElementsAs (insertMismatched ++ insertUnexpected)
          mismatchedByHash should contain theSameElementsAs Seq(insertMismatched(0))
          matched should contain theSameElementsAs insertMatched

          outstanding2 should contain theSameElementsAs outstanding
          mismatched2 should contain theSameElementsAs
            (insertMismatched(0) +: (insertMismatched2 ++ insertUnexpected))
          mismatchedByHash2 should contain theSameElementsAs insertMismatched2
          mismatchedByHash2a shouldBe empty
          mismatchedByHash2b should contain theSameElementsAs Seq(insertMismatched2(1))
          matched2 should contain theSameElementsAs
            (insertMatched2 ++ insertMatched)
        }
      }
    }

    "deleteOutstandingAfter" should {
      "delete all outstanding periods after the given timestamp" inUS {
        val store = newStore()
        val periodsToKeep = Seq(
          CommitmentMatchPeriod.outstanding(p1, ts(1), ts(10), "P1:1-10"),
          CommitmentMatchPeriod.outstanding(p2, ts(4), ts(15), "P2:4-15"),
          CommitmentMatchPeriod.outstanding(p3, ts(1), ts(10), "P3:1-10"),
        )
        val periodsToDelete = Seq(
          CommitmentMatchPeriod.outstanding(p1, ts(15), ts(20), "P1:12-20"),
          CommitmentMatchPeriod.outstanding(p1, ts(22), ts(25), "P1:22-25"),
          CommitmentMatchPeriod.outstanding(p3, ts(10), ts(15).immediateSuccessor, "P3:10-15+"),
        )
        for {
          _ <- store.markOutstanding(periodsToKeep ++ periodsToDelete)
          _ <- store.deleteOutstandingAfter(ts(15))
          remaining <- store.lookupOutstanding(
            Seq(p1 -> cp(0, 100), p2 -> cp(0, 100), p3 -> cp(0, 100))
          )
        } yield {
          remaining should contain theSameElementsAs periodsToKeep
        }
      }
    }

    "prune" should {
      "prune all entries before the given timestamp" inUS {
        val store = newStore()
        val periodsToPrune = Seq(
          CommitmentMatchPeriod.outstanding(p1, ts(1), ts(10), "P1:1-10"),
          CommitmentMatchPeriod.outstanding(p2, ts(4), ts(14).immediatePredecessor, "P2:4-14-"),
          CommitmentMatchPeriod.outstanding(p3, ts(1), ts(2), "P3:1-2"),
          CommitmentMatchPeriod.outstanding(p3, ts(5), ts(7), "P3:5-7"),
        )
        val periodsToKeep = Seq(
          CommitmentMatchPeriod.outstanding(p1, ts(11), ts(25), "P1:22-25"),
          CommitmentMatchPeriod.outstanding(p2, ts(14), ts(20), "P1:14-20"),
          CommitmentMatchPeriod.outstanding(p3, ts(10), ts(14), "P3:10-14"),
        )
        implicit val closeContext: CloseContext = testCloseContext
        for {
          _ <- store.markOutstanding(periodsToKeep ++ periodsToPrune)
          _ <- store.prune(ts(14))
          remaining <- store.lookupOutstanding(
            Seq(p1 -> cp(0, 100), p2 -> cp(0, 100), p3 -> cp(0, 100))
          )
        } yield {
          remaining should contain theSameElementsAs periodsToKeep
        }
      }

      val enableConsistencyChecks = true
      prunableByTime(mk(stringInterning, enableConsistencyChecks, _))
    }

    "checkInvariant" should {
      def checkInvariant(
          store: AcsCommitmentPeriodStore,
          affirmationWatermark: Option[CantonTimestamp],
      ): FutureUnlessShutdown[Throwable] =
        for {
          failure <- loggerFactory.assertLogs(
            FutureUnlessShutdown
              .fromTry(Try(store.checkInvariant(affirmationWatermark)))
              .flatten
              .failed,
            _.errorMessage shouldBe internalErrorMessage,
          )
        } yield {
          failure shouldBe an[IllegalStateException]
          failure
        }

      def assertInvalidPeriodError(
          failure: Throwable,
          participantId: InternedParticipantId,
          fromExclusive: CantonTimestamp,
          toInclusive: CantonTimestamp,
      ): Assertion =
        failure.getMessage should include(
          s"Invalid period for participant ${stringInterning.participantId.externalize(participantId)}: fromExclusive $fromExclusive must be less than toInclusive $toInclusive"
        )

      "detect an invalid period" inUS {
        val store = newStore(enableConsistencyChecks = false)
        val invalidPeriodOutstanding =
          CommitmentMatchPeriod.outstanding(p1, ts(10), ts(10), "P1:10-10")
        val invalidPeriodMismatched =
          CommitmentMatchPeriod.mismatched(p1, ts(10), ts(10), "P1:10-10", off(12))
        val invalidPeriodMatched = CommitmentMatchPeriod.matched(p1, ts(10), ts(10), off(13))
        for {
          _ <- store.markOutstanding(Seq(invalidPeriodOutstanding))
          failureOutstanding <- checkInvariant(store, None)
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq(invalidPeriodOutstanding),
            deleteMismatched = Seq.empty,
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq(invalidPeriodMismatched),
            insertMatched = Seq.empty,
          )
          failureMismatched <- checkInvariant(store, None)
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq(invalidPeriodMismatched),
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq.empty,
            insertMatched = Seq(invalidPeriodMatched),
          )
          failureMatched <- checkInvariant(store, None)
        } yield {
          assertInvalidPeriodError(failureOutstanding, p1, ts(10), ts(10))
          assertInvalidPeriodError(failureMismatched, p1, ts(10), ts(10))
          assertInvalidPeriodError(failureMatched, p1, ts(10), ts(10))
        }
      }

      "detect overlapping periods" inUS {
        val store = newStore(enableConsistencyChecks = false)
        val periodUnexpected = CommitmentMatchPeriod.unexpected(p1, ts(2), ts(5), off(23))
        val periodOutstanding1 = CommitmentMatchPeriod.outstanding(p1, ts(1), ts(10), "P1:1-10")
        val periodOutstanding2a = CommitmentMatchPeriod.outstanding(p1, ts(20), ts(30), "P1:20-30")
        val periodOutstanding2b = CommitmentMatchPeriod.outstanding(p1, ts(21), ts(29), "P1:21-29")
        val periodMismatched2a =
          CommitmentMatchPeriod.mismatched(p1, ts(19), ts(31), "P1:20-30", off(34))
        val periodMismatched2b =
          CommitmentMatchPeriod.mismatched(p1, ts(22), ts(28), "P1:21-29", off(32))
        val periodMismatched3a =
          CommitmentMatchPeriod.mismatched(p1, ts(40), ts(42), "P2:40-42", off(44))
        val periodUnexpected3b =
          CommitmentMatchPeriod.unexpected(
            p1,
            ts(42).immediatePredecessor,
            ts(42).immediateSuccessor,
            off(45),
          )
        val periodMatched2a = CommitmentMatchPeriod.matched(p1, ts(18), ts(32), off(45))
        val periodMatched2b = CommitmentMatchPeriod.matched(p1, ts(23), ts(27), off(33))
        implicit val closeContext: CloseContext = testCloseContext
        for {
          _ <- store.markOutstanding(Seq(periodOutstanding1))
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq.empty,
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq(periodUnexpected),
            insertMatched = Seq.empty,
          )
          failureUnexpected <- checkInvariant(store, Some(ts(8)))
          _ <- store.prune(ts(12))
          _ <- store.markOutstanding(Seq(periodOutstanding2a, periodOutstanding2b))
          failureOutstanding <- checkInvariant(store, Some(ts(30)))
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq(periodOutstanding2b),
            deleteMismatched = Seq.empty,
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq(periodMismatched2b),
            insertMatched = Seq.empty,
          )
          failureOutstandingMismatched <- checkInvariant(store, Some(ts(30)))
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq(periodMismatched2b),
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq.empty,
            insertMatched = Seq(periodMatched2b),
          )
          failureOutstandingMatched <- checkInvariant(store, Some(ts(30)))
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq(periodOutstanding2a),
            deleteMismatched = Seq.empty,
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq(periodMismatched2a),
            insertMatched = Seq.empty,
          )
          failureMismatchedMatched <- checkInvariant(store, Some(ts(30)))
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq(periodMismatched2a),
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq.empty,
            insertMatched = Seq(periodMatched2a),
          )
          failureMatched <- checkInvariant(store, Some(ts(30)))
          _ <- store.prune(ts(35))
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq.empty,
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq(periodMismatched3a, periodUnexpected3b),
            insertMatched = Seq.empty,
          )
          failureMismatched <- checkInvariant(store, Some(ts(30)))
        } yield {
          val overlapMsg =
            s"Overlapping periods for participant ${stringInterning.participantId.externalize(p1)}"
          failureUnexpected.getMessage should (include(overlapMsg) and
            include(ts(1).toString) and include(ts(10).toString) and
            include(ts(2).toString) and include(ts(5).toString))
          failureOutstanding.getMessage should (include(overlapMsg) and
            include(ts(20).toString) and include(ts(30).toString) and
            include(ts(21).toString) and include(ts(29).toString))
          failureOutstandingMismatched.getMessage should (include(overlapMsg) and
            include(ts(20).toString) and include(ts(30).toString) and
            include(ts(22).toString) and include(ts(28).toString))
          failureOutstandingMatched.getMessage should (include(overlapMsg) and
            include(ts(20).toString) and include(ts(30).toString) and
            include(ts(23).toString) and include(ts(27).toString))
          failureMismatchedMatched.getMessage should (include(overlapMsg) and
            include(ts(19).toString) and include(ts(31).toString) and
            include(ts(23).toString) and include(ts(27).toString))
          failureMatched.getMessage should (include(overlapMsg) and
            include(ts(18).toString) and include(ts(32).toString) and
            include(ts(23).toString) and include(ts(27).toString))
          failureMismatched.getMessage should (include(overlapMsg) and
            include(ts(40).toString) and include(ts(42).toString) and
            include(ts(42).immediatePredecessor.toString) and
            include(ts(42).immediateSuccessor.toString))
        }
      }

      "detect watermark consistency violation" inUS {
        val store = newStore(enableConsistencyChecks = false)
        val periodMismatched =
          CommitmentMatchPeriod.mismatched(p1, ts(10), ts(12), "P1:10-12", off(1))
        val periodMatched =
          CommitmentMatchPeriod.matched(p1, ts(12), ts(13), off(2))
        for {
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq.empty,
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq(periodMismatched),
            insertMatched = Seq.empty,
          )
          failureNoWatermark <- checkInvariant(store, Some(CantonTimestamp.MinValue))
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq.empty,
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq(periodMismatched),
            insertMatched = Seq.empty,
          )
          failureMismatched <- checkInvariant(store, Some(ts(11)))
          _ <- store.persistMatchingOutcome(
            deleteOutstanding = Seq.empty,
            deleteMismatched = Seq.empty,
            insertOutstanding = Seq.empty,
            insertMismatchedOrUnexpected = Seq.empty,
            insertMatched = Seq(periodMatched),
          )
          failureMatched <- checkInvariant(
            store,
            Some(periodMatched.toInclusive.immediatePredecessor),
          )
        } yield {
          failureNoWatermark.getMessage should include(
            s"Mismatched period (${ts(10)}, ${ts(12)}] for participant ${stringInterning.participantId
                .externalize(p1)} exceeds affirmation watermark ${CantonTimestamp.MinValue}"
          )
          failureMismatched.getMessage should include(
            s"Mismatched period (${ts(10)}, ${ts(12)}] for participant ${stringInterning.participantId
                .externalize(p1)} exceeds affirmation watermark ${ts(11)}"
          )
          failureMatched.getMessage should include(
            s"Matched period (${ts(12)}, ${ts(13)}] for participant ${stringInterning.participantId
                .externalize(p1)} exceeds affirmation watermark ${ts(13).immediatePredecessor}"
          )
        }
      }
    }

  }

  private def ts(i: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(i.toLong)
  private def cp(start: Int, end: Int): CommitmentPeriod =
    CommitmentPeriod.tryCreate(ts(start), ts(end))
  private def off(i: Int): Offset = Offset.tryFromLong(i.toLong)

  private implicit def bytestringFromString(s: String): ByteString = ByteString.copyFromUtf8(s)
}
