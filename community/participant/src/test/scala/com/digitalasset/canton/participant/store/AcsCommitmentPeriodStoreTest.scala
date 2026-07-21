// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.{
  CommitmentMatchPeriod,
  MatchingWatermark,
}
import com.digitalasset.canton.platform.store.interning.{MockStringInterning, StringInterning}
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.store.PrunableByTimeTest
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

class AcsCommitmentPeriodStoreTest extends AsyncWordSpec with BaseTest with PrunableByTimeTest {

  def acsCommitmentPeriodStore(
      mk: (StringInterning, ExecutionContext) => AcsCommitmentPeriodStore
  ): Unit = {
    val stringInterning = new MockStringInterning
    val p1 = stringInterning.participantId.internalize(DefaultTestIdentities.participant1.toLf)
    val p2 = stringInterning.participantId.internalize(DefaultTestIdentities.participant2.toLf)
    val p3 = stringInterning.participantId.internalize(DefaultTestIdentities.participant3.toLf)

    "increaseWatermarks" should {
      "increase the watermarks" in {
        val store = mk(stringInterning, executionContext)
        val ts0 = ts(0)
        val ts10 = ts(10)
        val ts100 = ts(100)
        for {
          empty <- store.watermarks()
          _ <- store.increaseWatermark(ts(0), affirmationOnly = false)
          epoch <- store.watermarks()
          _ <- store.increaseWatermark(ts10, affirmationOnly = true)
          affirmation10 <- store.watermarks()
          _ <- store.increaseWatermark(ts100, affirmationOnly = false)
          epoch100 <- store.watermarks()
        } yield {
          empty shouldBe MatchingWatermark.initial
          epoch shouldBe MatchingWatermark(ts0, ts0)
          affirmation10 shouldBe MatchingWatermark(ts0, ts10)
          epoch100 shouldBe MatchingWatermark(ts100, ts100)
        }
      }.failOnShutdown

      "never decrease" in {
        val store = mk(stringInterning, executionContext)
        val ts10 = ts(10)
        val ts50 = ts(50)
        val ts100 = ts(100)
        for {
          _ <- store.increaseWatermark(ts10, affirmationOnly = false)
          _ <- store.increaseWatermark(ts100, affirmationOnly = true)
          wm100 <- store.watermarks()
          _ <- store.increaseWatermark(CantonTimestamp.Epoch, affirmationOnly = false)
          wm100a <- store.watermarks()
          _ <- store.increaseWatermark(ts50, affirmationOnly = false)
          wm50 <- store.watermarks()
          _ <- store.increaseWatermark(ts10, affirmationOnly = true)
          wm50a <- store.watermarks()
        } yield {
          wm100 shouldBe MatchingWatermark(ts10, ts100)
          wm100a shouldBe wm100
          wm50 shouldBe MatchingWatermark(ts50, ts100)
          wm50a shouldBe wm50
        }
      }.failOnShutdown
    }

    "markOutstanding" should {
      "insert periods above the watermark" in {
        val store = mk(stringInterning, executionContext)
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
      }.failOnShutdown

      "cap periods at watermark" in {
        val store = mk(stringInterning, executionContext)
        val periods = Seq(
          // Skipped
          CommitmentMatchPeriod.outstanding(p1, ts(1), ts(10), "P1:1-10"),
          // Kept
          CommitmentMatchPeriod.outstanding(p2, ts(12), ts(20), "P2:12-20"),
          // Capped
          CommitmentMatchPeriod.outstanding(p3, ts(1), ts(20), "P3:1-20"),
        )
        for {
          _ <- store.increaseWatermark(ts(4), affirmationOnly = false)
          _ <- store.increaseWatermark(ts(15), affirmationOnly = true)
          _ <- store.markOutstanding(periods)
          lookupP1 <- store.lookupOutstanding(Seq(p1 -> cp(0, 100)))
          lookupP2 <- store.lookupOutstanding(Seq(p2 -> cp(0, 100)))
          lookupP3 <- store.lookupOutstanding(Seq(p3 -> cp(0, 100)))
        } yield {
          lookupP1 shouldBe Seq.empty
          lookupP2 shouldBe Seq(periods(1))
          lookupP3 shouldBe Seq(periods(2).copy(fromExclusive = ts(4)))
        }
      }.failOnShutdown
    }

    "persistMatchingOutcome" should {
      "move from outstanding/mismatched to mismatched/matched" in {
        val store = mk(stringInterning, executionContext)
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
          _ <- store.increaseWatermark(ts(25), affirmationOnly = true)
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
      }.failOnShutdown
    }

    "deleteOutstandingAfter" should {
      "delete all outstanding periods after the given timestamp" in {
        val store = mk(stringInterning, executionContext)
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
          _ <- store.increaseWatermark(ts(15), affirmationOnly = true)
          _ <- store.deleteOutstandingAfter(ts(15))
          remaining <- store.lookupOutstanding(
            Seq(p1 -> cp(0, 100), p2 -> cp(0, 100), p3 -> cp(0, 100))
          )
        } yield {
          remaining should contain theSameElementsAs periodsToKeep
        }
      }.failOnShutdown
    }

    "prune" should {
      "prune all entries before the given timestamp" in {
        val store = mk(stringInterning, executionContext)
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
        implicit val closeContext: CloseContext =
          CloseContext(FlagCloseable(logger, DefaultProcessingTimeouts.testing))
        for {
          _ <- store.markOutstanding(periodsToKeep ++ periodsToPrune)
          _ <- store.increaseWatermark(ts(20), affirmationOnly = false)
          _ <- store.prune(ts(14))
          remaining <- store.lookupOutstanding(
            Seq(p1 -> cp(0, 100), p2 -> cp(0, 100), p3 -> cp(0, 100))
          )
        } yield {
          remaining should contain theSameElementsAs periodsToKeep
        }
      }.failOnShutdown

      prunableByTime(mk(stringInterning, _))
    }

  }

  private def ts(i: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(i.toLong)
  private def cp(start: Int, end: Int): CommitmentPeriod =
    CommitmentPeriod.tryCreate(ts(start), ts(end))
  private def off(i: Int): Offset = Offset.tryFromLong(i.toLong)

  private implicit def bytestringFromString(s: String): ByteString = ByteString.copyFromUtf8(s)
}
