// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.CheckpointToBeWritten
import com.digitalasset.canton.participant.store.AcsCommitmentPeriodStore.{
  CommitmentMatchPeriod,
  MatchingWatermark,
}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigest,
  AcsDigestUpdate,
  Checkpoint,
  CheckpointType,
  InternedParticipantId,
  ParticipantAcsDigestUpdate,
}
import com.digitalasset.canton.participant.store.memory.InMemoryAcsCommitmentPeriodStore
import com.digitalasset.canton.participant.store.{
  AcsCommitmentPeriodStore,
  AcsDigestStore,
  AcsDigestTestBase,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

@AcsCommitmentTest
class AcsCommitmentPeriodWriterTest
    extends AsyncWordSpec
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with AcsDigestTestBase {
  self =>

  private val minimumProtocolVersion: ProtocolVersion = ProtocolVersion.acsCommitmentRedesign

  private def mkCommitmentPeriodWriter(
      testAcsDigestStore: AcsDigestStore,
      testAcsCommitmentPeriodStore: AcsCommitmentPeriodStore,
  ): AcsCommitmentPeriodWriter = new AcsCommitmentPeriodWriter(
    acsDigestStore = testAcsDigestStore,
    acsCommitmentPeriodStore = testAcsCommitmentPeriodStore,
    loggerFactory = this.loggerFactory,
  )

  private def mkPeriodStore(
      stringInterning: StringInterning = mockStringInterning,
      enableConsistencyChecks: Boolean = true,
  )(implicit
      executionContext: ExecutionContext
  ): AcsCommitmentPeriodStore =
    new InMemoryAcsCommitmentPeriodStore(
      Eval.now(stringInterning),
      loggerFactory,
      enableConsistencyChecks,
    )

  private def cp(start: Int, end: Int): CommitmentPeriod =
    CommitmentPeriod.tryCreate(ts(start), ts(end))

  "AcsCommitmentPeriodWriter.writeOutstandingAtTick" should {
    "not do anything with wrong checkpoint type" onlyRunWithOrGreaterThan minimumProtocolVersion inUS {
      val acsDigestStore = mkInMemoryDigestStore()
      val periodStore = mkPeriodStore()

      val periodWriter = mkCommitmentPeriodWriter(acsDigestStore, periodStore)
      val pageSize = PositiveInt.tryCreate(100)

      val tickTp = tp(1)
      val p1 = internedParticipantId(1)
      val p1Digest = genRawDigest(0x2a)

      // only CheckpointType.ReconciliationIntervalBoundary should work
      for {
        // Insert an active digest update for a p1 to check that the checkpoints do not trigger insertion
        _ <- acsDigestStore.participant.upsertDigestUpdates(
          Seq(
            AcsDigestUpdate(AcsDigest(p1, tickTp, Some(p1Digest), None), replacesOffset = None)
          )
        )
        reinit <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten = CheckpointToBeWritten(tp(2), CheckpointType.Reinitialization),
          reconciliationWatermark = CantonTimestamp.MinValue,
          pageSize = pageSize,
        )
        maxEvents <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten =
            CheckpointToBeWritten(tp(2), CheckpointType.MaxEventsWithoutCheckpoint),
          reconciliationWatermark = CantonTimestamp.MinValue,
          pageSize = pageSize,
        )
        partyHosting <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten = CheckpointToBeWritten(tp(2), CheckpointType.PartyHostingChange),
          reconciliationWatermark = CantonTimestamp.MinValue,
          pageSize = pageSize,
        )
        outstanding <- periodStore.lookupOutstanding(Seq(p1 -> cp(0, 3)))
      } yield {
        reinit shouldBe false
        maxEvents shouldBe false
        partyHosting shouldBe false
        outstanding shouldBe empty
      }
    }

    "populate outstanding periods on reconciliation tick" onlyRunWithOrGreaterThan minimumProtocolVersion inUS {
      val acsDigestStore = mkInMemoryDigestStore()
      val periodStore = mkPeriodStore()
      val periodWriter = mkCommitmentPeriodWriter(acsDigestStore, periodStore)

      val tickTp = tp(10)
      val tickTime = tickTp.recordTime
      val p1 = internedParticipantId(1)
      val p2 = internedParticipantId(2)

      val p1Digest = genRawDigest(0x2a)
      val p1HashedDigest = genHashedDigest(p1Digest)

      val pageSize = PositiveInt.tryCreate(100)

      for {
        // Insert an active digest update for a p1, and a tombstone for p2
        _ <- acsDigestStore.participant.upsertDigestUpdates(
          Seq(
            AcsDigestUpdate(AcsDigest(p1, tickTp, Some(p1Digest), None), replacesOffset = None),
            AcsDigestUpdate(AcsDigest(p2, tickTp, None, None), replacesOffset = None),
          )
        )
        // Add checkpoint
        _ <- acsDigestStore.insertCheckpointTime(
          Checkpoint(
            tickTp,
            CheckpointType.ReconciliationIntervalBoundary,
          )
        )
        // Check store's replaces invariant
        _ <- acsDigestStore.checkReplacesInvariant()

        // this should query the participant snapshot, write into outstanding period store
        isRecon <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten = CheckpointToBeWritten(
            tickTp,
            CheckpointType.ReconciliationIntervalBoundary,
          ),
          reconciliationWatermark = CantonTimestamp.MinValue,
          pageSize = pageSize,
        )
        _ <- periodStore.checkInvariant(Some(tickTp.recordTime))
        outstanding <- periodStore.lookupOutstanding(
          Seq(
            p1 -> cp(5, 10),
            p2 -> cp(5, 10),
          )
        )
        watermark <- periodStore.watermark()
      } yield {
        isRecon shouldBe true
        outstanding should contain theSameElementsAs Seq(
          CommitmentMatchPeriod
            .outstanding(p1, tickTime.immediatePredecessor, tickTime, p1HashedDigest)
            // No outstanding for p2 as it was a tombstone at tp(10)
        )
        watermark shouldBe MatchingWatermark.initial
      }
    }

    "skip over past reconciliation ticks" onlyRunWithOrGreaterThan minimumProtocolVersion inUS {
      val acsDigestStore = mkInMemoryDigestStore()
      val periodStore = mkPeriodStore()

      val periodWriter = mkCommitmentPeriodWriter(acsDigestStore, periodStore)
      val pageSize = PositiveInt.tryCreate(100)

      val tickTp1 = tp(1)
      val p1 = internedParticipantId(1)
      val p1Digest1 = genRawDigest(0x2a)
      val tickTp2 = tp(2)
      val p1Digest2 = genRawDigest(0x3a)

      for {
        // Insert an active digest update for a p1 to check that the checkpoints do not trigger insertion
        _ <- acsDigestStore.participant.upsertDigestUpdates(
          Seq(
            AcsDigestUpdate(AcsDigest(p1, tickTp1, Some(p1Digest1), None), replacesOffset = None),
            AcsDigestUpdate(
              AcsDigest(p1, tickTp2, Some(p1Digest2), None),
              replacesOffset = Some(tickTp1.offset),
            ),
          )
        )
        exact <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten =
            CheckpointToBeWritten(tp(1), CheckpointType.ReconciliationIntervalBoundary),
          reconciliationWatermark = tp(1).recordTime,
          pageSize = pageSize,
        )
        tooOld <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten =
            CheckpointToBeWritten(tp(1), CheckpointType.ReconciliationIntervalBoundary),
          reconciliationWatermark = tp(2).recordTime,
          pageSize = pageSize,
        )
        outstanding <- periodStore.lookupOutstanding(Seq(p1 -> cp(0, 3)))
      } yield {
        exact shouldBe false
        tooOld shouldBe false
        outstanding shouldBe empty
      }
    }

    "correctly handle pagination and batches full of tombstones across multiple pages" onlyRunWithOrGreaterThan minimumProtocolVersion inUS {
      val acsDigestStore = mkInMemoryDigestStore()
      val periodStore = mkPeriodStore()
      val periodWriter = mkCommitmentPeriodWriter(acsDigestStore, periodStore)

      val tickTp = tp(20)
      val tickOffset = tickTp.offset
      val tickTime = tickTp.recordTime
      val p1 = internedParticipantId(1)
      val p2 = internedParticipantId(2)
      val p3 = internedParticipantId(3)

      val p1Digest = genRawDigest(0x11)
      val p1HashedDigest = genHashedDigest(p1Digest)
      val p3Digest = genRawDigest(0x33)
      val p3HashedDigest = genHashedDigest(p3Digest)

      // Create a sequence of 6 updates with advancing offsets and proper replacement chains
      // so they don't overwrite each other in the journal.
      val p1Updates = (15 to tickOffset.unwrap.toInt).foldLeft(
        Seq.empty[ParticipantAcsDigestUpdate[InternedParticipantId]]
      ) { case (acc, i) =>
        val prevOffset = acc.lastOption.map(_.digestUpdate.offset)
        val update = AcsDigestUpdate(
          digestUpdate = AcsDigest(p1, tp(i), None, None), // all of them are tombstones
          replacesOffset = prevOffset,
        )
        acc :+ update
      }
      p1Updates.size shouldEqual 6

      val finalUpdates = p1Updates ++ Seq(
        AcsDigestUpdate(AcsDigest(p2, tickTp, Some(p1Digest), None), replacesOffset = None),
        AcsDigestUpdate(AcsDigest(p3, tickTp, Some(p3Digest), None), replacesOffset = None),
      )

      val pageSizeBy2 = PositiveInt.tryCreate(2)

      for {
        // Append all updates to the journal
        _ <- acsDigestStore.participant.upsertDigestUpdates(finalUpdates)

        // Add checkpoint boundary
        _ <- acsDigestStore.insertCheckpointTime(
          Checkpoint(tickTp, CheckpointType.ReconciliationIntervalBoundary)
        )
        _ <- acsDigestStore.checkReplacesInvariant()

        // Execute writeOutstandingAtTick with page size 2 to have more than one pages
        // among the pages, there should be empty process result (tombstones)
        isRecon <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten =
            CheckpointToBeWritten(tickTp, CheckpointType.ReconciliationIntervalBoundary),
          reconciliationWatermark = CantonTimestamp.MinValue,
          pageSize = pageSizeBy2,
        )
        _ <- periodStore.checkInvariant(Some(tickTp.recordTime))

        // Verify lookup results: p2 and p3 should have outstanding periods, p1 should not (due to being a tombstone)
        outstanding <- periodStore.lookupOutstanding(
          Seq(
            p1 -> cp(15, 20),
            p2 -> cp(15, 20),
            p3 -> cp(15, 20),
          )
        )
      } yield {
        isRecon shouldBe true
        outstanding should contain theSameElementsAs (Seq(
          CommitmentMatchPeriod
            .outstanding(p2, tickTime.immediatePredecessor, tickTime, p1HashedDigest),
          CommitmentMatchPeriod
            .outstanding(p3, tickTime.immediatePredecessor, tickTime, p3HashedDigest),
          // p1 ends as a tombstone, so it yields no outstanding entries.
        ))
      }
    }

    "correctly handle pagination, batches full of tombstones, and digests updated before the current tick" onlyRunWithOrGreaterThan minimumProtocolVersion inUS {
      val acsDigestStore = mkInMemoryDigestStore()
      val periodStore = mkPeriodStore()
      val periodWriter = mkCommitmentPeriodWriter(acsDigestStore, periodStore)

      val earlyTickTp = tp(10)
      val targetTickTp = tp(20)

      val earlyTickTime = earlyTickTp.recordTime
      val targetTickTime = targetTickTp.recordTime

      val targetTickOffset = targetTickTp.offset

      val p1 = internedParticipantId(1)
      val p2 = internedParticipantId(2)
      val p3 = internedParticipantId(3)

      val p1Digest = genRawDigest(0x11)
      val p1HashedDigest = genHashedDigest(p1Digest)
      val p3Digest = genRawDigest(0x33)
      val p3HashedDigest = genHashedDigest(p3Digest)

      // 1. Create a series of paginated updates for p2 (tombstones) to force multiple pages
      val p2Updates = (1 to targetTickOffset.unwrap.toInt by 2)
        .scanLeft(Option.empty[ParticipantAcsDigestUpdate[InternedParticipantId]]) {
          case (acc, i) =>
            val prevOffset = acc.map(_.digestUpdate.offset)
            val update = AcsDigestUpdate(
              digestUpdate = AcsDigest(p2, tp(i), None, None),
              replacesOffset = prevOffset,
            )
            Some(update)
        }
        .flatten

      // 2. Insert an update for p1 at an earlier tick
      val p1EarlyUpdate = Seq(
        AcsDigestUpdate(AcsDigest(p1, earlyTickTp, Some(p1Digest), None), replacesOffset = None)
      )

      // 3. Insert an update for p3 at the target tick
      val p3TargetUpdate = Seq(
        AcsDigestUpdate(AcsDigest(p3, targetTickTp, Some(p3Digest), None), replacesOffset = None)
      )

      val allUpdates = p1EarlyUpdate ++ p2Updates ++ p3TargetUpdate
      val pageSize = PositiveInt.tryCreate(5)

      for {
        // Append all updates to the journal
        _ <- acsDigestStore.participant.upsertDigestUpdates(allUpdates)

        // Insert checkpoints for both the early tick and target tick
        _ <- acsDigestStore.insertCheckpointTime(
          Checkpoint(earlyTickTp, CheckpointType.ReconciliationIntervalBoundary)
        )
        _ <- acsDigestStore.insertCheckpointTime(
          Checkpoint(targetTickTp, CheckpointType.ReconciliationIntervalBoundary)
        )
        _ <- acsDigestStore.checkReplacesInvariant()

        // Process the target tick with the page size 5, forcing pagination across the tombstone pages
        isRecon <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten =
            CheckpointToBeWritten(targetTickTp, CheckpointType.ReconciliationIntervalBoundary),
          reconciliationWatermark = CantonTimestamp.MinValue,
          pageSize = pageSize,
        )
        _ <- periodStore.checkInvariant(Some(targetTickTp.recordTime))

        // Verify:
        // - p1 should be present (carried forward from before the tick)
        // - p3 should be present (updated at target tick)
        // - p2 should be absent (ended as a tombstone)
        outstanding <- periodStore.lookupOutstanding(
          Seq(
            p1 -> cp(15, 20),
            p2 -> cp(15, 20),
            p3 -> cp(15, 20),
          )
        )
        watermark <- periodStore.watermark()
      } yield {
        isRecon shouldBe true
        outstanding should contain theSameElementsAs (Seq(
          // p1 was updated at earlyTickTime (=ts(10)) so it has the effective period started right before ts(10) (exclusiveFrom)
          CommitmentMatchPeriod
            .outstanding(p1, earlyTickTime.immediatePredecessor, targetTickTime, p1HashedDigest),
          CommitmentMatchPeriod
            .outstanding(p3, targetTickTime.immediatePredecessor, targetTickTime, p3HashedDigest),
        ))
        watermark shouldBe MatchingWatermark.initial
      }
    }

    "correctly handle digest updates unaligned with any tick" onlyRunWithOrGreaterThan minimumProtocolVersion inUS {
      val acsDigestStore = mkInMemoryDigestStore()
      val periodStore = mkPeriodStore()
      val periodWriter = mkCommitmentPeriodWriter(acsDigestStore, periodStore)

      val earlyTickTp = tp(10)
      val earlyTickOffset = earlyTickTp.offset
      val targetTickTp = tp(20)
      val targetTickTime = targetTickTp.recordTime

      // Timestamp between: not matching any checkpoint/tick time
      val timePointBetween = tp(14)
      val timeBetween = timePointBetween.recordTime

      val p1 = internedParticipantId(1)
      val p2 = internedParticipantId(2)
      val p3 = internedParticipantId(3)

      val p1Digest = genRawDigest(0x11)
      val p1HashedDigest = genHashedDigest(p1Digest)
      val p2Digest = genRawDigest(0x22)
      val p3Digest = genRawDigest(0x33)
      val p3HashedDigest = genHashedDigest(p3Digest)

      // 1. Create p1 digest update at early tick and then make it tombstone before the target tick
      val p2Updates = Seq(
        AcsDigestUpdate(
          digestUpdate = AcsDigest(p2, earlyTickTp, Some(p2Digest), None),
          replacesOffset = None,
        ),
        // We make it tombstone so at the recon time it won't appear in the result
        AcsDigestUpdate(
          digestUpdate = AcsDigest(p2, timePointBetween, None, None),
          replacesOffset = Some(earlyTickOffset),
        ),
      )

      // 2. Insert two updates for p1 at a timestamp/offset which is at early and before target tick
      val p1UnalignedUpdate = Seq(
        AcsDigestUpdate(AcsDigest(p1, earlyTickTp, Some(p1Digest), None), replacesOffset = None),
        AcsDigestUpdate(
          AcsDigest(p1, timePointBetween, Some(p1Digest), None),
          replacesOffset = Some(earlyTickOffset),
        ),
      )

      // 3. Insert an update for p3 at the target tick
      val p3TargetUpdate = Seq(
        AcsDigestUpdate(AcsDigest(p3, targetTickTp, Some(p3Digest), None), replacesOffset = None)
      )

      val allUpdates = p1UnalignedUpdate ++ p2Updates ++ p3TargetUpdate
      val smallPageSize = PositiveInt.tryCreate(2)

      for {
        // Append all updates to the journal
        _ <- acsDigestStore.participant.upsertDigestUpdates(allUpdates)

        // Insert a single checkpoint for the target tick boundary
        _ <- acsDigestStore.insertCheckpointTime(
          Checkpoint(
            targetTickTp,
            CheckpointType.ReconciliationIntervalBoundary,
          )
        )
        _ <- acsDigestStore.checkReplacesInvariant()

        // Process the target tick with a small page size, forcing pagination across tombstone pages
        isRecon <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten = CheckpointToBeWritten(
            targetTickTp,
            CheckpointType.ReconciliationIntervalBoundary,
          ),
          reconciliationWatermark = CantonTimestamp.MinValue,
          pageSize = smallPageSize,
        )
        _ <- periodStore.checkInvariant(Some(targetTickTime))

        // Verify:
        // - p1 should have an outstanding period starting from between early and target update time up to targetTickTp
        // - p3 should be present from targetTickTp's predecessor to targetTickTp
        // - p2 should be absent (ended as a tombstone)
        outstanding <- periodStore.lookupOutstanding(
          Seq(
            p1 -> cp(15, 20),
            p2 -> cp(15, 20),
            p3 -> cp(15, 20),
          )
        )
      } yield {
        isRecon shouldBe true
        outstanding should contain theSameElementsAs (Seq(
          CommitmentMatchPeriod
            .outstanding(p1, timeBetween.immediatePredecessor, targetTickTime, p1HashedDigest),
          CommitmentMatchPeriod
            .outstanding(p3, targetTickTime.immediatePredecessor, targetTickTime, p3HashedDigest),
        ))
      }
    }

    "correctly handle multiple checkpoints and state transitions across ticks" onlyRunWithOrGreaterThan minimumProtocolVersion inUS {
      val acsDigestStore = mkInMemoryDigestStore()
      val periodStore = mkPeriodStore()
      val periodWriter = mkCommitmentPeriodWriter(acsDigestStore, periodStore)

      val earlyTickTp = tp(10)
      val earlyTickOffset = earlyTickTp.offset
      val targetTickTp = tp(20)

      val timeBetweenTp = tp(15)

      val p1 = internedParticipantId(1)
      val p2 = internedParticipantId(2)
      val p3 = internedParticipantId(3)

      val p1Digest_At5 = genRawDigest(0x11)
      val p2Digest_At3 = genRawDigest(0x21)
      val p2Digest_AtEarly = genRawDigest(0x22)
      val p2HashedDigest_AtEarly = genHashedDigest(p2Digest_AtEarly)
      val p1Digest_AtBetween = genRawDigest(0x12)
      val p1HashedDigest_AtBetween = genHashedDigest(p1Digest_AtBetween)
      val p3Digest_AtTarget = genRawDigest(0x33)
      val p3HashedDigest_AtTarget = genHashedDigest(p3Digest_AtTarget)

      // Phase 1 updates (up to early tick):
      val phase1Updates = Seq(
        // p1: active digest before early tick, then a tombstone at early tick
        AcsDigestUpdate(AcsDigest(p1, tp(5), Some(p1Digest_At5), None), replacesOffset = None),
        AcsDigestUpdate(
          AcsDigest(p1, earlyTickTp, None, None),
          replacesOffset = Some(tp(5).offset),
        ),
        // p2: two active updates leading up to early tick
        AcsDigestUpdate(AcsDigest(p2, tp(3), Some(p2Digest_At3), None), replacesOffset = None),
        AcsDigestUpdate(
          AcsDigest(p2, earlyTickTp, Some(p2Digest_AtEarly), None),
          replacesOffset = Some(tp(3).offset),
        ),
      )

      val pageSize = PositiveInt.tryCreate(10)

      for {
        // --- Tick 1 --- (earlyTick)
        _ <- acsDigestStore.participant.upsertDigestUpdates(phase1Updates)
        _ <- acsDigestStore.insertCheckpointTime(
          Checkpoint(earlyTickTp, CheckpointType.ReconciliationIntervalBoundary)
        )
        _ <- acsDigestStore.checkReplacesInvariant()

        isRecon1 <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten =
            CheckpointToBeWritten(earlyTickTp, CheckpointType.ReconciliationIntervalBoundary),
          reconciliationWatermark = CantonTimestamp.MinValue,
          pageSize = pageSize,
        )
        _ <- periodStore.checkInvariant(Some(earlyTickTp.recordTime))

        // Outstanding at Tick 1: p2 should be active (p1 became a tombstone at earlyTickTp)
        outstandingTick1 <- periodStore.lookupOutstanding(
          Seq(
            p1 -> cp(5, 10),
            p2 -> cp(5, 10),
          )
        )

        // --- Phase 2 updates (between early tick and target tick) ---
        // - p1: gets an active digest update between ticks
        // - p3: gets an active digest update right at target tick
        phase2Updates = Seq(
          AcsDigestUpdate(
            AcsDigest(p1, timeBetweenTp, Some(p1Digest_AtBetween), None),
            replacesOffset = Some(earlyTickOffset),
          ),
          AcsDigestUpdate(
            AcsDigest(p3, targetTickTp, Some(p3Digest_AtTarget), None),
            replacesOffset = None,
          ),
        )

        _ <- acsDigestStore.participant.upsertDigestUpdates(phase2Updates)
        _ <- acsDigestStore.insertCheckpointTime(
          Checkpoint(targetTickTp, CheckpointType.ReconciliationIntervalBoundary)
        )
        _ <- acsDigestStore.checkReplacesInvariant()

        isRecon2 <- periodWriter.writeOutstandingAtTick(
          checkpointToBeWritten =
            CheckpointToBeWritten(targetTickTp, CheckpointType.ReconciliationIntervalBoundary),
          reconciliationWatermark = earlyTickTp.recordTime,
          pageSize = pageSize,
        )
        _ <- periodStore.checkInvariant(Some(targetTickTp.recordTime))

        // Outstanding at Tick 2:
        // p1 (revived between ticks), p2 didn't change but extended in this period and p3 (active at target tick) should appear
        // Note: it is between 10 and 20 so the previous reconciliation time is not involved!!!
        outstandingTick2 <- periodStore.lookupOutstanding(
          Seq(
            p1 -> cp(10, 20),
            p2 -> cp(10, 20),
            p3 -> cp(10, 20),
          )
        )

        // We check the same between the period 5 and 20
        // so both the previous reconciliation time at 10 and the reconciliation time at 20 is involved
        outstandingTick2_from5 <- periodStore.lookupOutstanding(
          Seq(
            p1 -> cp(5, 20),
            p2 -> cp(5, 20),
            p3 -> cp(5, 20),
          )
        )
      } yield {
        // Verify Tick 1 Results
        isRecon1 shouldBe true
        outstandingTick1 should contain theSameElementsAs Seq(
          CommitmentMatchPeriod.outstanding(
            p2,
            earlyTickTp.recordTime.immediatePredecessor,
            earlyTickTp.recordTime,
            p2HashedDigest_AtEarly,
          )
        )

        // Verify Tick 2 Watermark & Results
        isRecon2 shouldBe true
        outstandingTick2 should contain theSameElementsAs Seq(
          CommitmentMatchPeriod.outstanding(
            p1,
            timeBetweenTp.recordTime.immediatePredecessor,
            targetTickTp.recordTime,
            p1HashedDigest_AtBetween,
          ),
          CommitmentMatchPeriod
            .outstanding(
              p2,
              // cannot be earlyTickTp.recordTime.immediatePredecessor because it would be before the period we check!!!
              earlyTickTp.recordTime,
              targetTickTp.recordTime,
              p2HashedDigest_AtEarly,
            ),
          CommitmentMatchPeriod
            .outstanding(
              p3,
              targetTickTp.recordTime.immediatePredecessor,
              targetTickTp.recordTime,
              p3HashedDigest_AtTarget,
            ),
        )

        outstandingTick2_from5 should contain theSameElementsAs Seq(
          CommitmentMatchPeriod.outstanding(
            p1,
            timeBetweenTp.recordTime.immediatePredecessor,
            targetTickTp.recordTime,
            p1HashedDigest_AtBetween,
          ),
          CommitmentMatchPeriod.outstanding(
            p2,
            // earlyTickTp.recordTime.immediatePredecessor because it is in the period we check!!!
            earlyTickTp.recordTime.immediatePredecessor,
            // but this one is computed for earlyTickTp (previous) reconciliation time
            earlyTickTp.recordTime,
            p2HashedDigest_AtEarly,
          ),
          // we have another computed/outstanding for P2
          CommitmentMatchPeriod.outstanding(
            p2,
            // which is valid excludeFrom the previous reconciliation time
            earlyTickTp.recordTime,
            // to the current reconciliation time
            targetTickTp.recordTime,
            p2HashedDigest_AtEarly,
          ),
          CommitmentMatchPeriod.outstanding(
            p3,
            targetTickTp.recordTime.immediatePredecessor,
            targetTickTp.recordTime,
            p3HashedDigest_AtTarget,
          ),
        )
      }
    }
  }
}
