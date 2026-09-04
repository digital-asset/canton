// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.commitment.Timepoint
import com.digitalasset.canton.participant.store.AcsDigestStore.*
import com.digitalasset.canton.participant.store.AcsDigestStore.CheckpointType.{
  PartyHostingChange,
  ReconciliationIntervalBoundary,
  Reinitialization,
}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, InternedPartyId, ProtocolVersionChecksAsyncWordSpec}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.collection.immutable
import scala.concurrent.ExecutionContext

trait AcsDigestStoreTest extends ProtocolVersionChecksAsyncWordSpec with AcsDigestTestBase {
  this: AsyncWordSpecLike & BaseTest =>

  /** Reusable validation for verifying that any type of journal behaves correctly when completely
    * empty.
    */
  private def emptyDigestJournalTests[K](
      mkJournal: () => AcsDigestStore.DigestJournal[K],
      sampleKeys: List[K],
      largeKeySet: List[K],
      rangeStart: Offset,
      rangeEnd: Offset,
  ): Unit = {

    "pointwise 'lookup' calls should yield None" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val emptyJournal = mkJournal()
      val firstSampleKey =
        sampleKeys.headOption.getOrElse(fail("sampleKeys shouldn't be empty"))

      for {
        result <- emptyJournal.lookup(firstSampleKey, rangeEnd)
      } yield result shouldBe Option.empty
    }

    "batch 'bulkLookup' calls must return an empty map" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val emptyJournal = mkJournal()

      for {
        result <- emptyJournal.bulkLookup(sampleKeys, rangeEnd)
      } yield result shouldBe Map.empty
    }

    "batch 'bulkLookup' with large key set should execute without failure" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val emptyJournal = mkJournal()

      for {
        result <- emptyJournal.bulkLookup(largeKeySet, rangeEnd)
      } yield result shouldBe Map.empty
    }

    "requesting a pagination 'snapshot' should return empty page" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val emptyJournal = mkJournal()

      for {
        (page, nextToken) <- emptyJournal.snapshot(
          tokenOrStart = Right(rangeEnd),
          limit = 10,
        )
      } yield {
        page shouldBe empty
        nextToken shouldBe Left(PaginationTokenDone)
      }
    }

    "querying state 'changesBetween' ranges should yield completely empty page" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val emptyJournal = mkJournal()

      for {
        (page, nextToken) <- emptyJournal.changesBetween(
          Right(
            ChangesBetweenOffsetRange(
              fromInclusive = rangeStart,
              toExclusive = rangeEnd,
            )
          ),
          limit = 10,
        )
      } yield {
        page shouldBe empty
        nextToken shouldBe Left(PaginationTokenDone)
      }
    }

    "verifying chain consistency via 'checkReplacesInvariant' must pass safely" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val emptyJournal = mkJournal()

      for {
        _ <- emptyJournal.checkReplacesInvariant(upToInclusive = rangeEnd, None)
      } yield succeed
    }
  }

  /** Reusable operational verification covering record lifecycle transitions on a concrete journal.
    */
  private def functionalDigestJournalTests[K](
      mkJournal: () => AcsDigestStore.DigestJournal[K],
      key1: K,
      key2: K,
      digest1: RawDigest,
      digest2: RawDigest,
      offsetTime0: Timepoint,
      offsetTime1: Timepoint,
      offsetTime2: Timepoint,
  ): Unit = {

    val (offset0, t0) = offsetTime0.tupled
    val (offset1, t1) = offsetTime1.tupled
    val (offset2, t2) = offsetTime2.tupled

    val update1_K1T0 =
      AcsDigestUpdate(
        AcsDigest(key1, offset0, t0, Some(digest1), None),
        replacesOffset = None,
      )
    val update2_K2T0 =
      AcsDigestUpdate(
        AcsDigest(key2, offset0, t0, Some(digest2), None),
        replacesOffset = None,
      )
    // Update key1 at t1
    val update3_K1T1 = AcsDigestUpdate(
      AcsDigest(key1, offset1, t1, Some(digest2), None),
      replacesOffset = Some(offset0),
    )
    // 'Archive' key2 at t1
    val tombstone_K2T1 = AcsDigestUpdate(
      AcsDigest(key2, offset1, t1, None, None),
      replacesOffset = Some(offset0),
    )

    val update5_update_K1T1 = AcsDigestUpdate(
      AcsDigest(key1, offset1, t1, None, None),
      replacesOffset = Some(offset0),
    )

    def upsertUpdatesIn(journal: DigestJournal[K]) = for {
      _ <- journal.upsertDigestUpdates(List(update1_K1T0, update2_K2T0))
      _ <- journal.upsertDigestUpdates(List(update3_K1T1, tombstone_K2T1))
      _ <- journal.checkReplacesInvariant(offset2, None)
    } yield succeed

    "allow batch item insertion with upsertDigestUpdates" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      upsertUpdatesIn(mkJournal())
    }

    "return precise historical states on pointwise lookup" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()

      val tMin = Offset.firstOffset
      val tMax = Offset.MaxValue

      for {
        _ <- upsertUpdatesIn(testJournal)

        // before we have any record
        key1StateAtTMin <- testJournal.lookup(key1, tMin)
        key2StateAtTMin <- testJournal.lookup(key2, tMin)

        // at T0
        key1StateAtT0 <- testJournal.lookup(key1, offset0)
        key2StateAtT0 <- testJournal.lookup(key2, offset0)

        // at T1
        key1StateAtT1 <- testJournal.lookup(key1, offset1)
        key2StateAtT1 <- testJournal.lookup(key2, offset1)

        // at T2
        key1StateAtT2 <- testJournal.lookup(key1, offset2)
        key2StateAtT2 <- testJournal.lookup(key2, offset2)

        // after our records
        key1StateAtTMax <- testJournal.lookup(key1, tMax)
        key2StateAtTMax <- testJournal.lookup(key2, tMax)
      } yield {
        // T(min)
        key1StateAtTMin shouldBe None
        key2StateAtTMin shouldBe None

        // T0
        key1StateAtT0 shouldBe Some(update1_K1T0)
        key2StateAtT0 shouldBe Some(update2_K2T0)

        // T1
        key1StateAtT1 shouldBe Some(update3_K1T1)
        key2StateAtT1 shouldBe Some(tombstone_K2T1)

        // T2
        key1StateAtT2 shouldBe Some(update3_K1T1)
        key2StateAtT2 shouldBe Some(tombstone_K2T1)

        // T(max)
        key1StateAtTMax shouldBe Some(update3_K1T1)
        key2StateAtTMax shouldBe Some(tombstone_K2T1)
      }
    }

    "after updating an entry, pointwise lookup still works well" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()

      for {
        _ <- upsertUpdatesIn(testJournal)

        // updating K1 at T1 for a tombstone
        _ <- testJournal.upsertDigestUpdates(List(update5_update_K1T1))

        key1StateAtT1 <- testJournal.lookup(key1, offset1)
      } yield {
        // K1 at T1
        key1StateAtT1 shouldBe Some(update5_update_K1T1)
      }
    }

    "batch 'bulkLookup' call with empty keys return an empty map" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()

      for {
        _ <- upsertUpdatesIn(testJournal)
        result <- testJournal.bulkLookup(Seq.empty[K], offset2)
      } yield result shouldBe Map.empty
    }

    "evaluate range snapshots using pagination (2 digests on 1 page)" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()
      for {
        _ <- upsertUpdatesIn(testJournal)
        (page, token) <- testJournal.snapshot(tokenOrStart = Right(offset0), limit = 2)
      } yield {
        // no more page indeed
        token shouldBe Left(PaginationTokenDone)

        // At t0, both key1 and key2 are active
        page.map(d => d.digestUpdate.key -> d.digestUpdate.offset).toMap shouldBe Map(
          key1 -> offset0,
          key2 -> offset0,
        )
      }
    }

    "evaluate range snapshots using pagination (1 digest on 2 pages)" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()
      for {
        _ <- upsertUpdatesIn(testJournal)
        (page1, token1) <- testJournal.snapshot(tokenOrStart = Right(offset0), limit = 1)
        (page2, token2) <- testJournal.snapshot(
          tokenOrStart =
            Left(token1.getOrElse(fail("We expect 'token1' to be a continuation token!"))),
          limit = 1,
        )
      } yield {
        token2 shouldBe Left(PaginationTokenDone)

        // Each page has 1 digest
        page1.size shouldBe 1
        page2.size shouldBe 1

        // At t0, both key1 and key2 should be active (aggregated view)
        (page1 ++ page2).map(d => d.digestUpdate.key -> d.digestUpdate.offset).toMap shouldBe Map(
          key1 -> offset0,
          key2 -> offset0,
        )
      }
    }

    "isolate range updates using changesBetween boundaries (1 page)" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()
      for {
        _ <- upsertUpdatesIn(testJournal)
        (changes, token) <- testJournal.changesBetween(
          Right(
            ChangesBetweenOffsetRange(
              fromInclusive = offset0,
              toExclusive = offset2,
            )
          ),
          limit = 2,
        )
      } yield {
        token shouldBe Left(PaginationTokenDone)

        // Should fetch the latest revisions inside [t0, t2) for each key
        changes should have size 2
        changes should contain theSameElementsAs (
          List(update3_K1T1, tombstone_K2T1).map(_.digestUpdate)
        )
      }
    }

    "isolate range updates using changesBetween boundaries (2 pages)" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()
      for {
        _ <- upsertUpdatesIn(testJournal)
        (change1, token1) <- testJournal.changesBetween(
          Right(
            ChangesBetweenOffsetRange(
              fromInclusive = offset0,
              toExclusive = offset2,
            )
          ),
          limit = 1,
        )
        (change2, token2) <- testJournal.changesBetween(
          Left(token1.getOrElse(fail("We expect 'token1' to be a continuation token!"))),
          limit = 1,
        )
      } yield {
        token2 shouldBe Left(PaginationTokenDone)

        // Should fetch the latest revisions inside [t0, t2) for each key
        change1 should have size 1
        change2 should have size 1
        (change1 ++ change2) should contain theSameElementsAs
          List(update3_K1T1, tombstone_K2T1).map(_.digestUpdate)
      }
    }

    "catch 'future' link during checkReplacesInvariant invocation" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      // Intentionally insert a broken link at T2 that claims to replace an imaginary T99
      val testJournal = mkJournal()
      val offset99 = Offset.tryFromLong(99)
      val brokenUpdate =
        AcsDigestUpdate(
          AcsDigest(key1, offset2, t2, Some(digest1), None),
          replacesOffset = Some(offset99),
        )

      for {
        _ <- testJournal.upsertDigestUpdates(List(brokenUpdate))
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          within = testJournal.checkReplacesInvariant(offset2, None),
          assertion = _.getMessage should include(
            s"We cannot have replacesOffset=$offset99 which is gte to change offset $offset2"
          ),
        )
      } yield succeed
    }

    "catch empty link during checkReplacesInvariant invocation" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()
      val update1_K1T1 =
        AcsDigestUpdate(AcsDigest(key1, offset1, t1, Some(digest1), None), replacesOffset = None)
      // Not referencing to replace update1
      val update2_K1T2 =
        AcsDigestUpdate(AcsDigest(key1, offset2, t2, None, None), replacesOffset = None)

      for {
        _ <- testJournal.upsertDigestUpdates(List(update1_K1T1, update2_K1T2))
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          within = testJournal.checkReplacesInvariant(offset2, None),
          assertion = _.getMessage should include(
            s"Replaces offset should point to $offset1 but it is empty!"
          ),
        )
      } yield succeed
    }

    "catch invalid past reference during checkReplacesInvariant invocation" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testJournal = mkJournal()
      val update1_K1T0 =
        AcsDigestUpdate(AcsDigest(key1, offset0, t0, Some(digest1), None), replacesOffset = None)
      // Referencing to replace a non existent past digest update
      val update2_K1T2 =
        AcsDigestUpdate(AcsDigest(key1, offset2, t2, None, None), replacesOffset = Some(offset1))

      for {
        _ <- testJournal.upsertDigestUpdates(List(update1_K1T0, update2_K1T2))
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          within = testJournal.checkReplacesInvariant(offset2, None),
          assertion = _.getMessage should include(
            s"which is not pointing to the preceding offset=$offset0"
          ),
        )
      } yield succeed
    }
  }

  // Functional tests verifying the common operations on the store and sub-journals
  private def functionalCommonOperationsTests(
      mkStore: () => AcsDigestStore,
      party1: InternedPartyId,
      party2: InternedPartyId,
      participantId1: InternedParticipantId,
  ): Unit = {
    val (offset0, t0) = offsetTime(PositiveLong.tryCreate(10))
    val checkpoint1 = checkpoint(offsetTime(PositiveLong.tryCreate(20)))
    val (offset1, t1) = checkpoint1.timepoint.tupled
    val checkpoint2 = checkpoint(offsetTime(PositiveLong.tryCreate(30)))
    val (offset2, t2) = checkpoint2.timepoint.tupled
    val checkpoint3 = checkpoint(offsetTime(PositiveLong.tryCreate(40)))
    val (offset3, t3) = checkpoint3.timepoint.tupled

    val rawDigest = genRawDigest(0x2c)

    def checkReplacesIntervalCommonUpserts(store: AcsDigestStore) = {
      val party1UpdateAtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party1, offset0, t0, Some(rawDigest), None),
        replacesOffset = None,
      )
      val party2UpdateAtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party2, offset0, t0, Some(rawDigest), None),
        replacesOffset = None,
      )
      val party1EmptyUpdateAtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party1, offset1, t1, None, None),
        replacesOffset = Some(offset0),
      )
      val party2EmptyUpdateAtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party2, offset1, t1, None, None),
        replacesOffset = Some(offset0),
      )

      val participantUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, offset1, t1, Some(rawDigest), None),
        replacesOffset = None,
      )

      for {
        _ <- store.party.upsertDigestUpdates(
          List(
            party1UpdateAtT0,
            party2UpdateAtT0,
            party1EmptyUpdateAtT1,
            party2EmptyUpdateAtT1,
          )
        )
        _ <- store.participant.upsertDigestUpdates(
          List(
            participantUpdate1AtT1
          )
        )
      } yield ()
    }

    s"properly retain data on 'deleteAfter' when there is data at the boundary" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val store = mkStore()

      val party1UpdateAtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party1, offset0, t0, Some(rawDigest), None),
        replacesOffset = None,
      )
      val party2UpdateAtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party2, offset0, t0, Some(rawDigest), None),
        replacesOffset = None,
      )

      val participantUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, offset1, t1, Some(rawDigest), None),
        replacesOffset = None,
      )

      for {
        _ <- store.party.upsertDigestUpdates(List(party1UpdateAtT0, party2UpdateAtT0))
        _ <- store.participant.upsertDigestUpdates(List(participantUpdate1AtT1))

        // Verify the integrity of replaces time pointers across the updates
        _ <- store.insertCheckpointTime(checkpoint2)
        _ <- store.checkReplacesInvariant()

        _ <- store.deleteAfter(offset1)

        partyUpdates <- store.party.bulkLookup(
          keys = List(party1, party2),
          toInclusive = offset0,
        )
        participantUpdate <- store.participant.bulkLookup(
          keys = List(participantId1),
          toInclusive = offset1,
        )
      } yield {
        partyUpdates shouldBe (Map(
          party1 -> party1UpdateAtT0,
          party2 -> party2UpdateAtT0,
        ))
        participantUpdate shouldBe (Map(
          participantId1 -> participantUpdate1AtT1
        ))
      }
    }

    s"properly retain data on 'deleteUpTo' when there is one update per key in the past" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val store = mkStore()

      val party1UpdateAtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party1, offset0, t0, Some(rawDigest), None),
        replacesOffset = None,
      )
      val party2UpdateAtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party2, offset0, t0, Some(rawDigest), None),
        replacesOffset = None,
      )

      val participantUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, offset1, t1, Some(rawDigest), None),
        replacesOffset = None,
      )

      for {
        _ <- store.party.upsertDigestUpdates(List(party1UpdateAtT0, party2UpdateAtT0))
        _ <- store.participant.upsertDigestUpdates(List(participantUpdate1AtT1))

        // Verify the integrity of replaces time pointers across the updates
        _ <- store.insertCheckpointTime(checkpoint2)
        _ <- store.checkReplacesInvariant()

        _ <- store.deleteUpTo(offset1)

        partyUpdates <- store.party.bulkLookup(
          keys = List(party1, party2),
          toInclusive = offset0,
        )
        participantUpdate <- store.participant.bulkLookup(
          keys = List(participantId1),
          toInclusive = offset1,
        )
      } yield {
        partyUpdates shouldBe (Map(
          party1 -> party1UpdateAtT0,
          party2 -> party2UpdateAtT0,
        ))
        participantUpdate shouldBe (Map(
          participantId1 -> participantUpdate1AtT1
        ))
      }
    }

    s"properly execute global node boundaries across sub-journals" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val pruningStore = mkStore()

      val party1UpdateAtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party1, offset0, t0, Some(rawDigest), None),
        replacesOffset = None,
      )
      for {
        _ <- pruningStore.party.upsertDigestUpdates(List(party1UpdateAtT0))
        _ <- pruningStore.insertCheckpointTime(checkpoint1)

        // Test global prune boundaries across sub-journals
        _ <- pruningStore.deleteAfter(offset1)
        _ <- pruningStore.deleteUpTo(offset1)

        remainingParty <- pruningStore.party.lookup(party1, offset0)
      } yield remainingParty shouldBe Some(party1UpdateAtT0)
    }

    s"'checkReplacesInvariant' check is successful when there is no broken reference in the journals" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val store = mkStore()

      val participantUpdate1AtT2 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, offset2, t2, None, None),
        replacesOffset = Some(offset1),
      )

      for {
        // Inserting all entries except the last one
        _ <- checkReplacesIntervalCommonUpserts(store)

        // Last insertion is also correct
        _ <- store.participant.upsertDigestUpdates(List(participantUpdate1AtT2))

        result <- store.checkReplacesInvariant()
      } yield result shouldBe ()
    }

    s"'checkReplacesInvariant' check is successful when the first few updates has dangling reference (after pruning)" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val store = mkStore()

      val party1UpdateAtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party1, offset1, t1, Some(rawDigest), None),
        // referring to a deleted digest update
        replacesOffset = Some(offset0),
      )
      val party2UpdateAtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(party2, offset0, t0, Some(rawDigest), None),
        // referring to a deleted digest update
        replacesOffset = Some(offset0),
      )

      val participantUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, offset1, t1, Some(rawDigest), None),
        // referring to a deleted digest update
        replacesOffset = Some(offset0),
      )

      for {
        _ <- store.party.upsertDigestUpdates(
          List(
            party1UpdateAtT1,
            party2UpdateAtT0,
          )
        )
        _ <- store.participant.upsertDigestUpdates(
          List(
            participantUpdate1AtT1
          )
        )
        result <- store.checkReplacesInvariant()
      } yield result shouldBe ()
    }

    s"'checkReplacesInvariant' check fails when there is a broken reference in the journals" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val store = mkStore()

      val brokenParticipantUpdate1AtT2 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, offset2, t2, None, None),
        // broken, should point back to t1 (see the earlier entries in checkReplacesIntervalCommonUpserts)
        replacesOffset = None,
      )

      for {
        // Inserting all entries except the last one
        _ <- checkReplacesIntervalCommonUpserts(store)

        // Last insertion is the update with broken link
        _ <- store.participant.upsertDigestUpdates(List(brokenParticipantUpdate1AtT2))

        // it checks until the last checkpoint T2
        _ <- store.insertCheckpointTime(checkpoint2)
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          within = store.checkReplacesInvariant(),
          assertion = _.getMessage should include(
            s"ReplacesOffset check error for key ${externalizeParticipantId(participantId1)} at $offset2 - " +
              s"Replaces offset should point to $offset1 but it is empty!"
          ),
        )
      } yield succeed
    }

    "catch unpruned rows with non-dangling references during checkReplacesInvariant invocation" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val store = mkStore()
      val update1_K1T1 = AcsDigestUpdate(
        AcsDigest(participantId1, offset1, t1, Some(rawDigest), None),
        replacesOffset = Some(offset0),
      )
      val update1_K1T2 = AcsDigestUpdate(
        AcsDigest(participantId1, offset2, t2, Some(rawDigest), None),
        replacesOffset = Some(offset1),
      )
      for {
        _ <- store.participant.upsertDigestUpdates(List(update1_K1T1, update1_K1T2))
        _ <- store.insertCheckpointTime(checkpoint3)
        _ <- store.deleteUpTo(offset3)
        // Reinsert the deleted row to create the invariant violation
        _ <- store.participant.upsertDigestUpdates(List(update1_K1T1))
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          store.checkReplacesInvariant(),
          _.getMessage should include(
            s"ReplacesOffset check error for key ${externalizeParticipantId(participantId1)} at offset $offset2 below the latest pruning offset $offset3 references an existing replaces offset $offset1"
          ),
        )
      } yield succeed
    }

  }

  /** Parameterized verification covering high-density updates, concurrent tie-breakers, checkpoint
    * alignment, crash recovery, and pruning operations.
    */
  private def complexDensityJournalTests[K](
      mkStore: () => AcsDigestStore,
      mkJournal: AcsDigestStore => DigestJournal[K],
      key1: K,
      key2: K,
      digest1: RawDigest,
      digest2: RawDigest,
  ): Unit = {
    // Define t0,t1,...t11 starting from 0, increased by 5
    val (offset, t) =
      (5L to 60L by 5L).map(seconds => offsetTime(PositiveLong.tryCreate(seconds))).unzip

    // Define 5 explicit checkpoint positions
    val cpAtT2 = Checkpoint(offset(2), t(2), CheckpointType.ReconciliationIntervalBoundary)
    val cpAtT4 = Checkpoint(offset(4), t(4), CheckpointType.AffirmationIntervalBoundary)
    val cpAtT6 = Checkpoint(offset(6), t(6), CheckpointType.MaxEventsWithoutCheckpoint)
    val cpAtT8 = Checkpoint(offset(8), t(8), CheckpointType.Reinitialization)
    val cpAtT10 = Checkpoint(offset(10), t(10), CheckpointType.PartyHostingChange)

    val update1_K1T2 =
      AcsDigestUpdate(AcsDigest(key1, offset(2), t(2), Some(digest1), None), replacesOffset = None)
    val update2_K2T4 =
      AcsDigestUpdate(AcsDigest(key2, offset(4), t(4), Some(digest2), None), replacesOffset = None)
    val update3_K2T5 =
      AcsDigestUpdate(
        AcsDigest(key2, offset(5), t(5), Some(digest1), None),
        replacesOffset = Some(offset(4)),
      )

    val update4_K1T6V0 =
      AcsDigestUpdate(
        AcsDigest(key1, offset(6), t(6), Some(digest2), None),
        replacesOffset = Some(offset(2)),
      )
    // overwrite V0
    val update5_K1T6V1 =
      AcsDigestUpdate(
        AcsDigest(key1, offset(6), t(6), Some(digest1), None),
        replacesOffset = Some(offset(2)),
      )
    // overwrite V1
    val update6_K1T6V2 = // Tombstone
      AcsDigestUpdate(
        AcsDigest(key1, offset(6), t(6), None, None),
        replacesOffset = Some(offset(2)),
      )

    // Re-create key1
    val update7_K1T7 = AcsDigestUpdate(
      AcsDigest(key1, offset(7), t(7), Some(digest1), None),
      replacesOffset = Some(offset(6)),
    )
    val update8_K2T9 =
      AcsDigestUpdate(
        AcsDigest(key2, offset(9), t(9), Some(digest2), None),
        replacesOffset = Some(offset(5)),
      )
    val update9_K1T10 =
      AcsDigestUpdate(
        AcsDigest(key1, offset(10), t(10), Some(digest2), None),
        replacesOffset = Some(offset(7)),
      )
    val update10_K2T11 = // Tombstone
      AcsDigestUpdate(
        AcsDigest(key2, offset(11), t(11), None, None),
        replacesOffset = Some(offset(9)),
      )

    // batch insert all digest updates and checkpoints
    // we are interested about the final state not a realistic order of insertion
    // Note: in a realistic scenario the checkpoint insertions would interleave with the digest updates appropriately
    def upsertInsertCheckpointsInto(
        testStore: AcsDigestStore,
        testJournal: DigestJournal[K],
    ) = for {
      // 1. Commit all 10 updates
      _ <- testJournal.upsertDigestUpdates(
        List(
          update1_K1T2,
          update2_K2T4,
          update3_K2T5,
          update4_K1T6V0, // version0
          update5_K1T6V1, // version1 in this order overwrites
          update6_K1T6V2, // version2 in this order leaves eventually a tombstone at T6
          update7_K1T7,
          update8_K2T9,
          update9_K1T10,
          update10_K2T11,
        )
      )

      // 2. Commit all 5 checkpoints
      _ <- FutureUnlessShutdown
        .sequence(
          List( // Note: t0 is not persisted
            cpAtT2,
            cpAtT4,
            cpAtT6,
            cpAtT8,
            cpAtT10,
          ).map(testStore.insertCheckpointTime)
        )

      _ <- testStore.checkReplacesInvariant()
    } yield succeed

    "upsert -> insert checkpoints -> pointwise lookup verification" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        // 1. Commit all 10 updates and insert 5 checkpoints
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)

        // 2. Verify point-in-time state checks handle tie-breakers accurately
        lookupV2 <- testJournal.lookup(key1, offset(6))
      } yield {
        lookupV2 shouldBe Some(update6_K1T6V2) // tombstone
      }
    }

    "upsert -> insert checkpoints -> bulk lookup verification" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        // Commit all 10 updates and insert 5 checkpoints
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)
        // Evaluate batch retrieval across mixed chronological indices at the middle boundary
        bulkAtMiddle <- testJournal.bulkLookup(List(key1, key2), offset(6))
        bulkWithRepeatedKeys <- testJournal.bulkLookup(
          Seq(
            key1 -> offset(2),
            key1 -> offset(6),
            key2 -> offset(4),
            key2 -> offset(10),
          )
        )
      } yield {
        bulkAtMiddle shouldBe Map(
          key1 -> update6_K1T6V2, // tombstone
          key2 -> update3_K2T5,
        )
        bulkWithRepeatedKeys shouldBe Map(
          (key1, offset(2)) -> update1_K1T2,
          (key1, offset(6)) -> update6_K1T6V2,
          (key2, offset(4)) -> update2_K2T4,
          (key2, offset(10)) -> update8_K2T9,
        )
      }
    }

    "crash recovery (deleteAfter) rolling back to the middle checkpoint" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)

        // Roll back the entire store to the middle checkpoint at T6
        _ <- testStore.deleteAfter(offset(6))

        lastCheckpointAfterDelete <- testStore.latestCheckpointUpTo(offset(6), allCheckpointsFilter)

        // Verify that entries beyond checkpoint at t6 (update4 through update10) are completely wiped out
        checkKey1AtT11 <- testJournal.lookup(key1, offset(11))
        checkKey2AtT11 <- testJournal.lookup(key2, offset(11))

        // Verify that data up to and including the rollback point is fully preserved
        checkKey1AtT6 <- testJournal.lookup(key1, offset(6))
        checkKey2AtT6 <- testJournal.lookup(key2, offset(6))
      } yield {
        lastCheckpointAfterDelete shouldBe Some(cpAtT6)

        checkKey1AtT11 shouldBe Some(update6_K1T6V2)
        checkKey2AtT11 shouldBe Some(update3_K2T5)

        checkKey1AtT6 shouldBe checkKey1AtT11
        checkKey2AtT6 shouldBe checkKey2AtT11
      }
    }

    "pruning (deleteUpTo) safely scrubbing historical state up to the middle checkpoint" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)

        // 1. Prune history up to the middle checkpoint (exclusive)
        _ <- testStore.deleteUpTo(offset(6))

        // 2. Check first checkpoint after the prune
        firstCheckpointAfterDelete <- testStore.firstCheckpointAfter(
          offset(0),
          allCheckpointsFilter,
        )

        // 3. Query historical windows to check if the operation safely preserved active frontier references
        preservedHistoryKey1AtT6 <- testJournal.lookup(key1, offset(6))
        preservedHistoryKey2AtT6 <- testJournal.lookup(key2, offset(6))

        // we have to lookup for the last entry before the prune checkpoint to see if
        // those are indeed deleted
        preservedHistoryKey1AtT5 <- testJournal.lookup(key1, offset(5))

        retainedKey1AtT10 <- testJournal.lookup(key1, offset(10))

        // After prune, we still need to see that the replaces times are correct
        _ <- testStore.checkReplacesInvariant()

        // perform another pruning operation because now we have some rows with dangling previous references.

        _ <- testStore.deleteUpTo(offset(8))

        retainedKey1AtT10a <- testJournal.lookup(key1, offset(10))
        preservedKey1AtT7 <- testJournal.lookup(key1, offset(9))
        retainedKey2AtT10a <- testJournal.lookup(key2, offset(10))

        _ <- testStore.checkReplacesInvariant()
      } yield {
        firstCheckpointAfterDelete shouldBe Some(cpAtT6)

        // we preserve because update4 is the latest digest for K1 before cpAtT6
        preservedHistoryKey1AtT6 shouldBe Some(update6_K1T6V2)
        // but we delete all the updates before T6V0
        preservedHistoryKey1AtT5 shouldBe None

        // we preserve because update6 is the latest digest for K2 before cpAtT6
        preservedHistoryKey2AtT6 shouldBe Some(update3_K2T5)

        // Data past the boundary must stay intact
        retainedKey1AtT10 shouldBe Some(update9_K1T10)

        retainedKey1AtT10a shouldBe Some(update9_K1T10)
        preservedKey1AtT7 shouldBe Some(update7_K1T7)
        retainedKey2AtT10a shouldBe Some(update8_K2T9)
      }
    }

    "pruning (deleteUpTo) deletes last updates if they are tombstones" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)

        cp7 = Checkpoint(offset(7), t(7), ReconciliationIntervalBoundary)
        // 2. insert new checkpoint at update6 T7
        _ <- testStore.insertCheckpointTime(cp7)

        // 1. Prune history up to T7 checkpoint (exclusive)
        // because the latest T6_V2 update on Key1 is a tombstone (first less than T7)
        _ <- testStore.deleteUpTo(offset(7))

        // 2. Check first checkpoint after T0
        firstCheckpointAfterDelete <- testStore.firstCheckpointAfter(
          offset(0),
          allCheckpointsFilter,
        )

        // 3. Because deleteUpTo is exclusive for T7 and lookup is inclusive
        // we can verify if the tombstone is gone at T6_V2
        removedTombstoneKey1AtT6V2 <- testJournal.lookup(key1, offset(6))

        // 4. But at T7 the update7 at T7 should be the last entry
        lastPreservedHistoryKey1AtT7 <- testJournal.lookup(key1, offset(7))

        _ <- testStore.checkReplacesInvariant()
      } yield {
        // We should keep T7 because deleteUpTo is exclusive (unless it is a tombstone)
        firstCheckpointAfterDelete shouldBe Some(cp7)

        // we don't have anything because update 6 at T6V2 is a tombstone (inclusive)
        removedTombstoneKey1AtT6V2 shouldBe None

        lastPreservedHistoryKey1AtT7 shouldBe Some(update7_K1T7)
      }
    }

  }

  protected def acsDigestSingleStoreTests(
      mkStore: ExecutionContext => AcsDigestStore
  ): Unit = {

    val checkpoint0 @ Checkpoint(Timepoint(offset0), _) = checkpoint(
      offsetTime(PositiveLong.tryCreate(10))
    )
    val checkpoint1 @ Checkpoint(Timepoint(offset1), _) = checkpoint(
      offsetTime(PositiveLong.tryCreate(20))
    )
    val checkpoint2 @ Checkpoint(Timepoint(offset2), _) = checkpoint(
      offsetTime(PositiveLong.tryCreate(30)),
      CheckpointType.AffirmationIntervalBoundary,
    )
    val checkpoint3 @ Checkpoint(Timepoint(offset3), _) = checkpoint(
      offsetTime(PositiveLong.tryCreate(40)),
      CheckpointType.Reinitialization,
    )

    val party1: InternedPartyId = 1
    val party2: InternedPartyId = 2

    def twoThousandsPartyKeys: List[InternedPartyId] =
      (1 to 2000).toList

    val participantId1: InternedParticipantId = internedParticipantId(10_001)
    val participantId2: InternedParticipantId = internedParticipantId(10_002)

    val rawDigest1 = genRawDigest(0x2a)
    val rawDigest2 = genRawDigest(0x2b)

    val participantDigest1 = rawDigest1
    val participantDigest2 = rawDigest2

    def thousandParticipantIds: List[InternedParticipantId] =
      (11_000 to 12_000).toList

    "an empty store" should {
      "checkpointing: firstCheckpointAfter and latestCheckpointsUpTo calls yields None" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
        val emptyStore = mkStore(executionContext)

        for {
          firstAfterT0 <- emptyStore.firstCheckpointAfter(offset0, allCheckpointsFilter)
          firstAfterT0Reinit <- emptyStore.firstCheckpointAfter(
            offset0,
            Some(NonEmpty(Set, CheckpointType.Reinitialization)),
          )
          latestUpToT2 <- emptyStore.latestCheckpointUpTo(offset2, allCheckpointsFilter)
          latestUpToT2Party <- emptyStore.latestCheckpointUpTo(
            offset2,
            Some(NonEmpty(Set, CheckpointType.PartyHostingChange)),
          )

          // Latest checkpoints with various types
          latestReconciliationCp <- emptyStore.latestReconciliationCheckpoint()
          latestReinitializationCp <- emptyStore.latestReinitializationCheckpoint()
          latestTickCp <- emptyStore.latestTickCheckpoint()
        } yield {
          firstAfterT0 shouldEqual None
          firstAfterT0Reinit shouldEqual None
          latestUpToT2 shouldEqual None
          latestUpToT2Party shouldEqual None

          latestReconciliationCp shouldEqual None
          latestReinitializationCp shouldEqual None
          latestTickCp shouldEqual None
        }
      }

      "with an empty party journal" should {
        behave like emptyDigestJournalTests(
          () => mkStore(executionContext).party,
          sampleKeys = List(party1, party2),
          largeKeySet = twoThousandsPartyKeys,
          rangeStart = offset0,
          rangeEnd = offset3,
        )
      }

      "with an empty participant journal" should {
        behave like emptyDigestJournalTests(
          () => mkStore(executionContext).participant,
          sampleKeys = List(participantId1, participantId2),
          largeKeySet = thousandParticipantIds,
          rangeStart = offset0,
          rangeEnd = offset3,
        )
      }
    }

    "a non empty store with only checkpointing" should {
      "correctly index, retrieve boundaries, and return None when out of bounds" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
        val store = mkStore(executionContext)

        for {
          _ <- FutureUnlessShutdown
            .sequence(
              List( // Note: checkpoint T0 is not persisted
                checkpoint1,
                checkpoint2,
                checkpoint3,
              ).map(store.insertCheckpointTime)
            )

          // Test exact matches
          exactFloor <- store.latestCheckpointUpTo(offset3, allCheckpointsFilter)
          exactCeil <- store.firstCheckpointAfter(offset1, allCheckpointsFilter)

          // Test intermediate boundaries
          midFloor <- store.latestCheckpointUpTo(offset2, allCheckpointsFilter)
          midCeil <- store.firstCheckpointAfter(offset2, allCheckpointsFilter)

          // Test empty lookups
          emptyFloor <- store.latestCheckpointUpTo(offset0, allCheckpointsFilter)
          emptyCeil <- store.firstCheckpointAfter(offset3, allCheckpointsFilter)

          // Test filtering
          filterFloorExact <- store.latestCheckpointUpTo(
            offset3,
            Some(NonEmpty(Set, checkpoint3.checkpointType)),
          )
          filterCeilExact <- store.firstCheckpointAfter(
            offset0,
            Some(NonEmpty(Set, checkpoint1.checkpointType)),
          )

          filterFloorSkip <- store.latestCheckpointUpTo(
            offset3,
            Some(NonEmpty(Set, checkpoint2.checkpointType)),
          )
          filterCeilSkip <- store.firstCheckpointAfter(
            offset0,
            Some(NonEmpty(Set, checkpoint2.checkpointType)),
          )

          filterFloorEmpty <- store.latestCheckpointUpTo(
            offset3,
            Some(NonEmpty(Set, CheckpointType.MaxEventsWithoutCheckpoint)),
          )
          filterCeilEmpty <- store.firstCheckpointAfter(
            offset0,
            Some(NonEmpty(Set, CheckpointType.MaxEventsWithoutCheckpoint)),
          )

          filterFloorMultiple <- store.latestCheckpointUpTo(
            offset3,
            Some(NonEmpty(Set, checkpoint1.checkpointType, checkpoint2.checkpointType)),
          )
          filterCeilMultiple <- store.firstCheckpointAfter(
            offset0,
            Some(NonEmpty(Set, checkpoint2.checkpointType, checkpoint3.checkpointType)),
          )
        } yield {
          exactFloor shouldBe Some(checkpoint3)
          exactCeil shouldBe Some(checkpoint2)

          midFloor shouldBe Some(checkpoint2)
          midCeil shouldBe Some(checkpoint3)

          emptyFloor shouldBe None
          emptyCeil shouldBe None

          filterFloorExact shouldBe Some(checkpoint3)
          filterCeilExact shouldBe Some(checkpoint1)

          filterFloorSkip shouldBe Some(checkpoint2)
          filterCeilSkip shouldBe Some(checkpoint2)

          filterFloorEmpty shouldBe None
          filterCeilEmpty shouldBe None

          filterFloorMultiple shouldBe Some(checkpoint2)
          filterCeilMultiple shouldBe Some(checkpoint2)
        }
      }

      "allow overwriting checkpoints" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
        val store = mkStore(executionContext)

        val partyHostingChange = Checkpoint(Timepoint(off(10))(ts(8)), PartyHostingChange)
        val reconciliationCheckpoint =
          Checkpoint(Timepoint(off(10))(ts(9)), ReconciliationIntervalBoundary)

        for {
          _ <- store.insertCheckpointTime(partyHostingChange)
          originalCheckpoint <- store.latestCheckpointUpTo(Offset.MaxValue, checkpointTypes = None)
          _ <- store.insertCheckpointTime(reconciliationCheckpoint)
          replacementCheckpoint <- store.latestCheckpointUpTo(
            Offset.MaxValue,
            checkpointTypes = None,
          )
        } yield {
          originalCheckpoint.value shouldBe partyHostingChange
          replacementCheckpoint.value shouldBe reconciliationCheckpoint
        }
      }
    }

    "a non-empty store" should {
      "be operational for party journal updates" should {
        behave like
          functionalDigestJournalTests(
            () => mkStore(executionContext).party,
            key1 = party1,
            key2 = party2,
            digest1 = rawDigest1,
            digest2 = rawDigest2,
            checkpoint0.timepoint,
            checkpoint1.timepoint,
            checkpoint2.timepoint,
          )
      }

      "be operational for participant journal updates" should {
        behave like functionalDigestJournalTests(
          () => mkStore(executionContext).participant,
          key1 = participantId1,
          key2 = participantId2,
          digest1 = participantDigest1,
          digest2 = participantDigest2,
          checkpoint0.timepoint,
          checkpoint1.timepoint,
          checkpoint2.timepoint,
        )
      }

      "be operational on common (store and both sub-journals) updates" should {
        behave like functionalCommonOperationsTests(
          () => mkStore(executionContext),
          party1,
          party2,
          participantId1,
        )
      }

      "verify store and party journal operations on data with higher density and tie-breakers" should {
        behave like complexDensityJournalTests(
          () => mkStore(executionContext),
          _.party,
          key1 = party1,
          key2 = party2,
          digest1 = genRawDigest(0x2a.toByte),
          digest2 = genRawDigest(0x2b.toByte),
        )
      }

      "verify store and participant journal operations on data with higher density and tie-breakers" should {
        behave like complexDensityJournalTests(
          () => mkStore(executionContext),
          _.participant,
          key1 = participantId1,
          key2 = participantId2,
          digest1 = rawDigest1,
          digest2 = rawDigest2,
        )
      }
    }
  }

  protected def acsDigestMultiStoresTests(
      mkStore: (ExecutionContext, IndexedSynchronizer) => AcsDigestStore
  ): Unit = {
    val checkpoint0 = checkpoint(offsetTime(PositiveLong.tryCreate(10)))
    val (offset0, t0) = checkpoint0.timepoint.tupled
    val checkpoint1 = checkpoint(offsetTime(PositiveLong.tryCreate(20)))
    val (offset1, t1) = checkpoint1.timepoint.tupled
    val checkpoint2 = checkpoint(offsetTime(PositiveLong.tryCreate(30)))
    val (offset2, t2) = checkpoint2.timepoint.tupled
    val checkpoint3 = checkpoint(offsetTime(PositiveLong.tryCreate(40)))
    val (offset3, t3) = checkpoint3.timepoint.tupled
    val checkpoint4Reinitialization =
      checkpoint(offsetTime(PositiveLong.tryCreate(50)), Reinitialization)

    val syncA = indexedSynchronizer(synchronizerIndex = 2, name = "synchronizer-A")
    val syncB = indexedSynchronizer(synchronizerIndex = 3, name = "synchronizer-B")

    val partyKey = 1
    val participantKey = internedParticipantId(10_001)
    val rawDigestA = genRawDigest(0x1a.toByte)
    val rawDigestB = genRawDigest(0x2b.toByte)

    val partyUpdateDigestAAtT1 =
      AcsDigestUpdate(
        AcsDigest(partyKey, offset1, t1, Some(rawDigestA), None),
        replacesOffset = None,
      )
    val partyUpdateDigestAAtT2 =
      AcsDigestUpdate(
        AcsDigest(partyKey, offset2, t2, Some(rawDigestA), None),
        replacesOffset = Some(offset1),
      )
    val partyUpdateDigestAAtT3 =
      AcsDigestUpdate(
        AcsDigest(partyKey, offset3, t3, Some(rawDigestA), None),
        replacesOffset = Some(offset2),
      )

    val partyUpdateDigestBAtT1 =
      AcsDigestUpdate(
        AcsDigest(partyKey, offset1, t1, Some(rawDigestB), None),
        replacesOffset = None,
      )
    val partyUpdateDigestBAtT2 =
      AcsDigestUpdate(
        AcsDigest(partyKey, offset2, t2, Some(rawDigestB), None),
        replacesOffset = Some(offset1),
      )
    val partyUpdateDigestBAtT3 =
      AcsDigestUpdate(
        AcsDigest(partyKey, offset3, t3, Some(rawDigestB), None),
        replacesOffset = Some(offset2),
      )

    val participantUpdateAAtT1 = AcsDigestUpdate(
      AcsDigest(participantKey, offset1, t1, Some(rawDigestA), None),
      replacesOffset = None,
    )
    val participantUpdateAAtT2 = AcsDigestUpdate(
      AcsDigest(participantKey, offset2, t2, Some(rawDigestA), None),
      replacesOffset = Some(offset1),
    )
    val participantUpdateAAtT3 = AcsDigestUpdate(
      AcsDigest(participantKey, offset3, t3, Some(rawDigestA), None),
      replacesOffset = Some(offset2),
    )
    val participantUpdateBAtT1 = AcsDigestUpdate(
      AcsDigest(participantKey, offset1, t1, Some(rawDigestB), None),
      replacesOffset = None,
    )
    val participantUpdateBAtT2 = AcsDigestUpdate(
      AcsDigest(participantKey, offset2, t2, Some(rawDigestB), None),
      replacesOffset = Some(offset1),
    )
    val participantUpdateBAtT3 = AcsDigestUpdate(
      AcsDigest(participantKey, offset3, t3, Some(rawDigestB), None),
      replacesOffset = Some(offset2),
    )

    def upsertInsertInto(
        store: AcsDigestStore,
        acsDigestPartyUpdates: immutable.Iterable[AcsDigestUpdate[InternedPartyId]],
        acsParticipantUpdates: immutable.Iterable[AcsDigestUpdate[InternedParticipantId]],
    ) =
      for {
        _ <- store.party.upsertDigestUpdates(acsDigestPartyUpdates)
        _ <- store.participant.upsertDigestUpdates(acsParticipantUpdates)
        _ <- store.insertCheckpointTime(checkpoint1)
        _ <- store.insertCheckpointTime(checkpoint2)

        _ <- store.checkReplacesInvariant()

      } yield succeed

    def upsertInsertTestIntoBothStores(storeA: AcsDigestStore, storeB: AcsDigestStore) =
      for {
        _ <- upsertInsertInto(
          storeA,
          List(partyUpdateDigestAAtT1, partyUpdateDigestAAtT2, partyUpdateDigestAAtT3),
          List(participantUpdateAAtT1, participantUpdateAAtT2, participantUpdateAAtT3),
        )
        _ <- upsertInsertInto(
          storeB,
          List(partyUpdateDigestBAtT1, partyUpdateDigestBAtT2, partyUpdateDigestBAtT3),
          List(participantUpdateBAtT1, participantUpdateBAtT2, participantUpdateBAtT3),
        )
      } yield succeed

    "a multi-synchronizer isolation" should {
      "ensure checkpointing works properly" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
        // Provision two distinct stores with overlapping timelines
        val storeA = mkStore(executionContext, syncA)
        val storeB = mkStore(executionContext, syncB)

        for {
          // Insert T0,T1 into Store A
          _ <- storeA.insertCheckpointTime(checkpoint0)
          _ <- storeA.insertCheckpointTime(checkpoint1)

          // Insert T3 into Store B
          _ <- storeB.insertCheckpointTime(checkpoint3)
          _ <- storeB.insertCheckpointTime(checkpoint4Reinitialization)

          // Latest checkpoint at T3 (inclusive)
          latestCheckpointA_T3 <- storeA.latestCheckpointUpTo(offset3, allCheckpointsFilter)
          latestCheckpointB_T3 <- storeB.latestCheckpointUpTo(offset3, allCheckpointsFilter)

          // First checkpoint after T0 (exclusive)
          firstCheckpointA_T0 <- storeA.firstCheckpointAfter(offset0, allCheckpointsFilter)
          firstCheckpointB_T0 <- storeB.firstCheckpointAfter(offset0, allCheckpointsFilter)

          firstCheckpointA_T3 <- storeA.firstCheckpointAfter(offset3, allCheckpointsFilter)
          firstCheckpointB_T3 <- storeB.firstCheckpointAfter(offset3, allCheckpointsFilter)

          // Reconciliation is a tick checkpoint while Reinitialization is not
          latestReconciliationCpA <- storeA.latestReconciliationCheckpoint()
          latestReconciliationCpB <- storeB.latestReconciliationCheckpoint()

          latestReinitializationCpA <- storeA.latestReinitializationCheckpoint()
          latestReinitializationCpB <- storeB.latestReinitializationCheckpoint()

          latestTickCpA <- storeA.latestTickCheckpoint()
          latestTickCpB <- storeB.latestTickCheckpoint()
        } yield {
          latestCheckpointA_T3 shouldBe Some(checkpoint1)
          latestCheckpointB_T3 shouldBe Some(checkpoint3)

          firstCheckpointA_T0 shouldBe Some(checkpoint1)
          firstCheckpointB_T0 shouldBe Some(checkpoint3)

          firstCheckpointA_T3 shouldBe None
          firstCheckpointB_T3 shouldBe Some(checkpoint4Reinitialization)

          latestReconciliationCpA shouldBe Some(checkpoint1)
          latestReconciliationCpB shouldBe Some(checkpoint3)

          latestReinitializationCpA shouldBe None
          latestReinitializationCpB shouldBe Some(checkpoint4Reinitialization)

          latestTickCpA shouldEqual Some(checkpoint1)
          latestTickCpB shouldEqual Some(checkpoint3)
        }
      }

      "ensure pagination works on range queries (snapshot, changesBetween)" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
        // Provision two distinct stores with overlapping timelines
        val storeA = mkStore(executionContext, syncA)
        val storeB = mkStore(executionContext, syncB)

        for {
          // Setup Interleaved State
          _ <- upsertInsertTestIntoBothStores(storeA, storeB)

          // If the query lacks a synchronizer filter, changesBetween will fetch duplicates (A + B items)
          (changesOnB_T1_T3, _) <- storeB.party.changesBetween(
            tokenOrStart =
              Right(ChangesBetweenOffsetRange(fromInclusive = offset1, toExclusive = offset3)),
            limit = 10,
          )
          (snapshotOnBAtT2, _) <- storeB.party.snapshot(tokenOrStart = Right(offset2), limit = 10)
        } yield {
          // Slicing and Pagination filters must be isolated
          changesOnB_T1_T3 should have size 1
          changesOnB_T1_T3.map(_.digestO.value) should contain only (rawDigestB)

          snapshotOnBAtT2 should have size 1
          snapshotOnBAtT2.headOption.value.digestUpdate.digestO.value shouldBe rawDigestB
        }
      }

      "ensure crash recovery works across synchronizer boundaries" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
        // Provision two distinct stores with overlapping timelines
        val storeA = mkStore(executionContext, syncA)
        val storeB = mkStore(executionContext, syncB)

        for {
          // Setup Interleaved State
          _ <- upsertInsertTestIntoBothStores(storeA, storeB)

          // Crash Recovery Rollback (deleteAfter) ---
          // We wipe out T2 on Store A. Store B must remain unaffected.
          _ <- storeA.deleteAfter(offset1)

          checkpointA_afterRollback <- storeA.latestCheckpointUpTo(offset2, allCheckpointsFilter)
          checkpointB_afterRollback <- storeB.latestCheckpointUpTo(offset2, allCheckpointsFilter)

          rolledBackPartyAT2 <- storeA.party.lookup(partyKey, offset2)
          rolledBackParticipantAT2 <- storeA.participant.lookup(participantKey, offset2)

          preservedPartyBAtT2 <- storeB.party.lookup(partyKey, offset2)
          preservedParticipantBAtT2 <- storeB.participant.lookup(participantKey, offset2)

        } yield {
          checkpointA_afterRollback shouldBe Some(checkpoint1)
          checkpointB_afterRollback shouldBe Some(checkpoint2)

          // Rollbacks must not cross-contaminate shared timelines
          rolledBackPartyAT2 shouldBe Some(partyUpdateDigestAAtT1)
          rolledBackParticipantAT2 shouldBe Some(
            participantUpdateAAtT1
          )

          // Store B should have been completely protected
          preservedPartyBAtT2 shouldBe Some(partyUpdateDigestBAtT2)
          preservedParticipantBAtT2 shouldBe Some(participantUpdateBAtT2)
        }
      }

      "ensure pruning works across synchronizer boundaries" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign inUS {
        // Provision two distinct stores with overlapping timelines
        val storeA = mkStore(executionContext, syncA)
        val storeB = mkStore(executionContext, syncB)

        for {
          // Setup Interleaved State
          _ <- upsertInsertTestIntoBothStores(storeA, storeB)

          // Prune history up to T3 on Store A. Store B must keep its history.
          _ <- storeA.deleteUpTo(offset3)

          prunedPartyHistoryA_T2 <- storeA.party.lookup(partyKey, offset2)
          prunedParticipantHistoryA_T2 <- storeA.participant.lookup(participantKey, offset1)

          retainedPartyHistoryB_T3 <- storeB.party.lookup(partyKey, offset3)
          retainedParticipantHistoryB_T3 <- storeB.participant.lookup(participantKey, offset3)

          _ <- storeA.checkReplacesInvariant()
          _ <- storeB.checkReplacesInvariant()
        } yield {

          // Pruning must be strictly scoped
          prunedPartyHistoryA_T2 shouldBe None // Store A historical state was scrubbed
          prunedParticipantHistoryA_T2 shouldBe None

          retainedPartyHistoryB_T3 shouldBe Some(
            partyUpdateDigestBAtT3
          ) // Store B historical state was protected
          retainedParticipantHistoryB_T3 shouldBe Some(participantUpdateBAtT3)
        }
      }
    }
  }
}
