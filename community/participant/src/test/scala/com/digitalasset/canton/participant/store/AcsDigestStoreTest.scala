// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.crypto.MessageDigestPrototype
import com.digitalasset.canton.crypto.LtHash16
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.store.AcsDigestStore.*
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.{BaseTest, InUS, InternedPartyId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref.IdString
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.ExecutionContext
import scala.util.ChainingSyntax

trait AcsDigestStoreTest extends ChainingSyntax with InUS {
  this: AsyncWordSpecLike & BaseTest =>

  protected val rawDigestByteSize = 2048
  protected val sha256Digest = MessageDigestPrototype.Sha256.newDigest
  protected val mockStringInterning = new MockStringInterning()

  protected def genRawDigest(fill: Byte): RawDigest =
    LtHash16.tryCreate(Array.fill[Byte](rawDigestByteSize)(fill)).getByteString()

  protected def genHashedDigest(rawDigest: RawDigest): HashedDigest =
    sha256Digest
      .digest(rawDigest.toByteArray)
      .array
      .pipe(ByteString.copyFrom)

  protected def rt(epochSeconds: Long, tieBreaker: Long = 0L): RecordTime =
    RecordTime(CantonTimestamp.ofEpochSecond(epochSeconds), tieBreaker)

  protected def localOrderParty(partyIndex: Int): PartyAndOrder[InternedPartyId] =
    PartyAndOrder[InternedPartyId](internedPartyId(partyIndex), order = LocalPartyFirst)

  protected def remoteOrderParty(partyIndex: Int): PartyAndOrder[InternedPartyId] =
    PartyAndOrder[InternedPartyId](internedPartyId(partyIndex), order = RemotePartyFirst)

  protected def indexedSynchronizer(synchronizerIndex: Int, name: String): IndexedSynchronizer = {
    val synchronizerId: SynchronizerId = SynchronizerId.tryFromString(s"$name::id")
    IndexedSynchronizer.tryCreate(synchronizerId, synchronizerIndex)
  }

  protected def genParticipantDigest(rawDigest: RawDigest): (RawDigest, HashedDigest) =
    (rawDigest, genHashedDigest(rawDigest))

  private def internedPartyId(partyInt: Int): InternedPartyId =
    mockStringInterning.party.internalize(LfPartyId.assertFromString(s"testParty::$partyInt"))

  private def internedParticipantId(participantId: Int): InternedParticipantId =
    mockStringInterning.participantId.internalize(
      IdString.ParticipantId.assertFromString(s"testParticipant::$participantId")
    )

  private def externalizeParticipantId(participantId: InternedParticipantId) =
    mockStringInterning.participantId.externalize(participantId)

  /** Reusable validation for verifying that any type of journal behaves correctly when completely
    * empty.
    */
  private def emptyDigestJournalTests[K, V](
      mkJournal: () => AcsDigestStore.DigestJournal[K, V],
      sampleKeys: List[K],
      largeKeySet: List[K],
      tRangeStart: RecordTime,
      tRangeEnd: RecordTime,
  ): Unit = {

    "pointwise 'lookup' calls should yield None" inUS {
      val emptyJournal = mkJournal()
      val firstSampleKey =
        sampleKeys.headOption.getOrElse(fail("sampleKeys shouldn't be empty"))

      for {
        result <- emptyJournal.lookup(firstSampleKey, tRangeEnd)
      } yield result shouldBe Option.empty
    }

    "batch 'bulkLookup' calls must return an empty map" inUS {
      val emptyJournal = mkJournal()

      for {
        result <- emptyJournal.bulkLookup(sampleKeys, tRangeEnd)
      } yield result shouldBe Map.empty
    }

    "batch 'bulkLookup' with huge key spaces should execute without structural failure" inUS {
      val emptyJournal = mkJournal()

      for {
        result <- emptyJournal.bulkLookup(largeKeySet, tRangeEnd)
      } yield result shouldBe Map.empty
    }

    "requesting a pagination 'snapshot' should return empty page" inUS {
      val emptyJournal = mkJournal()

      for {
        (page, nextToken) <- emptyJournal.snapshot(
          tokenOrStart = Right(tRangeEnd),
          limit = 10,
        )
      } yield {
        page shouldBe empty
        nextToken shouldBe Left(PaginationTokenDone)
      }
    }

    "querying state 'changesBetween' ranges should yield completely empty page" inUS {
      val emptyJournal = mkJournal()

      for {
        (page, nextToken) <- emptyJournal.changesBetween(
          Right(
            ChangesBetweenTimeRange(
              fromInclusive = tRangeStart,
              toExclusive = tRangeEnd,
            )
          ),
          limit = 10,
        )
      } yield {
        page shouldBe empty
        nextToken shouldBe Left(PaginationTokenDone)
      }
    }

    "verifying chain consistency via 'checkReplacesInvariant' must pass safely" inUS {
      val emptyJournal = mkJournal()

      for {
        _ <- emptyJournal.checkReplacesInvariant(upToInclusive = tRangeEnd)
      } yield succeed
    }
  }

  /** Reusable operational verification suite covering record lifecycle transitions on a concrete
    * journal.
    */
  private def functionalDigestJournalTests[K, V](
      mkJournal: () => AcsDigestStore.DigestJournal[K, V],
      key1: K,
      key2: K,
      digest1: V,
      digest2: V,
      t0: RecordTime,
      t1: RecordTime,
      t2: RecordTime,
  ): Unit = {

    val update1_K1T0 =
      AcsDigestUpdate(AcsDigest(key1, Some(digest1), t0), replacesRecordTime = None)
    val update2_K2T0 =
      AcsDigestUpdate(AcsDigest(key2, Some(digest2), t0), replacesRecordTime = None)
    // Update key1 at t1
    val update3_K1T1 = AcsDigestUpdate(
      AcsDigest(key1, Some(digest2), t1),
      replacesRecordTime = Some(t0),
    )
    // 'Archive' key2 at t1
    val tombstone_K2T1 = AcsDigestUpdate(
      AcsDigest(key2, None, t1),
      replacesRecordTime = Some(t0),
    )

    val update5_update_K1T1 = AcsDigestUpdate(
      AcsDigest(key1, None, t1),
      replacesRecordTime = Some(t0),
    )

    def upsertUpdatesIn(journal: DigestJournal[K, V]) = for {
      _ <- journal.upsertDigestUpdates(List(update1_K1T0, update2_K2T0))
      _ <- journal.upsertDigestUpdates(List(update3_K1T1, tombstone_K2T1))
      _ <- journal.checkReplacesInvariant(t2)
    } yield succeed

    "allow standard batch item insertion with upsertDigestUpdates" inUS {
      upsertUpdatesIn(mkJournal())
    }

    "return precise historical states on pointwise lookup entries" inUS {
      val testJournal = mkJournal()

      val tMin = RecordTime.MinValue
      val tMax = RecordTime.MaxValue

      for {
        _ <- upsertUpdatesIn(testJournal)

        // before we have any record
        key1StateAtTMin <- testJournal.lookup(key1, tMin)
        key2StateAtTMin <- testJournal.lookup(key2, tMin)

        // at T0
        key1StateAtT0 <- testJournal.lookup(key1, t0)
        key2StateAtT0 <- testJournal.lookup(key2, t0)

        // at T1
        key1StateAtT1 <- testJournal.lookup(key1, t1)
        key2StateAtT1 <- testJournal.lookup(key2, t1)

        // at T2
        key1StateAtT2 <- testJournal.lookup(key1, t2)
        key2StateAtT2 <- testJournal.lookup(key2, t2)

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

    "after updating an entry, pointwise lookup still works well" inUS {
      val testJournal = mkJournal()

      for {
        _ <- upsertUpdatesIn(testJournal)

        // updating K1 at T1 for a tombstone
        _ <- testJournal.upsertDigestUpdates(List(update5_update_K1T1))

        key1StateAtT1 <- testJournal.lookup(key1, t1)
      } yield {
        // K1 at T1
        key1StateAtT1 shouldBe Some(update5_update_K1T1)
      }
    }

    "batch 'bulkLookup' call with empty keys return an empty map" inUS {
      val testJournal = mkJournal()

      for {
        _ <- upsertUpdatesIn(testJournal)
        result <- testJournal.bulkLookup(Seq.empty[K], t2)
      } yield result shouldBe Map.empty
    }

    "evaluate range snapshots using pagination (2 digests on 1 page)" inUS {
      val testJournal = mkJournal()
      for {
        _ <- upsertUpdatesIn(testJournal)
        (page, token) <- testJournal.snapshot(tokenOrStart = Right(t0), limit = 2)
      } yield {
        // no more page indeed
        token shouldBe Left(PaginationTokenDone)

        // At t0, both key1 and key2 are active
        page.map(d => d.key -> d.recordTime).toMap shouldBe Map(
          key1 -> t0,
          key2 -> t0,
        )
      }
    }

    "evaluate range snapshots using pagination (1 digest on 2 pages)" inUS {
      val testJournal = mkJournal()
      for {
        _ <- upsertUpdatesIn(testJournal)
        (page1, token1) <- testJournal.snapshot(tokenOrStart = Right(t0), limit = 1)
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
        (page1 ++ page2).map(d => d.key -> d.recordTime).toMap shouldBe Map(
          key1 -> t0,
          key2 -> t0,
        )
      }
    }

    "isolate range updates using changesBetween boundaries (1 page)" inUS {
      val testJournal = mkJournal()
      for {
        _ <- upsertUpdatesIn(testJournal)
        (changes, token) <- testJournal.changesBetween(
          Right(
            ChangesBetweenTimeRange(
              fromInclusive = t0,
              toExclusive = t2,
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

    "isolate range updates using changesBetween boundaries (2 pages)" inUS {
      val testJournal = mkJournal()
      for {
        _ <- upsertUpdatesIn(testJournal)
        (change1, token1) <- testJournal.changesBetween(
          Right(
            ChangesBetweenTimeRange(
              fromInclusive = t0,
              toExclusive = t2,
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

    "catch future link during checkReplacesInvariant invocation" inUS {
      // Intentionally insert a broken link at T2 that claims to replace an imaginary T99
      val testJournal = mkJournal()
      val rt99 = rt(99)
      val brokenUpdate =
        AcsDigestUpdate(AcsDigest(key1, Some(digest1), t2), replacesRecordTime = Some(rt99))

      for {
        _ <- testJournal.upsertDigestUpdates(List(brokenUpdate))
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          within = testJournal.checkReplacesInvariant(t2),
          assertion = _.getMessage should include(
            s"We cannot have replacesTime=$rt99 which is gte to recordTime $t2"
          ),
        )
      } yield succeed
    }

    "catch empty link during checkReplacesInvariant invocation" inUS {
      val testJournal = mkJournal()
      val update1_K1T1 =
        AcsDigestUpdate(AcsDigest(key1, Some(digest1), t1), replacesRecordTime = None)
      // Not referencing to replace update1
      val update2_K1T2 =
        AcsDigestUpdate(AcsDigest(key1, None, t2), replacesRecordTime = None)

      for {
        _ <- testJournal.upsertDigestUpdates(List(update1_K1T1, update2_K1T2))
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          within = testJournal.checkReplacesInvariant(t2),
          assertion =
            _.getMessage should include(s"Replaces time should point to $t1 but it is empty!"),
        )
      } yield succeed
    }

    "catch invalid past reference during checkReplacesInvariant invocation" inUS {
      val testJournal = mkJournal()
      val update1_K1T0 =
        AcsDigestUpdate(AcsDigest(key1, Some(digest1), t0), replacesRecordTime = None)
      // Referencing to replace a non existent past digest update
      val update2_K1T2 =
        AcsDigestUpdate(AcsDigest(key1, None, t2), replacesRecordTime = Some(t1))

      for {
        _ <- testJournal.upsertDigestUpdates(List(update1_K1T0, update2_K1T2))
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          within = testJournal.checkReplacesInvariant(t2),
          assertion = _.getMessage should include(
            s"which is not pointing to the preceding recordTime=$t0"
          ),
        )
      } yield succeed
    }
  }

  // Functional tests verifying the common operations on the store and sub-journals
  private def functionalCommonOperationsTests(
      mkStore: () => AcsDigestStore,
      partyAndLocalOrderKey1: PartyAndOrder[InternedPartyId],
      partyAndRemoteOrderKey1: PartyAndOrder[InternedPartyId],
      participantId1: InternedParticipantId,
  ): Unit = {
    val t0 = rt(0)
    val t1 = rt(1)
    val t2 = rt(2)

    val rawDigest = genRawDigest(0x2c)
    val participantDigest = genParticipantDigest(rawDigest)

    def checkReplacesIntervalCommonUpserts(store: AcsDigestStore) = {
      // key1 local order
      val partyUpdate1AtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndLocalOrderKey1, Some(rawDigest), t0),
        replacesRecordTime = None,
      )
      // key1 remote order
      val partyUpdate2AtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndRemoteOrderKey1, Some(rawDigest), t0),
        replacesRecordTime = None,
      )
      // key1 local order reference to the T0 data
      val partyUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndLocalOrderKey1, None, t1),
        replacesRecordTime = Some(t0),
      )
      // key1 remote order reference to the T0 data
      val partyUpdate2AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndRemoteOrderKey1, None, t1),
        replacesRecordTime = Some(t0),
      )

      val participantUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, Some(participantDigest), t1),
        replacesRecordTime = None,
      )

      for {
        _ <- store.party.upsertDigestUpdates(
          List(
            partyUpdate1AtT0,
            partyUpdate2AtT0,
            partyUpdate1AtT1,
            partyUpdate2AtT1,
          )
        )
        _ <- store.participant.upsertDigestUpdates(
          List(
            participantUpdate1AtT1
          )
        )
      } yield ()
    }

    s"properly retain data on 'deleteAfter' when there is data at the boundary" inUS {
      val store = mkStore()

      // key1 local order
      val partyUpdate1AtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndLocalOrderKey1, Some(rawDigest), t0),
        replacesRecordTime = None,
      )
      // key1 remote order
      val partyUpdate2AtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndRemoteOrderKey1, Some(rawDigest), t0),
        replacesRecordTime = None,
      )

      val participantUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, Some(participantDigest), t1),
        replacesRecordTime = None,
      )

      for {
        _ <- store.party.upsertDigestUpdates(List(partyUpdate1AtT0, partyUpdate2AtT0))
        _ <- store.participant.upsertDigestUpdates(List(participantUpdate1AtT1))

        // Verify the integrity of replaces time pointers across the updates
        _ <- store.insertCheckpointTime(t2)
        _ <- store.checkReplacesInvariant()

        _ <- store.deleteAfter(t1)

        partyUpdates <- store.party.bulkLookup(
          keys = List(partyAndLocalOrderKey1, partyAndRemoteOrderKey1),
          toInclusive = t0,
        )
        participantUpdate <- store.participant.bulkLookup(
          keys = List(participantId1),
          toInclusive = t1,
        )
      } yield {
        partyUpdates shouldBe (Map(
          partyAndLocalOrderKey1 -> partyUpdate1AtT0,
          partyAndRemoteOrderKey1 -> partyUpdate2AtT0,
        ))
        participantUpdate shouldBe (Map(
          participantId1 -> participantUpdate1AtT1
        ))
      }
    }

    s"properly retain data on 'deleteUpTo' when there is no precedent data" inUS {
      val store = mkStore()

      // key1 local order
      val partyUpdate1AtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndLocalOrderKey1, Some(rawDigest), t0),
        replacesRecordTime = None,
      )
      // key1 remote order
      val partyUpdate2AtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndRemoteOrderKey1, Some(rawDigest), t0),
        replacesRecordTime = None,
      )

      val participantUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, Some(participantDigest), t1),
        replacesRecordTime = None,
      )

      for {
        _ <- store.party.upsertDigestUpdates(List(partyUpdate1AtT0, partyUpdate2AtT0))
        _ <- store.participant.upsertDigestUpdates(List(participantUpdate1AtT1))

        // Verify the integrity of replaces time pointers across the updates
        _ <- store.insertCheckpointTime(t2)
        _ <- store.checkReplacesInvariant()

        _ <- store.deleteUpTo(t1)

        partyUpdates <- store.party.bulkLookup(
          keys = List(partyAndLocalOrderKey1, partyAndRemoteOrderKey1),
          toInclusive = t0,
        )
        participantUpdate <- store.participant.bulkLookup(
          keys = List(participantId1),
          toInclusive = t1,
        )
      } yield {
        partyUpdates shouldBe (Map(
          partyAndLocalOrderKey1 -> partyUpdate1AtT0,
          partyAndRemoteOrderKey1 -> partyUpdate2AtT0,
        ))
        participantUpdate shouldBe (Map(
          participantId1 -> participantUpdate1AtT1
        ))
      }
    }

    s"properly execute global node boundaries across sub-journals" inUS {
      val pruningStore = mkStore()

      // key1 local order
      val partyUpdate1AtT0 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndLocalOrderKey1, Some(rawDigest), t0),
        replacesRecordTime = None,
      )
      for {
        _ <- pruningStore.party.upsertDigestUpdates(List(partyUpdate1AtT0))
        _ <- pruningStore.insertCheckpointTime(t1)

        // Test global prune boundaries across sub-journals
        _ <- pruningStore.deleteAfter(t1)
        _ <- pruningStore.deleteUpTo(t1)

        remainingParty <- pruningStore.party.lookup(partyAndLocalOrderKey1, t0)
      } yield remainingParty shouldBe Some(partyUpdate1AtT0)
    }

    s"'checkReplacesInvariant' check is successful when there is no broken reference in the journals" inUS {
      val store = mkStore()

      val participantUpdate1AtT2 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, None, t2),
        replacesRecordTime = Some(t1),
      )

      for {
        // Inserting all entries except the last one
        _ <- checkReplacesIntervalCommonUpserts(store)

        // Last insertion is also correct
        _ <- store.participant.upsertDigestUpdates(List(participantUpdate1AtT2))

        result <- store.checkReplacesInvariant()
      } yield result shouldBe ()
    }

    s"'checkReplacesInvariant' check is successful when the first few updates has dangling reference (after pruning)" inUS {
      val store = mkStore()

      // key1 local order
      val partyUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndLocalOrderKey1, Some(rawDigest), t1),
        // referring to a deleted digest update
        replacesRecordTime = Some(t0),
      )
      // key1 remote order
      val partyUpdate2AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(partyAndRemoteOrderKey1, Some(rawDigest), t0),
        // referring to a deleted digest update
        replacesRecordTime = Some(t0),
      )

      val participantUpdate1AtT1 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, Some(participantDigest), t1),
        // referring to a deleted digest update
        replacesRecordTime = Some(t0),
      )

      for {
        _ <- store.party.upsertDigestUpdates(
          List(
            partyUpdate1AtT1,
            partyUpdate2AtT1,
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

    s"'checkReplacesInvariant' check fails when there is a broken reference in the journals" inUS {
      val store = mkStore()

      val brokenParticipantUpdate1AtT2 = AcsDigestUpdate(
        digestUpdate = AcsDigest(participantId1, None, t2),
        // broken, should point back to t1
        replacesRecordTime = None,
      )

      for {
        // Inserting all entries except the last one
        _ <- checkReplacesIntervalCommonUpserts(store)

        // Last insertion is the updatw with broken link
        _ <- store.participant.upsertDigestUpdates(List(brokenParticipantUpdate1AtT2))

        // it checks until the last checkpoint T2
        _ <- store.insertCheckpointTime(t2)
        _ <- loggerFactory.assertInternalErrorAsyncUS[IllegalStateException](
          within = store.checkReplacesInvariant(),
          assertion = _.getMessage should include(
            s"ReplacesRecordTime check error for key ${externalizeParticipantId(participantId1)} at $t2 - " +
              s"Replaces time should point to $t1 but it is empty!"
          ),
        )
      } yield succeed
    }
  }

  /** Parameterized verification covering high-density updates, concurrent tie-breakers, checkpoint
    * alignment, crash recovery, and pruning operations.
    */
  private def complexDensityJournalTests[K, V](
      mkStore: () => AcsDigestStore,
      mkJournal: AcsDigestStore => DigestJournal[K, V],
      key1: K,
      key2: K,
      digest1: V,
      digest2: V,
  ): Unit = {
    // Define t0,t1,...t11 starting from 0, increased by 5
    val t = (0L to 55L by 5L).map(seconds => rt(seconds))

    // Define 5 explicit checkpoint positions
    val cpAtT2 = t(2)
    val cpAtT4 = t(4)
    val cpAtT6 = t(6)
    val cpAtT8 = t(8)
    val cpAtT10 = t(10)

    // 3 updates at the exact same timestamp separated by tieBreaker
    val t6_v0 = cpAtT6
    val t6_v1 = cpAtT6.copy(tieBreaker = 1)
    val t6_v2 = cpAtT6.copy(tieBreaker = 2)

    val update1_K1T2 =
      AcsDigestUpdate(AcsDigest(key1, Some(digest1), t(2)), replacesRecordTime = None)
    val update2_K2T4 =
      AcsDigestUpdate(AcsDigest(key2, Some(digest2), t(4)), replacesRecordTime = None)
    val update3_K2T5 =
      AcsDigestUpdate(
        AcsDigest(key2, Some(digest1), t(5)),
        replacesRecordTime = Some(t(4)),
      )

    val update4_K1T6V0 =
      AcsDigestUpdate(
        AcsDigest(key1, Some(digest2), t6_v0),
        replacesRecordTime = Some(t(2)),
      )
    val update5_K1T6V1 =
      AcsDigestUpdate(AcsDigest(key1, Some(digest1), t6_v1), replacesRecordTime = Some(t6_v0))
    val update6_K1T6V2 = // Tombstone
      AcsDigestUpdate(AcsDigest(key1, None, t6_v2), replacesRecordTime = Some(t6_v1))

    // Re-create key1
    val update7_K1T7 = AcsDigestUpdate(
      AcsDigest(key1, Some(digest1), t(7)),
      replacesRecordTime = Some(t6_v2),
    )
    val update8_K2T9 =
      AcsDigestUpdate(AcsDigest(key2, Some(digest2), t(9)), replacesRecordTime = Some(t(5)))
    val update9_K1T10 =
      AcsDigestUpdate(
        AcsDigest(key1, Some(digest2), t(10)),
        replacesRecordTime = Some(t(7)),
      )
    val update10_K2T11 = // Tombstone
      AcsDigestUpdate(AcsDigest(key2, None, t(11)), replacesRecordTime = Some(t(9)))

    // batch insert all digest updates and checkpoints
    // we are interested about the final state not a realistic order of insertion
    // Note: in a realistic scenario the checkpoint insertions would interleave with the digest updates appropriately
    def upsertInsertCheckpointsInto(
        testStore: AcsDigestStore,
        testJournal: DigestJournal[K, V],
    ) = for {
      // 1. Commit all 10 updates
      _ <- testJournal.upsertDigestUpdates(
        List(
          update1_K1T2,
          update2_K2T4,
          update3_K2T5,
          update4_K1T6V0,
          update5_K1T6V1,
          update6_K1T6V2,
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

    "upsert -> insert checkpoints -> pointwise lookup verification" inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        // 1. Commit all 10 updates and insert 5 checkpoints
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)

        // 2. Verify point-in-time state checks handle tie-breakers accurately
        lookupV0 <- testJournal.lookup(key1, t6_v0)
        lookupV1 <- testJournal.lookup(key1, t6_v1)
        lookupV2 <- testJournal.lookup(key1, t6_v2)
      } yield {
        lookupV0 shouldBe Some(update4_K1T6V0)
        lookupV1 shouldBe Some(update5_K1T6V1)
        lookupV2 shouldBe Some(update6_K1T6V2)
      }
    }

    "upsert -> insert checkpoints -> bulk lookup verification" inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        // Commit all 10 updates and insert 5 checkpoints
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)
        // Evaluate batch retrieval across mixed chronological indices at the middle boundary
        bulkAtMiddle <- testJournal.bulkLookup(List(key1, key2), t6_v2)
      } yield bulkAtMiddle shouldBe Map(
        key1 -> update6_K1T6V2,
        key2 -> update3_K2T5,
      )
    }

    "crash recovery (deleteAfter) rolling back to the middle checkpoint" inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)

        // Roll back the entire store to the middle checkpoint (atCheckpoint3 = rt(30, 0))
        _ <- testStore.deleteAfter(cpAtT6)

        lastCheckpointAfterDelete <- testStore.latestCheckpointUpTo(cpAtT6)

        // Verify that entries beyond checkpoint at t6 (update4 through update10) are completely wiped out
        checkKey1AtT11 <- testJournal.lookup(key1, t(11))
        checkKey2AtT11 <- testJournal.lookup(key2, t(11))

        // Verify that data up to and including the rollback point is fully preserved
        checkKey1AtT6 <- testJournal.lookup(key1, cpAtT6)
        checkKey2AtT6 <- testJournal.lookup(key2, cpAtT6)
      } yield {
        lastCheckpointAfterDelete shouldBe Some(cpAtT6)

        checkKey1AtT11 shouldBe Some(update4_K1T6V0)
        checkKey2AtT11 shouldBe Some(update3_K2T5)

        checkKey1AtT6 shouldBe checkKey1AtT11
        checkKey2AtT6 shouldBe checkKey2AtT11
      }
    }

    "pruning (deleteUpTo) safely scrubbing historical state up to the middle checkpoint" inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)

        // 1. Prune history up to the middle checkpoint (exclusive)
        _ <- testStore.deleteUpTo(cpAtT6)

        // 2. Check first checkpoint after the prune
        firstCheckpointAfterDelete <- testStore.firstCheckpointAfter(t(0))

        // 3. Query historical windows to check if the operation safely preserved active frontier references
        preservedHistoryKey1AtT6 <- testJournal.lookup(key1, cpAtT6)
        preservedHistoryKey2AtT6 <- testJournal.lookup(key2, cpAtT6)

        // we have to lookup for the last entry before the prune checkpoint to see if
        // those are indeed deleted
        preservedHistoryKey1AtT5 <- testJournal.lookup(key1, t(5))

        retainedKey1AtT10 <- testJournal.lookup(key1, cpAtT10)

        // After prune, we still need to see that the replaces times are correct
        _ <- testStore.checkReplacesInvariant()
      } yield {
        firstCheckpointAfterDelete shouldBe Some(cpAtT6)

        // we preserve because update4 is the latest digest for K1 before cpAtT6
        preservedHistoryKey1AtT6 shouldBe Some(update4_K1T6V0)
        // but we delete all the updates before T6V0
        preservedHistoryKey1AtT5 shouldBe None

        // we preserve because update6 is the latest digest for K2 before cpAtT6
        preservedHistoryKey2AtT6 shouldBe Some(update3_K2T5)

        // Data past the boundary must stay intact
        retainedKey1AtT10 shouldBe Some(update9_K1T10)
      }
    }

    "pruning (deleteUpTo) deletes last updates if they are tombstones" inUS {
      val testStore = mkStore()
      val testJournal = mkJournal(testStore)

      for {
        _ <- upsertInsertCheckpointsInto(testStore, testJournal)

        // 2. insert new checkpoint at update6 T7
        _ <- testStore.insertCheckpointTime(t(7))

        // 1. Prune history up to T7 checkpoint (exclusive)
        // because the latest T6_V2 update on Key1 is a tombstone (first less than T7)
        _ <- testStore.deleteUpTo(t(7))

        // 2. Check first checkpoint after T0
        firstCheckpointAfterDelete <- testStore.firstCheckpointAfter(t(0))

        // 3. Because deleteUpTo is exclusive for T7 and lookup is inclusive
        // we can verify if the tombstone is gone at T6_V2
        removedTombstoneKey1AtT6V2 <- testJournal.lookup(key1, t6_v2)

        // 4. But at T7 the update7 at T7 should be the last entry
        lastPreservedHistoryKey1AtT7 <- testJournal.lookup(key1, t(7))

        _ <- testStore.checkReplacesInvariant()
      } yield {
        // We should keep T7 because deleteUpTo is exclusive (unless it is a tombstone)
        firstCheckpointAfterDelete shouldBe Some(t(7))

        // we don't have anything because update 6 at T6V2 is a tombstone (inclusive)
        removedTombstoneKey1AtT6V2 shouldBe None

        lastPreservedHistoryKey1AtT7 shouldBe Some(update7_K1T7)
      }
    }

  }

  protected def acsDigestSingleStoreTests(
      mkStore: ExecutionContext => AcsDigestStore
  ): Unit = {

    val t0 = rt(0)
    val t1 = rt(1)
    val t1_1 = rt(1, 1)
    val t2 = rt(2)
    val t3 = rt(3)

    val partyAndLocalOrderKey1 = localOrderParty(partyIndex = 1)
    val partyAndRemoteOrderKey1 = remoteOrderParty(partyIndex = 1)
    val partyAndLocalOrderKey2 = localOrderParty(partyIndex = 2)

    def twoThousandsPartyKeys =
      (1 to 1000).flatMap(i => List(localOrderParty(i), remoteOrderParty(i))).toList

    val participantId1: InternedParticipantId = internedParticipantId(10_001)
    val participantId2: InternedParticipantId = internedParticipantId(10_002)

    val rawDigest1 = genRawDigest(0x2a)
    val rawDigest2 = genRawDigest(0x2b)

    val participantDigest1 = (rawDigest1, genHashedDigest(rawDigest1))
    val participantDigest2 = (rawDigest2, genHashedDigest(rawDigest2))

    def thousandParticipantIds: List[InternedParticipantId] =
      (11_000 to 12_000).toList

    "an empty store" should {
      "checkpointing: firstCheckpointAfter and latestCheckpointsUpTo calls yields None" inUS {
        val emptyStore = mkStore(executionContext)

        for {
          firstAfterT0 <- emptyStore.firstCheckpointAfter(t0)
          latestUpToT2 <- emptyStore.latestCheckpointUpTo(t2)
        } yield {
          firstAfterT0 shouldEqual None
          latestUpToT2 shouldEqual None
        }
      }

      "with an empty party journal" should {
        behave like emptyDigestJournalTests(
          () => mkStore(executionContext).party,
          sampleKeys =
            List(partyAndLocalOrderKey1, partyAndRemoteOrderKey1, partyAndLocalOrderKey2),
          largeKeySet = twoThousandsPartyKeys,
          tRangeStart = t0,
          tRangeEnd = t2,
        )
      }

      "with an empty participant journal" should {
        behave like emptyDigestJournalTests(
          () => mkStore(executionContext).participant,
          sampleKeys = List(participantId1, participantId2),
          largeKeySet = thousandParticipantIds,
          tRangeStart = t0,
          tRangeEnd = t2,
        )
      }
    }

    "a non empty store with only checkpointing" should {
      "correctly index, retrieve boundaries, and return None when out of bounds" inUS {
        val store = mkStore(executionContext)

        for {
          _ <- FutureUnlessShutdown
            .sequence(
              List( // Note: t0 is not persisted
                t1,
                t1_1,
                t2,
                t3,
              ).map(store.insertCheckpointTime)
            )

          // Test exact matches
          exactFloor <- store.latestCheckpointUpTo(t3)
          exactCeil <- store.firstCheckpointAfter(t1)

          // Test intermediate boundaries
          midFloor <- store.latestCheckpointUpTo(t1_1)
          midCeil <- store.firstCheckpointAfter(t1_1)

          // Test empty lookups
          emptyFloor <- store.latestCheckpointUpTo(t0)
          emptyCeil <- store.firstCheckpointAfter(t3)
        } yield {
          exactFloor shouldBe Some(t3)
          exactCeil shouldBe Some(t1_1)

          midFloor shouldBe Some(t1_1)
          midCeil shouldBe Some(t2)

          emptyFloor shouldBe None
          emptyCeil shouldBe None
        }
      }
    }

    "a non-empty store" should {
      "be operational for party journal updates" should {
        behave like
          functionalDigestJournalTests(
            () => mkStore(executionContext).party,
            key1 = partyAndLocalOrderKey1,
            key2 = partyAndLocalOrderKey2,
            digest1 = rawDigest1,
            digest2 = rawDigest2,
            t0 = t0,
            t1 = t1,
            t2 = t2,
          )
      }

      "be operational for participant journal updates" should {
        behave like functionalDigestJournalTests(
          () => mkStore(executionContext).participant,
          key1 = participantId1,
          key2 = participantId2,
          digest1 = participantDigest1,
          digest2 = participantDigest2,
          t0 = t0,
          t1 = t1,
          t2 = t2,
        )
      }

      "be operational on common (store and both sub-journals) updates" should {
        behave like functionalCommonOperationsTests(
          () => mkStore(executionContext),
          partyAndLocalOrderKey1,
          partyAndLocalOrderKey2,
          participantId1,
        )
      }

      "verify store and party journal operations on data with higher density and tie-breakers" should {
        behave like complexDensityJournalTests(
          () => mkStore(executionContext),
          _.party,
          key1 = partyAndLocalOrderKey1,
          key2 = partyAndLocalOrderKey2,
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
          digest1 = (rawDigest1, genHashedDigest(rawDigest1)),
          digest2 = (rawDigest2, genHashedDigest(rawDigest2)),
        )
      }
    }
  }

  protected def acsDigestMultiStoresTests(
      mkStore: (ExecutionContext, IndexedSynchronizer) => AcsDigestStore
  ): Unit = {
    val t0 = rt(0)
    val t1 = rt(10)
    val t2 = rt(20)
    val t3 = rt(30)

    val syncA = indexedSynchronizer(synchronizerIndex = 2, name = "synchronizer-A")
    val syncB = indexedSynchronizer(synchronizerIndex = 3, name = "synchronizer-B")

    val partyKey = localOrderParty(partyIndex = 1)
    val rawDigestA = genRawDigest(0x1a.toByte)
    val rawDigestB = genRawDigest(0x2b.toByte)

    val participantKey = internedParticipantId(10_001)
    val participantDigestA = genParticipantDigest(rawDigestA)
    val participantDigestB = genParticipantDigest(rawDigestB)

    val partyUpdateDigestAAtT1 =
      AcsDigestUpdate(AcsDigest(partyKey, Some(rawDigestA), t1), replacesRecordTime = None)
    val partyUpdateDigestAAtT2 =
      AcsDigestUpdate(AcsDigest(partyKey, Some(rawDigestA), t2), replacesRecordTime = Some(t1))
    val partyUpdateDigestAAtT3 =
      AcsDigestUpdate(AcsDigest(partyKey, Some(rawDigestA), t3), replacesRecordTime = Some(t2))

    val partyUpdateDigestBAtT1 =
      AcsDigestUpdate(AcsDigest(partyKey, Some(rawDigestB), t1), replacesRecordTime = None)
    val partyUpdateDigestBAtT2 =
      AcsDigestUpdate(AcsDigest(partyKey, Some(rawDigestB), t2), replacesRecordTime = Some(t1))
    val partyUpdateDigestBAtT3 =
      AcsDigestUpdate(AcsDigest(partyKey, Some(rawDigestB), t3), replacesRecordTime = Some(t2))

    val participantUpdateAAtT1 = AcsDigestUpdate(
      AcsDigest(participantKey, Some(participantDigestA), t1),
      replacesRecordTime = None,
    )
    val participantUpdateAAtT2 = AcsDigestUpdate(
      AcsDigest(participantKey, Some(participantDigestA), t2),
      replacesRecordTime = Some(t1),
    )
    val participantUpdateAAtT3 = AcsDigestUpdate(
      AcsDigest(participantKey, Some(participantDigestA), t3),
      replacesRecordTime = Some(t2),
    )
    val participantUpdateBAtT1 = AcsDigestUpdate(
      AcsDigest(participantKey, Some(participantDigestB), t1),
      replacesRecordTime = None,
    )
    val participantUpdateBAtT2 = AcsDigestUpdate(
      AcsDigest(participantKey, Some(participantDigestB), t2),
      replacesRecordTime = Some(t1),
    )
    val participantUpdateBAtT3 = AcsDigestUpdate(
      AcsDigest(participantKey, Some(participantDigestB), t3),
      replacesRecordTime = Some(t2),
    )

    def upsertInsertInto(
        store: AcsDigestStore,
        acsDigestPartyUpdates: List[AcsDigestUpdate[PartyAndOrder[InternedPartyId], RawDigest]],
        acsParticipantUpdates: List[
          AcsDigestUpdate[InternedParticipantId, (RawDigest, HashedDigest)]
        ],
    ) =
      for {
        _ <- store.party.upsertDigestUpdates(
          acsDigestPartyUpdates
        )
        _ <- store.participant.upsertDigestUpdates(
          acsParticipantUpdates
        )
        _ <- store.insertCheckpointTime(t1)
        _ <- store.insertCheckpointTime(t2)

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
      "ensure checkpointing works properly" inUS {
        // Provision two distinct stores with overlapping timelines
        val storeA = mkStore(executionContext, syncA)
        val storeB = mkStore(executionContext, syncB)

        for {
          // Insert T0,T1 into Store A
          _ <- storeA.insertCheckpointTime(t0)
          _ <- storeA.insertCheckpointTime(t1)

          // Insert T3 into Store B
          _ <- storeB.insertCheckpointTime(t3)

          // Latest checkpoint at T3 (inclusive)
          latestCheckpointA_T3 <- storeA.latestCheckpointUpTo(t3)
          latestCheckpointB_T3 <- storeB.latestCheckpointUpTo(t3)

          // First checkpoint after T0 (exclusive)
          firstCheckpointA_T0 <- storeA.firstCheckpointAfter(t0)
          firstCheckpointB_T0 <- storeB.firstCheckpointAfter(t0)

          firstCheckpointA_T3 <- storeA.firstCheckpointAfter(t3)
          firstCheckpointB_T3 <- storeB.firstCheckpointAfter(t3)
        } yield {
          latestCheckpointA_T3 shouldBe Some(t1)
          latestCheckpointB_T3 shouldBe Some(t3)

          firstCheckpointA_T0 shouldBe Some(t1)
          firstCheckpointB_T0 shouldBe Some(t3)

          firstCheckpointA_T3 shouldBe None
          firstCheckpointB_T3 shouldBe None
        }
      }

      "ensure pagination works on range queries (snapshot, changesBetween)" inUS {
        // Provision two distinct stores with overlapping timelines
        val storeA = mkStore(executionContext, syncA)
        val storeB = mkStore(executionContext, syncB)

        for {
          // Setup Interleaved State
          _ <- upsertInsertTestIntoBothStores(storeA, storeB)

          // If the query lacks a synchronizer filter, changesBetween will fetch duplicates (A + B items)
          (changesOnB_T1_T3, _) <- storeB.party.changesBetween(
            tokenOrStart = Right(ChangesBetweenTimeRange(fromInclusive = t1, toExclusive = t3)),
            limit = 10,
          )
          (snapshotOnBAtT2, _) <- storeB.party.snapshot(tokenOrStart = Right(t2), limit = 10)
        } yield {
          // Slicing and Pagination filters must be isolated
          changesOnB_T1_T3 should have size 1
          changesOnB_T1_T3.map(_.digestO.value) should contain only (rawDigestB)

          snapshotOnBAtT2 should have size 1
          snapshotOnBAtT2.headOption.value.digestO.value shouldBe rawDigestB
        }
      }

      "ensure crash recovery works across synchronizer boundaries" inUS {
        // Provision two distinct stores with overlapping timelines
        val storeA = mkStore(executionContext, syncA)
        val storeB = mkStore(executionContext, syncB)

        for {
          // Setup Interleaved State
          _ <- upsertInsertTestIntoBothStores(storeA, storeB)

          // Crash Recovery Rollback (deleteAfter) ---
          // We wipe out T2 on Store A. Store B must remain unaffected.
          _ <- storeA.deleteAfter(t1)

          checkpointA_afterRollback <- storeA.latestCheckpointUpTo(t2)
          checkpointB_afterRollback <- storeB.latestCheckpointUpTo(t2)

          rolledBackPartyAT2 <- storeA.party.lookup(partyKey, t2)
          rolledBackParticipantAT2 <- storeA.participant.lookup(participantKey, t2)

          preservedPartyBAtT2 <- storeB.party.lookup(partyKey, t2)
          preservedParticipantBAtT2 <- storeB.participant.lookup(participantKey, t2)

        } yield {
          checkpointA_afterRollback shouldBe Some(t1)
          checkpointB_afterRollback shouldBe Some(t2)

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

      "ensure pruning works across synchronizer boundaries" inUS {
        // Provision two distinct stores with overlapping timelines
        val storeA = mkStore(executionContext, syncA)
        val storeB = mkStore(executionContext, syncB)

        for {
          // Setup Interleaved State
          _ <- upsertInsertTestIntoBothStores(storeA, storeB)

          // Prune history up to T3 on Store A. Store B must keep its history.
          _ <- storeA.deleteUpTo(t3)

          prunedPartyHistoryA_T2 <- storeA.party.lookup(partyKey, t2)
          prunedParticipantHistoryA_T2 <- storeA.participant.lookup(participantKey, t1)

          retainedPartyHistoryB_T3 <- storeB.party.lookup(partyKey, t3)
          retainedParticipantHistoryB_T3 <- storeB.participant.lookup(participantKey, t3)

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
