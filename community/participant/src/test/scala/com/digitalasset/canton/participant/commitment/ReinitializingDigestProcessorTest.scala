// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.{InternalIndexService, SynchronizerIndex}
import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  AcsUpdate,
  CheckpointFence,
  NotCheckpointFence,
  ProcessingContext,
}
import com.digitalasset.canton.participant.config.{AcsCommitmentConfig, AcsDigestTracingMode}
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigestUpdate,
  Checkpoint,
  CheckpointType,
  HashedDigest,
  RawDigest,
}
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import com.digitalasset.canton.participant.store.{
  AcsDigestStore,
  AcsDigestTestBase,
  PaginationTokenDone,
}
import com.digitalasset.canton.platform.store.backend.LedgerEnd
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.protocol.SynchronizerParameters.WithValidity
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ParticipantId,
  TestingIdentityFactory,
  TestingTopology,
}
import com.digitalasset.canton.{HasActorSystem, HasExecutionContext}
import org.apache.pekko.stream.scaladsl.Sink

import scala.util.ChainingSyntax

@AcsCommitmentTest
class ReinitializingDigestProcessorTest
    extends BaseDigestProcessorTest
    with HasExecutionContext
    with HasActorSystem
    with ChainingSyntax
    with AcsDigestTestBase {

  import BaseDigestProcessorTest.*

  private def mkInMemoryDigestStore(
      stringInterning: StringInterning = mockStringInterning
  ) =
    InMemoryAcsDigestStore.create(Eval.now(stringInterning), loggerFactory)

  private def mkReinitializingDigestProcessor(
      reinitTimepoint: Timepoint,
      participant: ParticipantId = thisParticipant,
      indexService: InternalIndexService = mkIndexService(),
      acsDigestStore: AcsDigestStore = mkInMemoryDigestStore(),
      counterpartyBatchSize: Int = 10,
      writeJournalTombstonesBatchSize: PositiveInt = PositiveInt.tryCreate(5),
      testingTopologyFactory: TestingIdentityFactory = TestingTopology(
        topology = Map.empty,
        synchronizerParameters = List(
          WithValidity(
            CantonTimestamp.MinValue,
            None,
            DynamicSynchronizerParameters
              .defaultValues(testedProtocolVersion),
          )
        ),
      ).build(),
      hasLedgerEnd: Boolean = true,
  ): ReinitializingDigestProcessor = {
    val testSynchronizerId = DefaultTestIdentities.synchronizerId
    val mockLedgerApiStore: LedgerApiStore = {
      val ledgerEndO = Option.when(hasLedgerEnd)(
        LedgerEnd(
          reinitTimepoint.offset,
          reinitTimepoint.offset.unwrap,
          reinitTimepoint.offset.unwrap.toInt,
          reinitTimepoint.recordTime,
          Map(
            testSynchronizerId -> SynchronizerIndex(
              repairIndex = None,
              sequencerIndex = Some(reinitTimepoint.recordTime),
              recordTime = reinitTimepoint.recordTime,
            )
          ),
        )
      )
      val mockStore = mock[LedgerApiStore]
      when(
        mockStore.ledgerEnd
      ).thenAnswer(ledgerEndO)
      mockStore
    }
    new ReinitializingDigestProcessor(
      thisParticipantId = participant,
      synchronizerId = testSynchronizerId,
      stringInterningEval = Eval.now(mockStringInterning),
      indexService = indexService,
      ledgerApiStore = mockLedgerApiStore,
      acsDigestStore = acsDigestStore,
      acsCommitmentConfig = AcsCommitmentConfig(
        counterpartyBatchSize = PositiveInt.tryCreate(counterpartyBatchSize),
        reinitializingJournalTombstonesBatchSize = writeJournalTombstonesBatchSize,
        tracing = AcsDigestTracingMode.Disabled,
      ),
      hashOps = TestHash,
      getTopologySnapshot = ts =>
        testingTopologyFactory.topologySnapshot(timestampOfSnapshot = ts.value),
      loggerFactory = loggerFactory,
    )
  }

  "ReinitializingDigestProcessor" when {
    val internedP1Id = mockStringInterning.participantId.internalize(p1.toLf)
    val internedP2Id = mockStringInterning.participantId.internalize(p2.toLf)
    val internedP4Id = mockStringInterning.participantId.internalize(p4.toLf)

    // participant Digest updates for testing
    val p1DigestOff100 =
      acsDigest(100, internedP1Id, Some(genParticipantDigest(genRawDigest(0x2a))))
    val p1DigestOff99 =
      acsDigest(99, internedP1Id, Some(genParticipantDigest(genRawDigest(0x3a))))
    val p1DigestOff98 =
      acsDigest(98, internedP1Id, Some(genParticipantDigest(genRawDigest(0x4a))))
    val p1DigestTombstoneOff99 = p1DigestOff99.copy(digestO = None)
    val p1DigestTombstoneOff100 = acsDigest(100, internedP1Id, None)

    val p1DigestUpdate3_AtOff100 = AcsDigestUpdate(p1DigestOff100, Some(off(99)))
    val p1DigestUpdate2_AtOff99 = AcsDigestUpdate(p1DigestOff99, Some(off(98)))
    val p1DigestUpdate1_AtOff98 = AcsDigestUpdate(p1DigestOff98, None)
    val p1DigestUpdateTombstone_AtOff99 = AcsDigestUpdate(p1DigestTombstoneOff99, Some(off(98)))
    val p1DigestUpdateTombstone_AtOff100 = AcsDigestUpdate(p1DigestTombstoneOff100, Some(off(99)))

    val p2DigestOff100 =
      acsDigest(100, internedP2Id, Some(genParticipantDigest(genRawDigest(0x0a))))
    val p2DigestOff99 = acsDigest(99, internedP2Id, Option.empty[(RawDigest, HashedDigest)])
    val p2DigestOff98 =
      acsDigest(98, internedP2Id, Some(genParticipantDigest(genRawDigest(0x1a))))
    val p2DigestTombstoneOff99 = p2DigestOff99.copy(digestO = None)
    val p2DigestTombstoneOff100 = acsDigest(100, internedP2Id, None)

    val p2DigestUpdate3_AtOff100 = AcsDigestUpdate(p2DigestOff100, Some(off(99)))
    val p2DigestUpdate2_AtOff99 = AcsDigestUpdate(p2DigestOff99, Some(off(98)))
    val p2DigestUpdate1_AtOff98 = AcsDigestUpdate(p2DigestOff98, None)
    val p2DigestUpdateTombstone_AtOff99 = AcsDigestUpdate(p2DigestTombstoneOff99, Some(off(98)))
    val p2DigestUpdateTombstone_AtOff100 = AcsDigestUpdate(p2DigestTombstoneOff100, Some(off(99)))

    val p4DigestTombstoneOff99 = acsDigest(99, internedP4Id, None)
    val p4DigestUpdateTombstone_AtOff99 = AcsDigestUpdate(p4DigestTombstoneOff99, None)

    // Party Digest Updates for testing
    val partyAliceDigestOff98 = acsDigest(98, localOrderParty(alice), Some(genRawDigest(0x5a)))
    val partyAliceDigestOff99 = acsDigest(99, localOrderParty(alice), Some(genRawDigest(0x6a)))

    val partyAliceUpdate1_AtOff98 = AcsDigestUpdate(partyAliceDigestOff98, None)
    val partyAliceUpdate2_AtOff99 = AcsDigestUpdate(partyAliceDigestOff99, Some(off(98)))

    val partyBobDigestOff98 = acsDigest(98, remoteOrderParty(bob), Some(genRawDigest(0x7a)))
    val partyBobDigestOff99 = acsDigest(99, remoteOrderParty(bob), Some(genRawDigest(0x6a)))

    val partyBobUpdate1_AtOff98 = AcsDigestUpdate(partyBobDigestOff98, None)
    val partyBobUpdate2_AtOff99 = AcsDigestUpdate(partyBobDigestOff99, Some(off(98)))

    "reinitializingTimepointsFUS" should {
      "give back proper reinit time" in {
        val rdp = mkReinitializingDigestProcessor(reinitTimepoint = tp100)
        val reinitTp =
          rdp
            .ledgerEndTimepointFUS()
            .futureValueUS // uses by default off100 = off(100) = Offset(100)

        reinitTp shouldEqual tp(100)
      }

      s"fail when the ledger end is not set" in {
        val rdp = mkReinitializingDigestProcessor(reinitTimepoint = tp100, hasLedgerEnd = false)

        loggerFactory
          .assertInternalErrorAsyncUS[IllegalStateException](
            within = rdp.ledgerEndTimepointFUS(),
            assertion = _.getMessage shouldEqual "There is no suitable last offset in the Ledger",
          )
          .futureValueUS
      }
    }

    "writeTombstonesToJournals" should {
      "do nothing when there is no digest update (empty store)" in {
        val testDigestStore = mkInMemoryDigestStore()
        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = tp100,
          acsDigestStore = testDigestStore,
        )
        rdp
          .writeTombstonesToJournals(
            tombstoneTimepoint = tp100
          )
          .futureValueUS

        val (partySnapshot, partyPaginationToken) = testDigestStore.party
          .snapshot(
            Right(tp100.offset),
            limit = 1000,
          )
          .futureValueUS

        val (participantSnapshot, participantPaginationToken) = testDigestStore.participant
          .snapshot(
            Right(tp100.offset),
            limit = 1000,
          )
          .futureValueUS

        partyPaginationToken shouldBe Left(PaginationTokenDone)
        participantPaginationToken shouldBe Left(PaginationTokenDone)

        partySnapshot.isEmpty shouldEqual true
        participantSnapshot.isEmpty shouldEqual true
      }

      s"make active entries tombstone in both party and participant stores at a target offset" in {
        val targetTimepoint_At99 = tp(99)
        val testDigestStore = mkInMemoryDigestStore()
        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = targetTimepoint_At99,
          acsDigestStore = testDigestStore,
        )
        testDigestStore.participant
          .upsertDigestUpdates(
            Seq(
              p2DigestUpdate1_AtOff98,
              p1DigestUpdate1_AtOff98,
            )
          )
          .futureValueUS

        testDigestStore.party
          .upsertDigestUpdates(
            Seq(
              partyAliceUpdate1_AtOff98,
              partyBobUpdate1_AtOff98,
            )
          )
          .futureValueUS
        // We don't need checkpoint insertion!

        rdp.writeTombstonesToJournals(tombstoneTimepoint = targetTimepoint_At99).futureValueUS

        // party bulk results
        val partyBulkMap = testDigestStore.party
          .bulkLookup(
            keys = Seq(localOrderParty(alice), remoteOrderParty(bob)),
            toInclusive = targetTimepoint_At99.offset,
          )
          .futureValueUS

        // participants with bulkLookup
        val participantBulk = testDigestStore.participant
          .bulkLookup(
            Seq(internedP1Id, internedP2Id),
            toInclusive = targetTimepoint_At99.offset,
          )
          .futureValueUS

        // Verify parties are tombstones at offset 99
        partyBulkMap should not be empty
        partyBulkMap.keys should contain theSameElementsAs Seq(
          localOrderParty(alice),
          remoteOrderParty(bob),
        )
        partyBulkMap.values.foreach(_.digestUpdate.digestO shouldBe None)

        // Verify participants are tombstones at offset 99
        participantBulk(internedP1Id) shouldEqual p1DigestUpdateTombstone_AtOff99
        participantBulk(internedP2Id) shouldEqual p2DigestUpdateTombstone_AtOff99
      }

      "set tombstones properly for updates only before the tombstone timepoint" in {
        val testDigestStore = mkInMemoryDigestStore()
        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = tp100,
          acsDigestStore = testDigestStore,
        )
        testDigestStore.participant
          .upsertDigestUpdates(
            Seq(
              p2DigestUpdate1_AtOff98,
              p1DigestUpdate1_AtOff98,
            )
          )
          .futureValueUS

        testDigestStore.party
          .upsertDigestUpdates(
            Seq(
              partyAliceUpdate1_AtOff98,
              partyBobUpdate1_AtOff98,
            )
          )
          .futureValueUS
        // We don't need checkpoint insertion!

        rdp.writeTombstonesToJournals(tombstoneTimepoint = tp100).futureValueUS

        val partyBulkMap = testDigestStore.party
          .bulkLookup(
            keys = Seq(localOrderParty(alice), remoteOrderParty(bob)),
            toInclusive = tp100.offset,
          )
          .futureValueUS
        val participantsBulkMap = testDigestStore.participant
          .bulkLookup(
            keys = Seq(internedP1Id, internedP2Id),
            toInclusive = tp100.offset,
          )
          .futureValueUS

        partyBulkMap should not be empty
        partyBulkMap.keys should contain theSameElementsAs Seq(
          localOrderParty(alice),
          remoteOrderParty(bob),
        )
        partyBulkMap.values.foreach(_.digestUpdate.digestO shouldBe None)

        participantsBulkMap should not be empty
        participantsBulkMap(internedP1Id) shouldEqual AcsDigestUpdate(
          p1DigestTombstoneOff100,
          replacesOffset = Some(off(98)),
        )
        participantsBulkMap(internedP2Id) shouldEqual AcsDigestUpdate(
          p2DigestTombstoneOff100,
          replacesOffset = Some(off(98)),
        )
      }

      "correctly write tombstones and preserve journal chain invariants when historical updates exist" in {
        val testDigestStore = mkInMemoryDigestStore()
        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = tp100,
          acsDigestStore = testDigestStore,
        )
        testDigestStore.participant
          .upsertDigestUpdates(
            Seq(
              p2DigestUpdate1_AtOff98,
              p2DigestUpdate2_AtOff99,
              p2DigestUpdate3_AtOff100,
              p1DigestUpdate1_AtOff98,
              p1DigestUpdate2_AtOff99,
              p1DigestUpdate3_AtOff100,
            )
          )
          .futureValueUS
        testDigestStore.party
          .upsertDigestUpdates(
            Seq(
              partyAliceUpdate1_AtOff98,
              partyAliceUpdate2_AtOff99,
              partyBobUpdate1_AtOff98,
              partyBobUpdate2_AtOff99,
            )
          )
          .futureValueUS

        rdp
          .writeTombstonesToJournals(tombstoneTimepoint = tp100)
          .futureValueUS

        // insert checkpoint, because we want to check if the store's structure is appropriate!
        testDigestStore
          .insertCheckpointTime(
            Checkpoint(
              tp100.offset,
              tp100.recordTime,
              CheckpointType.ReconciliationIntervalBoundary,
            )
          )
          .futureValueUS
        testDigestStore.checkReplacesInvariant().futureValueUS

        val partyBulk = testDigestStore.party
          .bulkLookup(
            keys = Seq(localOrderParty(alice), remoteOrderParty(bob)),
            toInclusive = tp100.offset,
          )
          .futureValueUS

        val participantBulk = testDigestStore.participant
          .bulkLookup(
            Seq(internedP1Id, internedP2Id),
            toInclusive = tp100.offset,
          )
          .futureValueUS

        partyBulk should not be empty

        // Verify parties have tombstones created at offset 100
        partyBulk.keys should contain theSameElementsAs Seq(
          localOrderParty(alice),
          remoteOrderParty(bob),
        )
        partyBulk.values.foreach { update =>
          update.digestUpdate.offset shouldBe tp100.offset
          update.digestUpdate.digestO shouldBe None
          update.replacesOffset shouldBe Some(off(99))
        }

        participantBulk should not be empty
        // no off(100) entries present as they are deleted
        participantBulk(internedP1Id) shouldEqual p1DigestUpdateTombstone_AtOff100
        participantBulk(internedP2Id) shouldEqual p2DigestUpdateTombstone_AtOff100
      }

      "handle tombstone creation when reinitialization occurs at initial ledger offset" in {
        val testDigestStore = mkInMemoryDigestStore()
        val initialTimepoint = tp1_0

        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = initialTimepoint,
          acsDigestStore = testDigestStore,
          // no TopologySnapshot is given
        )

        val initialDigestUpdate = AcsDigestUpdate(
          digestUpdate = acsDigest(1, internedP1Id, Some(genParticipantDigest(genRawDigest(0x11)))),
          replacesOffset = None,
        )

        testDigestStore.participant
          .upsertDigestUpdates(Seq(initialDigestUpdate))
          .futureValueUS

        // Attempt writing tombstones at the minimum timepoint
        rdp
          .writeTombstonesToJournals(tombstoneTimepoint = initialTimepoint)
          .futureValueUS

        val (_, paginationToken) = testDigestStore.participant
          .snapshot(Right(Offset.firstOffset), limit = 100)
          .futureValueUS

        paginationToken shouldBe Left(PaginationTokenDone)
      }
    }

    "reinitAcsUpdates" should {
      val topologySnapshotFactory = TestingTopology(topology =
        Map(
          partyHosting(alice)(p1, p2),
          partyHosting(bob)(p2, p3),
          partyHosting(charlie)(p1, p3, p4),
        )
      ).build()

      "handle a simple case of a few ACS updates" in {
        val reinitAtTp = tp(3)
        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = reinitAtTp,
          indexService = mkIndexService(
            (off(1), cid(1), Seq(party(alice), party(bob))),
            (off(2), cid(2), Seq(party(alice), party(bob), party(charlie))),
          ),
          testingTopologyFactory = topologySnapshotFactory,
        )
        val topologySnapshot =
          topologySnapshotFactory.topologySnapshot(timestampOfSnapshot = reinitAtTp.recordTime)

        val acsUpdates = rdp
          .reinitAcsUpdates(reinitAtTp, topologySnapshot)
          .runWith(Sink.seq)
          .futureValue

        acsUpdates shouldBe Seq(
          ProcessingContext(
            reinitAtTp,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                ),
                locallyHostedStakeholders = Seq(alice),
                cid = cid(1),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            reinitAtTp,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                  charlie -> Set(p1.toLf, p3.toLf, p4.toLf),
                ),
                locallyHostedStakeholders = Seq(alice, charlie),
                cid = cid(2),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            reinitAtTp,
            CheckpointFence(CheckpointType.Reinitialization),
          ),
        )
      }

      "handle grouping and filtering by record time" in {
        val before = off(1)
        val requestedTimepoint = tp(2)
        val after = off(3)

        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = requestedTimepoint,
          indexService = mkIndexService(
            (before, cid(1), Seq(party(alice), party(bob))),
            (before, cid(2), Seq(party(alice), party(bob), party(charlie))),
            (requestedTimepoint.offset, cid(3), Seq(party(alice), party(bob))),
            (after, cid(4), Seq(party(alice), party(charlie))), // Should be filtered out
          ),
          counterpartyBatchSize = 2,
        )
        val topologySnapshot = topologySnapshotFactory.topologySnapshot(timestampOfSnapshot =
          requestedTimepoint.recordTime
        )

        val acsUpdates = rdp
          .reinitAcsUpdates(
            requestedTimepoint,
            topologySnapshot,
          )
          .runWith(Sink.seq)
          .futureValue

        acsUpdates shouldBe Seq(
          ProcessingContext(
            requestedTimepoint,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                  // charlie is "missing" here because the counterparty batch size is 2
                ),
                locallyHostedStakeholders = Seq(alice),
                cid = cid(1),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            requestedTimepoint,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                ),
                locallyHostedStakeholders = Seq(alice, charlie),
                cid = cid(2),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            requestedTimepoint,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  alice -> Set(p1.toLf, p2.toLf),
                  bob -> Set(p2.toLf, p3.toLf),
                ),
                locallyHostedStakeholders = Seq(alice),
                cid = cid(3),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            requestedTimepoint,
            NotCheckpointFence(
              topologySnapshot,
              AcsUpdate(
                stakeholders = Map(
                  charlie -> Set(p1.toLf, p3.toLf, p4.toLf)
                ),
                locallyHostedStakeholders = Seq(alice, charlie),
                cid = cid(2),
                rc = rc,
                isActivation = true,
              ),
            ),
          ),
          ProcessingContext(
            requestedTimepoint,
            CheckpointFence(CheckpointType.Reinitialization),
          ),
        )
      }
    }

    "start" should {
      val topologySnapshotFactory = TestingTopology(topology =
        Map(
          partyHosting(alice)(p1, p2),
          partyHosting(bob)(p2, p3),
          partyHosting(charlie)(p1, p3, p4),
        )
      ).build()

      "do nothing on an empty digest store" in {
        val testDigestStore = mkInMemoryDigestStore()
        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = tp100,
          acsDigestStore = testDigestStore,
          testingTopologyFactory = topologySnapshotFactory,
        )

        rdp.start().futureValueUS

        val (participantDigests, participantPageToken) =
          testDigestStore.participant.snapshot(Right(tp100.offset), limit = 100).futureValueUS
        val (partyDigests, partyPageToken) =
          testDigestStore.party.snapshot(Right(tp100.offset), limit = 100).futureValueUS

        participantPageToken shouldBe Left(PaginationTokenDone)
        partyPageToken shouldBe Left(PaginationTokenDone)

        participantDigests.isEmpty shouldBe true
        partyDigests.isEmpty shouldBe true
      }

      "prepare tombstones and set new values with checkpoint" in {
        val testDigestStore = mkInMemoryDigestStore()
        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = tp100,
          acsDigestStore = testDigestStore,
          indexService = mkIndexService(
            (off(97), cid(1), Seq(party(alice), party(bob))),
            (off(98), cid(1), Seq(party(alice), party(bob), party(charlie))),
            (off(98), cid(2), Seq(party(alice), party(charlie))),
            (off(100), cid(2), Seq(party(alice), party(bob), party(charlie))),
          ),
          testingTopologyFactory = topologySnapshotFactory,
        )

        testDigestStore.participant
          .upsertDigestUpdates(
            Seq(
              p1DigestUpdate1_AtOff98,
              p1DigestUpdate2_AtOff99,
              p1DigestUpdate3_AtOff100,
              p2DigestUpdate1_AtOff98,
              p2DigestUpdate2_AtOff99,
              p2DigestUpdate3_AtOff100,
              p4DigestUpdateTombstone_AtOff99, // -> this doesn't have any digest value in the store
            )
          )
          .futureValueUS

        // Run the whole reinitialization process
        rdp.start().futureValueUS

        val lastCheckpoint = testDigestStore.latestCheckpointUpTo(Offset.MaxValue).futureValueUS

        lastCheckpoint.isDefined shouldBe true
        lastCheckpoint.value shouldBe Checkpoint(tp(100), CheckpointType.Reinitialization)

        // Do we still have a valid chain of digest journals?
        testDigestStore.checkReplacesInvariant().futureValueUS

        // At Offset(98) all digests remain the same as before
        val participantDigests_At98 =
          testDigestStore.participant
            .bulkLookup(Seq(internedP1Id, internedP2Id, internedP4Id), off(98))
            .futureValueUS
        val partyDigests_At98 =
          testDigestStore.party
            .bulkLookup(
              keys = Seq(
                localOrderParty(alice),
                remoteOrderParty(alice),
                localOrderParty(bob),
                remoteOrderParty(bob),
                localOrderParty(charlie),
                remoteOrderParty(charlie),
              ),
              toInclusive = off(98),
            )
            .futureValueUS

        participantDigests_At98(internedP1Id) shouldBe p1DigestUpdate1_AtOff98
        participantDigests_At98(internedP2Id) shouldBe p2DigestUpdate1_AtOff98
        participantDigests_At98.get(internedP4Id) shouldBe None
        partyDigests_At98.isEmpty shouldBe true

        // At Offset(99) all digests remain the same as before
        val participantDigests_At99 =
          testDigestStore.participant
            .bulkLookup(Seq(internedP1Id, internedP2Id, internedP4Id), off(99))
            .futureValueUS
        val partyDigestUpdates_At99 =
          testDigestStore.party
            .bulkLookup(
              keys = Seq(
                localOrderParty(alice),
                remoteOrderParty(alice),
                localOrderParty(bob),
                remoteOrderParty(bob),
                localOrderParty(charlie),
                remoteOrderParty(charlie),
              ),
              toInclusive = off(99),
            )
            .futureValueUS

        participantDigests_At99(internedP1Id) shouldBe p1DigestUpdate2_AtOff99
        participantDigests_At99(internedP2Id) shouldBe p2DigestUpdate2_AtOff99
        participantDigests_At99(internedP4Id) shouldBe p4DigestUpdateTombstone_AtOff99
        partyDigestUpdates_At99.isEmpty shouldBe true

        // At 100, we have the new calculated digests
        val participantDigests_At100 =
          testDigestStore.participant
            .bulkLookup(Seq(internedP1Id, internedP2Id, internedP4Id), off(100))
            .futureValueUS
        val partyDigestUpdates_At100 =
          testDigestStore.party
            .bulkLookup(
              keys = Seq(
                localOrderParty(alice),
                remoteOrderParty(alice),
                localOrderParty(bob),
                remoteOrderParty(bob),
                localOrderParty(charlie),
                remoteOrderParty(charlie),
              ),
              toInclusive = off(100),
            )
            .futureValueUS

        // Recalculated digests are at Offset(100)
        participantDigests_At100(internedP1Id).digestUpdate.digestO.isDefined shouldBe true
        participantDigests_At100(internedP2Id).digestUpdate.digestO.isDefined shouldBe true
        participantDigests_At100(internedP4Id).digestUpdate.digestO.isDefined shouldBe true

        // They refer back to the tombstones: Offset(99)
        participantDigests_At100(internedP1Id).replacesOffset shouldBe Some(off(99))
        participantDigests_At100(internedP2Id).replacesOffset shouldBe Some(off(99))
        participantDigests_At100(internedP4Id).replacesOffset shouldBe Some(off(99))

        partyDigestUpdates_At100 should not be empty
        partyDigestUpdates_At100.values.foreach(_.digestUpdate.offset shouldBe off(100))
        partyDigestUpdates_At100.keys should contain theSameElementsAs Seq(
          localOrderParty(alice),
          localOrderParty(bob),
          localOrderParty(charlie),
          remoteOrderParty(alice),
          remoteOrderParty(bob),
          remoteOrderParty(charlie),
        )
      }

      "truncate journals properly when the reinitialization offset is not at the end of the journal" in {
        val testDigestStore = mkInMemoryDigestStore()
        val reinitTpAt98 = tp(98)
        val rdp = mkReinitializingDigestProcessor(
          reinitTimepoint = reinitTpAt98,
          acsDigestStore = testDigestStore,
          indexService = mkIndexService(
            (off(97), cid(1), Seq(party(alice), party(bob))),
            (off(98), cid(1), Seq(party(alice), party(bob), party(charlie))),
          ),
          testingTopologyFactory = topologySnapshotFactory,
        )

        testDigestStore.participant
          .upsertDigestUpdates(
            Seq(
              p1DigestUpdate1_AtOff98,
              p1DigestUpdate2_AtOff99, // after reinit
              p1DigestUpdate3_AtOff100, // after reinit
              p2DigestUpdate1_AtOff98,
              p2DigestUpdate2_AtOff99, // after reinit
              p2DigestUpdate3_AtOff100, // after reinit
              p4DigestUpdateTombstone_AtOff99, // after reinit
            )
          )
          .futureValueUS

        // Run the whole reinitialization process
        rdp.start().futureValueUS

        val participantDigests_At100 =
          testDigestStore.participant
            .bulkLookup(Seq(internedP1Id, internedP2Id, internedP4Id), off(100))
            .futureValueUS

        // They are at Offset(98)
        participantDigests_At100(internedP1Id).digestUpdate.offset shouldBe off(98)
        participantDigests_At100(internedP2Id).digestUpdate.offset shouldBe off(98)
        participantDigests_At100(internedP4Id).digestUpdate.offset shouldBe off(98)

        // They are NOT tombstones because all is recalculated
        participantDigests_At100(internedP1Id).digestUpdate.digestO should not be empty
        participantDigests_At100(internedP2Id).digestUpdate.digestO should not be empty
        participantDigests_At100(internedP4Id).digestUpdate.digestO should not be empty

        // They refer back to nothing
        participantDigests_At100(internedP1Id).replacesOffset shouldBe None
        participantDigests_At100(internedP2Id).replacesOffset shouldBe None
        participantDigests_At100(internedP4Id).replacesOffset shouldBe None
      }
    }
  }
}
