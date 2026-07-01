// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.digitalasset.canton.crypto.{LtHash16Blake3, TestHash}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.{
  AcsUpdate,
  CheckpointFence,
  CheckpointFenceOr,
  CheckpointWritten,
  Classification,
  NotCheckpointFence,
  PartyAddedToParticipant,
  PartyRemovedFromParticipant,
  ProcessingContext,
}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  HashedDigest,
  LocalPartyFirst,
  ParticipantAcsDigestUpdate,
  PartyAcsDigestUpdate,
  PartyAndOrder,
  PartyOrder,
  RawDigest,
  RemotePartyFirst,
}
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LedgerParticipantId,
  LfPartyId,
  ReassignmentCounter,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class SequentialDigestAccumulatorTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private val synchronizerId = DefaultTestIdentities.synchronizerId
  private val p1 = DefaultTestIdentities.participant1.toLf
  private val p2 = DefaultTestIdentities.participant2.toLf
  private val alice = DefaultTestIdentities.party1.toLf
  private val bob = DefaultTestIdentities.party2.toLf
  private val rc0 = ReassignmentCounter.Genesis
  private val cid0 = cid(0)
  private val cid1 = cid(1)
  private val cid2 = cid(2)

  implicit def anyToCheckpointFenceOr(a: Classification): CheckpointFenceOr[Classification] =
    NotCheckpointFence(mock[TopologySnapshot], a)

  class Fixture(participant: LedgerParticipantId) {
    val stringInterning = new MockStringInterning()
    val digestStore = new InMemoryAcsDigestStore(
      IndexedSynchronizer.tryCreate(synchronizerId, 1),
      stringInterning,
      loggerFactory,
    )
    val accumulator =
      new SequentialDigestAccumulator(
        participant,
        digestStore,
        stringInterning,
        TestHash,
        loggerFactory,
      )

    def process(
        inputs: ((CantonTimestamp, Offset), CheckpointFenceOr[Classification])*
    ): FutureUnlessShutdown[Seq[CheckpointWritten]] =
      MonadUtil
        .sequentialTraverse(inputs) { case ((rt, offset), classification) =>
          accumulator.process(
            ProcessingContext(rt, offset, classification)
          )
        }
        .map(_.flatten)

    def lookupPartyDigest(
        party: LfPartyId,
        order: PartyOrder,
    ): Option[AcsDigestStore.AcsDigestUpdate[PartyAndOrder[LfPartyId], RawDigest]] =
      digestStore.party
        .lookup(
          PartyAndOrder(stringInterning.party.internalize(party), order),
          Offset.MaxValue,
        )
        .futureValueUS
        .map(PartyAcsDigestUpdate.externalize(stringInterning, _))

    def lookupParticipantDigest(
        participant: LedgerParticipantId
    ): Option[AcsDigestStore.AcsDigestUpdate[LedgerParticipantId, (RawDigest, HashedDigest)]] =
      digestStore.participant
        .lookup(
          stringInterning.participantId.internalize(participant),
          Offset.MaxValue,
        )
        .futureValueUS
        .map(ParticipantAcsDigestUpdate.externalize(stringInterning, _))

  }

  "SequentialInMemoryDigestAccumulator" should {
    "process a simple AcsUpdate" in {
      val fixture = new Fixture(p1)
      import fixture.*

      process(
        (ts(1), off(1)) ->
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1)),
            locallyHostedStakeholders = Seq(alice),
            cid0,
            rc0,
            isActivation = true,
          )
      ).futureValueUS shouldBe empty

      digestStore.firstCheckpointAfter(Offset.firstOffset).futureValueUS shouldBe None
      val partyDigest = lookupPartyDigest(alice, RemotePartyFirst).value
      val participantDigest = lookupParticipantDigest(p1).value

      partyDigest.digestUpdate.digestO.value shouldBe participantDigest.digestUpdate.digestO.value._1
    }

    // given cid0 with stakeholders alice and bob,
    // this test checks that the digests are the same in the following two scenarios:
    // 1. p1 hosts only alice, receives cid0, and then onboards bob
    // 2. p1 hosts alice and bob, and then receives cid0
    "process a party onboarding scenario" in {
      val fixtureBobOnboarded = new Fixture(p1)

      fixtureBobOnboarded
        .process(
          (ts(1), off(1)) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(alice),
              cid0,
              rc0,
              isActivation = true,
            ),
          // simulate onboarding of bob
          (ts(2), off(2)) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(bob),
              cid0,
              rc0,
              isActivation = true,
            ),
          (ts(2), off(2)) -> PartyAddedToParticipant(bob, p1),
        )
        .futureValueUS shouldBe empty

      val fixtureBobAlreadyHosted = new Fixture(p1)
      fixtureBobAlreadyHosted
        .process(
          (ts(1), off(1)) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
              locallyHostedStakeholders = Seq(alice, bob),
              cid0,
              rc0,
              isActivation = true,
            )
        )
        .futureValueUS shouldBe empty

      fixtureBobAlreadyHosted
        .lookupParticipantDigest(p1)
        .value
        .digestUpdate
        .digestO shouldBe fixtureBobOnboarded
        .lookupParticipantDigest(p1)
        .value
        .digestUpdate
        .digestO

      for {
        party <- Seq(alice, bob)
        order <- Seq(LocalPartyFirst, RemotePartyFirst)
      } yield fixtureBobAlreadyHosted
        .lookupPartyDigest(party, order)
        .value
        .digestUpdate
        .digestO shouldBe fixtureBobOnboarded
        .lookupPartyDigest(party, order)
        .value
        .digestUpdate
        .digestO

    }

    // given cid0 has stakeholders alice and bob,
    // this test checks that the digests are the same in the following two scenarios:
    // 1. p1 hosts alice and bob, receives cid0, and then offboards bob
    // 2. p1 hosts only alice and receives cid0
    "process a party offboarding scenario" in {
      val fixtureBobOffboarded = new Fixture(p1)

      fixtureBobOffboarded
        .process(
          (ts(1), off(1)) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
              locallyHostedStakeholders = Seq(alice, bob),
              cid0,
              rc0,
              isActivation = true,
            ),
          // simulate offboarding of bob
          (ts(2), off(2)) -> PartyRemovedFromParticipant(bob, p1),
          (ts(2), off(2)) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(bob),
              cid0,
              rc0,
              isActivation = false,
            ),
        )
        .futureValueUS shouldBe empty

      val fixtureBobNotHosted = new Fixture(p1)
      fixtureBobNotHosted
        .process(
          (ts(2), off(2)) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(alice),
              cid0,
              rc0,
              isActivation = true,
            )
        )
        .futureValueUS shouldBe empty

      fixtureBobNotHosted
        .lookupParticipantDigest(p1)
        .value
        .digestUpdate
        .digestO shouldBe fixtureBobOffboarded
        .lookupParticipantDigest(p1)
        .value
        .digestUpdate
        .digestO

      for {
        party <- Seq(alice, bob)
        order <- Seq(LocalPartyFirst, RemotePartyFirst)
      } yield fixtureBobNotHosted
        .lookupPartyDigest(party, order)
        .value
        .digestUpdate
        .digestO shouldBe fixtureBobOffboarded
        .lookupPartyDigest(party, order)
        .value
        .digestUpdate
        .digestO
    }

    "write a checkpoint when requested" in {
      val fixture = new Fixture(p1)
      import fixture.*

      val targetCheckpoint = (off(17), ts(1))

      val checkpoint = process(
        (ts(1), off(17)) -> CheckpointFence
      ).futureValueUS.loneElement

      // verify that the right CheckpointWritten notification is emitted
      checkpoint.recordTimeInclusive shouldBe ts(1)
      checkpoint.offsetInclusive shouldBe off(17)

      // verify that the checkpoint was actually written
      digestStore
        .latestCheckpointUpTo(Offset.MaxValue)
        .futureValueUS
        .value shouldBe targetCheckpoint
    }

    "not store empty initial digests" in {
      val fixture = new Fixture(p1)
      import fixture.*

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty

      process(
        (ts(1), off(2)) -> PartyAddedToParticipant(alice, p2),
        (ts(2), off(3)) -> PartyRemovedFromParticipant(alice, p2),
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty
    }

    "store empty digests after a non-empty initial digest" in {
      val fixture = new Fixture(p1)
      import fixture.*

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty

      // add a contract to alice, bob, p1, p2
      process(
        (ts(1), off(2)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
          locallyHostedStakeholders = Seq(alice),
          cid0,
          rc0,
          isActivation = true,
        )
      ).futureValueUS shouldBe empty // no checkpoints written
      LtHash16Blake3
        .tryCreate(lookupParticipantDigest(p1).value.digestUpdate.digestO.value._1)
        .isEmpty shouldBe false

      process(
        (ts(2), off(3)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
          locallyHostedStakeholders = Seq(alice),
          cid0,
          rc0,
          isActivation = false,
        )
      ).futureValueUS shouldBe empty // no checkpoints written

      // check digests for participants
      Seq(p1, p2).foreach { participant =>
        val digest = lookupParticipantDigest(participant).value
        digest.digestUpdate.offset shouldBe off(3)
        digest.replacesOffset shouldBe Some(off(2))
        LtHash16Blake3.tryCreate(digest.digestUpdate.digestO.value._1).isEmpty shouldBe true
      }

      // check digests for parties
      for {
        party <- Seq(alice, bob)
        order <- Seq(RemotePartyFirst, LocalPartyFirst)
      } yield {
        val digest = lookupPartyDigest(party, order).value
        digest.digestUpdate.offset shouldBe off(3)
        digest.replacesOffset shouldBe Some(off(2))
        LtHash16Blake3.tryCreate(digest.digestUpdate.digestO.value).isEmpty shouldBe true
      }
    }

    "not create cycles in the replacement chain" in {
      val fixture = new Fixture(p1)
      import fixture.*

      process(
        (ts(1), off(1)) ->
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
            locallyHostedStakeholders = Seq(alice, bob),
            cid0,
            rc0,
            isActivation = true,
          )
      ).futureValueUS shouldBe empty // no checkpoints written

      // the first entry doesn't replace anything
      for {
        party <- Seq(alice, bob)
        order <- Seq(RemotePartyFirst, LocalPartyFirst)
      } {
        lookupPartyDigest(party, order).value.replacesOffset shouldBe empty
      }
      lookupParticipantDigest(p1).value.replacesOffset shouldBe empty

      // now process two acs updates of cid1 and cid2 at the same offset
      process(
        (ts(2), off(2)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
          locallyHostedStakeholders = Seq(alice, bob),
          cid1,
          rc0,
          isActivation = true,
        )
      ).futureValueUS shouldBe empty // no checkpoints written

      // processing an update on a later offset should properly set replacesOffset
      for {
        party <- Seq(alice, bob)
        order <- Seq(RemotePartyFirst, LocalPartyFirst)
      } {
        lookupPartyDigest(party, order).value.replacesOffset.value shouldBe off(1)
      }
      lookupParticipantDigest(p1).value.replacesOffset.value shouldBe off(1)

      // now process another update at the same time and offset
      process(
        (ts(2), off(2)) ->
          AcsUpdate(
            stakeholders = Map(alice -> Set(p1), bob -> Set()),
            locallyHostedStakeholders = Seq(bob),
            cid2,
            rc0,
            isActivation = false,
          )
      ).futureValueUS shouldBe empty // no checkpoints written

      // since the first persisted update at off(2) was just an intermediate result that got persisted,
      // another update for the same offset should retain the original replacesOffset
      for {
        party <- Seq(alice, bob)
        order <- Seq(RemotePartyFirst, LocalPartyFirst)
      } {
        lookupPartyDigest(party, order).value.replacesOffset.value shouldBe off(1)
      }
      lookupParticipantDigest(p1).value.replacesOffset.value shouldBe off(1)
    }
  }

  private def ts(offsetFromEpoch: Int): CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(offsetFromEpoch.toLong)
  private def off(offset: Int): Offset = Offset.tryFromLong(offset.toLong)

  private def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(i, i)
}
