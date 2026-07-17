// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.crypto.{LtHash16Blake3, TestHash}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
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
import com.digitalasset.canton.participant.config.AcsDigestTracingMode
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
import com.digitalasset.canton.participant.store.db.{BaseDbAcsDigestStoreTest, DbAcsDigestStore}
import com.digitalasset.canton.participant.store.memory.InMemoryAcsDigestStore
import com.digitalasset.canton.platform.store.interning.{MockStringInterning, StringInterning}
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LedgerParticipantId,
  LfPartyId,
  ProtocolVersionChecksAnyWordSpec,
  ReassignmentCounter,
}
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

trait SequentialDigestAccumulatorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

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

  protected def minimumVersionToRunTest: ProtocolVersion

  protected def createStore(stringInterning: StringInterning): AcsDigestStore

  class Fixture(
      participant: LedgerParticipantId,
      tracingMode: AcsDigestTracingMode = AcsDigestTracingMode.Disabled,
  ) {
    val stringInterning = new MockStringInterning()
    val digestStore = createStore(stringInterning)
    val accumulator =
      new SequentialDigestAccumulator(
        participant,
        digestStore,
        stringInterning,
        TestHash,
        tracingMode,
        loggerFactory,
      )

    def process(
        inputs: (Timepoint, CheckpointFenceOr[Classification])*
    ): FutureUnlessShutdown[Seq[CheckpointWritten]] =
      MonadUtil
        .sequentialTraverse(inputs) { case (timepoint, classification) =>
          accumulator.process(ProcessingContext(timepoint, classification))
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
    "process a simple AcsUpdate" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixture = new Fixture(p1)
      import fixture.*

      process(
        tp(1) ->
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
    "process a party onboarding scenario" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixtureBobOnboarded = new Fixture(p1)

      fixtureBobOnboarded
        .process(
          tp(1) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(alice),
              cid0,
              rc0,
              isActivation = true,
            ),
          // simulate onboarding of bob
          tp(2) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set()),
              locallyHostedStakeholders = Seq(bob),
              cid0,
              rc0,
              isActivation = true,
            ),
          tp(2) -> PartyAddedToParticipant(bob, p1),
        )
        .futureValueUS shouldBe empty

      val fixtureBobAlreadyHosted = new Fixture(p1)
      fixtureBobAlreadyHosted
        .process(
          tp(1) ->
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
    "process a party offboarding scenario" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixtureBobOffboarded = new Fixture(p1)

      fixtureBobOffboarded
        .process(
          tp(1) ->
            AcsUpdate(
              stakeholders = Map(alice -> Set(p1), bob -> Set(p1)),
              locallyHostedStakeholders = Seq(alice, bob),
              cid0,
              rc0,
              isActivation = true,
            ),
          // simulate offboarding of bob
          tp(2) -> PartyRemovedFromParticipant(bob, p1),
          tp(2) ->
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
          tp(2) ->
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

    "write a checkpoint when requested" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixture = new Fixture(p1)
      import fixture.*

      val targetCheckpoint = (off(17), ts(1))

      val checkpoint = process(
        Timepoint(off(17))(ts(1)) -> CheckpointFence
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

    "not store empty initial digests" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixture = new Fixture(p1)
      import fixture.*

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty

      process(
        tp(1) -> PartyAddedToParticipant(alice, p2),
        Timepoint(off(3))(ts(2)) -> PartyRemovedFromParticipant(alice, p2),
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty
    }

    "store empty digests after a non-empty initial digest" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixture = new Fixture(p1)
      import fixture.*

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty

      // add a contract to alice, bob, p1, p2
      process(
        Timepoint(off(2))(ts(1)) -> AcsUpdate(
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
        Timepoint(off(3))(ts(2)) -> AcsUpdate(
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

    "not create cycles in the replacement chain" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixture = new Fixture(p1)
      import fixture.*

      process(
        tp(1) ->
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
        tp(2) -> AcsUpdate(
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
        tp(2) ->
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

    "store incremental traces" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixture = new Fixture(p1, AcsDigestTracingMode.Incremental)

      import fixture.*

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty

      process(
        Timepoint(off(2))(ts(1)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
          locallyHostedStakeholders = Seq(alice),
          cid0,
          rc0,
          isActivation = true,
        )
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(
        p2
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, bob, isActivation = true)
      )

      process(
        Timepoint(off(3))(ts(2)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
          locallyHostedStakeholders = Seq(alice),
          cid0,
          rc0,
          isActivation = false,
        )
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(
        p2
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, bob, isActivation = false)
      )

      // observe the activation of another contract at the same time and offset as the previous deactivation
      process(
        Timepoint(off(3))(ts(2)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
          locallyHostedStakeholders = Seq(alice),
          cid1,
          rc0,
          isActivation = true,
        )
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(
        p2
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, bob, isActivation = false),
        SingleTrace(cid1, rc0, alice, bob, isActivation = true),
      )

      process(
        Timepoint(off(4))(ts(3)) -> PartyAddedToParticipant(alice, p2)
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(
        p2
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        // in incremental tracing, only the latest trace of the party is retained
        TraceGroup(
          s"onboarded $alice",
          Seq(
            SingleTrace(cid0, rc0, alice, alice, isActivation = false),
            SingleTrace(cid1, rc0, alice, alice, isActivation = true),
          ),
          addedToHash = true,
        )
      )
    }

    "store full traces" onlyRunWithOrGreaterThan minimumVersionToRunTest in {
      val fixture = new Fixture(p1, AcsDigestTracingMode.Full)

      import fixture.*

      lookupParticipantDigest(p2) shouldBe empty
      lookupPartyDigest(alice, RemotePartyFirst) shouldBe empty
      lookupPartyDigest(alice, LocalPartyFirst) shouldBe empty

      // add a contract to alice, bob, p1, p2
      process(
        Timepoint(off(2))(ts(1)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
          locallyHostedStakeholders = Seq(alice),
          cid0,
          rc0,
          isActivation = true,
        )
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(
        p2
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, bob, isActivation = true)
      )

      process(
        Timepoint(off(3))(ts(2)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
          locallyHostedStakeholders = Seq(alice),
          cid0,
          rc0,
          isActivation = false,
        )
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(
        p1
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, alice, isActivation = true),
        SingleTrace(cid0, rc0, alice, alice, isActivation = false),
      )

      lookupParticipantDigest(
        p2
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, bob, isActivation = true),
        SingleTrace(cid0, rc0, alice, bob, isActivation = false),
      )

      // observe the activation of another contract at the same time and offset as the previous deactivation
      process(
        Timepoint(off(3))(ts(2)) -> AcsUpdate(
          stakeholders = Map(alice -> Set(p1), bob -> Set(p2)),
          locallyHostedStakeholders = Seq(alice),
          cid1,
          rc0,
          isActivation = true,
        )
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(
        p1
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, alice, isActivation = true),
        SingleTrace(cid0, rc0, alice, alice, isActivation = false),
        SingleTrace(cid1, rc0, alice, alice, isActivation = true),
      )

      lookupParticipantDigest(
        p2
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, bob, isActivation = true),
        SingleTrace(cid0, rc0, alice, bob, isActivation = false),
        SingleTrace(cid1, rc0, alice, bob, isActivation = true),
      )

      process(
        Timepoint(off(4))(ts(3)) -> PartyAddedToParticipant(alice, p2)
      ).futureValueUS shouldBe empty // no checkpoints written

      lookupParticipantDigest(
        p2
      ).value.digestUpdate.trace.value.traces should contain theSameElementsAs Seq(
        SingleTrace(cid0, rc0, alice, bob, isActivation = true),
        SingleTrace(cid0, rc0, alice, bob, isActivation = false),
        SingleTrace(cid1, rc0, alice, bob, isActivation = true),
        TraceGroup(
          s"onboarded $alice",
          Seq(
            SingleTrace(cid0, rc0, alice, alice, isActivation = true),
            SingleTrace(cid0, rc0, alice, alice, isActivation = false),
            SingleTrace(cid1, rc0, alice, alice, isActivation = true),
          ),
          addedToHash = true,
        ),
      )
    }
  }

  private def ts(offsetFromEpoch: Int): CantonTimestamp =
    CantonTimestamp.Epoch.plusSeconds(offsetFromEpoch.toLong)
  private def off(offset: Int): Offset = Offset.tryFromLong(offset.toLong)
  private def tp(i: Int): Timepoint = Timepoint(off(i))(ts(i))

  private def cid(i: Int): LfContractId = ExampleTransactionFactory.suffixedId(i, i)
}

@AcsCommitmentTest
class SequentialDigestAccumulatorTestInMemory extends SequentialDigestAccumulatorTest {
  // the in-memory test runs with any version, because it doesn't depend on dev-DB migrations
  override protected def minimumVersionToRunTest: ProtocolVersion = ProtocolVersion.minimum

  override protected def createStore(stringInterning: StringInterning): AcsDigestStore =
    InMemoryAcsDigestStore.create(Eval.now(stringInterning), loggerFactory)
}

abstract class BaseDbSequentialDigestAccumulatorTest
    extends SequentialDigestAccumulatorTest
    with BaseDbAcsDigestStoreTest { self: DbTest =>

  // the DB test requires the protocol version that also runs the corresponding DB migrations
  override protected def minimumVersionToRunTest: ProtocolVersion =
    ProtocolVersion.acsCommitmentRedesign

  override protected def createStore(
      stringInterning: StringInterning
  ): AcsDigestStore =
    new DbAcsDigestStore(
      IndexedSynchronizer.tryCreate(DefaultTestIdentities.synchronizerId, 1),
      Eval.now(stringInterning),
      storage,
      loggerFactory,
      timeouts,
    )
}

@AcsCommitmentTest
class SequentialDigestAccumulatorTestH2 extends BaseDbSequentialDigestAccumulatorTest with H2Test

@AcsCommitmentTest
class SequentialDigestAccumulatorTestPostgres
    extends BaseDbSequentialDigestAccumulatorTest
    with PostgresTest
