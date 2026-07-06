// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.InternalIndexService.AcsUpdate
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  InternalIndexService,
  InternalIndexServiceImpl,
  Update,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.platform.config.IndexServiceConfig
import com.digitalasset.canton.protocol.ContractInstance
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ValueParty
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

trait AcsCommitmentStreamsComponentTest extends AnyWordSpec with IndexComponentTest {

  private val nextRecordTime = new SingleStepIncreasingRecordTime

  private val alice: ValueParty = ValueParty(Ref.Party.assertFromString("alice"))
  private val bob: ValueParty = ValueParty(Ref.Party.assertFromString("bob"))
  private val charlie: ValueParty = ValueParty(Ref.Party.assertFromString("charlie"))

  private def createTx(
      stakeholders: Set[ValueParty]
  ): (Update.SequencedTransactionAccepted, ContractInstance) =
    createTxOn(synchronizer1, stakeholders)

  private def createTxOn(
      synchronizerId: SynchronizerId,
      stakeholders: Set[ValueParty],
  ): (Update.SequencedTransactionAccepted, ContractInstance) = {
    val (_, update, contract) = createTxWithRecordTime(synchronizerId, stakeholders)
    update -> contract
  }

  private def createTxWithRecordTime(
      synchronizerId: SynchronizerId,
      stakeholders: Set[ValueParty],
  ): (CantonTimestamp, Update.SequencedTransactionAccepted, ContractInstance) = {
    val recordTime = nextRecordTime()
    val contract = genContract(
      argumentPayload = randomString(16),
      template = templates.head,
      signatories = stakeholders,
      ledgerEffectiveTime = recordTime.underlying,
    )
    val txBuilder = TxBuilder()
    txBuilder.add(contract.inst.toCreateNode)
    val tx = txBuilder.buildCommitted()
    val update = transaction(synchronizerId = synchronizerId, recordTime = recordTime)(
      tx,
      Vector(contract),
    )
    (recordTime, update, contract)
  }

  private lazy val internalIndexService = new InternalIndexServiceImpl(index)

  private def acsUpdates(
      synchronizerId: SynchronizerId,
      fromExclusive: Option[Offset],
  ): Source[InternalIndexService.AcsUpdateContainer, NotUsed] =
    internalIndexService.acsUpdates(synchronizerId, fromExclusive)

  private def counterPartiesOf(
      synchronizerId: SynchronizerId,
      activeAt: Offset,
      party: Option[Ref.Party],
  ): Source[Ref.Party, NotUsed] =
    internalIndexService.counterParties(synchronizerId, activeAt, party)

  private def acsUpdatesRaw(
      fromExclusive: Option[Offset],
      expected: Int,
  ): Seq[AcsUpdate] =
    acsUpdates(synchronizer1, fromExclusive)
      .map(_.acsUpdate)
      .take(expected.toLong)
      .runWith(Sink.seq)
      .futureValue

  private def acsUpdateContainers(
      fromExclusive: Option[Offset],
      expected: Int,
  ): Seq[InternalIndexService.AcsUpdateContainer] =
    acsUpdates(synchronizer1, fromExclusive)
      .take(expected.toLong)
      .runWith(Sink.seq)
      .futureValue

  private def asAcsChange(acsUpdate: AcsUpdate): AcsChange =
    inside(acsUpdate) { case AcsUpdate.AcsChangeUpdate(acsChange) => acsChange }

  private def acsCommitment(
      synchronizerId: SynchronizerId,
      payload: ByteString,
  ): Update.ReceivedAcsCommitment =
    Update.ReceivedAcsCommitment(
      synchronizerId = synchronizerId,
      recordTime = nextRecordTime(),
      payload = payload,
    )

  "acsUpdates" should {
    "emit an activation for every created contract with reassignment counter 0" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)
      val (recordTime1, create1, contract1) =
        createTxWithRecordTime(synchronizer1, Set(dsoParty, alice))
      val (recordTime2, create2, contract2) =
        createTxWithRecordTime(synchronizer1, Set(dsoParty, bob))
      val lastOffset = ingestUpdates(
        create1 -> Vector(contract1),
        create2 -> Vector(contract2),
      )

      val containers = acsUpdateContainers(rangeStart, expected = 2)
      containers should have size 2

      val container1 = containers(0)
      val container2 = containers(1)
      container1.synchronizerTime shouldBe recordTime1
      container2.synchronizerTime shouldBe recordTime2
      container1.offset shouldBe lastOffset.decrement.value
      container2.offset shouldBe lastOffset

      val acsChanges = containers.map(c => asAcsChange(c.acsUpdate))

      val activations = acsChanges.flatMap(_.activations)
      val deactivations = acsChanges.flatMap(_.deactivations)

      activations shouldBe Vector(
        contract1.contractId -> ContractStakeholdersAndReassignmentCounter(
          stakeholders = Set(dsoParty.value, alice.value),
          reassignmentCounter = ReassignmentCounter(0L),
        ),
        contract2.contractId -> ContractStakeholdersAndReassignmentCounter(
          stakeholders = Set(dsoParty.value, bob.value),
          reassignmentCounter = ReassignmentCounter(0L),
        ),
      )

      deactivations shouldBe empty
    }

    "emit a deactivation with reassignment counter 0 when a created contract is archived" in {
      val (create, contract) = createTx(Set(dsoParty, alice))
      ingestUpdates(create -> Vector(contract))

      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      val archive = archives(
        recordTime = nextRecordTime,
        argumentLength = 8,
        resultLength = 8,
      )(Seq(contract.inst.toCreateNode))
      ingestUpdates(archive -> Vector.empty)

      val acsChange = acsUpdatesRaw(rangeStart, expected = 1).map(asAcsChange).loneElement

      acsChange.activations shouldBe empty
      val (deactivatedCid, stakeholdersAndCounter) = acsChange.deactivations.loneElement
      deactivatedCid shouldBe contract.contractId
      stakeholdersAndCounter.stakeholders should contain theSameElementsAs Set(
        dsoParty.value,
        alice.value,
      )
      stakeholdersAndCounter.reassignmentCounter.v shouldBe 0L
    }

    "omit an archived contract whose activation is not in the index DB" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      // Build (but never ingest) the creation of a contract, then ingest only its archive.
      val orphanContract = genContract(
        argumentPayload = randomString(16),
        template = templates.head,
        signatories = Set(dsoParty, alice),
        ledgerEffectiveTime = nextRecordTime().underlying,
      )
      val archive = archives(
        recordTime = nextRecordTime,
        argumentLength = 8,
        resultLength = 8,
      )(Seq(orphanContract.inst.toCreateNode))

      // Ingesting the archive (synchronously) emits the indexer warnings about the missing
      // activation, since the activation reference cannot be resolved.
      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
        ingestUpdates(archive -> Vector.empty).discard,
        logs => {
          forAtLeast(1, logs)(
            _.warningMessage should include("not found, deactivation reference cannot be computed")
          )
          forAtLeast(1, logs)(
            _.warningMessage should include("Activation is missing for a deactivation")
          )
        },
      )

      val acsUpdate =
        acsUpdatesRaw(rangeStart, expected = 1).loneElement

      val acsChange = inside(acsUpdate) { case AcsUpdate.AcsChangeUpdate(acsChange) => acsChange }
      acsChange shouldBe AcsChange(activations = Map.empty, deactivations = Map.empty)
    }

    "emit the right ACS updates for a mix of a create, an archive, two ACS commitments and a topology transaction" in {
      val (preCreate, archivedContract) = createTx(Set(dsoParty, alice))
      ingestUpdates(preCreate -> Vector(archivedContract))

      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      val payload1 = ByteString.copyFromUtf8("commitment-payload-1")
      val payload2 = ByteString.copyFromUtf8("commitment-payload-2")

      val (create, createdContract) = createTx(Set(dsoParty, bob))
      val archive = archives(
        recordTime = nextRecordTime,
        argumentLength = 8,
        resultLength = 8,
      )(Seq(archivedContract.inst.toCreateNode))

      // Ingest the mix in a deterministic order: create, archive, commitment(p1), commitment(p2).
      ingestUpdates(
        create -> Vector(createdContract),
        archive -> Vector.empty,
      )
      ingestUpdateSync(acsCommitment(synchronizer1, payload1)).discard
      ingestUpdateSync(acsCommitment(synchronizer1, payload2)).discard
      ingestPartyOnboarding(Set("mixed-topology-party"), nextRecordTime()).discard

      // Expected: create (AcsChange), archive (AcsChange), commitment p1, commitment p2, topology tx.
      val acsUpdates = acsUpdatesRaw(rangeStart, expected = 5)

      acsUpdates shouldBe Seq[AcsUpdate](
        // create (AcsChange)
        AcsUpdate.AcsChangeUpdate(
          AcsChange(
            activations = Map(
              createdContract.contractId -> ContractStakeholdersAndReassignmentCounter(
                stakeholders = Set(dsoParty.value, bob.value),
                reassignmentCounter = ReassignmentCounter(0L),
              )
            ),
            deactivations = Map.empty,
          )
        ),
        // archive (AcsChange)
        AcsUpdate.AcsChangeUpdate(
          AcsChange(
            activations = Map.empty,
            deactivations = Map(
              archivedContract.contractId -> ContractStakeholdersAndReassignmentCounter(
                stakeholders = Set(dsoParty.value, alice.value),
                reassignmentCounter = ReassignmentCounter(0L),
              )
            ),
          )
        ),
        // commitment p1
        AcsUpdate.AcsCommitment(payload1),
        // commitment p2
        AcsUpdate.AcsCommitment(payload2),
        // topology tx
        AcsUpdate.EffectivePartyToParticipantMappings(
          Set(
            Update.TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization(
              party = Ref.Party.assertFromString("mixed-topology-party"),
              participant = Ref.ParticipantId.assertFromString("participant"),
              authorizationEvent = Update.TopologyTransactionEffective.AuthorizationEvent
                .Onboarding(Update.TopologyTransactionEffective.AuthorizationLevel.Observation),
            )
          )
        ),
      )
    }

    "ignore updates from a different synchronizer" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      ingestUpdateSync(
        acsCommitment(synchronizer2, ByteString.copyFromUtf8("other-synchronizer-commitment"))
      ).discard

      // A create on synchronizer1 which must be the first update observed.
      val (create, contract) = createTx(Set(dsoParty, alice))
      ingestUpdates(create -> Vector(contract))

      val acsUpdates = acsUpdatesRaw(rangeStart, expected = 1)

      // Only the activation is observed: the foreign-synchronizer commitment was ignored.
      val acsChange = asAcsChange(acsUpdates.loneElement)
      acsChange.activations.keySet should contain(contract.contractId)
    }

    "emit an activation with the assigned reassignment counter for a reassignment" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      val reassignmentCounter = 17L
      val (assign, contract) =
        sequencedAssign(nextRecordTime(), Set(dsoParty, alice), reassignmentCounter)
      ingestUpdates(assign -> Vector(contract))

      val acsChanges = acsUpdatesRaw(rangeStart, expected = 1).map(asAcsChange)

      val acsChange = acsChanges.loneElement
      acsChange.deactivations shouldBe empty
      val (activatedCid, stakeholdersAndCounter) = acsChange.activations.loneElement
      activatedCid shouldBe contract.contractId
      stakeholdersAndCounter.stakeholders should contain theSameElementsAs Set(
        dsoParty.value,
        alice.value,
      )
      stakeholdersAndCounter.reassignmentCounter.v shouldBe reassignmentCounter
    }

    "emit a deactivation with its reassignment counter when an assigned contract is archived" in {
      val assignedCounter = 23L
      val (assign, contract) =
        sequencedAssign(nextRecordTime(), Set(dsoParty, alice), assignedCounter)
      ingestUpdates(assign -> Vector(contract))

      // Start reading after the assignment so only the archive's deactivation is in range.
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      val archive = archives(
        recordTime = nextRecordTime,
        argumentLength = 8,
        resultLength = 8,
      )(Seq(contract.inst.toCreateNode))
      ingestUpdates(archive -> Vector.empty)

      val acsChange = acsUpdatesRaw(rangeStart, expected = 1).map(asAcsChange).loneElement

      acsChange.activations shouldBe empty
      val (deactivatedCid, stakeholdersAndCounter) = acsChange.deactivations.loneElement
      deactivatedCid shouldBe contract.contractId
      stakeholdersAndCounter.stakeholders should contain theSameElementsAs Set(
        dsoParty.value,
        alice.value,
      )
      stakeholdersAndCounter.reassignmentCounter.v shouldBe assignedCounter
    }

    "emit a deactivation for an unassignment" in {
      val (create, contract) = createTx(Set(dsoParty, alice))
      ingestUpdates(create -> Vector(contract))

      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      val unassignCounter = 42L
      ingestUpdates(
        sequencedUnassign(nextRecordTime(), contract, unassignCounter) -> Vector.empty
      )

      val acsChange = acsUpdatesRaw(rangeStart, expected = 1).map(asAcsChange).loneElement

      acsChange.activations shouldBe empty
      val (deactivatedCid, stakeholdersAndCounter) = acsChange.deactivations.loneElement
      deactivatedCid shouldBe contract.contractId
      stakeholdersAndCounter.stakeholders should contain theSameElementsAs Set(
        dsoParty.value,
        alice.value,
      )
      stakeholdersAndCounter.reassignmentCounter.v shouldBe unassignCounter
    }

    "emit one ACS change for a transaction with multiple creates and archives" in {
      val (assign1, archived1) =
        sequencedAssign(nextRecordTime(), Set(dsoParty, alice), reassignmentCounter = 5L)
      val (assign2, archived2) =
        sequencedAssign(nextRecordTime(), Set(dsoParty, bob), reassignmentCounter = 6L)
      ingestUpdates(
        assign1 -> Vector(archived1),
        assign2 -> Vector(archived2),
      )

      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)

      // Two new contracts created in the same transaction that archives the two assigned ones.
      val recordTime = nextRecordTime()
      val created1 = genContract(
        argumentPayload = randomString(16),
        template = templates.head,
        signatories = Set(dsoParty, charlie),
        ledgerEffectiveTime = recordTime.underlying,
      )
      val created2 = genContract(
        argumentPayload = randomString(16),
        template = templates.head,
        signatories = Set(dsoParty, alice, bob),
        ledgerEffectiveTime = recordTime.underlying,
      )
      val combinedTx = createAndArchiveTx(
        recordTime = recordTime,
        contractsToCreate = Seq(created1, created2),
        createsToArchive = Seq(archived1.inst.toCreateNode, archived2.inst.toCreateNode),
      )
      ingestUpdates(combinedTx -> Vector(created1, created2))

      val acsChange = acsUpdatesRaw(rangeStart, expected = 1).map(asAcsChange).loneElement

      acsChange.activations.keySet should contain theSameElementsAs Set(
        created1.contractId,
        created2.contractId,
      )
      acsChange.activations.values.foreach(_.reassignmentCounter.v shouldBe 0L)

      acsChange.deactivations.keySet should contain theSameElementsAs Set(
        archived1.contractId,
        archived2.contractId,
      )
      acsChange.deactivations(archived1.contractId).reassignmentCounter.v shouldBe 5L
      acsChange.deactivations(archived2.contractId).reassignmentCounter.v shouldBe 6L
    }

    "not emit ACS changes when the range is empty" in {
      val rangeEnd = index.currentLedgerEnd().map(_.lastOffset)
      acsUpdatesRaw(rangeEnd, expected = 0) shouldBe empty
    }
  }

  "acs" should {
    "return active contracts that have at least one stakeholder in stakeholders1" in {
      val party1 = ValueParty(Ref.Party.assertFromString("acs-party-1"))
      val party2 = ValueParty(Ref.Party.assertFromString("acs-party-2"))
      val partyCommon = ValueParty(Ref.Party.assertFromString("acs-party-common"))

      val (create1, contract1) = createTx(Set(party1, partyCommon))
      val (create2, contract2) = createTx(Set(partyCommon, party2))
      ingestUpdates(
        create1 -> Vector(contract1),
        create2 -> Vector(contract2),
      )
      val activeAt = index.currentLedgerEnd().value.lastOffset

      val contractsParty1All = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set(party1.value),
          stakeholders2 = Set.empty,
        )
        .runWith(Sink.seq)
        .futureValue

      contractsParty1All shouldBe Seq(
        InternalIndexService.ActiveContract(
          contractId = contract1.contractId,
          stakeholders = Set(party1.value, partyCommon.value),
          reassignmentCounter = ReassignmentCounter(0L),
        )
      )

      val contractsParty1 = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set(party1.value),
          stakeholders2 = Set(party1.value),
        )
        .runWith(Sink.seq)
        .futureValue

      contractsParty1 shouldBe contractsParty1All

      val contractsParty1Common = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set(party1.value),
          stakeholders2 = Set(partyCommon.value),
        )
        .runWith(Sink.seq)
        .futureValue

      contractsParty1Common shouldBe contractsParty1All

      val contractsPartyCommon1 = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set(partyCommon.value),
          stakeholders2 = Set(party1.value),
        )
        .runWith(Sink.seq)
        .futureValue

      contractsPartyCommon1 shouldBe contractsParty1All

      val contractsParty2All = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set(party2.value),
          stakeholders2 = Set.empty,
        )
        .runWith(Sink.seq)
        .futureValue

      contractsParty2All shouldBe Seq(
        InternalIndexService.ActiveContract(
          contractId = contract2.contractId,
          stakeholders = Set(party2.value, partyCommon.value),
          reassignmentCounter = ReassignmentCounter(0L),
        )
      )

      val contractsPartyCommonAll = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set(partyCommon.value),
          stakeholders2 = Set.empty,
        )
        .runWith(Sink.seq)
        .futureValue

      contractsPartyCommonAll shouldBe contractsParty1All ++ contractsParty2All

    }

    "treat an empty stakeholders1 as any party, filtered by stakeholders2" in {
      val party1 = ValueParty(Ref.Party.assertFromString("acs-any-party-1"))
      val party2 = ValueParty(Ref.Party.assertFromString("acs-any-party-2"))

      val (create1, contract1) = createTx(Set(dsoParty, party1))
      val (create2, contract2) = createTx(Set(dsoParty, party2))
      ingestUpdates(
        create1 -> Vector(contract1),
        create2 -> Vector(contract2),
      )
      val activeAt = index.currentLedgerEnd().value.lastOffset

      // An empty stakeholders1 with an empty stakeholders2 returns all active contracts.
      val allContracts = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set.empty,
          stakeholders2 = Set.empty,
        )
        .runWith(Sink.seq)
        .futureValue

      allContracts.map(_.contractId) should contain allOf (
        contract1.contractId,
        contract2.contractId,
      )

      val party1Contracts = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set.empty,
          stakeholders2 = Set(party1.value),
        )
        .runWith(Sink.seq)
        .futureValue

      party1Contracts.map(_.contractId) should contain(contract1.contractId)
      party1Contracts.map(_.contractId) should not contain contract2.contractId
    }

    "return the full stakeholder set for matching contracts and reassignment counter 0" in {
      val (create, contract) = createTx(Set(dsoParty, alice, bob))
      ingestUpdates(create -> Vector(contract))
      val activeAt = index.currentLedgerEnd().value.lastOffset

      val active = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set(alice.value),
          stakeholders2 = Set.empty,
        )
        .runWith(Sink.seq)
        .futureValue
        .find(_.contractId == contract.contractId)
        .value

      active.stakeholders should contain theSameElementsAs Set(
        dsoParty.value,
        alice.value,
        bob.value,
      )
      active.reassignmentCounter.v shouldBe 0L
    }

    "only return contracts that have a stakeholder in both stakeholders1 and stakeholders2" in {
      val (createAB, contractAB) = createTx(Set(dsoParty, alice, bob))
      val (createA, contractA) = createTx(Set(dsoParty, alice))
      ingestUpdates(
        createAB -> Vector(contractAB),
        createA -> Vector(contractA),
      )
      val activeAt = index.currentLedgerEnd().value.lastOffset

      val contracts = index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAt,
          stakeholders1 = Set(alice.value),
          stakeholders2 = Set(bob.value),
        )
        .runWith(Sink.seq)
        .futureValue

      contracts.map(_.contractId) should contain(contractAB.contractId)
      contracts.map(_.contractId) should not contain contractA.contractId
    }

    "return no contracts for a synchronizer without active contracts" in {
      val (create, contract) = createTx(Set(dsoParty, alice))
      ingestUpdates(create -> Vector(contract))
      val activeAt = index.currentLedgerEnd().value.lastOffset

      index
        .acs(
          synchronizerId = synchronizer2,
          activeAt = activeAt,
          stakeholders1 = Set(alice.value),
          stakeholders2 = Set.empty,
        )
        .runWith(Sink.seq)
        .futureValue shouldBe empty
    }

    "exclude archived contracts" in {
      val acsArchivedParty = ValueParty(Ref.Party.assertFromString("acs-archived-party"))

      val (create, contract) = createTx(Set(dsoParty, acsArchivedParty))
      ingestUpdates(create -> Vector(contract))

      // The contract is active right after the create.
      val activeBeforeArchive = index.currentLedgerEnd().value.lastOffset
      index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeBeforeArchive,
          stakeholders1 = Set(acsArchivedParty.value),
          stakeholders2 = Set.empty,
        )
        .runWith(Sink.seq)
        .futureValue
        .map(_.contractId) should contain(contract.contractId)

      val archive = archives(
        recordTime = nextRecordTime,
        argumentLength = 8,
        resultLength = 8,
      )(Seq(contract.inst.toCreateNode))
      ingestUpdates(archive -> Vector.empty)

      // After the archive the contract is no longer part of the ACS.
      val activeAfterArchive = index.currentLedgerEnd().value.lastOffset
      index
        .acs(
          synchronizerId = synchronizer1,
          activeAt = activeAfterArchive,
          stakeholders1 = Set(acsArchivedParty.value),
          stakeholders2 = Set.empty,
        )
        .runWith(Sink.seq)
        .futureValue shouldBe empty
    }
  }

  "counterParties" should {
    "return the distinct stakeholders of the contracts of a given party" in {
      val (create1, contract1) = createTx(Set(dsoParty, alice))
      val (create2, contract2) = createTx(Set(dsoParty, alice, bob))
      ingestUpdates(
        create1 -> Vector(contract1),
        create2 -> Vector(contract2),
      )
      val activeAt = index.currentLedgerEnd().value.lastOffset

      val counterParties = counterPartiesOf(synchronizer1, activeAt, Some(alice.value))
        .runWith(Sink.seq)
        .futureValue

      // each party emerges exactly once even across multiple contracts
      counterParties.distinct should contain theSameElementsAs counterParties
      counterParties should contain allElementsOf Seq(dsoParty.value, alice.value, bob.value)

      val counterPartiesBob = counterPartiesOf(synchronizer1, activeAt, Some(bob.value))
        .runWith(Sink.seq)
        .futureValue

      counterPartiesBob should contain allElementsOf counterParties

      val counterPartiesDso = counterPartiesOf(synchronizer1, activeAt, Some(dsoParty.value))
        .runWith(Sink.seq)
        .futureValue

      counterPartiesDso should contain allElementsOf counterParties
    }

    "return every distinct stakeholder when no party is given" in {
      val (create1, contract1) = createTx(Set(dsoParty, alice))
      val (create2, contract2) = createTx(Set(dsoParty, charlie))
      ingestUpdates(
        create1 -> Vector(contract1),
        create2 -> Vector(contract2),
      )
      val activeAt = index.currentLedgerEnd().value.lastOffset

      val counterParties = counterPartiesOf(synchronizer1, activeAt, None)
        .runWith(Sink.seq)
        .futureValue

      counterParties.distinct should contain theSameElementsAs counterParties
      counterParties should contain allElementsOf Seq(
        dsoParty.value,
        alice.value,
        charlie.value,
      )
    }

    "return no parties for a different synchronizer without active contracts" in {
      val (create, contract) = createTx(Set(dsoParty, alice))
      ingestUpdates(create -> Vector(contract))
      val activeAt = index.currentLedgerEnd().value.lastOffset

      counterPartiesOf(synchronizer2, activeAt, Some(alice.value))
        .runWith(Sink.seq)
        .futureValue shouldBe empty
    }

    "exclude parties from contracts of other synchronizers" in {
      val onlyOnSynchronizer2 = ValueParty(Ref.Party.assertFromString("only-on-synchronizer2"))

      // A contract living on synchronizer2 only.
      val (create2, contract2) = createTxOn(synchronizer2, Set(dsoParty, onlyOnSynchronizer2))
      ingestUpdates(create2 -> Vector(contract2))
      val activeAt = index.currentLedgerEnd().value.lastOffset

      counterPartiesOf(synchronizer1, activeAt, None)
        .runWith(Sink.seq)
        .futureValue should not contain onlyOnSynchronizer2.value
    }

    "exclude parties not sharing a contract with the given party" in {
      val sharingParty = ValueParty(Ref.Party.assertFromString("sharing-party"))
      val unrelatedParty = ValueParty(Ref.Party.assertFromString("unrelated-party"))

      val (createShared, contractShared) = createTx(Set(dsoParty, sharingParty))
      val (createUnrelated, contractUnrelated) = createTx(Set(dsoParty, unrelatedParty))
      ingestUpdates(
        createShared -> Vector(contractShared),
        createUnrelated -> Vector(contractUnrelated),
      )
      val activeAt = index.currentLedgerEnd().value.lastOffset

      // The counterparties of sharingParty must not include the unrelated party.
      val counterParties = counterPartiesOf(synchronizer1, activeAt, Some(sharingParty.value))
        .runWith(Sink.seq)
        .futureValue

      counterParties should contain(sharingParty.value)
      counterParties should not contain unrelatedParty.value
    }

    "honor activeAt" in {
      val earlyParty = ValueParty(Ref.Party.assertFromString("counterparties-early-party"))
      val lateParty = ValueParty(Ref.Party.assertFromString("counterparties-late-party"))

      // A first contract with earlyParty, then snapshot the offset.
      val (createEarly, contractEarly) = createTx(Set(dsoParty, earlyParty))
      ingestUpdates(createEarly -> Vector(contractEarly))
      val activeAtBefore = index.currentLedgerEnd().value.lastOffset

      // A second contract introducing lateParty, committed after the snapshot.
      val (createLate, contractLate) = createTx(Set(dsoParty, earlyParty, lateParty))
      ingestUpdates(createLate -> Vector(contractLate))

      val archive = archives(
        recordTime = nextRecordTime,
        argumentLength = 8,
        resultLength = 8,
      )(Seq(contractEarly.inst.toCreateNode, contractLate.inst.toCreateNode))
      ingestUpdates(archive -> Vector.empty)
      val activeAtAfter = index.currentLedgerEnd().value.lastOffset

      // Querying at the earlier snapshot must not surface the party introduced afterwards.
      val counterParties = counterPartiesOf(synchronizer1, activeAtBefore, Some(earlyParty.value))
        .runWith(Sink.seq)
        .futureValue

      counterParties should contain(earlyParty.value)
      counterParties should not contain lateParty.value

      counterPartiesOf(synchronizer1, activeAtAfter, Some(earlyParty.value))
        .runWith(Sink.seq)
        .futureValue shouldBe empty
    }
  }
}

final class AcsCommitmentStreamsComponentTestCachesDisabledH2
    extends AcsCommitmentStreamsComponentTest {
  override protected val indexServiceConfig: IndexServiceConfig =
    IndexServiceConfig(maxTransactionsInMemoryFanOutBufferSize = 0)
}

final class AcsCommitmentStreamsComponentTestCachesDisabledPostgres
    extends AcsCommitmentStreamsComponentTest
    with IndexComponentTest.WithPostgres {
  override protected val indexServiceConfig: IndexServiceConfig =
    IndexServiceConfig(maxTransactionsInMemoryFanOutBufferSize = 0)
}

final class AcsCommitmentStreamsComponentTestTinyBuffersPostgres
    extends AcsCommitmentStreamsComponentTest
    with IndexComponentTest.WithPostgres {
  override protected val indexServiceConfig: IndexServiceConfig =
    IndexServiceConfig(maxTransactionsInMemoryFanOutBufferSize = 3)
}

final class AcsCommitmentStreamsComponentTestDefaultPostgres
    extends AcsCommitmentStreamsComponentTest
    with IndexComponentTest.WithPostgres {
  override protected val indexServiceConfig: IndexServiceConfig = IndexServiceConfig()
}
