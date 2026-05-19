// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.{ReassignmentCounter, RepairCounter}
import monocle.Monocle.toAppliedFocusOps

trait ImportContractsIntegrationTestBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  protected def enableAlphaMultiSynchronizerSupport: Boolean

  private var alice: PartyId = _
  private var bob: PartyId = _
  private var charlie: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.alphaMultiSynchronizerSupport)
            .replace(enableAlphaMultiSynchronizerSupport)
        ),
        ConfigTransforms.enableAlphaMultiSynchronizerTopologyFeatureFlag,
      )
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.synchronizers.connect_local(sequencer2, alias = acmeName)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participants.all.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        PartiesAllocator(Set(participant1, participant2))(
          newParties =
            Seq("Alice" -> participant1, "Bob" -> participant2, "Charlie" -> participant1),
          targetTopology = Map(
            "Alice" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission)),
              acmeId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission)),
            ),
            "Bob" -> Map(
              daId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission)),
              acmeId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission)),
            ),
            "Charlie" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission)),
              acmeId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission)),
            ),
          ),
        )

        alice = "Alice".toPartyId(participant1)
        bob = "Bob".toPartyId(participant2)
        charlie = "Charlie".toPartyId(participant1)

      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1")),
          Set(InstanceName.tryCreate("sequencer2")),
        )
      ),
    )
  )

  "Importing an ACS" should {

    s"handle contracts with non-zero reassignment counter (multi-synchronizer-support=$enableAlphaMultiSynchronizerSupport)" in {
      implicit env =>
        import env.*

        val contract = IouSyntax.createIou(participant1, synchronizerId = Some(daId))(alice, bob)
        val reassignedContractCid = LfContractId.assertFromString(contract.id.contractId)

        participant1.ledger_api.commands.submit_reassign(
          alice,
          Seq(reassignedContractCid),
          source = daId,
          target = acmeId,
          submissionId = "some-submission-id",
        )

        participant1.ledger_api.commands.submit_reassign(
          alice,
          Seq(reassignedContractCid),
          source = acmeId,
          target = daId,
          submissionId = "some-submission-id",
        )

        participant1.health.ping(participant1)

        File.usingTemporaryFile() { file =>
          participant1.repair.export_acs(
            parties = Set(alice),
            exportFilePath = file.toString,
            synchronizerId = Some(daId),
            ledgerOffset = participant1.ledger_api.state.end(),
          )

          val contracts = repair.acs.read_from_file(file.canonicalPath)
          contracts.loneElement.reassignmentCounter shouldBe 2

          participant3.synchronizers.disconnect_all()

          if (enableAlphaMultiSynchronizerSupport) {
            participant3.repair.import_acs(daId, file.canonicalPath)

            val reassignedContract = participant3.ledger_api.state.acs
              .active_contracts_of_party(alice)
              .filter(_.getCreatedEvent.contractId == reassignedContractCid.coid)
              .loneElement

            withClue("Reassignment counter should be two after two reassignments") {
              reassignedContract.reassignmentCounter shouldBe 2
            }
          } else {
            assertThrowsAndLogsCommandFailures(
              participant3.repair.import_acs(daId, file.canonicalPath),
              _.errorMessage should include("with a non-zero reassignment counter"),
            )
          }
        }
    }

    /** Ensure the repair ACS import can import an ACS snapshot which contains contracts that are
      * associated to different synchronizers. Such snapshot can be imported, however the ACS import
      * needs to be called once per synchronizer since it requires the synchronizer (ID) as
      * mandatory parameter.
      *
      * The ACS import will simply drop contracts that are associated to a different synchronizer;
      * and logs that fact once.
      */
    s"import a multi-synchronizer ACS snapshot (multi-synchronizer-support=$enableAlphaMultiSynchronizerSupport)" in {
      implicit env =>
        import env.*

        // Create several contracts for both synchronizers having reassignment counter zero (thus independent of enableAlphaMultiSynchronizerSupport)
        val contractsDa = (1 to 2).map { _ =>
          IouSyntax.createIou(participant1, synchronizerId = Some(daId))(charlie, bob)
        }
        val cidsDa = contractsDa.map(c => LfContractId.assertFromString(c.id.contractId))

        val contractsAcme = (1 to 3).map { _ =>
          IouSyntax.createIou(participant1, synchronizerId = Some(acmeId))(charlie, bob)
        }
        val cidsAcme = contractsAcme.map(c => LfContractId.assertFromString(c.id.contractId))

        File.usingTemporaryFile() { acsSnapshot =>
          // Export ACS for all synchronizers (setting `synchronizerId` to `None`)
          participant1.repair.export_acs(
            parties = Set(charlie),
            exportFilePath = acsSnapshot.canonicalPath,
            synchronizerId = None,
            ledgerOffset = participant1.ledger_api.state.end(),
          )

          participant3.synchronizers.disconnect_all()

          participant3.repair.import_acs(daId, acsSnapshot.canonicalPath)
          participant3.repair.import_acs(acmeId, acsSnapshot.canonicalPath)

          // Verify ALL contracts from both synchronizers were imported
          val importedContracts = participant3.ledger_api.state.acs
            .active_contracts_of_party(charlie)
            .map(_.getCreatedEvent.contractId)

          withClue("Contracts from both daId and acmeId should be imported") {
            importedContracts should contain allElementsOf (
              cidsDa.map(_.coid) ++ cidsAcme.map(_.coid)
            )
          }
        }
    }

    // Scenario: Mid-crash recovery of `repair.import_acs` for a reassigned contract.
    //
    // Simulates a crash after internal storage persistence but before Ledger API indexing
    // completes (mechanics detailed in `OfflinePartyReplicationAcsImportMidCrashIntegrationTest`).
    //
    // As opposed to the offline party replication ACS import mid-crash test:
    //   1. Non-zero reassignment counter in recovered contract
    //   2. Imports contract using Assign/Unassign events
    //   3. Recovery retains pre-existing active contracts
    s"recover successfully ACS import mid-crash for a reassigned contract preserving pre-existing state (multi-synchronizer-support=$enableAlphaMultiSynchronizerSupport)" in {
      implicit env =>
        import env.*

        val contract = IouSyntax.createIou(participant1, synchronizerId = Some(daId))(charlie, bob)
        val contractIdStr = contract.id.contractId
        val cid = LfContractId.assertFromString(contractIdStr)

        participant1.ledger_api.commands.submit_reassign(
          charlie,
          Seq(cid),
          source = daId,
          target = acmeId,
          submissionId = "reassign-charlie-da-to-acme",
        )
        participant1.ledger_api.commands.submit_reassign(
          charlie,
          Seq(cid),
          source = acmeId,
          target = daId,
          submissionId = "reassign-charlie-acme-to-da",
        )

        // Ensure participant1 finishes processing in-flight operations
        participant1.health.ping(participant1, synchronizerId = Some(daId))

        File.usingTemporaryFile() { file =>
          participant1.repair.export_acs(
            parties = Set(charlie),
            exportFilePath = file.canonicalPath,
            synchronizerId = Some(daId),
            ledgerOffset = participant1.ledger_api.state.end(),
          )

          participant3.synchronizers.disconnect_all()

          if (enableAlphaMultiSynchronizerSupport) {
            val contractInstance =
              participant1.underlying.value.sync.participantNodePersistentState.value.contractStore
                .lookup(cid)
                .value
                .futureValueUS
                .value

            // Capture pre-existing active contracts established during the preceding sequential test cases
            val preexistingCids = participant3.ledger_api.state.acs
              .active_contracts_of_party(charlie)
              .map(c => LfContractId.assertFromString(c.getCreatedEvent.contractId))

            // Simulate crash via direct persistence injection
            val targetStores =
              participant3.underlying.value.sync.participantNodePersistentState.value
            val targetAcsStore = participant3.underlying.value.sync.stateInspection
              .getAcsInspection(daId)
              .value
              .activeContractStore

            clue("Inject dirty records preserving reassignment counter 2") {
              targetStores.contractStore.storeContracts(Seq(contractInstance)).futureValueUS

              val dirtyRepairCounter = RepairCounter.Genesis.increment.value
              val dirtyReassignmentCounter =
                ReassignmentCounter.Genesis.increment.value.increment.value
              val dirtyToc = TimeOfChange(CantonTimestamp.now(), Some(dirtyRepairCounter))

              targetAcsStore
                .markContractsAdded(Seq((cid, dirtyReassignmentCounter, dirtyToc)))
                .value
                .futureValueUS
            }

            clue(
              "Verify simulated mid-crash state: Internal store has the contract, Ledger API lacks it"
            ) {
              val internalStates = targetAcsStore.fetchStates(Seq(cid)).futureValueUS
              internalStates.get(cid) shouldBe defined

              participant3.ledger_api.state.acs
                .of_party(charlie)
                .map(_.contractId) shouldNot contain(cid.coid)
            }

            clue("Execute post-crash recovery retry via repair.import_acs") {
              participant3.repair.import_acs(daId, file.canonicalPath)
            }

            eventually() {
              val postRecoveryLedgerCids = participant3.ledger_api.state.acs
                .of_party(charlie)
                .map(_.contractId)

              // Recovered contract is visible on the Ledger API
              postRecoveryLedgerCids should contain(cid.coid)

              // Pre-existing contracts remain visible on the Ledger API
              postRecoveryLedgerCids should contain allElementsOf preexistingCids.map(_.coid)

              // Internal store contains both recovered and pre-existing contract records
              val internalStatesTarget = targetAcsStore.fetchStates(Seq(cid)).futureValueUS
              internalStatesTarget.get(cid) shouldBe defined

              // Filter pre-existing CIDs down to those associated with daId to verify their internal store records
              val preexistingDaCids = preexistingCids.filter(postRecoveryLedgerCids.contains)
              if (preexistingDaCids.nonEmpty) {
                val internalStatesPreexisting =
                  targetAcsStore.fetchStates(preexistingDaCids).futureValueUS
                preexistingDaCids.foreach { pCid =>
                  internalStatesPreexisting.get(pCid) shouldBe defined
                }
              }

              val reassignedContract = participant3.ledger_api.state.acs
                .active_contracts_of_party(charlie)
                .filter(_.getCreatedEvent.contractId == cid.coid)
                .loneElement

              reassignedContract.reassignmentCounter shouldBe 2
            }

          } else {
            assertThrowsAndLogsCommandFailures(
              participant3.repair.import_acs(daId, file.canonicalPath),
              _.errorMessage should include("with a non-zero reassignment counter"),
            )
          }
        }
    }
  }
}

final class ImportContractsIntegrationTest extends ImportContractsIntegrationTestBase {
  override protected def enableAlphaMultiSynchronizerSupport: Boolean = false
}

final class ImportContractsWithReassignmentIntegrationTest
    extends ImportContractsIntegrationTestBase {
  override protected def enableAlphaMultiSynchronizerSupport: Boolean = true
}
