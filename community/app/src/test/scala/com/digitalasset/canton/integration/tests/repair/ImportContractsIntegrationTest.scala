// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.*
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
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
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
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
        )
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
      * and logs that face once.
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
  }
}

final class ImportContractsIntegrationTest extends ImportContractsIntegrationTestBase {
  override protected def enableAlphaMultiSynchronizerSupport: Boolean = false
}

final class ImportContractsWithReassignmentIntegrationTest
    extends ImportContractsIntegrationTestBase {
  override protected def enableAlphaMultiSynchronizerSupport: Boolean = true
}
