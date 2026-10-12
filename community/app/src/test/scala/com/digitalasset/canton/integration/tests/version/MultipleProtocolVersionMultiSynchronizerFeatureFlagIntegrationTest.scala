// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.{AcsInspection, MultiSynchronizerFeatureFlag}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentValidationError.MultiSynchronizerIsNotEnabled
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import monocle.macros.syntax.lens.*

/** We test the multi-synchronizer topology feature flag for reassignments between a synchronizer
  * running a protocol version before v37 and a synchronizer running v37.
  *
  * The topology used:
  *   - S1 (da) runs v36, S2 (acme) runs v37
  *   - P1 is connected to S1 and S2
  *
  * The flag is only required on the synchronizer running a protocol version before v37: on the
  * source synchronizer for a reassignment from S1 to S2, and on the target synchronizer for a
  * reassignment from S2 to S1.
  */
sealed trait MultipleProtocolVersionMultiSynchronizerFeatureFlagIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AcsInspection {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .P1_S1M1_S1M1_WithStaticParameters(
        Map(
          "synchronizer1" -> StaticSynchronizerParameters.defaults(ProtocolVersion.v36),
          "synchronizer2" -> StaticSynchronizerParameters.defaults(ProtocolVersion.v37),
        )
      )
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant1") {
          _.focus(_.parameters.minimumProtocolVersion)
            .replace(Some(ParticipantProtocolVersion(ProtocolVersion.v36)))
        },
        _.focus(_.parameters.enableAlphaStateViaConfig).replace(true),
      )
      .addConfigTransforms(ConfigTransforms.enableAlphaVersionSupport*)
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        Seq(daId, acmeId)
          .foreach(psid => participant1.dars.upload(CantonExamplesPath, synchronizerId = psid))

        MultiSynchronizerFeatureFlag.enable(Seq(participant1), daId.logical)
      }

  private def createCycleCommand(
      id: String
  )(implicit env: TestConsoleEnvironment): Command = {
    import env.*
    val pkg = participant1.packages.find_by_module("Cycle").headOption.value
    ledger_api_utils.create(
      pkg.packageId,
      "Cycle",
      "Cycle",
      Map[String, Any]("owner" -> participant1.adminParty, "id" -> id),
    )
  }

  private def createCycle(
      synchronizerId: PhysicalSynchronizerId,
      synchronizerAlias: SynchronizerAlias,
  )(implicit env: TestConsoleEnvironment): LfContractId = {
    import env.*

    participant1.ledger_api.commands.submit(
      Seq(participant1.adminParty),
      Seq(createCycleCommand("cycle-contract")),
      Some(synchronizerId),
    )

    searchAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = synchronizerAlias,
      module = "Cycle",
      template = "Cycle",
    )
  }

  private def archiveCycle(
      synchronizerId: PhysicalSynchronizerId,
      synchronizerAlias: SynchronizerAlias,
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val party = participant1.adminParty
    val cid = participant1.ledger_api.javaapi.state.acs.await(M.Cycle.COMPANION)(party)
    participant1.ledger_api.javaapi.commands.submit(
      Seq(party),
      Seq(cid.id.exerciseArchive().commands.loneElement),
      Some(synchronizerId),
    )

    assertNotInAcsSync(
      participantRefs = List(participant1),
      synchronizerAlias = synchronizerAlias,
      module = "Cycle",
      template = "Cycle",
    )
  }

  "A contract" should {
    "be reassignable when the flag is only enabled on the synchronizer running a protocol version before v37" onlyRunWith ProtocolVersion.v36 in {
      implicit env =>
        import env.*

        val party = participant1.adminParty
        val cid = createCycle(daId, daName)

        participant1.ledger_api.commands.submit_reassign(party, Seq(cid), daId, acmeId)
        searchAcsSync(
          participantRefs = List(participant1),
          synchronizerAlias = acmeName,
          module = "Cycle",
          template = "Cycle",
        )

        participant1.ledger_api.commands.submit_reassign(party, Seq(cid), acmeId, daId)
        searchAcsSync(
          participantRefs = List(participant1),
          synchronizerAlias = daName,
          module = "Cycle",
          template = "Cycle",
        )

        archiveCycle(daId, daName)
    }

    "not be reassignable when the flag is not enabled on the synchronizer running a protocol version before v37" onlyRunWith ProtocolVersion.v36 in {
      implicit env =>
        import env.*

        val party = participant1.adminParty
        MultiSynchronizerFeatureFlag.disable(Seq(participant1), daId.logical)

        val expectedError = MultiSynchronizerIsNotEnabled(Set(participant1.id), daId).message

        val cidOnDa = createCycle(daId, daName)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.commands
            .submit_unassign(party, Seq(cidOnDa), source = daId, target = acmeId),
          _.errorMessage should include(expectedError),
        )
        archiveCycle(daId, daName)

        val cidOnAcme = createCycle(acmeId, acmeName)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant1.ledger_api.commands
            .submit_unassign(party, Seq(cidOnAcme), source = acmeId, target = daId),
          _.errorMessage should include(expectedError),
        )
        archiveCycle(acmeId, acmeName)
    }
  }
}

class MultipleProtocolVersionMultiSynchronizerFeatureFlagIntegrationTestPostgres
    extends MultipleProtocolVersionMultiSynchronizerFeatureFlagIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer.tryCreate(
        Set("sequencer1"),
        Set("sequencer2"),
      ),
    )
  )
}
