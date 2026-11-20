// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError
import com.digitalasset.canton.participant.config.AlphaAcsReplicationConfig
import com.digitalasset.canton.synchronizer.sequencer.config.AlphaSequencerChannelConfig
import com.digitalasset.canton.topology.ExternalParty
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

/** Objective: Test that get_remote_acs fails if the source participant does not support ACS
  * replication via sequencer channels, because the source participant rejects the ACS replication
  * proposal of the target participant.
  *
  * Setup:
  *   - 2 participants: participant1 hosts an external party to remote read by participant2.
  *     Participant1 (source) does not have the ACS replication enabled, participant2 (target) does.
  *   - 1 mediator/sequencer each
  */
sealed trait GetRemoteAcsWithoutSourceChannelSupportIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private var aliceE: ExternalParty = _

  lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  // Participant1 also loads the built-in ACS replication DAR even when ACS replication is not
  // enabled so that participant1 observes the proposal via its ACS replication admin workflow
  // subscription and can reject the proposal.

  private val expectedRejectReason = "does not support ACS replication via sequencer channels"

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.alphaAcsReplicationSupport)
            .replace(Some(AlphaAcsReplicationConfig()))
        ),
        ConfigTransforms.updateAllSequencerConfigs_(
          _.focus(_.parameters.alphaSequencerChannelSupport)
            .replace(Some(AlphaSequencerChannelConfig()))
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)
        darPaths.foreach(darPath => participants.all.foreach(_.dars.upload(darPath)))

        aliceE = participant1.parties.testing.external
          .enable("aliceE", synchronizeParticipants = Seq(participant2))
      }

  private def awaitHostingState(state: HostingState)(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    eventually() {
      Seq(participant1, participant2).foreach { participant =>
        participant.topology.party_to_participant_mappings
          .list(daId, filterParty = aliceE.filterString)
          .loneElement
          .item
          .participants
          .map(p => (p.participantId, p.state))
          .toSet shouldBe Set(
          participant1.id -> HostingState.Active,
          participant2.id -> state,
        )
      }
    }
  }

  "Nominate and freeze external party on target participant" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
    implicit env =>
      import env.*

      clue("Nominating Alice on P2") {
        aliceE.topology.party_to_participant_mappings.propose_delta(
          participant1,
          adds = Seq(participant2.id -> ParticipantPermission.Confirmation),
          mustFullyAuthorize = true, // since submitting with external party keys on TP
          store = daId,
          participantStates = Map(participant2.id -> HostingState.Nominated),
        )
        awaitHostingState(HostingState.Nominated)
      }

      clue("P2 freezing Alice") {
        participant2.topology.party_to_participant_mappings.propose_delta(
          aliceE,
          mustFullyAuthorize = true, // since submitting with external party keys on TP
          store = daId,
          participantStates = Map(participant2.id -> HostingState.Freezing),
        )
        awaitHostingState(HostingState.Freezing)
      }
  }

  "get_remote_acs fails because source participant rejects the proposal" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
    implicit env =>
      import env.*

      val asOf = participant2.parties.get_party_freezing_time(aliceE, daId)

      loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
        participant2.parties.get_remote_acs(
          Seq(aliceE),
          daId,
          participant1,
          asOf,
          getRemoteAcsFilePath = s"canton-remote-acs-${this.getClass.getSimpleName}.gz",
        ),
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              entry => {
                entry.loggerName should include(participant1.name)
                entry.warningMessage should include(
                  s"Participant ${participant1.id} $expectedRejectReason"
                )
              },
              "source participant rejects proposal",
            ),
            (
              entry => {
                entry shouldBeCantonErrorCode PartyManagementServiceError.GetRemoteAcsFailed
                entry.errorMessage should include(expectedRejectReason)
              },
              "get_remote_acs fails on target participant",
            ),
          )
        ),
      )
  }
}

class GetRemoteAcsWithoutSourceChannelSupportIntegrationTestH2
    extends GetRemoteAcsWithoutSourceChannelSupportIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}
