// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseH2,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.PartyNotInFreezingHostingState
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.mutable.ListBuffer

/** Objective: Test get_remote_acs remote read on behalf of an external party. Verify correctness of
  * the returned ACS snapshot by comparing the result against what export_party-acs returns.
  *
  * Setup:
  *   - 2 participants: participant1 hosts an external party to remote read by participant2
  *   - 1 mediator/sequencer each
  */
sealed trait GetRemoteAcsIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private var aliceE: ExternalParty = _
  private var bob: PartyId = _
  private var carol: PartyId = _

  lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  private val numContractsInCreateBatch = 100

  private val remoteReadFileName = s"canton-remote-acs-${this.getClass.getSimpleName}.gz"
  private val exportAcsFileName = s"canton-acs-export-${this.getClass.getSimpleName}.gz"

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.enableAlphaAcsReplicationSupport()*)
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)
        darPaths.foreach(darPath => participants.all.foreach(_.dars.upload(darPath)))

        aliceE = participant1.parties.testing.external
          .enable("aliceE", synchronizeParticipants = Seq(participant2))
        bob = participant2.parties.enable("bob", synchronizeParticipants = Seq(participant1))
        carol = participant1.parties.enable("carol", synchronizeParticipants = Seq(participant2))
      }

  private def awaitHostingState(state: HostingState)(implicit env: TestConsoleEnvironment) = {
    import env.*
    eventually() {
      val ptp1 = participant1.topology.party_to_participant_mappings
        .list(daId, filterParty = aliceE.filterString)
        .loneElement
      val ptp2 = participant2.topology.party_to_participant_mappings
        .list(daId, filterParty = aliceE.filterString)
        .loneElement

      logger.debug(s"PTP on P1: $ptp1")
      logger.debug(s"PTP on P2: $ptp2")

      ptp1.item.participants.map(p => (p.participantId, p.state)).toSet shouldBe Set(
        participant1.id -> HostingState.Active,
        participant2.id -> state,
      )
      ptp2.item.participants.map(p => (p.participantId, p.state)).toSet shouldBe Set(
        participant1.id -> HostingState.Active,
        participant2.id -> state,
      )
    }
  }

  // TODO(#37839): Switch this test to pv37 along with marking acs_replication.proto
  //  replication protocol messages to stable/pv37
  "Create external party with contracts" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    val amounts = 1 to numContractsInCreateBatch
    ListBuffer.from(IouSyntax.createIous(participant1, carol, aliceE, amounts))
    // Create some contracts shared with a party (Bob) on the target participant P2.
    ListBuffer.from(IouSyntax.createIous(participant2, bob, aliceE, amounts))

    clue("Nominating Alice on P2") {
      aliceE.topology.party_to_participant_mappings.propose_delta(
        participant1,
        adds = Seq(
          participant2.id -> ParticipantPermission.Confirmation
        ),
        mustFullyAuthorize = true, // since submitting with external party keys on TP
        store = daId,
        participantStates = Map(participant2.id -> HostingState.Nominated),
      )
      awaitHostingState(HostingState.Nominated)
    }

    clue("Not ready to read Alice P2 freezing time") {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant2.parties.get_party_freezing_time(aliceE, daId),
        entry => {
          entry shouldBeCantonErrorCode (PartyNotInFreezingHostingState)
          entry.errorMessage should include(
            PartyNotInFreezingHostingState.Error(aliceE, daId, participant2.id).cause
          )
        },
      )
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

  "get_remote_acs on P2" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    val asOf = clue("Read Alice P2 freezing time") {
      participant2.parties.get_party_freezing_time(aliceE, daId)
    }

    clue("P2 remote-reads from P1") {
      participant2.parties.get_remote_acs(
        aliceE,
        daId,
        participant1,
        asOf,
        getRemoteAcsFilePath = remoteReadFileName,
      )
    }
  }

  "Invoke export_party_acs on P1 for comparison" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val exportComparisonLedgerOffset = participant1.ledger_api.state.end()

      clue("Onboarding Alice on P2") {
        participant2.topology.party_to_participant_mappings.propose_delta(
          aliceE,
          mustFullyAuthorize = true, // since submitting with external party keys on TP
          store = daId,
          participantStates = Map(participant2.id -> HostingState.Onboarding),
        )
        awaitHostingState(HostingState.Onboarding)
      }

      // Ensure the LAPI observes the Onboarding transaction on P1.
      eventually() {
        val txWrapperO = participant1.ledger_api.updates
          .topology_transactions(
            completeAfter = PositiveInt.one,
            partyIds = Seq(aliceE),
            beginOffsetExclusive = exportComparisonLedgerOffset,
          )
          .headOption

        val topologyEventO = txWrapperO
          .flatMap(_.topologyTransaction.events.headOption)
          .map(_.event)
        logger.debug(s"Source LAPI topology event: $topologyEventO")
        topologyEventO.map(_.isParticipantAuthorizationOnboarding) shouldBe Some(true)
      }

      clue("Exporting ACS of Alice from P1") {
        participant1.parties.export_party_acs(
          aliceE,
          daId,
          participant2,
          exportComparisonLedgerOffset,
          exportFilePath = exportAcsFileName,
        )
      }
  }

  "Result of P2.get_remote_acs(P1) matches P1.export_party_acs" onlyRunWith ProtocolVersion.dev in {
    _ =>
      import better.files.File

      val remoteReadFile = File.apply(remoteReadFileName)
      val exportAcsFile = File.apply(exportAcsFileName)

      assert(
        remoteReadFile.isSameContentAs(exportAcsFile),
        s"get_remote_acs file $remoteReadFileName unexpectedly differs from export_party_acs file $exportAcsFileName",
      )
  }
}

class GetRemoteAcsIntegrationTestH2 extends GetRemoteAcsIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
}

class GetRemoteAcsIntegrationTestPostgres extends GetRemoteAcsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
