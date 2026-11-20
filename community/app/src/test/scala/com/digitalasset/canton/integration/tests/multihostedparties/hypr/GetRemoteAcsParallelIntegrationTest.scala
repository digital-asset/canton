// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
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
import com.digitalasset.canton.participant.admin.party.AcsReplicationTestInterceptor
import com.digitalasset.canton.participant.admin.party.AcsReplicationTestInterceptor.{
  Proceed,
  ProceedOrWait,
  Wait,
}
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationProcessor
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ExternalParty, ForceFlag, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future

/** Objective: Test two get_remote_acs remote reads of different parties running in parallel on the
  * same target participant from the same source participant.
  *
  * Verify correctness of each returned ACS snapshot by comparing the result against
  * export_party_acs result.
  *
  * Setup:
  *   - 2 participants: participant1 hosts two external parties to remote read by participant2
  *   - 1 mediator/sequencer each
  *
  * Initialization steps:
  *   - Create contracts on participant1 and participant2 for each external party, as well as
  *     contracts shared by both external parties
  *   - Nominate and freeze the first external party on participant2, then the second one
  *
  * Scenario:
  *   - Start get_remote_acs on participant2 for the first party and stall it mid-transfer
  *   - Start get_remote_acs on participant2 for the second party and stall it mid-transfer
  *   - Stop stalling and verify that both get_remote_acs complete successfully
  *   - Verify that the result of each get_remote_acs matches the result of export_party_acs on
  *     participant1
  */
sealed trait GetRemoteAcsParallelIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private var aliceE: ExternalParty = _
  private var danaE: ExternalParty = _
  private var bob: PartyId = _
  private var carol: PartyId = _

  lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  private val numContractsInCreateBatch = 100

  private val className: String = this.getClass.getSimpleName
  private def remoteReadFileName(party: ExternalParty) =
    s"canton-remote-acs-$className-${party.partyId.identifier}.gz"
  private def exportAcsFileName(party: ExternalParty) =
    s"canton-acs-export-$className-${party.partyId.identifier}.gz"

  // Allows the test to stall the TP mid-transfer for all ACS replications
  private val shouldStallTargetParticipant = new AtomicBoolean(false)
  // The TP processors, i.e. the ACS replications, that have stalled
  private val stalledReplications = ConcurrentHashMap.newKeySet[AcsReplicationProcessor]()

  private def stallingInterceptor(): AcsReplicationTestInterceptor =
    new AcsReplicationTestInterceptor {
      override def onTargetParticipantProgress(
          progress: AcsReplicationStatus.AcsReplicationProgress
      )(implicit traceContext: TraceContext): ProceedOrWait =
        if (
          shouldStallTargetParticipant
            .get() && progress.processedContractCount.unwrap > 0 && !progress.fullyProcessedAcs
        ) {
          progress.processorO.foreach(stalledReplications.add)
          Wait
        } else Proceed
    }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAlphaAcsReplicationSupport(
          Map("participant2" -> (() => stallingInterceptor()))
        )*
      )
      // Request contracts in multiple batches so that the TP can be stalled mid-transfer.
      .addConfigTransform(
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.alphaAcsReplicationSupport)
            .modify(_.map(_.focus(_.target.acsBatchSize).replace(PositiveInt.tryCreate(10))))
        )
      )
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, daName)
        darPaths.foreach(darPath => participants.all.foreach(_.dars.upload(darPath)))

        aliceE = participant1.parties.testing.external
          .enable("aliceE", synchronizeParticipants = Seq(participant2))
        danaE = participant1.parties.testing.external
          .enable("danaE", synchronizeParticipants = Seq(participant2))
        bob = participant2.parties.enable("bob", synchronizeParticipants = Seq(participant1))
        carol = participant1.parties.enable("carol", synchronizeParticipants = Seq(participant2))
      }

  private def awaitHostingState(party: ExternalParty, state: HostingState)(implicit
      env: TestConsoleEnvironment
  ) = {
    import env.*
    eventually() {
      val ptp1 = participant1.topology.party_to_participant_mappings
        .list(daId, filterParty = party.filterString)
        .loneElement
      val ptp2 = participant2.topology.party_to_participant_mappings
        .list(daId, filterParty = party.filterString)
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

  private def awaitParticipantRemoved(party: ExternalParty)(implicit
      env: TestConsoleEnvironment
  ) = {
    import env.*
    eventually() {
      val ptp1 = participant1.topology.party_to_participant_mappings
        .list(daId, filterParty = party.filterString)
        .loneElement
      val ptp2 = participant2.topology.party_to_participant_mappings
        .list(daId, filterParty = party.filterString)
        .loneElement

      logger.debug(s"PTP on P1: $ptp1")
      logger.debug(s"PTP on P2: $ptp2")

      ptp1.item.participants.map(p => (p.participantId, p.state)).toSet shouldBe Set(
        participant1.id -> HostingState.Active
      )
      ptp2.item.participants.map(p => (p.participantId, p.state)).toSet shouldBe Set(
        participant1.id -> HostingState.Active
      )
    }
  }

  private def nominateAndFreeze(party: ExternalParty)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    clue(s"Nominating $party on P2") {
      party.topology.party_to_participant_mappings.propose_delta(
        participant1,
        adds = Seq(participant2.id -> ParticipantPermission.Confirmation),
        mustFullyAuthorize = true, // since submitting with external party keys on TP
        store = daId,
        participantStates = Map(participant2.id -> HostingState.Nominated),
      )
      awaitHostingState(party, HostingState.Nominated)
    }

    clue(s"P2 freezing $party") {
      participant2.topology.party_to_participant_mappings.propose_delta(
        party,
        mustFullyAuthorize = true, // since submitting with external party keys on TP
        store = daId,
        participantStates = Map(participant2.id -> HostingState.Freezing),
      )
      awaitHostingState(party, HostingState.Freezing)
    }
  }

  "Executing get_remote_acs in parallel for two different parties on the same TP" should {
    "Create external parties with contracts" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      implicit env =>
        import env.*

        val amounts = 1 to numContractsInCreateBatch
        Seq(aliceE, danaE).foreach { party =>
          IouSyntax.createIous(participant1, carol, party, amounts).discard
          // Create some contracts shared with a party (Bob) on the target participant P2.
          IouSyntax.createIous(participant2, bob, party, amounts).discard
        }
        // Create some contracts shared by both replicated parties.
        IouSyntax
          .createIous(participant1, carol, aliceE, amounts, observers = Seq(danaE.partyId))
          .discard

        // Freeze both parties before starting either remote read. Neither a Nominated nor a Freezing
        // entry makes P2 a host, so both reads must include the contracts shared by the parties.
        // For the export baseline below, remove each party from P2 after exporting it so that it
        // does not exclude shared contracts from the other party's export.
        nominateAndFreeze(aliceE)
        nominateAndFreeze(danaE)
    }

    "Call get_remote_acs in parallel on TP for both parties" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      implicit env =>
        import env.*

        val asOfTimes: Map[ExternalParty, CantonTimestamp] = clue("Read P2 freezing times") {
          Seq(aliceE, danaE)
            .map(p => p -> participant2.parties.get_party_freezing_time(p, daId))
            .toMap
        }

        def startRemoteRead(party: ExternalParty): Future[Unit] =
          Future(
            participant2.parties.get_remote_acs(
              Seq(party),
              daId,
              participant1,
              asOfTimes(party),
              getRemoteAcsFilePath = remoteReadFileName(party),
            )
          )

        shouldStallTargetParticipant.set(true)

        val aliceRemoteRead = clue("P2 starts remote-reading aliceE from P1 and stalls") {
          val remoteRead = startRemoteRead(aliceE)
          eventually()(stalledReplications.size shouldBe 1)
          remoteRead
        }

        val danaRemoteRead = clue("P2 starts remote-reading danaE from P1 and stalls") {
          val remoteRead = startRemoteRead(danaE)
          eventually()(stalledReplications.size shouldBe 2)
          remoteRead
        }

        clue("Both remote reads are stalled") {
          aliceRemoteRead.isCompleted shouldBe false
          danaRemoteRead.isCompleted shouldBe false
        }

        clue("P2 resumes and completes both remote reads") {
          shouldStallTargetParticipant.set(false)
          aliceRemoteRead.futureValue
          danaRemoteRead.futureValue
        }
    }

    "Invoke export_party_acs on SP for comparison" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      implicit env =>
        import env.*

        // Onboard and export aliceE before danaE, see the comment on the freezing order.
        Seq(aliceE, danaE).foreach { party =>
          val exportComparisonLedgerOffset = participant1.ledger_api.state.end()

          clue(s"Onboarding $party on P2") {
            participant2.topology.party_to_participant_mappings.propose_delta(
              party,
              mustFullyAuthorize = true, // since submitting with external party keys on TP
              store = daId,
              participantStates = Map(participant2.id -> HostingState.Onboarding),
            )
            awaitHostingState(party, HostingState.Onboarding)
          }

          // Ensure the LAPI observes the Onboarding transaction on P1.
          eventually() {
            val txWrapperO = participant1.ledger_api.updates
              .topology_transactions(
                completeAfter = PositiveInt.one,
                partyIds = Seq(party),
                beginOffsetExclusive = exportComparisonLedgerOffset,
              )
              .headOption

            val topologyEventO = txWrapperO
              .flatMap(_.topologyTransaction.events.headOption)
              .map(_.event)
            logger.debug(s"Source LAPI topology event: $topologyEventO")
            topologyEventO.map(_.isParticipantAuthorizationOnboarding) shouldBe Some(true)
          }

          clue(s"Exporting ACS of $party from P1") {
            participant1.parties.export_party_acs(
              party,
              daId,
              participant2,
              exportComparisonLedgerOffset,
              exportFilePath = exportAcsFileName(party),
            )
          }

          // In order to be able to use export_party_acs as a baseline, we need the respective
          // "other party" to not be considered hosted on the TP. One convenient way to achieve
          // is to remove the party from P2, so that both the exports produce an ACS assuming the
          // respective other party is not already hosted on the TP/P2.
          clue(s"Removing party $party from P2") {
            participant2.topology.party_to_participant_mappings.propose_delta(
              party,
              removes = Seq(participant2),
              mustFullyAuthorize = true, // participant allowed to remove party unilaterally
              store = daId,
              forceFlags = ForceFlag.DisablePartyWithActiveContracts,
            )
            awaitParticipantRemoved(party)
          }
        }
    }

    "Compare results of TP.get_remote_acs with SP.export_party_acs" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      _ =>
        import better.files.File

        Seq(aliceE, danaE).foreach { party =>
          val remoteReadFile = File(remoteReadFileName(party))
          val exportAcsFile = File(exportAcsFileName(party))

          assert(
            remoteReadFile.isSameContentAs(exportAcsFile),
            s"get_remote_acs file ${remoteReadFileName(party)} of $party " +
              s"unexpectedly differs from export_party_acs file ${exportAcsFileName(party)}",
          )
        }

        assert(
          !File(remoteReadFileName(aliceE)).isSameContentAs(File(remoteReadFileName(danaE))),
          "get_remote_acs files of different parties unexpectedly have the same content",
        )
    }
  }

}

class GetRemoteAcsParallelIntegrationTestPostgres extends GetRemoteAcsParallelIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
