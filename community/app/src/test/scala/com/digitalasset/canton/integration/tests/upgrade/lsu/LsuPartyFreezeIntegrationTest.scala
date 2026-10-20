// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.UnknownInformees
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseProgrammableSequencer}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.{
  Event,
  Hosting,
  Request,
  Signer,
}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.TopologyManagerError.AnnouncedLsuTopologyFreeze
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration

/** Test scenario: a logical synchronizer upgrade (LSU) is announced while a target of HyPR (Hybrid
  * Party Replication) freezes a party, after the target has imported the party's ACS.
  *
  * Test goal: show that
  *   - the announcement rejects every change of the party's mapping, so that neither the party nor
  *     the target can end the freeze until the upgrade;
  *   - the Freezing entry, with its effective time t_F, carries over to the successor, where the
  *     party stays frozen;
  *   - the replication completes on the successor, with the import done before the upgrade.
  *
  * Test setup:
  *   - 3 participants: SP (participant1), DA (participant2), and CP (participant3), which hosts
  *     Bob. Simulated clock, upgrade time 30 seconds after the epoch.
  *   - The party is hosted on SP with Submission permission and threshold 1. Bob's exercise of a
  *     nonconsuming choice on a contract with the party as signatory is the request that needs the
  *     party's confirmation.
  *   - The remote ACS read and its import are simulated with the repair service.
  */
final class LsuPartyFreezeIntegrationTest extends LsuBase with HasProgrammableSequencer {

  override protected def testName: String = "lsu-party-freeze"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*
        defaultEnvironmentSetup()
        participants.all.dars.upload(CantonTestsPath)
        bob = participant3.parties.enable("Bob")
      }

  private var bob: Party = _

  private def expectRejected(code: ErrorCode)(write: => Unit): Unit =
    loggerFactory.assertThrowsAndLogs[CommandFailure](write, _.shouldBeCantonErrorCode(code))

  "A logical synchronizer upgrade" should {
    "U1: suspend the frozen party's mapping changes, and let the replication complete on the successor" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env: TestConsoleEnvironment =>
        import env.*
        val (sp, da, cp) = (participant1, participant2, participant3)
        val fixture = fixtureWithDefaults()

        val scenario = PartyFreezeScenario(
          sp.parties.enable("Party-U1"),
          source = sp,
          partySigner = PartyFreezeScenario.PartySigner.Local(sp),
          counterparty = bob,
          counterpartyParticipant = cp,
          participants = Seq(sp, da, cp),
          synchronizerId = daId,
          sequencer = getProgrammableSequencer(sequencer1.name),
          routingRejection = UnknownInformees,
          loggerFactory = loggerFactory,
        )
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id)),
        )
        val frozenAt = scenario.freeze(da)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.Rejected,
        )
        scenario.importAcs(da, scenario.readAcs(frozenAt))

        performSynchronizerNodesLsu(fixture)

        clue("Neither the party nor DA can change the party's mapping after the announcement") {
          expectRejected(AnnouncedLsuTopologyFreeze)(scenario.remove(da, by = Signer.Party).discard)
          expectRejected(AnnouncedLsuTopologyFreeze)(
            scenario.remove(da, by = Signer.Participant).discard
          )
          expectRejected(AnnouncedLsuTopologyFreeze)(
            scenario.move(da, HostingState.Onboarding).discard
          )
        }

        environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
        transferTraffic()
        eventually() {
          environment.simClock.value.advance(Duration.ofSeconds(1))
          participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
        }
        waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)
        oldSynchronizerNodes.all.stop()

        val onSuccessor = eventually() {
          scenario.continueOn(fixture.newPsid, getProgrammableSequencer(sequencer2.name))
        }
        clue("The Freezing entry carries over with t_F, and the party stays frozen") {
          forAll(Seq(sp, da, cp)) { participant =>
            val mapping = participant.topology.party_to_participant_mappings
              .list(fixture.newPsid, filterParty = scenario.party.filterString)
              .loneElement
            CantonTimestamp.fromInstant(mapping.context.validFrom).value shouldBe frozenAt
          }
          onSuccessor.expectStep(
            Hosting.Frozen(PositiveInt.one, Set(sp.id)),
            events = Set.empty,
            Request.Rejected,
          )
        }

        val beforeOnboarding = onSuccessor.onboard(da)
        onSuccessor.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Onboarding(da.id)),
          Request.ApprovedBy(Set(sp.id)),
        )
        onSuccessor.activate(da, beforeOnboarding)
        onSuccessor.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        onSuccessor.assertCommitmentsMatchAmongHosts()
    }
  }
}
