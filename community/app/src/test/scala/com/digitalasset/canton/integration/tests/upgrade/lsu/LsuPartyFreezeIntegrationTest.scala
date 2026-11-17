// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseProgrammableSequencer}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.{
  Entry,
  Event,
  Hosting,
  Request,
  Signer,
}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.TopologyManagerError.AnnouncedLsuTopologyFreeze
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.version.ProtocolVersion

import java.time.Duration

/** A logical synchronizer upgrade (LSU) during a party freeze of HyPR (Hybrid Party Replication).
  *
  * Test setup:
  *   - 3 participants: SP (participant1), DA (participant2), and CP (participant3), which hosts
  *     Bob. Simulated clock, upgrade time 30 seconds after the epoch.
  *   - The party is hosted on SP with Submission permission and threshold 1. Bob's exercise of a
  *     nonconsuming choice on a contract with the party as signatory is the request that needs the
  *     party's confirmation.
  */
abstract class LsuPartyFreezeIntegrationTestBase extends LsuBase with HasProgrammableSequencer {

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

  /** Starts a scenario for a new party on SP, with DA nominated as Confirmation host. */
  protected def nominatedScenario(testCase: String)(implicit
      env: TestConsoleEnvironment
  ): PartyFreezeScenario = {
    import env.*
    val scenario = PartyFreezeScenario(
      participant1.parties.enable(s"Party-$testCase"),
      source = participant1,
      partySigner = PartyFreezeScenario.PartySigner.Local(participant1),
      counterparty = bob,
      counterpartyParticipant = participant3,
      participants = Seq(participant1, participant2, participant3),
      synchronizerId = daId,
      sequencer = getProgrammableSequencer(sequencer1.name),
      loggerFactory = loggerFactory,
    )
    scenario.nominate(Seq(participant2 -> Confirmation), PositiveInt.one)
    scenario.expectStep(
      Hosting.Hosted(PositiveInt.one, Set(participant1.id)),
      events = Set.empty,
      Request.ApprovedBy(Set(participant1.id)),
    )
    scenario
  }

  /** Upgrades the synchronizer at the upgrade time, and continues `scenario` on the successor. */
  protected def upgrade(scenario: PartyFreezeScenario, fixture: Fixture)(implicit
      env: TestConsoleEnvironment
  ): PartyFreezeScenario = {
    import env.*
    environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
    transferTraffic()
    eventually() {
      environment.simClock.value.advance(Duration.ofSeconds(1))
      participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
    }
    waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)
    oldSynchronizerNodes.all.stop()
    eventually() {
      scenario.continueOn(fixture.newPsid, getProgrammableSequencer(sequencer2.name))
    }
  }
}

/** Test scenario: an LSU is announced while a target of HyPR freezes a party, after the target has
  * imported the party's ACS.
  *
  * Test goal: show that
  *   - the announcement rejects every change of the party's mapping, so that neither the party nor
  *     the target can end the freeze until the upgrade;
  *   - the Freezing entry, with its effective time t_F, carries over to the successor, where the
  *     party stays frozen;
  *   - the replication completes on the successor, with the import done before the upgrade.
  *
  * Test setup: see [[LsuPartyFreezeIntegrationTestBase]]. The remote ACS read and its import are
  * simulated with the repair service, so that the target moves to Onboarding only when the test
  * does.
  */
final class LsuPartyFreezeIntegrationTest extends LsuPartyFreezeIntegrationTestBase {

  override protected def testName: String = "lsu-party-freeze"

  private def expectRejected(code: ErrorCode)(write: => Unit): Unit =
    loggerFactory.assertThrowsAndLogs[CommandFailure](write, _.shouldBeCantonErrorCode(code))

  "A logical synchronizer upgrade" should {
    "U1: suspend the frozen party's mapping changes, and let the replication complete on the successor" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env: TestConsoleEnvironment =>
        import env.*
        val (sp, da, cp) = (participant1, participant2, participant3)
        val fixture = fixtureWithDefaults()

        val scenario = nominatedScenario("U1")
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

        val onSuccessor = upgrade(scenario, fixture)
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

/** Test scenario: a target of HyPR imports a frozen party's ACS with the party ACS import while
  * disconnected. An LSU is announced before the target reconnects.
  *
  * Test goal: show that the target's move from Freezing to Onboarding, which the import records,
  * waits for the upgrade, and that the target then moves to Onboarding and to Active on the
  * successor without manual steps.
  *
  * Test setup: see [[LsuPartyFreezeIntegrationTestBase]].
  */
final class LsuPartyFreezePendingMoveIntegrationTest extends LsuPartyFreezeIntegrationTestBase {

  override protected def testName: String = "lsu-party-freeze-pending-move"

  "A logical synchronizer upgrade" should {
    "U2: defer the move from Freezing to Onboarding after import_party_acs until the upgrade" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env: TestConsoleEnvironment =>
        import env.*
        val (sp, da) = (participant1, participant2)
        val fixture = fixtureWithDefaults()

        val scenario = nominatedScenario("U2")
        val frozenAt = scenario.freeze(da)
        def expectFrozen(): Unit =
          scenario.expectStep(
            Hosting.Frozen(PositiveInt.one, Set(sp.id)),
            events = Set.empty,
            Request.Rejected,
          )
        expectFrozen()
        scenario.importPartyAcs(da, scenario.readAcs(frozenAt)) {
          performSynchronizerNodesLsu(fixture)
          // Time passes before DA reconnects. DA's move waits until DA has caught up to the current
          // time, and so has seen the announcement.
          environment.simClock.value.advance(Duration.ofSeconds(1))
        }

        clue("DA is still Freezing after it has caught up with the announcement") {
          da.testing.await_synchronizer_time(daId, sp.testing.fetch_synchronizer_time(daId))
          expectFrozen()
        }

        val onSuccessor = upgrade(scenario, fixture)
        val onboardedAt = onSuccessor.expectWritten(
          onSuccessor.entries.updated(da.id, Entry(Confirmation, HostingState.Onboarding)),
          PositiveInt.one,
          signers = Seq(da),
        )
        onSuccessor.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Onboarding(da.id)),
          Request.ApprovedBy(Set(sp.id)),
        )
        onSuccessor.advancePastDecisionDeadline(onboardedAt)
        onSuccessor.expectWritten(
          onSuccessor.entries.updated(da.id, Entry(Confirmation, HostingState.Active)),
          PositiveInt.one,
          signers = Seq(da),
        )
        onSuccessor.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        onSuccessor.assertCommitmentsMatchAmongHosts()
    }
  }
}
