// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.HasTempDirectory
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.{
  Event,
  Hosting,
  Request,
  Signer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.protocol.party.{
  FreezingToOnboardingOperation,
  OnboardingClearanceOperation,
}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.version.ProtocolVersion
import org.slf4j.event.Level

import java.time.Duration

/** Test scenario: a party hosted on SP nominates DA and Beta and raises its threshold from 1 to 2.
  * The targets replicate the party one after the other or in parallel: each freezes the party,
  * imports its ACS, and becomes Onboarding and then Active. In B1, DA is then restored from a
  * backup of its database taken before the import, and replicates the party again from there. In
  * A1, the party removes DA after its import, before DA reconnects.
  *
  * Test goal: show that the party is frozen exactly while a target is Freezing, that its Active
  * hosts confirm for it otherwise, and that the ACS commitments among its hosts match at the end;
  * and that a target removed after its import drops its pending operations, with one warning.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]].
  */
final class PartyFreezeReplicationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasTempDirectory {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition = setup.environmentDefinition

  "Happy path" should {
    "H1: the party alone nominates DA and Beta and raises the threshold to 2" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H1")

        // The nomination is signed by the party alone (checked by the scenario)
        scenario.nominate(Seq(da -> Confirmation, beta -> Confirmation), PositiveInt.two)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id)),
        )

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "H2, C1: DA and then Beta replicate the party sequentially, and the ACS commitments among the hosts match" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H2")
        scenario.nominate(Seq(da -> Confirmation, beta -> Confirmation), PositiveInt.two)

        scenario.replicate(da)(
          afterFreeze = scenario.expectStep(
            Hosting.Frozen(PositiveInt.one, Set(sp.id)),
            events = Set.empty,
            Request.Rejected,
          ),
          afterOnboarding = scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Onboarding(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          ),
          afterActivation = scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id)),
            events = Set(Event.Added(da.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          ),
        )

        scenario.replicate(beta)(
          afterFreeze = scenario.expectStep(
            Hosting.Frozen(PositiveInt.two, Set(sp.id, da.id)),
            events = Set.empty,
            Request.Rejected,
          ),
          afterOnboarding = scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, beta.id)),
            events = Set(Event.Onboarding(beta.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          ),
          afterActivation = scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, beta.id)),
            events = Set(Event.Added(beta.id)),
            Request.ApprovedBy(Set(sp.id, da.id, beta.id)),
          ),
        )

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "H5: submit the pending F → O after a restart of the target between the import and the reconnect" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H5")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        val frozenAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        val snapshot = scenario.readAcs(frozenAt)
        scenario.importPartyAcs(da, snapshot) {
          da.stop()
          da.start()
          // As after a crash during the first import: the second one accepts the contracts and the
          // pending operations that the first one has written
          da.parties.import_party_acs(env.daId.logical, Some(scenario.party.partyId), snapshot.path)
        }
        expectOnboardedAndActivated(scenario, da)
    }

    "H6: DA and Beta import the party's ACS in parallel, and both move F → O and O → A without manual steps" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H6")
        scenario.nominate(Seq(da -> Confirmation, beta -> Confirmation), PositiveInt.two)
        expectHostedBySp(scenario)
        val frozenByDaAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        val frozenByBetaAt = scenario.freeze(beta)
        expectFrozenWithHostSp(scenario)

        // Both reconnect right after each other, so that their moves compete for the same serials
        val daSnapshot = scenario.readAcs(frozenByDaAt)
        val betaSnapshot = scenario.readAcs(frozenByBetaAt)
        scenario.importPartyAcs(da, daSnapshot) {
          scenario.importPartyAcs(beta, betaSnapshot)(whileDisconnected = ())
        }

        val onboardedAt =
          expectMovedOnTheirOwn(scenario, Seq(da, beta), HostingState.Onboarding, PositiveInt.two)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
          events = Set(Event.Onboarding(da.id), Event.Onboarding(beta.id)),
          Request.ApprovedBy(Set(sp.id)),
        )
        scenario.advancePastDecisionDeadline(onboardedAt)
        expectMovedOnTheirOwn(scenario, Seq(da, beta), HostingState.Active, PositiveInt.two).discard
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, beta.id)),
          events = Set(Event.Added(da.id), Event.Added(beta.id)),
          Request.ApprovedBy(Set(sp.id, da.id, beta.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "H3: DA and Beta replicate the party in parallel" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H3")
        scenario.nominate(Seq(da -> Confirmation, beta -> Confirmation), PositiveInt.two)

        val frozenByDaAt = scenario.freeze(da)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.Rejected,
        )
        val frozenByBetaAt = scenario.freeze(beta)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.Rejected,
        )

        scenario.importAcs(da, scenario.readAcs(frozenByDaAt))
        scenario.importAcs(beta, scenario.readAcs(frozenByBetaAt))

        // Beta is still Freezing, so the party stays frozen
        val beforeDaOnboarding = scenario.onboard(da)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Onboarding(da.id)),
          Request.Rejected,
        )
        scenario.activate(da, beforeDaOnboarding)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.two, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.Rejected,
        )

        // No entry is Freezing anymore
        val beforeBetaOnboarding = scenario.onboard(beta)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, beta.id)),
          events = Set(Event.Onboarding(beta.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        scenario.activate(beta, beforeBetaOnboarding)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, beta.id)),
          events = Set(Event.Added(beta.id)),
          Request.ApprovedBy(Set(sp.id, da.id, beta.id)),
        )

        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

  "Restore from backup" should {
    "B1: restore the target from its backup before the import, import the ACS again, and catch up without moving again" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*
        val scenario = newScenario("B1")
        // In the snapshot, and replaced after the activation: the restored target must apply the
        // replacement on top of the import
        val replaced = createContract(
          sp,
          scenario.party,
          daId,
          signatories = Seq(scenario.party),
          actors = Seq(scenario.party),
        )
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        val frozenAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        val snapshot = scenario.readAcs(frozenAt)

        // The recovery point of the replication: DA is disconnected and has processed the freeze
        val backup = tempDirectory.toTempFile("party-freeze-b1-da.tar")
        da.synchronizers.disconnect_all()
        postgresDumpRestore.saveDump(da, backup).futureValue
        scenario.importPartyAcs(da, snapshot)(whileDisconnected = ())
        expectOnboardedAndActivated(scenario, da)
        replace(sp, scenario.party, daId, replaced)
        val serialBeforeRestore = scenario.nextSerial

        da.stop()
        postgresDumpRestore.restoreDump(da, backup.path).futureValue
        da.start()
        // Time passes during a restore. DA's moves wait until DA has caught up to the current time.
        environment.simClock.value.advance(Duration.ofSeconds(10))
        clue("The restored DA is Freezing, without the party's contracts") {
          da.topology.party_to_participant_mappings
            .list(daId, filterParty = scenario.party.filterString)
            .loneElement
            .item
            .participants
            .find(_.participantId == da.id)
            .value
            .state shouldBe HostingState.Freezing
          da.ledger_api.state.acs.of_party(scenario.party) shouldBe empty
        }
        da.parties.import_party_acs(daId.logical, Some(scenario.party.partyId), snapshot.path)
        loggerFactory.assertLogsSeq(
          SuppressionRule.LevelAndAbove(Level.WARN) ||
            (SuppressionRule.LoggerNameContains("TopologyStateProcessor") &&
              SuppressionRule.Level(Level.INFO))
        )(
          {
            da.synchronizers.reconnect_all()
            scenario.awaitCaughtUpAfterRestore(da)
          },
          { entries =>
            val (warnings, topologyProcessing) = entries.partition(_.level == Level.WARN)
            // The restored DA processes the requests since the backup as new ones, so it responds
            // to those it confirmed in the original run after their deadline
            forAll(warnings)(
              _.warningMessage should include regex "Response message for request \\[.*\\] timed out at"
            )
            // DA does not propose its move to Onboarding again. Its clearance may propose the move
            // to Active again before processing it; that proposal is identical and merged.
            withClue("None of DA's proposals is rejected: ") {
              forAll(topologyProcessing)(_.message should not include "Rejected transaction")
            }
          },
        )

        clue(
          "DA has caught up to its moves to Onboarding and Active, without a new serial"
        ) {
          scenario.nextSerial shouldBe serialBeforeRestore
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id, da.id)),
          )
        }
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

  "Abort" should {
    "A1: the party removes DA after the import, before DA reconnects" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*
        val scenario = newScenario("A1")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        val frozenAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        val snapshot = scenario.readAcs(frozenAt)

        val state = da.testing.state_inspection.syncPersistentStateManager.get(daId).value
        def pendingMoves() = state.pendingFreezingToOnboardingStore
          .getAll(FreezingToOnboardingOperation.operationName, Some(daId.logical))
          .futureValueUS
        def pendingClearances() = state.pendingOnboardingClearanceStore
          .getAll(OnboardingClearanceOperation.operationName, Some(daId.logical))
          .futureValueUS

        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          scenario.importPartyAcs(da, snapshot) {
            pendingMoves() should have size 1
            pendingClearances() should have size 1
            // DA is disconnected: the removal is awaited on the connected participants only
            scenario.remove(da, by = Signer.Party).discard
          },
          entries => {
            forExactly(1, entries)(
              _.warningMessage should include(
                "The contracts that the import added remain: restore the participant from the backup"
              )
            )
            forAll(entries)(_.warningMessage should include("The contracts that the import added"))
          },
        )
        eventually() {
          pendingMoves() shouldBe empty
          pendingClearances() shouldBe empty
        }
        expectHostedBySp(scenario)
        withClue("DA keeps the imported contracts: ") {
          da.testing
            .acs_search(daName, filterStakeholder = Some(scenario.party.partyId))
            .map(_.contractId.coid)
            .toSet shouldBe snapshot.contractIds
        }
    }
  }
}
