// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.{
  Event,
  Hosting,
  Request,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: a party hosted on SP nominates DA and MPCH and raises its threshold from 1 to 2.
  * The targets replicate the party one after the other or in parallel: each freezes the party,
  * imports its ACS, and becomes Onboarding and then Active.
  *
  * Test goal: show that the party is frozen exactly while a target is Freezing, that its Active
  * hosts confirm for it otherwise, and that the ACS commitments among its hosts match at the end.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]].
  */
final class PartyFreezeReplicationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition = setup.environmentDefinition

  "Happy path" should {
    "H1: the party alone nominates DA and MPCH and raises the threshold to 2" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H1")

        // The nomination is signed by the party alone (checked by the scenario)
        scenario.nominate(Seq(da -> Confirmation, mpch -> Confirmation), PositiveInt.two)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id)),
        )

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "H2, C1: DA and then MPCH replicate the party sequentially, and the ACS commitments among the hosts match" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H2")
        scenario.nominate(Seq(da -> Confirmation, mpch -> Confirmation), PositiveInt.two)

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

        scenario.replicate(mpch)(
          afterFreeze = scenario.expectStep(
            Hosting.Frozen(PositiveInt.two, Set(sp.id, da.id)),
            events = Set.empty,
            Request.Rejected,
          ),
          afterOnboarding = scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, mpch.id)),
            events = Set(Event.Onboarding(mpch.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          ),
          afterActivation = scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, mpch.id)),
            events = Set(Event.Added(mpch.id)),
            Request.ApprovedBy(Set(sp.id, da.id, mpch.id)),
          ),
        )

        scenario.assertCommitmentsMatchAmongHosts()
    }

    // TODO(#36698): Replace `ignore` once import_party_acs moves a Freezing target to Onboarding
    "H4: move F → O and O → A without manual steps after import_party_acs" ignore { implicit env =>
      import env.*
      val scenario = newScenario("H4")
      scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
      expectHostedBySp(scenario)
      val frozenAt = scenario.freeze(da)
      expectFrozenWithHostSp(scenario)
      val snapshot = scenario.readAcs(frozenAt)

      da.synchronizers.disconnect_all()
      da.parties.import_party_acs(daId, Some(scenario.party), snapshot.path)
      da.synchronizers.reconnect_all()
      expectOnboardedAndActivated(scenario, da)
    }

    // TODO(#36698): Replace `ignore` once import_party_acs moves a Freezing target to Onboarding,
    //  also after a restart
    "H5: submit the pending F → O after a restart of the target between the import and the reconnect" ignore {
      implicit env =>
        import env.*
        val scenario = newScenario("H5")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        val frozenAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        val snapshot = scenario.readAcs(frozenAt)

        da.synchronizers.disconnect_all()
        da.parties.import_party_acs(daId, Some(scenario.party), snapshot.path)
        da.stop()
        da.start()
        da.synchronizers.reconnect_all()
        expectOnboardedAndActivated(scenario, da)
    }

    "H3: DA and MPCH replicate the party in parallel" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H3")
        scenario.nominate(Seq(da -> Confirmation, mpch -> Confirmation), PositiveInt.two)

        val frozenByDaAt = scenario.freeze(da)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.Rejected,
        )
        val frozenByMpchAt = scenario.freeze(mpch)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.Rejected,
        )

        scenario.importAcs(da, scenario.readAcs(frozenByDaAt))
        scenario.importAcs(mpch, scenario.readAcs(frozenByMpchAt))

        // MPCH is still Freezing, so the party stays frozen
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
        val beforeMpchOnboarding = scenario.onboard(mpch)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, mpch.id)),
          events = Set(Event.Onboarding(mpch.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        scenario.activate(mpch, beforeMpchOnboarding)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, mpch.id)),
          events = Set(Event.Added(mpch.id)),
          Request.ApprovedBy(Set(sp.id, da.id, mpch.id)),
        )

        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

}
