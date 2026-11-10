// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: a party hosted on SP nominates DA. DA replicates the party with the admin
  * commands of HyPR alone: it freezes the party, looks up the time as of which its freeze applies
  * (get_party_freezing_time), reads the party's ACS from SP as of that time (get_remote_acs),
  * imports it (import_party_acs), and then moves to Onboarding and Active on its own.
  *
  * Test goal: show that these commands complete the replication together: the read returns the
  * party's ACS as of t_F + 1µs, the import accepts it, DA hosts the party's contracts, and the ACS
  * commitments among the hosts match.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]], with ACS replication and sequencer channels
  * enabled for the remote read, which runs only on the dev protocol version so far.
  */
final class PartyFreezeRemoteReadIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition =
    setup.environmentDefinition.addConfigTransforms(
      ConfigTransforms.enableAlphaAcsReplicationSupport()*
    )

  "End to end" should {
    // TODO(#37839): Run from PV37 on once the remote read is available there
    "E1: DA replicates the party with the remote read and the import, without manual steps after the import" onlyRunWith ProtocolVersion.dev in {
      implicit env =>
        val scenario = newScenario("E1")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        val frozenAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        scenario.importPartyAcs(da, scenario.readRemoteAcs(da, frozenAt))(whileDisconnected = ())
        expectOnboardedAndActivated(scenario, da)
    }
  }
}
