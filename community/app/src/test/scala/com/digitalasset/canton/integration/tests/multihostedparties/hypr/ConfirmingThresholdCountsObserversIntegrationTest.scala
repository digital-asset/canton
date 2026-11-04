// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
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
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
}
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: a party is hosted on SP (Submission) and DA (Confirmation) with threshold 2. DA
  * leaves the confirming hosts on its own, first by removing itself, then by downgrading itself to
  * Observation. Both leave SP as the only host that can confirm.
  *
  * Test goal: show that the topology manager's check of the confirming threshold
  * (`TOPOLOGY_CONFIRMING_THRESHOLD_CANNOT_BE_REACHED`, overridden by
  * `FORCE_FLAG_ALLOW_CONFIRMING_THRESHOLD_CANNOT_BE_MET`) counts the Observation entry as if it
  * could confirm: the removal is rejected without the force flag, the downgrade is accepted, and
  * afterwards every request that needs the party's confirmation times out until the party lowers
  * its threshold.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]].
  */
final class ConfirmingThresholdCountsObserversIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition = setup.environmentDefinition

  "The confirming threshold check" should {
    // TODO(#37597): Expect the downgrade to be rejected once the check counts only confirming hosts
    "accept a downgrade to Observation that leaves the threshold unreachable" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("downgrade")
        scenario.add(da, Confirmation, HostingState.Active)
        scenario.setThreshold(PositiveInt.two)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )

        clue("DA removes itself: rejected without the force flag") {
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            scenario.remove(da, by = Signer.Participant),
            _.shouldBeCommandFailure(TopologyManagerError.ConfirmingThresholdCannotBeReached),
          )
        }

        clue("DA downgrades itself to Observation: accepted without the force flag") {
          scenario.downgrade(da, Observation)
          // Only SP can confirm, but the threshold stays 2: the party's requests time out
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id)),
            events = Set(Event.ChangedTo(da.id, Observation)),
            Request.TimesOut,
          )
        }

        clue("The party lowers its threshold to 1: SP confirms alone") {
          scenario.setThreshold(PositiveInt.one)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id)),
          )
        }
    }
  }
}
