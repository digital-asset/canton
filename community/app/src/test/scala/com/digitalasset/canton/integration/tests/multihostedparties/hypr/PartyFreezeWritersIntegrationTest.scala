// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.{
  Entry,
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
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{Namespace, PartyId}
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: other writers of the party's mapping run while a target freezes the party:
  * `propose_delta` and `AllocateParty`, a legacy offline party replication, and the topology steps
  * of a disaster recovery.
  *
  * Test goal: show that these writers keep the hosting states they do not change, and that a legacy
  * replication and a disaster recovery work alongside a freeze.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]].
  */
final class PartyFreezeWritersIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition = setup.environmentDefinition

  "Writers, legacy and disaster recovery" should {
    "M1: keep the hosting states when propose_delta changes permissions during the freeze" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("M1")
        scenario.nominate(Seq(da -> Confirmation, mpch -> Confirmation), PositiveInt.two)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        sp.topology.party_to_participant_mappings
          .propose_delta(scenario.party.partyId, adds = Seq(sp.id -> Confirmation), store = daId)
          .discard
        scenario.expectWritten(
          scenario.entries.updated(sp.id, Entry(Confirmation, HostingState.Active)),
          PositiveInt.two,
          signers = Seq(sp),
        )
        // Ledger API topology events read the entries, not the hosting view, so SP's permission
        // change is reported although the party is frozen
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id)),
          events = Set(Event.ChangedTo(sp.id, Confirmation)),
          Request.Rejected,
        )

        // MPCH alone could move itself from Nominated to Active, skipping the freeze and the
        // import, so listing it without a state must keep it Nominated
        mpch.topology.party_to_participant_mappings
          .propose_delta(scenario.party.partyId, adds = Seq(mpch.id -> Observation), store = daId)
          .discard
        scenario.expectWritten(
          scenario.entries.updated(mpch.id, Entry(Observation, HostingState.Nominated)),
          PositiveInt.two,
          signers = Seq(mpch),
        )
        expectFrozenWithHostSp(scenario)
    }

    // TODO(#36637): Replace `ignore` once AllocateParty preserves the hosting states, and add the
    //  declarative API with a Nominated or Freezing entry, which needs a participant configured
    //  with declared parties (see DeclarativeApiIntegrationTest)
    "M1: reject AllocateParty for a party of which the participant is a nominee" ignore {
      implicit env =>
        import env.*
        // The party's namespace is MPCH's, MPCH is Nominated, and SP hosts the party
        val nomineesParty = PartyId.tryCreate("M1-nominee", mpch.namespace)
        mpch.topology.party_to_participant_mappings
          .propose(
            nomineesParty,
            newParticipants = Seq(sp.id -> Submission, mpch.id -> Confirmation),
            store = daId,
            participantStates = Map(mpch.id -> HostingState.Nominated),
          )
          .discard
        sp.topology.party_to_participant_mappings
          .propose(
            nomineesParty,
            newParticipants = Seq(sp.id -> Submission, mpch.id -> Confirmation),
            store = daId,
            participantStates = Map(mpch.id -> HostingState.Nominated),
          )
          .discard
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          mpch.parties.enable("M1-nominee", synchronizer = Some(daName)).discard,
          _.errorMessage should include(HostingState.Nominated.toString),
        )
    }

    "L1: let a legacy offline party replication onboard a target while another target freezes the party" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        // Its own namespace keeps the legacy import from anchoring MPCH's onboarding clearance at
        // MPCH's onboarding for another party of SP's namespace: the import looks up onboarding
        // entries by namespace, not by party
        val namespaceKey =
          sp.keys.secret.generate_signing_key("L1-namespace", SigningKeyUsage.NamespaceOnly)
        val namespace = Namespace(namespaceKey.fingerprint)
        sp.topology.namespace_delegations
          .propose_delegation(namespace, namespaceKey, CanSignAllMappings, store = daId)
          .discard
        val scenario = newScenario("L1", namespace = Some(namespace))
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        // The export and the clearance read MPCH's hosting state, not the hosting view
        val beforeOnboarding = scenario.onboardLegacy(mpch, Confirmation)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id, mpch.id)),
          events = Set(Event.Onboarding(mpch.id)),
          Request.Rejected,
        )
        // The export includes the party's contract shared with Bob, whom MPCH does not host
        mpch.ledger_api.state.acs
          .of_party(scenario.party)
          .map(_.contractId) should contain(scenario.counterpartysContract.contractId)

        scenario.activate(mpch, beforeOnboarding)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id, mpch.id)),
          events = Set(Event.Added(mpch.id)),
          Request.Rejected,
        )

        scenario.remove(da, by = Signer.Participant)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, mpch.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id, mpch.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "DR1: accept the topology steps of disaster recovery" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        val scenario = newScenario("DR1")
        // DA stands for the broken participant, MPCH for the new one
        scenario.add(da, Confirmation, HostingState.Active)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )

        clue("The party removes the broken participant and nominates the new one") {
          scenario.replace(da, mpch, Confirmation)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id)),
            events = Set(Event.Revoked(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
        }
        clue("The new participant moves N → F and removes itself") {
          scenario.freeze(mpch)
          expectFrozenWithHostSp(scenario)
          scenario.remove(mpch, by = Signer.Participant)
          expectHostedBySp(scenario)
        }
        clue("The party and the new participant co-sign no entry → F") {
          scenario.add(mpch, Confirmation, HostingState.Freezing)
          expectFrozenWithHostSp(scenario)
          scenario.remove(mpch, by = Signer.Party)
          expectHostedBySp(scenario)
        }
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

}
