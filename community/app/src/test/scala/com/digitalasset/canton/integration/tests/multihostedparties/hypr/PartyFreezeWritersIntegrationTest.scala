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
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.config.{
  DeclarativeParticipantConfig,
  DeclarativePartyConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.TopologyManagerError.{
  HostingStatesNotExpressible,
  UnauthorizedTransaction,
}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, Namespace, PartyId}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

/** Test scenario: other writers of the party's mapping run while a target freezes the party:
  * `propose_delta`, Ledger API party allocation, `parties.enable` and the declarative API, a legacy
  * offline party replication, a wallet that knows only the `v30` mapping, and the topology steps of
  * a disaster recovery.
  *
  * Test goal: show that these writers keep the hosting states they do not change, and that a legacy
  * replication and a disaster recovery work alongside a freeze.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]], plus the declarative API enabled.
  */
final class PartyFreezeWritersIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition =
    setup.environmentDefinition.addConfigTransforms(
      _.focus(_.parameters.enableAlphaStateViaConfig).replace(true)
    )

  "Writers, legacy and disaster recovery" should {
    "M1: keep the hosting states when propose_delta changes permissions during the freeze" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*
        val scenario = newScenario("M1")
        scenario.nominate(Seq(da -> Confirmation, beta -> Confirmation), PositiveInt.two)
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

        // Beta alone could move itself from Nominated to Active, skipping Freezing and
        // Onboarding, so listing it without a state must keep it Nominated
        beta.topology.party_to_participant_mappings
          .propose_delta(scenario.party.partyId, adds = Seq(beta.id -> Observation), store = daId)
          .discard
        scenario.expectWritten(
          scenario.entries.updated(beta.id, Entry(Observation, HostingState.Nominated)),
          PositiveInt.two,
          signers = Seq(beta),
        )
        expectFrozenWithHostSp(scenario)
    }

    "M1: reject the allocation of a party of which the participant is a nominee" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*
        // The party's namespace is Beta's, Beta is Nominated, and SP hosts the party
        val nomineesParty = PartyId.tryCreate("M1-nominee", beta.namespace)
        beta.topology.party_to_participant_mappings
          .propose(
            nomineesParty,
            newParticipants = Seq(sp.id -> Submission, beta.id -> Confirmation),
            store = daId,
            participantStates = Map(beta.id -> HostingState.Nominated),
          )
          .discard
        sp.topology.party_to_participant_mappings
          .propose(
            nomineesParty,
            newParticipants = Seq(sp.id -> Submission, beta.id -> Confirmation),
            store = daId,
            participantStates = Map(beta.id -> HostingState.Nominated),
          )
          .discard
        def mapping() = eventually() {
          beta.topology.party_to_participant_mappings
            .list(daId, filterParty = nomineesParty.filterString)
            .loneElement
            .item
        }
        val nominated = mapping()
        nominated.participants.find(_.participantId == beta.id).map(_.state) shouldBe Some(
          HostingState.Nominated
        )

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          beta.ledger_api.parties.allocate("M1-nominee", synchronizerId = Some(daId)).discard,
          _.errorMessage should include(HostingState.Nominated.toString),
        )
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          beta.parties.enable("M1-nominee", synchronizer = Some(daName)).discard,
          _.errorMessage should include(HostingState.Nominated.toString),
        )
        def updateDeclarativeConfig(
            update: DeclarativeParticipantConfig => DeclarativeParticipantConfig
        ): Unit =
          environment.pokeOrUpdateConfig(
            Some(
              Right(
                ConfigTransforms.updateParticipantConfig(beta.name)(
                  _.focus(_.alphaDynamic).modify(update)
                )(environment.config)
              )
            )
          )
        // Declares the party with Submission, which differs from Beta's Confirmation entry
        updateDeclarativeConfig(
          _.focus(_.parties)
            .replace(Seq(DeclarativePartyConfig("M1-nominee", synchronizers = Seq(daName.unwrap))))
        )
        mapping() shouldBe nominated
        // Removes the parties that are not declared
        updateDeclarativeConfig(
          _.focus(_.parties).replace(Seq.empty).focus(_.removeParties).replace(true)
        )
        mapping() shouldBe nominated
        updateDeclarativeConfig(_.focus(_.removeParties).replace(false))
    }

    "L1: let a legacy offline party replication onboard a target while another target freezes the party" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*
        // Its own namespace keeps the legacy import from anchoring Beta's onboarding clearance at
        // Beta's onboarding for another party of SP's namespace: the import looks up onboarding
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

        // The export and the clearance read Beta's hosting state, not the hosting view
        val beforeOnboarding = scenario.onboardLegacy(beta, Confirmation)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id, beta.id)),
          events = Set(Event.Onboarding(beta.id)),
          Request.Rejected,
        )
        // The export includes the party's contract shared with Bob, whom Beta does not host
        beta.ledger_api.state.acs
          .of_party(scenario.party)
          .map(_.contractId) should contain(scenario.counterpartysContract.contractId)

        scenario.activate(beta, beforeOnboarding)
        scenario.expectStep(
          Hosting.Frozen(PositiveInt.one, Set(sp.id, beta.id)),
          events = Set(Event.Added(beta.id)),
          Request.Rejected,
        )

        scenario.remove(da, by = Signer.Participant)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, beta.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id, beta.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "L2: reject a v30 wallet's write while an entry is Freezing unless forced; forced, keep the Freezing entry when relisted and unfreeze when left out" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newExternalScenario("L2")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        // A v30 transaction cannot express the Freezing state, so the participant that receives the
        // wallet's write rejects it, whether the write relists the Freezing entry or leaves it out
        Seq(
          "Relisted" -> scenario.entries.updated(da.id, Entry(Confirmation, HostingState.Active)),
          "Left out" -> (scenario.entries - da.id),
        ).foreach { case (write, entries) =>
          clue(write) {
            loggerFactory.assertThrowsAndLogs[CommandFailure](
              scenario.writeAsV30WalletRejected(entries, ForceFlags.none),
              _.shouldBeCommandFailure(HostingStatesNotExpressible, s"${da.id} -> Freezing"),
            )
          }
        }
        expectFrozenWithHostSp(scenario)

        // Forced, a v30 entry is Onboarding with the onboarding flag and Active without it, so
        // relisting the Freezing entry moves it forward, which needs DA's signature. Only the
        // operator's log names DA's namespace as missing; the wallet gets a redacted
        // security-sensitive error.
        val forced = ForceFlags(ForceFlag.AllowV30PartyToParticipantAfterHostingStates)
        Seq(HostingState.Onboarding, HostingState.Active).foreach { relisted =>
          clue(s"Relisted as $relisted") {
            loggerFactory.assertThrowsAndLogs[CommandFailure](
              scenario.writeAsV30WalletRejected(
                scenario.entries.updated(da.id, Entry(Confirmation, relisted)),
                forced,
              ),
              _.shouldBeCantonError(
                UnauthorizedTransaction,
                _ should include(s"missing authorizations by namespaces=Set(${da.namespace})"),
              ),
              _.errorMessage should include("A security-sensitive error has been received"),
            )
          }
        }
        expectFrozenWithHostSp(scenario)

        // The party alone may remove an entry
        scenario.writeAsV30Wallet(scenario.entries - da.id, forced).discard
        expectHostedBySp(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "DR1: accept the topology steps of disaster recovery" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("DR1")
        // DA stands for the broken participant, Beta for the new one
        scenario.add(da, Confirmation, HostingState.Active)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )

        clue("The party removes the broken participant and nominates the new one") {
          scenario.replace(da, beta, Confirmation)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id)),
            events = Set(Event.Revoked(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
        }
        clue("The new participant moves N → F and removes itself") {
          scenario.freeze(beta)
          expectFrozenWithHostSp(scenario)
          scenario.remove(beta, by = Signer.Participant)
          expectHostedBySp(scenario)
        }
        clue("The party and the new participant co-sign no entry → F") {
          scenario.add(beta, Confirmation, HostingState.Freezing)
          expectFrozenWithHostSp(scenario)
          scenario.remove(beta, by = Signer.Party)
          expectHostedBySp(scenario)
        }
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

}
