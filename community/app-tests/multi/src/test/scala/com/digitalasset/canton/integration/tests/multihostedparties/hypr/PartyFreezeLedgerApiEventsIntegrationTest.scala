// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
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
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
}
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: a party's entries go through every hosting-state transition, observed by the
  * moving participant, SP and the other target.
  *
  * Test goal: show the Ledger API topology events of each transition: none while an entry does not
  * host the party (Nominated or Freezing), and Onboarding, Added, Revoked or ChangedTo as the entry
  * starts hosting, becomes Active, leaves or changes its permission.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]].
  */
final class PartyFreezeLedgerApiEventsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition = setup.environmentDefinition

  "H6: Ledger API topology events" should {
    "be none for <> → N, N → F, <> → F, removal from N or F, and another entry entering or leaving F" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H6-none")
        def expectHostedBySp(): Unit = scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id)),
        )

        clue("<> → N, then removal from N by the target") {
          scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
          expectHostedBySp()
          scenario.remove(da, by = Signer.Participant)
          expectHostedBySp()
        }

        clue("N → F, then removal from F by the party") {
          scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
          expectHostedBySp()
          scenario.freeze(da)
          scenario.expectStep(
            Hosting.Frozen(PositiveInt.one, Set(sp.id)),
            events = Set.empty,
            Request.Rejected,
          )
          scenario.remove(da, by = Signer.Party)
          expectHostedBySp()
        }

        clue("<> → F of Beta while DA is Active, then removal from F by Beta") {
          scenario.add(da, Confirmation, HostingState.Active)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Added(da.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          )
          scenario.add(beta, Confirmation, HostingState.Freezing)
          scenario.expectStep(
            Hosting.Frozen(PositiveInt.one, Set(sp.id, da.id)),
            events = Set.empty,
            Request.Rejected,
          )
          scenario.remove(beta, by = Signer.Participant)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id, da.id)),
          )
        }

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "be Onboarding for F → O, N → O and <> → O" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H6-onboarding")
        def expectOnboarded(target: LocalParticipantReference, events: Set[Event]): Unit =
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, target.id)),
            events,
            Request.ApprovedBy(Set(sp.id)),
          )

        clue("F → O") {
          scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id)),
          )
          scenario.freeze(da)
          scenario.expectStep(
            Hosting.Frozen(PositiveInt.one, Set(sp.id)),
            events = Set.empty,
            Request.Rejected,
          )
          scenario.move(da, HostingState.Onboarding)
          expectOnboarded(da, events = Set(Event.Onboarding(da.id)))
        }

        clue("N → O") {
          scenario.nominate(Seq(beta -> Confirmation), PositiveInt.one)
          expectOnboarded(da, events = Set.empty)
          scenario.move(beta, HostingState.Onboarding)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
            events = Set(Event.Onboarding(beta.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
        }

        clue("<> → O") {
          scenario.remove(da, by = Signer.Participant)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, beta.id)),
            events = Set(Event.Revoked(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
          scenario.add(da, Confirmation, HostingState.Onboarding)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
            events = Set(Event.Onboarding(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
        }

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "be Added for O → A, <> → A, N → A and F → A" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H6-added")
        def expectHostedBySpAndDa(events: Set[Event] = Set.empty): Unit = scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events,
          Request.ApprovedBy(Set(sp.id, da.id)),
        )

        clue("O → A") {
          scenario.add(da, Confirmation, HostingState.Onboarding)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Onboarding(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
          scenario.move(da, HostingState.Active)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Added(da.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          )
        }

        clue("<> → A") {
          scenario.add(beta, Confirmation, HostingState.Active)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
            events = Set(Event.Added(beta.id)),
            Request.ApprovedBy(Set(sp.id, da.id, beta.id)),
          )
        }

        clue("N → A") {
          scenario.remove(beta, by = Signer.Party)
          expectHostedBySpAndDa(events = Set(Event.Revoked(beta.id)))
          scenario.nominate(Seq(beta -> Confirmation), PositiveInt.one)
          expectHostedBySpAndDa()
          scenario.move(beta, HostingState.Active)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
            events = Set(Event.Added(beta.id)),
            Request.ApprovedBy(Set(sp.id, da.id, beta.id)),
          )
        }

        clue("F → A") {
          scenario.remove(beta, by = Signer.Participant)
          expectHostedBySpAndDa(events = Set(Event.Revoked(beta.id)))
          scenario.nominate(Seq(beta -> Confirmation), PositiveInt.one)
          expectHostedBySpAndDa()
          scenario.freeze(beta)
          scenario.expectStep(
            Hosting.Frozen(PositiveInt.one, Set(sp.id, da.id)),
            events = Set.empty,
            Request.Rejected,
          )
          scenario.move(beta, HostingState.Active)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
            events = Set(Event.Added(beta.id)),
            Request.ApprovedBy(Set(sp.id, da.id, beta.id)),
          )
        }

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "be Revoked for a removal from O or A, and ChangedTo for a permission change of an O or A entry" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("H6-revoked")
        scenario.add(da, Confirmation, HostingState.Onboarding)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Onboarding(da.id)),
          Request.ApprovedBy(Set(sp.id)),
        )
        scenario.add(beta, Confirmation, HostingState.Active)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
          events = Set(Event.Added(beta.id)),
          Request.ApprovedBy(Set(sp.id, beta.id)),
        )

        clue("Permission change of an O entry, and of an A entry") {
          scenario.downgrade(da, Observation)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
            events = Set(Event.ChangedTo(da.id, Observation)),
            Request.ApprovedBy(Set(sp.id, beta.id)),
          )
          scenario.downgrade(beta, Observation)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, beta.id)),
            events = Set(Event.ChangedTo(beta.id, Observation)),
            Request.ApprovedBy(Set(sp.id)),
          )
        }

        clue("Removal from O, and from A") {
          scenario.remove(da, by = Signer.Participant)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, beta.id)),
            events = Set(Event.Revoked(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
          scenario.remove(beta, by = Signer.Party)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id)),
            events = Set(Event.Revoked(beta.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
        }

        scenario.assertCommitmentsMatchAmongHosts()
    }
  }
}
