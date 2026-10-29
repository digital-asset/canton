// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
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
import com.digitalasset.canton.topology.TopologyManagerError.SerialMismatch
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags}
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: participants try to freeze a party without being nominated or a second time, and
  * replications are aborted by removing the target from each hosting state, also concurrently with
  * the target's own move.
  *
  * Test goal: show that only a nominated participant can freeze the party, once; that a removal
  * unfreezes the party unless another target is Freezing, revokes it on a hosting target, and emits
  * the expected Ledger API topology events; and that a new replication needs a new nomination.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]].
  */
final class PartyFreezeSignatureAndAbortIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition = setup.environmentDefinition

  // PartyToParticipantRequiredAuthTest and RequiredTopologyMappingChecksTest cover the signature
  // matrix. These cases check the consequences of a rejection end to end.
  "Signatures" should {
    "S1: keep the party hosted when a participant without a Nominated entry freezes it alone" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("S1")

        clue("No entry → F, signed by DA alone, lacks the party's signature") {
          scenario.proposeWithoutAuthorization(
            signer = da,
            participant = da,
            Entry(Confirmation, HostingState.Freezing),
          )
          expectHostedBySp(scenario)
        }

        clue("O → F is a backward move") {
          scenario.add(da, Confirmation, HostingState.Onboarding)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Onboarding(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
          expectBackwardMoveRejected(scenario, da, HostingState.Freezing)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id)),
          )
        }

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "S2: reject a second freeze by a participant past Freezing" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("S2")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        val frozenAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        scenario.importAcs(da, scenario.readAcs(frozenAt))
        val beforeOnboarding = scenario.onboard(da)
        def expectHostedBySpAndDa(events: Set[Event]): Unit = scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events,
          Request.ApprovedBy(Set(sp.id)),
        )
        expectHostedBySpAndDa(events = Set(Event.Onboarding(da.id)))

        expectBackwardMoveRejected(scenario, da, HostingState.Freezing)
        expectHostedBySpAndDa(events = Set.empty)

        scenario.activate(da, beforeOnboarding)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

  "Abort and rollback" should {
    "X1: emit no events for a removal from N, by the party or by the target, and recompute the effective threshold" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("X1")
        scenario.nominate(Seq(da -> Confirmation, beta -> Confirmation), PositiveInt.two)
        expectHostedBySp(scenario)

        scenario.remove(da, by = Signer.Party)
        expectHostedBySp(scenario)

        // SP is the only entry left, but the threshold is still 2: unconfirmable
        scenario.remove(
          beta,
          by = Signer.Participant,
          forceFlags = ForceFlags(ForceFlag.AllowConfirmingThresholdCanBeMet),
        )
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id)),
          events = Set.empty,
          Request.TimesOut,
        )

        scenario.setThreshold(PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "X2: unfreeze on a removal from F before the import only if no other entry is Freezing" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("X2")
        scenario.nominate(Seq(da -> Confirmation, beta -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        scenario.freeze(beta)
        expectFrozenWithHostSp(scenario)

        scenario.remove(da, by = Signer.Party)
        expectFrozenWithHostSp(scenario)
        scenario.remove(beta, by = Signer.Participant)
        expectHostedBySp(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "X3: unfreeze on a removal from F after the import, and leave no contracts of the party on the target after the purge" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("X3")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        val frozenAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        scenario.importAcs(da, scenario.readAcs(frozenAt))

        scenario.remove(da, by = Signer.Participant)
        expectHostedBySp(scenario)
        scenario.purge(da)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "X4: revoke the party on a participant removed from O or from A, and let the party continue" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("X4")
        scenario.nominate(Seq(da -> Confirmation, beta -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)

        clue("Removal from O") {
          val frozenAt = scenario.freeze(da)
          expectFrozenWithHostSp(scenario)
          scenario.importAcs(da, scenario.readAcs(frozenAt))
          scenario.onboard(da).discard
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Onboarding(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
          scenario.remove(da, by = Signer.Participant)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id)),
            events = Set(Event.Revoked(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
          scenario.purge(da)
        }

        clue("Removal from A") {
          scenario.replicate(beta)(
            afterFreeze = expectFrozenWithHostSp(scenario),
            afterOnboarding = scenario.expectStep(
              Hosting.Hosted(PositiveInt.one, Set(sp.id, beta.id)),
              events = Set(Event.Onboarding(beta.id)),
              Request.ApprovedBy(Set(sp.id)),
            ),
            afterActivation = scenario.expectStep(
              Hosting.Hosted(PositiveInt.one, Set(sp.id, beta.id)),
              events = Set(Event.Added(beta.id)),
              Request.ApprovedBy(Set(sp.id, beta.id)),
            ),
          )
          scenario.remove(beta, by = Signer.Party)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id)),
            events = Set(Event.Revoked(beta.id)),
            Request.ApprovedBy(Set(sp.id)),
          )
          scenario.purge(beta)
        }

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "X5: when the party removes the target while the target moves N → F, reject the second write with a serial mismatch, and let the party resubmit its removal" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("X5")

        clue("The party's removal goes first") {
          scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
          expectHostedBySp(scenario)
          val serialProposedByDa = scenario.nextSerial
          val frozenByDa =
            scenario.entries.updated(da.id, Entry(Confirmation, HostingState.Freezing))
          scenario.remove(da, by = Signer.Party)
          expectHostedBySp(scenario)
          // DA cannot resubmit: without its Nominated entry, freezing needs the party's signature
          expectRejected(SerialMismatch)(
            scenario.proposeRejected(da, frozenByDa, serialProposedByDa)
          )
          expectHostedBySp(scenario)
        }

        clue("DA's N → F goes first") {
          scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
          expectHostedBySp(scenario)
          val serialProposedByParty = scenario.nextSerial
          scenario.freeze(da)
          expectFrozenWithHostSp(scenario)
          expectRejected(SerialMismatch)(
            scenario.remove(da, by = Signer.Party, serial = Some(serialProposedByParty)).discard
          )
          expectFrozenWithHostSp(scenario)
          scenario.remove(da, by = Signer.Party)
          expectHostedBySp(scenario)
        }

        scenario.assertCommitmentsMatchAmongHosts()
    }

    "X6: unfreeze when the party alone removes a stalled target" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("X6")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        // Signed by the party alone (checked by the scenario)
        scenario.remove(da, by = Signer.Party)
        expectHostedBySp(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "X7: require a new nomination to replicate again after an abort" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("X7")
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        scenario.remove(da, by = Signer.Participant)
        expectHostedBySp(scenario)

        scenario.proposeWithoutAuthorization(
          signer = da,
          participant = da,
          Entry(Confirmation, HostingState.Freezing),
        )
        expectHostedBySp(scenario)

        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.replicate(da)(
          afterFreeze = expectFrozenWithHostSp(scenario),
          afterOnboarding = scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Onboarding(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          ),
          afterActivation = scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Added(da.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          ),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

}
