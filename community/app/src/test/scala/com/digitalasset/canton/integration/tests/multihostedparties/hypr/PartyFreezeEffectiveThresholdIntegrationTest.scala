// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.{
  DecentralizedPartyCannotSubmit,
  UnknownSubmitters,
}
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
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags}
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: a party's mapping goes through the states of replications in which targets are
  * pending (Nominated, Freezing or Onboarding) while others leave or become Active, with different
  * thresholds. T1 to T3 reach the examples below as live topology states. T1 covers all examples,
  * T2 the unconfirmable ones (4 and 7), and T3 the drop of the effective threshold (9 and 3). An
  * entry is Active unless marked "→ N" (Nominated) or "→ O" (Onboarding), and every entry has
  * Confirmation permission. T is the mapping's threshold, eT the effective confirmation threshold:
  * {{{
  * eT = max(1, min(T, max(numConfirming, T) - numConfirmingPending))
  * }}}
  * with numConfirming the entries with Confirmation or Submission permission and
  * numConfirmingPending those of them that are not Active (see
  * `PartyInfo.effectiveConfirmationThreshold`).
  * {{{
  * 1. [SP, DA → N, MPCH → N], T = 2, eT = 1: two replications running, SP confirms alone
  * 2. [SP, DA, MPCH → N],     T = 2, eT = 2: DA finished, SP and DA must confirm
  * 3. [SP, MPCH → N],         T = 2, eT = 1: DA aborted and was removed
  * 4. [SP],                   T = 2, eT = 2: both removed, T not lowered: unconfirmable until
  *                                           the party lowers T
  * 5. [SP, DA, MPCH],         T = 2, eT = 2: both finished, any two hosts confirm
  * 6. [SP, DA, MPCH → O],     T = 3, eT = 2: the Onboarding entry does not block
  * 7. [SP, MPCH → O],         T = 3, eT = 2: DA left while MPCH was pending: unconfirmable
  *                                           until the party lowers T
  * 8. [DA → N, MPCH → N],     T = 2:         no Active host, requests are rejected
  * 9. From 2 to 3: DA leaves while MPCH is pending, so eT drops from 2 to 1 although the party
  *    did not lower T (accepted: it is the quorum the party gets when it recovers anyway)
  * }}}
  * T4 and T5 cover the liveness of the party while it raises its threshold, and who may submit for
  * it.
  *
  * Test goal: show that in each state the party's requests need exactly the confirmations given by
  * the effective confirmation threshold, which counts only Active entries as confirmers, and that a
  * Submission host may submit alone only while that threshold is 1.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]].
  */
final class PartyFreezeEffectiveThresholdIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition = setup.environmentDefinition

  "Effective confirmation threshold" should {
    "T1, T2, T3: reach the effective confirmation threshold of each example in live topology states" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        val unconfirmable = ForceFlags(ForceFlag.AllowConfirmingThresholdCanBeMet)

        clue("Examples 1, 2, 9, 3 and 4") {
          val scenario = newScenario("T1-a")

          // 1. [SP, DA → N, MPCH → N], T = 2: SP confirms alone
          scenario.nominate(Seq(da -> Confirmation, mpch -> Confirmation), PositiveInt.two)
          expectHostedBySp(scenario)

          // 2. [SP, DA, MPCH → N], T = 2: both SP and DA must confirm
          scenario.move(da, HostingState.Active)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id)),
            events = Set(Event.Added(da.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          )
          scenario.expectQuorum(respondingHosts = Set(sp.id), approved = false)

          // 9 and 3 (T3). [SP, MPCH → N], T = 2: DA left while MPCH is pending, so the quorum
          // drops from 2 to 1, although only SP can confirm (known issue, see CV5 in
          // CovalidationReplicationIntegrationTest)
          scenario.remove(da, by = Signer.Participant)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id)),
            events = Set(Event.Revoked(da.id)),
            Request.ApprovedBy(Set(sp.id)),
          )

          // 4 (T2). [SP], T = 2: unconfirmable until the party lowers T
          scenario.remove(mpch, by = Signer.Party, forceFlags = unconfirmable)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id)),
            events = Set.empty,
            Request.TimesOut,
          )
          scenario.setThreshold(PositiveInt.one)
          expectHostedBySp(scenario)
          scenario.assertCommitmentsMatchAmongHosts()
        }

        clue("Examples 5, 6 and 7") {
          val scenario = newScenario("T1-b")
          scenario.add(da, Confirmation, HostingState.Active)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
            events = Set(Event.Added(da.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          )
          scenario.add(mpch, Confirmation, HostingState.Active)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id, mpch.id)),
            events = Set(Event.Added(mpch.id)),
            Request.ApprovedBy(Set(sp.id, da.id, mpch.id)),
          )

          // 5. [SP, DA, MPCH], T = 2: any two hosts
          scenario.setThreshold(PositiveInt.two)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, mpch.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id, da.id, mpch.id)),
          )
          scenario.expectQuorum(respondingHosts = Set(sp.id, mpch.id), approved = true)
          scenario.expectQuorum(respondingHosts = Set(sp.id), approved = false)

          // 6. [SP, DA, MPCH → O], T = 3: the Onboarding entry does not block.
          // MPCH cannot move back from Active to Onboarding, so it leaves and is re-added as
          // Onboarding.
          scenario.remove(mpch, by = Signer.Participant)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id)),
            events = Set(Event.Revoked(mpch.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          )
          scenario.add(mpch, Confirmation, HostingState.Onboarding)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, mpch.id)),
            events = Set(Event.Onboarding(mpch.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          )
          scenario.setThreshold(PositiveInt.three)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, mpch.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id, da.id)),
          )

          // 7 (T2). [SP, MPCH → O], T = 3: DA left while MPCH is pending, so the party is
          // unconfirmable until it lowers T. Onboarding counts as pending like Nominated.
          scenario.remove(da, by = Signer.Participant, forceFlags = unconfirmable)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, mpch.id)),
            events = Set(Event.Revoked(da.id)),
            Request.TimesOut,
          )
          scenario.setThreshold(PositiveInt.two)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.one, Set(sp.id, mpch.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id)),
          )
          scenario.assertCommitmentsMatchAmongHosts()
        }

        clue("Example 8") {
          val scenario = newScenario("T1-c")
          scenario.nominate(Seq(da -> Confirmation, mpch -> Confirmation), PositiveInt.two)
          expectHostedBySp(scenario)

          // 8. [DA → N, MPCH → N], T = 2: no host, requests are rejected
          scenario.remove(sp, by = Signer.Party)
          scenario.expectStep(
            Hosting.NotHosted,
            events = Set(Event.Revoked(sp.id)),
            Request.Rejected,
          )
        }
    }

    "T4: dip in liveness when the nomination and the higher threshold go together, but not with the Observation-first procedure" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        // CP, which also hosts Bob, is the fourth host. Its responses carry Bob's confirmation, so
        // the quorum checks never drop them.
        def twoOfThree(testCase: String): PartyFreezeScenario = {
          val scenario = newScenario(testCase)
          Seq(da, mpch).foreach { host =>
            scenario.add(host, Confirmation, HostingState.Active)
            scenario.expectStep(
              Hosting.Hosted(PositiveInt.one, scenario.entries.keySet),
              events = Set(Event.Added(host.id)),
              Request.ApprovedBy(scenario.entries.keySet),
            )
          }
          scenario.setThreshold(PositiveInt.two)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id, mpch.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(sp.id, da.id, mpch.id)),
          )
          scenario
        }
        val existingHosts = Set(sp.id, da.id, mpch.id)
        val allHosts = existingHosts + cp.id

        clue("The nomination and T = 3 in one transaction") {
          val scenario = twoOfThree("T4-together")
          scenario.nominate(Seq(cp -> Confirmation), PositiveInt.three)
          // All three Active hosts must confirm until CP is Active
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.three, existingHosts),
            events = Set.empty,
            Request.ApprovedBy(existingHosts),
          )
          scenario.expectQuorum(respondingHosts = Set(sp.id, da.id), approved = false)
          scenario.replicate(cp)(
            afterFreeze = scenario.expectStep(
              Hosting.Frozen(PositiveInt.three, existingHosts),
              events = Set.empty,
              Request.Rejected,
            ),
            afterOnboarding = scenario.expectStep(
              Hosting.Hosted(PositiveInt.three, allHosts),
              events = Set(Event.Onboarding(cp.id)),
              Request.ApprovedBy(existingHosts),
            ),
            afterActivation = scenario.expectStep(
              Hosting.Hosted(PositiveInt.three, allHosts),
              events = Set(Event.Added(cp.id)),
              Request.ApprovedBy(allHosts),
            ),
          )
          scenario.expectQuorum(respondingHosts = Set(sp.id, da.id, cp.id), approved = true)
          scenario.assertCommitmentsMatchAmongHosts()
        }

        clue("Observation first, then the permission and T = 3 in one transaction") {
          val scenario = twoOfThree("T4-observation-first")
          scenario.nominate(Seq(cp -> Observation), PositiveInt.two)
          // An Observation entry does not count, so any two of the Active hosts suffice
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, existingHosts),
            events = Set.empty,
            Request.ApprovedBy(existingHosts),
          )
          scenario.expectQuorum(respondingHosts = Set(sp.id, da.id), approved = true)
          scenario.replicate(cp)(
            afterFreeze = scenario.expectStep(
              Hosting.Frozen(PositiveInt.two, existingHosts),
              events = Set.empty,
              Request.Rejected,
            ),
            afterOnboarding = scenario.expectStep(
              Hosting.Hosted(PositiveInt.two, allHosts),
              events = Set(Event.Onboarding(cp.id)),
              Request.ApprovedBy(existingHosts),
            ),
            afterActivation = scenario.expectStep(
              Hosting.Hosted(PositiveInt.two, allHosts),
              events = Set(Event.Added(cp.id)),
              Request.ApprovedBy(existingHosts),
            ),
          )
          // CP observes only, so 2-of-3 is kept throughout
          scenario.expectQuorum(respondingHosts = Set(sp.id, da.id), approved = true)

          scenario.upgrade(cp, Confirmation, PositiveInt.three)
          scenario.expectStep(
            Hosting.Hosted(PositiveInt.three, allHosts),
            events = Set(Event.ChangedTo(cp.id, Confirmation)),
            Request.ApprovedBy(allHosts),
          )
          scenario.expectQuorum(respondingHosts = Set(sp.id, da.id, cp.id), approved = true)
          scenario.assertCommitmentsMatchAmongHosts()
        }
    }

    "T5: let a Submission host submit alone only while the effective threshold is 1 and the party is not frozen" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("T5")
        val party = scenario.party
        def submitAlone(): Unit = createContract(sp, party, daId, Seq(party)).discard

        submitAlone()
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.two)
        expectHostedBySp(scenario)
        submitAlone()

        val frozenAt = scenario.freeze(da)
        expectFrozenWithHostSp(scenario)
        expectRejected(UnknownSubmitters)(submitAlone())

        scenario.importAcs(da, scenario.readAcs(frozenAt))
        val beforeOnboarding = scenario.onboard(da)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Onboarding(da.id)),
          Request.ApprovedBy(Set(sp.id)),
        )
        submitAlone()

        scenario.activate(da, beforeOnboarding)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        expectRejected(DecentralizedPartyCannotSubmit)(submitAlone())

        scenario.setThreshold(PositiveInt.one)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        submitAlone()
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

}
