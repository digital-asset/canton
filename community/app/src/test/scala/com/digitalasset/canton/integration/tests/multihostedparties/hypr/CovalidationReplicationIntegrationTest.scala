// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.ConfigurationErrors.InvalidPrescribedSynchronizerId
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyFreezeScenario.{
  Event,
  Hosting,
  PartySigner,
  Request,
  Signer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
}
import com.digitalasset.canton.topology.{ExternalParty, ForceFlag, ForceFlags, ParticipantId, Party}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty

/** Test scenario: the flows of the Covalidation Service. An external party hosted 1-of-1 on the
  * primary, with contracts, moves to 2-of-3 with the covalidators DA and MPCH, using HyPR (Hybrid
  * Party Replication): in one transaction, the party nominates both and raises its threshold to 2;
  * then each covalidator freezes the party, imports its ACS, and becomes Onboarding and Active. The
  * cases include parallel replication, partial and full failures, the departure of the primary, and
  * the replacement of a covalidator.
  *
  * Test goal: show for each case, after every step and on every participant, the hosting states,
  * the hosting views with the effective confirmation threshold, the exact Ledger API topology
  * events, and whether a request that needs the party's confirmation is approved and by which
  * hosts. While the party is not frozen, no single covalidator meets the quorum alone, except in
  * CV5, a known issue. The ACS commitments among the party's hosts match at the end of every case.
  *
  * Test setup:
  *   - 5 participants, 1 synchronizer, with a simulated clock: the primary (participant1), DA
  *     (participant2), MPCH (participant3), the replacement X (participant4), and CP
  *     (participant5), which hosts Bob.
  *   - Each case uses its own external party, hosted on the primary only with Confirmation
  *     permission and threshold 1. Each subclass creates another kind of external party.
  *   - All hosts have Confirmation permission, except in the Observation-first procedure.
  *   - The remote ACS read and the import of its snapshot are simulated with the repair service.
  */
abstract class CovalidationReplicationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  // Short timeouts keep the decision deadline, and thus the wait before activation, short
  private val confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(10)
  private val mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofSeconds(10)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P5_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.synchronizers.connect_local(sequencer1, alias = daName)
        participants.local.dars.upload(CantonTestsPath)
        sequencer1.topology.synchronizer_parameters.propose_update(
          daId,
          _.update(
            confirmationResponseTimeout = confirmationResponseTimeout,
            mediatorReactionTimeout = mediatorReactionTimeout,
          ),
        )

        primary = participant1
        da = participant2
        mpch = participant3
        x = participant4
        cp = participant5
        bob = cp.parties.enable("Bob")
      }

  private var primary: LocalParticipantReference = _
  private var da: LocalParticipantReference = _
  private var mpch: LocalParticipantReference = _
  private var x: LocalParticipantReference = _
  private var cp: LocalParticipantReference = _
  private var bob: Party = _

  /** Creates an external party hosted on `primary` only with Confirmation permission and threshold
    * 1, and the signer of its topology transactions.
    */
  protected def createParty(name: String, primary: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): (ExternalParty, PartySigner.External)

  /** Allocates an external party on the primary and starts a scenario for it. */
  private def newScenario(testCase: String)(implicit
      env: TestConsoleEnvironment
  ): PartyFreezeScenario = {
    import env.*
    val (party, partySigner) = createParty(s"Party-$testCase", primary)
    PartyFreezeScenario(
      party,
      source = primary,
      partySigner = partySigner,
      counterparty = bob,
      counterpartyParticipant = cp,
      participants = Seq(primary, da, mpch, x, cp),
      synchronizerId = daId,
      sequencer = getProgrammableSequencer(sequencer1.name),
      loggerFactory = loggerFactory,
    )
  }

  /** Like [[PartyFreezeScenario.expectStep]], and, if the party is not frozen, checks that no
    * single covalidator meets the quorum alone.
    */
  private def expectStep(
      scenario: PartyFreezeScenario,
      hosting: Hosting,
      events: Set[Event],
      request: Request,
  ): Unit = {
    scenario.expectStep(hosting, events, request)
    hosting match {
      case Hosting.Hosted(_, hosts) =>
        (hosts - primary.id).foreach(covalidator =>
          clue(s"$covalidator alone does not meet the quorum")(
            scenario.expectQuorum(respondingHosts = Set(covalidator), approved = false)
          )
        )
      case _: Hosting.Frozen | Hosting.NotHosted => ()
    }
  }

  private def expectHostedByPrimary(scenario: PartyFreezeScenario): Unit =
    expectStep(
      scenario,
      Hosting.Hosted(PositiveInt.one, Set(primary.id)),
      events = Set.empty,
      Request.ApprovedBy(Set(primary.id)),
    )

  /** Replicates the party to `covalidator`, which is Nominated with Confirmation permission.
    *
    * @param hostsBefore
    *   The party's hosts before the replication.
    * @param thresholdBefore
    *   The effective confirmation threshold before the replication, which also applies while the
    *   covalidator is Freezing or Onboarding.
    * @param thresholdAfter
    *   The effective confirmation threshold once the covalidator is Active.
    */
  private def replicate(
      scenario: PartyFreezeScenario,
      covalidator: LocalParticipantReference,
      hostsBefore: Set[ParticipantId],
      thresholdBefore: PositiveInt,
      thresholdAfter: PositiveInt,
  ): Unit = {
    val hostsAfter = hostsBefore + covalidator.id
    scenario.replicate(covalidator)(
      afterFreeze = {
        expectStep(
          scenario,
          Hosting.Frozen(thresholdBefore, hostsBefore),
          events = Set.empty,
          Request.Rejected,
        )
        // Reads on the primary keep working
        primary.ledger_api.state.acs
          .of_party(scenario.party)
          .map(_.contractId) should contain(scenario.counterpartysContract.contractId)
      },
      afterOnboarding = expectStep(
        scenario,
        Hosting.Hosted(thresholdBefore, hostsAfter),
        events = Set(Event.Onboarding(covalidator.id)),
        Request.ApprovedBy(hostsBefore),
      ),
      afterActivation = expectStep(
        scenario,
        Hosting.Hosted(thresholdAfter, hostsAfter),
        events = Set(Event.Added(covalidator.id)),
        Request.ApprovedBy(hostsAfter),
      ),
    )
  }

  /** Step 1: the party alone nominates DA and MPCH and raises its threshold to 2. */
  private def nominateCovalidators(scenario: PartyFreezeScenario): Unit = {
    scenario.nominate(Seq(da -> Confirmation, mpch -> Confirmation), PositiveInt.two)
    expectHostedByPrimary(scenario)
  }

  "The Covalidation Service" should {
    "CV1, CV7: move a party with contracts from 1-of-1 to 2-of-3, one covalidator after the other, each signing only its own moves" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("CV1")
        // The party signs Step 1 alone, each covalidator its own moves alone (checked by the
        // scenario)
        nominateCovalidators(scenario)
        replicate(
          scenario,
          da,
          Set(primary.id),
          thresholdBefore = PositiveInt.one,
          thresholdAfter = PositiveInt.two,
        )
        replicate(
          scenario,
          mpch,
          Set(primary.id, da.id),
          thresholdBefore = PositiveInt.two,
          thresholdAfter = PositiveInt.two,
        )
        scenario.expectQuorum(respondingHosts = Set(primary.id, mpch.id), approved = true)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "CV2: freeze the party once while both covalidators replicate in parallel" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("CV2")
        nominateCovalidators(scenario)

        def expectFrozenWithHostPrimary(): Unit = expectStep(
          scenario,
          Hosting.Frozen(PositiveInt.one, Set(primary.id)),
          events = Set.empty,
          Request.Rejected,
        )
        val frozenByDaAt = scenario.freeze(da)
        expectFrozenWithHostPrimary()
        val frozenByMpchAt = scenario.freeze(mpch)
        expectFrozenWithHostPrimary()
        scenario.importAcs(da, scenario.readAcs(frozenByDaAt))
        scenario.importAcs(mpch, scenario.readAcs(frozenByMpchAt))

        val beforeDaOnboarding = scenario.onboard(da)
        expectStep(
          scenario,
          Hosting.Frozen(PositiveInt.one, Set(primary.id, da.id)),
          events = Set(Event.Onboarding(da.id)),
          Request.Rejected,
        )
        val beforeMpchOnboarding = scenario.onboard(mpch)
        expectStep(
          scenario,
          Hosting.Hosted(PositiveInt.one, Set(primary.id, da.id, mpch.id)),
          events = Set(Event.Onboarding(mpch.id)),
          Request.ApprovedBy(Set(primary.id)),
        )
        scenario.activate(da, beforeDaOnboarding)
        expectStep(
          scenario,
          Hosting.Hosted(PositiveInt.two, Set(primary.id, da.id, mpch.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(primary.id, da.id)),
        )
        scenario.activate(mpch, beforeMpchOnboarding)
        expectStep(
          scenario,
          Hosting.Hosted(PositiveInt.two, Set(primary.id, da.id, mpch.id)),
          events = Set(Event.Added(mpch.id)),
          Request.ApprovedBy(Set(primary.id, da.id, mpch.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "CV3: need both remaining hosts when one covalidator fails, until the party nominates a replacement" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("CV3")
        nominateCovalidators(scenario)
        replicate(
          scenario,
          da,
          Set(primary.id),
          thresholdBefore = PositiveInt.one,
          thresholdAfter = PositiveInt.two,
        )

        clue("MPCH fails after its import and removes itself") {
          val frozenAt = scenario.freeze(mpch)
          expectStep(
            scenario,
            Hosting.Frozen(PositiveInt.two, Set(primary.id, da.id)),
            events = Set.empty,
            Request.Rejected,
          )
          scenario.importAcs(mpch, scenario.readAcs(frozenAt))
          scenario.remove(mpch, by = Signer.Participant)
          // 2-of-2: both the primary and DA must confirm
          expectStep(
            scenario,
            Hosting.Hosted(PositiveInt.two, Set(primary.id, da.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(primary.id, da.id)),
          )
          scenario.expectQuorum(respondingHosts = Set(primary.id), approved = false)
          scenario.purge(mpch)
        }

        clue("The party nominates X, which replicates") {
          scenario.nominate(Seq(x -> Confirmation), PositiveInt.two)
          expectStep(
            scenario,
            Hosting.Hosted(PositiveInt.two, Set(primary.id, da.id)),
            events = Set.empty,
            Request.ApprovedBy(Set(primary.id, da.id)),
          )
          replicate(
            scenario,
            x,
            Set(primary.id, da.id),
            thresholdBefore = PositiveInt.two,
            thresholdAfter = PositiveInt.two,
          )
        }
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "CV4: leave the party unconfirmable when both covalidators are removed, until the party lowers the threshold" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("CV4")
        nominateCovalidators(scenario)
        replicate(
          scenario,
          da,
          Set(primary.id),
          thresholdBefore = PositiveInt.one,
          thresholdAfter = PositiveInt.two,
        )

        scenario.remove(mpch, by = Signer.Party)
        expectStep(
          scenario,
          Hosting.Hosted(PositiveInt.two, Set(primary.id, da.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(primary.id, da.id)),
        )
        scenario.remove(
          da,
          by = Signer.Party,
          forceFlags = ForceFlags(ForceFlag.AllowConfirmingThresholdCanBeMet),
        )
        expectStep(
          scenario,
          Hosting.Hosted(PositiveInt.two, Set(primary.id)),
          events = Set(Event.Revoked(da.id)),
          Request.TimesOut,
        )
        scenario.purge(da)

        scenario.setThreshold(PositiveInt.one)
        expectHostedByPrimary(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "CV5: let DA alone meet the quorum when the primary leaves while MPCH is pending (known issue)" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("CV5")
        nominateCovalidators(scenario)
        replicate(
          scenario,
          da,
          Set(primary.id),
          thresholdBefore = PositiveInt.one,
          thresholdAfter = PositiveInt.two,
        )

        // [DA, MPCH → N], T = 2: the effective threshold drops to 1. This contradicts the
        // Covalidation Service's promise that DA never meets the threshold alone, and is exempt
        // from the per-step check.
        scenario.remove(primary, by = Signer.Participant)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(da.id)),
          events = Set(Event.Revoked(primary.id)),
          Request.ApprovedBy(Set(da.id)),
        )
        scenario.expectQuorum(respondingHosts = Set(da.id), approved = true)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "CV5b: leave no confirmer, so DA alone cannot meet the quorum, when the primary leaves after the Observation-first procedure" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("CV5b")
        scenario.nominate(Seq(da -> Observation, mpch -> Observation), PositiveInt.one)
        expectHostedByPrimary(scenario)
        scenario.replicate(da)(
          afterFreeze = expectStep(
            scenario,
            Hosting.Frozen(PositiveInt.one, Set(primary.id)),
            events = Set.empty,
            Request.Rejected,
          ),
          afterOnboarding = expectStep(
            scenario,
            Hosting.Hosted(PositiveInt.one, Set(primary.id, da.id)),
            events = Set(Event.Onboarding(da.id)),
            Request.ApprovedBy(Set(primary.id)),
          ),
          afterActivation = expectStep(
            scenario,
            Hosting.Hosted(PositiveInt.one, Set(primary.id, da.id)),
            events = Set(Event.Added(da.id)),
            Request.ApprovedBy(Set(primary.id)),
          ),
        )

        // [DA (Observation), MPCH (Observation) → N], T = 1: DA hosts the party but cannot confirm.
        // The primary's topology manager refuses to leave a signatory without a confirming host.
        scenario.remove(
          primary,
          by = Signer.Participant,
          forceFlags = ForceFlags(ForceFlag.AllowInsufficientParticipantPermissionForSignatoryParty),
        )
        // Routing finds no participant that can confirm for the party
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(da.id)),
          events = Set(Event.Revoked(primary.id)),
          Request.RejectedWith(InvalidPrescribedSynchronizerId),
        )
    }

    "CV6: freeze the party while X replaces a lost covalidator, and need both remaining hosts until X is Active" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("CV6")
        nominateCovalidators(scenario)
        replicate(
          scenario,
          da,
          Set(primary.id),
          thresholdBefore = PositiveInt.one,
          thresholdAfter = PositiveInt.two,
        )
        replicate(
          scenario,
          mpch,
          Set(primary.id, da.id),
          thresholdBefore = PositiveInt.two,
          thresholdAfter = PositiveInt.two,
        )

        // MPCH is lost: the party removes it and nominates X in one transaction
        scenario.replace(mpch, x, Confirmation)
        expectStep(
          scenario,
          Hosting.Hosted(PositiveInt.two, Set(primary.id, da.id)),
          events = Set(Event.Revoked(mpch.id)),
          Request.ApprovedBy(Set(primary.id, da.id)),
        )
        scenario.expectQuorum(respondingHosts = Set(primary.id), approved = false)
        replicate(
          scenario,
          x,
          Set(primary.id, da.id),
          thresholdBefore = PositiveInt.two,
          thresholdAfter = PositiveInt.two,
        )
        scenario.purge(mpch)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "CV8: unfreeze when the party removes a stalled covalidator" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        val scenario = newScenario("CV8")
        nominateCovalidators(scenario)
        scenario.freeze(da)
        expectStep(
          scenario,
          Hosting.Frozen(PositiveInt.one, Set(primary.id)),
          events = Set.empty,
          Request.Rejected,
        )

        // Signed with the party's namespace, or at the threshold of its decentralized namespace
        scenario.remove(da, by = Signer.Party)
        expectHostedByPrimary(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "CV10: keep the covalidator's other parties confirmable by their other hosts while it is disconnected for the import" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*
        // A party hosted 2-of-3 on the primary, DA and MPCH
        val otherParty = PartyFreezeScenario(
          primary.parties.enable("Other-CV10"),
          source = primary,
          partySigner = PartySigner.Local(primary),
          counterparty = bob,
          counterpartyParticipant = cp,
          participants = Seq(primary, da, mpch, x, cp),
          synchronizerId = daId,
          sequencer = getProgrammableSequencer(sequencer1.name),
          loggerFactory = loggerFactory,
        )
        Seq(da, mpch).foreach(otherParty.add(_, Confirmation, HostingState.Active).discard)
        otherParty.setThreshold(PositiveInt.two)
        otherParty.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(primary.id, da.id, mpch.id)),
          events = Set(Event.Added(da.id), Event.Added(mpch.id)),
          Request.ApprovedBy(Set(primary.id, da.id, mpch.id)),
        )

        val scenario = newScenario("CV10")
        nominateCovalidators(scenario)
        val frozenAt = scenario.freeze(da)
        expectStep(
          scenario,
          Hosting.Frozen(PositiveInt.one, Set(primary.id)),
          events = Set.empty,
          Request.Rejected,
        )
        scenario.importAcsWhile(da, scenario.readAcs(frozenAt))(
          otherParty.expectQuorum(respondingHosts = Set(primary.id, mpch.id), approved = true)
        )
        val beforeOnboarding = scenario.onboard(da)
        expectStep(
          scenario,
          Hosting.Hosted(PositiveInt.one, Set(primary.id, da.id)),
          events = Set(Event.Onboarding(da.id)),
          Request.ApprovedBy(Set(primary.id)),
        )
        scenario.activate(da, beforeOnboarding)
        expectStep(
          scenario,
          Hosting.Hosted(PositiveInt.two, Set(primary.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(primary.id, da.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }
}

/** The party's namespace is a decentralized namespace of three owners with threshold 2, which sign
  * its topology transactions at the threshold.
  */
final class CovalidationReplicationDecentralizedPartyIntegrationTest
    extends CovalidationReplicationIntegrationTest {

  override protected def createParty(name: String, primary: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): (ExternalParty, PartySigner.External) = {
    import env.*
    val owners = Seq.fill(3)(primary.parties.testing.external.create_external_namespace())
    val (onboardingTransactions, party) = primary.parties.testing.external
      .onboarding_transactions(
        name,
        decentralizedNamespaceOwners = owners.toSet,
        namespaceThreshold = PositiveInt.two,
      )
      .futureValueUS
      .value
    primary.ledger_api.parties
      .allocate_external(
        daId,
        onboardingTransactions.transactionsWithSingleSignature,
        multiSignatures = onboardingTransactions.multiTransactionSignatures,
      )
      .discard
    eventually() {
      primary.topology.party_to_participant_mappings
        .list(daId, filterParty = party.filterString) should not be empty
    }
    (party, PartySigner.External(NonEmpty.mk(Seq, owners(0).fingerprint, owners(1).fingerprint)))
  }
}

/** The party has one namespace key and three protocol signing keys with threshold 2. */
final class CovalidationReplicationMultiSignaturePartyIntegrationTest
    extends CovalidationReplicationIntegrationTest {

  override protected def createParty(name: String, primary: LocalParticipantReference)(implicit
      env: TestConsoleEnvironment
  ): (ExternalParty, PartySigner.External) = {
    val party = primary.parties.testing.external.enable(
      name,
      keysCount = PositiveInt.three,
      keysThreshold = PositiveInt.two,
    )
    (party, PartySigner.External(NonEmpty.mk(Seq, party.partyId.namespace.fingerprint)))
  }
}
