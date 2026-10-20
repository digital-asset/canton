// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.daml.ledger.api.v2.interactive.interactive_submission_service.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.TransactionRoutingError.TopologyErrors.{
  UnknownInformees,
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
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.protocol.LocalAbstainError
import com.digitalasset.canton.synchronizer.sequencer.{
  HasProgrammableSequencer,
  ProgrammableSequencerPolicies,
  SendDecision,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.UUID
import scala.concurrent.Promise
import scala.jdk.CollectionConverters.*

/** Test scenario: a target freezes a party hosted on SP, and requests of all kinds involve the
  * party: writes in which it is a stakeholder, requests prepared or sequenced before the freeze,
  * reads, requests on another synchronizer, reassignments, and requests of SP's other parties.
  *
  * Test goal: show that the freeze rejects exactly the requests that need the party as a
  * stakeholder on the synchronizer on which it is frozen, and leaves everything else working.
  *
  * Test setup: see [[PartyFreezeIntegrationTestSetup]].
  */
final class PartyFreezeSemanticsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private val setup = new PartyFreezeIntegrationTestSetup(this)
  import setup.*

  override def environmentDefinition: EnvironmentDefinition = setup.environmentDefinition

  "Freeze semantics" should {
    "F1: reject writes in which the frozen party is submitter, signatory, observer, controller or choice observer" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("F1")
        val party = scenario.party
        val partyActs = createContract(cp, bob, daId, Seq(bob), observers = Seq(party), Seq(party))
        val bobActs = createContract(cp, bob, daId, Seq(bob), actors = Seq(bob))
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        // Includes the party as signatory of a contract on which Bob acts
        expectFrozenWithHostSp(scenario)

        clue("Submitter and signatory") {
          expectRejected(UnknownSubmitters)(
            createContract(sp, party, daId, Seq(party)).discard
          )
        }
        clue("Observer only") {
          expectRejected(UnknownInformees)(
            createContract(cp, bob, daId, Seq(bob), observers = Seq(party)).discard
          )
        }
        clue("Controller") {
          expectRejected(UnknownSubmitters)(touch(sp, party, daId, partyActs))
        }
        clue("Choice observer") {
          expectRejected(UnknownInformees)(
            touch(cp, bob, daId, bobActs, choiceObservers = Seq(party))
          )
        }

        scenario.remove(da, by = Signer.Party)
        expectHostedBySp(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "F2: reject a request prepared before the freeze and executed after it" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("F2")
        val party = scenario.party
        val external =
          cp.parties.testing.external.enable("External-F2", synchronizer = Some(daName))
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)

        // A contract with the party as observer, prepared while the party is hosted
        val prepared = cp.ledger_api.javaapi.interactive_submission.prepare(
          Seq(external),
          universalContract(Seq(external), Seq(party), Seq.empty).create.commands.asScala.toSeq,
          synchronizerId = Some(daId),
        )
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        // The execution routes the request again, which rejects it before the mediator sees it
        expectRejected(UnknownInformees, cause = party.toProtoPrimitive)(
          cp.ledger_api.interactive_submission
            .execute(
              prepared.getPreparedTransaction,
              Map(
                external.partyId -> global_secret.sign(prepared.preparedTransactionHash, external)
              ),
              UUID.randomUUID().toString,
              HASHING_SCHEME_VERSION_V2,
            )
            .discard
        )

        scenario.remove(da, by = Signer.Party)
        expectHostedBySp(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "F3: keep the frozen party's reads on SP unchanged, and omit the party from its hosted parties" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("F3", alsoOnAcme = true)
        val party = scenario.party
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)

        def activeContracts(): Set[String] =
          sp.ledger_api.state.acs.of_party(party).map(_.contractId).toSet
        def transactions(upTo: Long): Seq[String] =
          sp.ledger_api.updates
            .transactions(Set(party), PositiveInt.MaxValue, endOffsetInclusive = Some(upTo))
            .map(_.updateId)
        def isLocal(): Boolean =
          sp.ledger_api.parties.list().find(_.party == party.partyId).value.isLocal
        def connectedSynchronizers(): Set[String] =
          sp.ledger_api.state
            .connected_synchronizers(Some(party.partyId))
            .connectedSynchronizers
            .map(_.synchronizerId)
            .toSet
        def hostedOn(): Set[SynchronizerId] =
          sp.parties
            .list(filterParty = party.filterString, filterParticipant = sp.filterString)
            .filter(_.party == party.partyId)
            .flatMap(_.participants)
            .flatMap(_.synchronizers.map(_.synchronizerId))
            .toSet

        val ledgerEndBeforeFreeze = sp.ledger_api.state.end()
        val activeContractsBeforeFreeze = activeContracts()
        val transactionsBeforeFreeze = transactions(upTo = ledgerEndBeforeFreeze)
        isLocal() shouldBe true
        connectedSynchronizers() shouldBe Set(
          daId.logical.toProtoPrimitive,
          acmeId.logical.toProtoPrimitive,
        )
        hostedOn() shouldBe Set(daId.logical, acmeId.logical)

        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        activeContracts() shouldBe activeContractsBeforeFreeze
        transactions(upTo = sp.ledger_api.state.end()) shouldBe transactionsBeforeFreeze
        isLocal() shouldBe true
        connectedSynchronizers() shouldBe Set(acmeId.logical.toProtoPrimitive)
        hostedOn() shouldBe Set(acmeId.logical)

        scenario.remove(da, by = Signer.Participant)
        expectHostedBySp(scenario)
        connectedSynchronizers() shouldBe Set(
          daId.logical.toProtoPrimitive,
          acmeId.logical.toProtoPrimitive,
        )
        hostedOn() shouldBe Set(daId.logical, acmeId.logical)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "F4: commit a request sequenced before the freeze and decided after it, and include its contract in the ACS read" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("F4")
        val party = scenario.party
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)

        // Hold back SP's confirmation of the party's request until after the freeze
        val sequencer = getProgrammableSequencer(sequencer1.name)
        val confirmationHeldBack = Promise[Unit]()
        val releaseConfirmation = Promise[Unit]()
        sequencer.setPolicy_("hold back SP's confirmation") { submissionRequest =>
          if (
            submissionRequest.sender == sp.id &&
            ProgrammableSequencerPolicies.isConfirmationResponse(submissionRequest)
          ) {
            confirmationHeldBack.trySuccess(()).discard
            SendDecision.HoldBack(releaseConfirmation.future)
          } else SendDecision.Process
        }
        val ledgerEnd = sp.ledger_api.state.end()
        sp.ledger_api.javaapi.commands.submit_async(
          Seq(party),
          universalContract(Seq(party), Seq.empty, Seq.empty).create.commands.asScala.toSeq,
          synchronizerId = Some(daId),
        )
        confirmationHeldBack.future.futureValue

        val frozenAt = scenario.freeze(da)
        releaseConfirmation.success(())
        sequencer.resetPolicy()
        val created = eventually() {
          val completion = sp.ledger_api.completions.list(party, 1, ledgerEnd).loneElement
          completion.status.value.code shouldBe 0
          sp.ledger_api.updates
            .transactions(Set(party), PositiveInt.one, beginOffsetExclusive = ledgerEnd)
            .loneElement
        }
        CantonTimestamp
          .fromProtoTimestamp(created.transaction.recordTime.value)
          .value should be <= frozenAt
        expectFrozenWithHostSp(scenario)

        val snapshot = scenario.readAcs(frozenAt)
        snapshot.contractIds should contain allElementsOf created.createEvents
          .map(_.contractId)
          .toSeq
        scenario.importAcs(da, snapshot)
        val beforeOnboarding = scenario.onboard(da)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Onboarding(da.id)),
          Request.ApprovedBy(Set(sp.id)),
        )
        scenario.activate(da, beforeOnboarding)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set(Event.Added(da.id)),
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "F5: accept the frozen party's requests on another synchronizer" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("F5", alsoOnAcme = true)
        val party = scenario.party
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        val onAcme = createContract(sp, party, acmeId, Seq(party), Seq(bob), Seq(bob))
        touch(cp, bob, acmeId, onAcme)

        scenario.remove(da, by = Signer.Party)
        expectHostedBySp(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "F6: reject reassignments out of and into the synchronizer on which the party is frozen" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("F6", alsoOnAcme = true)
        val party = scenario.party
        val onDa = createContract(sp, party, daId, Seq(party), Seq(bob))
        val onAcme = createContract(sp, party, acmeId, Seq(party), Seq(bob))
        val towardsDa =
          unassign(party, createContract(sp, party, acmeId, Seq(party), Seq(bob)), acmeId, daId)
        val towardsAcme =
          unassign(party, createContract(sp, party, daId, Seq(party), Seq(bob)), daId, acmeId)
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        clue("Out of da") {
          expectRejected(InvalidArgument, cause = "is not hosted on")(
            unassign(party, onDa, daId, acmeId).discard
          )
        }
        clue("Into da, by unassigning from acme") {
          expectRejected(InvalidArgument, "stakeholders are not active on the target synchronizer")(
            unassign(party, onAcme, acmeId, daId).discard
          )
        }
        clue("Into da, unassigned from acme before the freeze") {
          expectRejected(InvalidArgument, cause = "is not hosted on")(
            sp.ledger_api.commands.submit_assign(party, towardsDa, acmeId, daId).discard
          )
        }
        clue("Into acme, unassigned from da before the freeze") {
          sp.ledger_api.commands.submit_assign(party, towardsAcme, daId, acmeId).discard
        }

        scenario.remove(da, by = Signer.Party)
        expectHostedBySp(scenario)
        clue("Into da, unassigned before the freeze, after the unfreeze") {
          sp.ledger_api.commands.submit_assign(party, towardsDa, acmeId, daId).discard
        }
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "F7: keep SP's admin party and SP's other parties unaffected" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("F7")
        val otherParty = sp.parties.enable("Other-F7", synchronizer = Some(daName))
        scenario.nominate(Seq(da -> Confirmation), PositiveInt.one)
        expectHostedBySp(scenario)
        scenario.freeze(da)
        expectFrozenWithHostSp(scenario)

        sp.health.ping(cp.id)
        val contract = createContract(sp, otherParty, daId, Seq(otherParty), Seq(bob), Seq(bob))
        touch(cp, bob, daId, contract)

        scenario.remove(da, by = Signer.Party)
        expectHostedBySp(scenario)
        scenario.assertCommitmentsMatchAmongHosts()
    }

    "F8: assign a contract unassigned towards da before the freeze after the unfreeze, but after O → A only with a lower threshold" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*
        val scenario = newScenario("F8", alsoOnAcme = true)
        val party = scenario.party
        val assignedBeforeActivation =
          unassign(party, createContract(sp, party, acmeId, Seq(party), Seq(bob)), acmeId, daId)
        val assignedAfterActivation =
          unassign(party, createContract(sp, party, acmeId, Seq(party), Seq(bob)), acmeId, daId)
        // The topology manager checks the raw threshold against the incomplete reassignments, not
        // the effective confirmation threshold, which stays 1 until DA is Active
        scenario.nominate(
          Seq(da -> Confirmation),
          PositiveInt.two,
          ForceFlags(ForceFlag.AllowInsufficientSignatoryAssigningParticipantsForParty),
        )
        expectHostedBySp(scenario)

        scenario.replicate(da)(
          afterFreeze = expectFrozenWithHostSp(scenario),
          afterOnboarding = {
            scenario.expectStep(
              Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
              events = Set(Event.Onboarding(da.id)),
              Request.ApprovedBy(Set(sp.id)),
            )
            sp.ledger_api.commands
              .submit_assign(party, assignedBeforeActivation, acmeId, daId)
              .discard
          },
          afterActivation = scenario.expectStep(
            Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id)),
            events = Set(Event.Added(da.id)),
            Request.ApprovedBy(Set(sp.id, da.id)),
          ),
        )

        // DA hosts the party but is not a reassigning participant of the unassignment, so it
        // abstains, and SP alone cannot reach the threshold of 2
        expectRejected(LocalAbstainError.CannotPerformAllValidations)(
          sp.ledger_api.commands.submit_assign(party, assignedAfterActivation, acmeId, daId).discard
        )
        scenario.setThreshold(PositiveInt.one)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.one, Set(sp.id, da.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        sp.ledger_api.commands.submit_assign(party, assignedAfterActivation, acmeId, daId).discard
        scenario.setThreshold(PositiveInt.two)
        scenario.expectStep(
          Hosting.Hosted(PositiveInt.two, Set(sp.id, da.id)),
          events = Set.empty,
          Request.ApprovedBy(Set(sp.id, da.id)),
        )
        scenario.assertCommitmentsMatchAmongHosts()
    }
  }

}
