// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.ReinitCommitments.{
  DigestCommitmentReinitializationInfo,
  DigestCommitmentReinitializationStatusInfo,
}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyHostingStateTestHelpers.{
  freezeParty,
  unfreezeParty,
}
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Confirmation, Submission}
import com.digitalasset.canton.topology.{ForceFlag, ForceFlags, Party}
import com.digitalasset.canton.version.ProtocolVersion

/** Test scenario: Alice is frozen on the synchronizer and unfrozen again, as HyPR (Hybrid Party
  * Replication) does. A freeze emits no Ledger API topology events, so ACS commitments read the
  * freeze-ignoring hosting view. Otherwise a change during the freeze that the commitments process
  * would leave a mismatch that no later event corrects.
  *
  * Test goal: show that the ACS commitments between SP and DA match after the unfreeze, when during
  * the freeze
  *   - nothing else changes (baseline);
  *   - SP processes Bob's removal from SP, which reads Alice's hosts;
  *   - SP alone reinitializes its commitments, which reads Alice's hosts.
  *
  * The commitments match if the participant digests SP and DA compute for each other are equal,
  * since each participant derives its commitment to a counter-participant from that digest.
  *
  * Test setup:
  *   - 3 participants, 1 synchronizer: SP (participant1), DA (participant2) and TP (participant3).
  *   - Per test case, Alice is hosted Active on SP and DA, and Bob on SP. Without the second Active
  *     host of Alice, SP keeps no commitment state for Alice's other hosts.
  *   - An IOU between Alice and Bob, so that SP and DA share a contract of Alice.
  *   - Freeze: Alice nominates TP, which moves itself to Freezing. Unfreeze: Alice removes TP.
  *
  * The test cases share the environment, so a mismatch left by one test case also fails the
  * baseline check of the later ones.
  */
final class PartyFreezeAcsCommitmentIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .withSetup { implicit env =>
        import env.*

        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)

        sp = participant1
        da = participant2
        tp = participant3
      }

  private var sp: LocalParticipantReference = _
  private var da: LocalParticipantReference = _
  private var tp: LocalParticipantReference = _

  /** Allocates Alice on SP and DA and Bob on SP, and creates an IOU between them.
    *
    * @return
    *   Alice and Bob.
    */
  private def allocateAliceAndBobWithSharedContract(
      testCase: String
  )(implicit env: TestConsoleEnvironment): (Party, Party) = {
    import env.*

    val aliceName = s"Alice-$testCase"
    val alice = PartiesAllocator(Set(sp, da))(
      newParties = Seq(aliceName -> sp.id),
      targetTopology = Map(
        aliceName -> Map(daId -> (PositiveInt.one, Set(sp.id -> Submission, da.id -> Confirmation)))
      ),
    ).loneElement
    val bob = sp.parties.enable(s"Bob-$testCase")

    val iou = IouSyntax.createIou(sp, Some(daId))(alice, bob)
    eventually() {
      da.ledger_api.state.acs.of_party(alice).map(_.contractId) should contain(iou.id.contractId)
    }
    (alice, bob)
  }

  private def freezeAlice(alice: Party)(implicit env: TestConsoleEnvironment): CantonTimestamp =
    freezeParty(alice, sp, tp, env.daId, observers = Seq(sp, da, tp))

  private def unfreezeAlice(alice: Party)(implicit env: TestConsoleEnvironment): CantonTimestamp =
    unfreezeParty(alice, sp, tp, env.daId, observers = Seq(sp, da, tp))

  private def assertCommitmentsMatch(
      since: CantonTimestamp
  )(implicit env: TestConsoleEnvironment): Unit =
    PartyHostingStateTestHelpers.assertCommitmentsMatch(Seq(sp, da), env.daId, since)

  "The ACS commitments between SP and DA" should {
    "keep matching while Alice is frozen and after she is unfrozen" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*

        val (alice, _) = allocateAliceAndBobWithSharedContract("baseline")
        assertCommitmentsMatch(since = sp.testing.fetch_synchronizer_time(daId))
        val ledgerEndsBeforeFreeze = Seq(sp, da).map(_.ledger_api.state.end())

        val frozenAt = freezeAlice(alice)
        assertCommitmentsMatch(since = frozenAt)

        val unfrozenAt = unfreezeAlice(alice)
        assertCommitmentsMatch(since = unfrozenAt)

        // Neither the freeze nor the unfreeze emits a Ledger API topology event.
        forAll(Seq(sp, da).zip(ledgerEndsBeforeFreeze)) { case (participant, ledgerEnd) =>
          participant.ledger_api.updates.topology_transactions(
            completeAfter = PositiveInt.MaxValue,
            partyIds = Seq(alice),
            beginOffsetExclusive = ledgerEnd,
            endOffsetInclusive = Some(participant.ledger_api.state.end()),
          ) shouldBe empty
        }
    }

    "match after Bob is removed from SP while Alice is frozen" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*

        val (alice, bob) = allocateAliceAndBobWithSharedContract("removal")
        assertCommitmentsMatch(since = sp.testing.fetch_synchronizer_time(daId))

        freezeAlice(alice)

        // SP is Bob's only host, so this removes Bob's mapping. Bob still has the IOU.
        sp.topology.party_to_participant_mappings.propose_delta(
          party = bob,
          removes = Seq(sp.id),
          store = daId,
          forceFlags = ForceFlags(ForceFlag.DisablePartyWithActiveContracts),
        )
        eventually() {
          forAll(Seq(sp, da))(
            _.topology.party_to_participant_mappings
              .list(daId, filterParty = bob.filterString) shouldBe empty
          )
        }

        val unfrozenAt = unfreezeAlice(alice)
        assertCommitmentsMatch(since = unfrozenAt)
    }

    "match after SP alone reinitializes its commitments while Alice is frozen" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
      implicit env =>
        import env.*

        val (alice, _) = allocateAliceAndBobWithSharedContract("reinit")
        assertCommitmentsMatch(since = sp.testing.fetch_synchronizer_time(daId))

        val frozenAt = freezeAlice(alice)
        // Its self-pings add updates on da after the freeze. The reinitialization reads the
        // topology at the record time of SP's ledger end, so it sees Alice frozen.
        assertCommitmentsMatch(since = frozenAt)

        val DigestCommitmentReinitializationInfo(reinitializedAt) =
          sp.commitments.reinitialize_digest_commitments(daId)
        withClue("The reinitialization must read the topology after the freeze: ") {
          reinitializedAt should be > frozenAt
        }
        eventually() {
          sp.commitments.digest_commitments_reinitialization_status(
            daId
          ) shouldBe DigestCommitmentReinitializationStatusInfo(Some(reinitializedAt))
        }

        val unfrozenAt = unfreezeAlice(alice)
        assertCommitmentsMatch(since = unfrozenAt)
    }
  }
}
