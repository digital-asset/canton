// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.hypr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyHostingStateTestHelpers.{
  awaitHostingStates,
  proposeAsParticipant,
  proposeAsParty,
}
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.Party
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.version.ProtocolVersion

/** Intention:
  *   - Assert that the hosting state of a participant in the `PartyToParticipant` mapping can be
  *     updated and observed.
  *   - Assert who authorizes a hosting state change: the party nominates a participant, the
  *     nominated participant moves itself forward, and nobody moves it backward.
  *   - Ensure the Nominated and Freezing hosting states are only accepted from
  *     `ProtocolVersion.offPRWithSequencerChannels` on, and rejected on older versions.
  *
  * Setup:
  *   - 2 Participants, 1 Synchronizer.
  *   - Alice is hosted only on participant1 (Submission). participant1 holds Alice's namespace.
  *
  * Test: Nominate participant2 to host Alice (Observation)
  *   - Participant 1 adds participant2 in the Nominated state, authorized by Alice alone.
  *   - Observe that participant2 is Nominated.
  *
  * Test: Nominate participant3 to host Alice and remove it again
  *   - do this via the participant admin API and via PartyTopologyUtils
  *   - observe that the hosting state of participant2 stays Nominated
  *
  * Test: Move participant2 forward through the hosting states
  *   - Verify that participant1 cannot move participant2 to Freezing.
  *   - Participant 2 moves itself to Freezing, Onboarding and Active (in this order).
  *   - Observe each hosting state on participant2.
  *
  * Test: Reject moving participant2 backward through the hosting states
  *   - Participant 1 proposes moving participant2 to Onboarding, Freezing and Nominated.
  *   - Observe that each proposal is rejected as an invalid hosting state transition.
  *
  * Test: Add a party to a participant directly in Active state
  *   - Participant1 and 3 both submit the transaction to host Alice on participant3 in Active
  *     state.
  *   - Observe that the mapping is effective on participant3.
  *
  * Test: Add a party to a participant directly in Onboarding state
  *   - Participant1 and 3 both submit the transaction to host Alice on participant3 in Onboarding
  *     state.
  *   - Observe that the mapping is effective on participant3.
  *
  * Test: Reject inconsistent topology mapping proposals
  *   - Participant 1 proposes moving participant2 to Active while requiring it to be Onboarding.
  *   - Participant 1 proposes participants to be onboarded but sets no new participants.
  *
  * Test: Reject Nominated on older protocol versions
  *   - Participant 1 proposes moving itself to Nominated, which fails because older protocol
  *     versions cannot serialize the Nominated and Freezing states.
  *
  * Test: Nominate an external party to be hosted on participant2
  *   - External party Bob nominates participant2 to host it
  *   - Observe that Bob on participant2 is in Nominated state.
  *
  * Caveat: The focus is on setting the hosting states. Their effect on hosting and on Ledger API
  * topology events is covered by unit tests.
  */
final class PartyHostingStateIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var alice: Party = _
  private var bobE: Party = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1.withSetup { implicit env =>
      import env.*

      participants.local.synchronizers.connect_local(sequencer1, daName)

      PartiesAllocator(Set(participant1))(
        newParties = Seq(
          "Alice" -> participant1.id
        ),
        targetTopology = Map(
          "Alice" -> Map(
            daId -> (
              PositiveInt.one,
              Set((participant1.id, Submission)),
            )
          )
        ),
      )

      alice = env.participant1.parties.list("Alice").headOption.value.partyResult

      participant1.parties.testing.external.enable("Bob", Some(daName))
      bobE = env.participant1.parties.testing.external.list("Bob").headOption.value.partyResult
    }

  "PartyToParticipant mapping" should {

    "simple ping test" in { env =>
      import env.*

      participant1.health.ping(participant2)

      val aliceOnP1 = participant1.parties.hosted("Alice")
      val aliceOnP2 = participant2.parties.hosted("Alice")

      aliceOnP1.head.party.toProtoPrimitive should startWith("Alice")
      aliceOnP2 shouldBe empty

      val bobOnP1 = participant1.parties.testing.external.list("Bob", "participant1")
      val bobOnP2 = participant2.parties.testing.external.list("Bob", "participant2")

      bobOnP1.head.party.toProtoPrimitive should startWith("Bob")
      bobOnP2 shouldBe empty
    }

    "nominate party to be hosted on participant2" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*

        proposeAsParty(
          alice,
          participant1,
          participant2,
          daId,
          Map(participant2.id -> HostingState.Nominated),
        )

        awaitHostingStates(
          alice,
          daId,
          Seq(participant2),
          Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Nominated),
        )
    }

    "keep the hosting state of participant unchanged when hosting state not specified" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*

        // Nominate participant3 to host Alice via PartyTopologyUtils
        proposeAsParty(
          party = alice,
          partyParticipant = participant1,
          participant = participant3,
          synchronizerId = daId,
          hostingStates = Map(participant3.id -> HostingState.Nominated),
        )

        awaitHostingStates(
          alice,
          daId,
          Seq(participant1, participant2, participant3),
          Map(
            participant1.id -> HostingState.Active,
            participant2.id -> HostingState.Nominated,
            participant3.id -> HostingState.Nominated,
          ),
        )

        // Remove Alice from participant3 via PartyTopologyUtils
        alice.topology.party_to_participant_mappings.propose_delta(
          node = participant1,
          store = daId,
          removes = Seq(participant3.id),
        )

        awaitHostingStates(
          alice,
          daId,
          Seq(participant1, participant2, participant3),
          Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Nominated),
        )

        // Nominate participant3 to host Alice via the participant admin API
        participant1.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          store = daId,
          adds = Seq(participant3.id -> ParticipantPermission.Observation),
          participantStates = Map(participant3.id -> HostingState.Nominated),
        )

        awaitHostingStates(
          alice,
          daId,
          Seq(participant1, participant2, participant3),
          Map(
            participant1.id -> HostingState.Active,
            participant2.id -> HostingState.Nominated,
            participant3.id -> HostingState.Nominated,
          ),
        )

        // Remove Alice from participant3 via the participant admin API
        participant3.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          store = daId,
          removes = Seq(participant3.id),
        )

        awaitHostingStates(
          alice,
          daId,
          Seq(participant1, participant2, participant3),
          Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Nominated),
        )
    }

    "reject onboarding a participant that already hosts a party" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*

        // Participant 2 proposes to move participant2 to Onboarding
        // but participant2 is already in Nominated state
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          proposeAsParticipant(
            alice,
            participant2,
            daId,
            requiresPartyToBeOnboarded = true,
          ),
          _.errorMessage should include(
            "Unable to compute new participant states: Participants required to onboard the party already have " +
              "a hosting state other than Onboarding: Set(PAR::participant2"
          ),
        )

        awaitHostingStates(
          alice,
          daId,
          Seq(participant1, participant2, participant3),
          Map(
            participant1.id -> HostingState.Active,
            participant2.id -> HostingState.Nominated,
          ),
        )
    }

    "allow moving the hosting state of a participant forward" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*

        def proposeAndObserve(participant2State: HostingState): Unit = {
          val expected = Map(participant2.id -> participant2State)
          proposeAsParticipant(alice, participant2, daId, expected)
          // Observe the transaction becoming effective and visible on participant2
          awaitHostingStates(
            alice,
            daId,
            Seq(participant2),
            expected + (participant1.id -> HostingState.Active),
          ).discard
        }

        awaitHostingStates(
          alice,
          daId,
          Seq(participant2),
          Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Nominated),
        )

        // Only the nominated participant can move itself forward
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          proposeAsParty(
            alice,
            participant1,
            participant2,
            daId,
            Map(participant2.id -> HostingState.Freezing),
          ),
          _.errorMessage should include(
            "Could not find an appropriate signing key to issue the topology transaction"
          ),
        )

        Seq(
          HostingState.Freezing,
          HostingState.Onboarding,
          HostingState.Active,
        ).foreach(proposeAndObserve)
    }

    "reject moving the hosting state of a participant backwards" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*

        // Backward moves require Alice's authorization, which participant1 holds
        def propose(participant2State: HostingState): Unit =
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            proposeAsParty(
              alice,
              participant1,
              participant2,
              daId,
              Map(participant2.id -> participant2State),
            ),
            _.errorMessage should (include("Invalid hosting state transitions") and include(
              s"${HostingState.Active} -> $participant2State"
            )),
          )

        awaitHostingStates(
          alice,
          daId,
          Seq(participant2),
          Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Active),
        )

        Seq(
          HostingState.Onboarding,
          HostingState.Freezing,
          HostingState.Nominated,
        ).foreach(propose)
    }

    Seq(HostingState.Active, HostingState.Onboarding).foreach { hostingState =>
      s"allow adding a participant directly in $hostingState state" in { implicit env =>
        import env.*

        // Both participants have to sign the transaction
        Seq(participant1, participant3).foreach(
          _.topology.party_to_participant_mappings.propose_delta(
            party = alice,
            store = daId,
            adds = Seq(participant3.id -> ParticipantPermission.Observation),
            participantStates = Map(participant3.id -> hostingState),
          )
        )

        val additionalParticipantStates =
          if (testedProtocolVersion >= ProtocolVersion.offPRWithSequencerChannels)
            Map(participant2.id -> HostingState.Active)
          else Map.empty

        awaitHostingStates(
          alice,
          daId,
          Seq(participant1, participant2, participant3),
          Map(
            participant1.id -> HostingState.Active,
            participant3.id -> hostingState,
          ) ++ additionalParticipantStates,
        )

        // Remove Alice from participant3
        alice.topology.party_to_participant_mappings.propose_delta(
          node = participant1,
          store = daId,
          removes = Seq(participant3.id),
        )

        awaitHostingStates(
          alice,
          daId,
          Seq(participant1, participant2, participant3),
          Map(participant1.id -> HostingState.Active) ++ additionalParticipantStates,
        )
      }
    }

    "reject inconsistent topology mapping delta proposal" in { implicit env =>
      import env.*

      // Reject a proposal that requires participant2 to be onboarded, but specifies an Active hosting state
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          store = daId,
          adds = Seq(participant2.id -> ParticipantPermission.Observation),
          requiresPartyToBeOnboarded = true,
          participantStates = Map(participant2.id -> HostingState.Active),
        ),
        _.errorMessage should include(
          "Unable to compute new participant states: Participants required to onboard the party have " +
            "a specified hosting state other than Onboarding: Set(PAR::participant2"
        ),
      )

    }

    "reject the Nominated hosting state on older protocol versions" onlyRunLessThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*

        awaitHostingStates(
          alice,
          daId,
          Seq(participant1),
          Map(participant1.id -> HostingState.Active),
        )

        // The console command will refuse to submit the transaction because canBeSerializedTo(30) returns a Left
        loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
          participant1.topology.party_to_participant_mappings.propose_delta(
            party = alice,
            store = daId,
            participantStates = Map(participant1.id -> HostingState.Nominated),
          ),
          // 1. The server-side Topology Manager logs the exact serialization failure
          _.errorMessage should include(
            "Unable to serialize PartyToParticipant mapping to v30 because of unsupported hosting states"
          ),
          // 2. The server-side gRPC interceptor logs the internal failure
          _.errorMessage should include("failed with INTERNAL/An error occurred"),
          // 3. The client-side console log receives the masked security error
          _.errorMessage should include("Request failed for participant1"),
        )
    }

    // Tests PartyTopologyUtils external party path
    "nominate external party to be hosted on participant2" onlyRunWithOrGreaterThan ProtocolVersion.offPRWithSequencerChannels in {
      implicit env =>
        import env.*

        proposeAsParty(
          bobE,
          participant1,
          participant2,
          daId,
          Map(participant2.id -> HostingState.Nominated),
        )

        awaitHostingStates(
          bobE,
          daId,
          Seq(participant1, participant2),
          Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Nominated),
        )

        // Remove the external party from participant2
        bobE.topology.party_to_participant_mappings.propose_delta(
          node = participant1,
          store = daId,
          removes = Seq(participant2.id),
        )

        awaitHostingStates(
          bobE,
          daId,
          Seq(participant1, participant2),
          Map(participant1.id -> HostingState.Active),
        )
    }

  }
}
