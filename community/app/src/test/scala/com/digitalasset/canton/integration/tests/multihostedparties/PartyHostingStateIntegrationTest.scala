// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.integration.util.PartiesAllocator
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.{ParticipantId, Party, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion

/** Intention:
  *   - Assert that the hosting state of a participant in the `PartyToParticipant` mapping can be
  *     updated and observed.
  *   - Ensure the NOMINATED and FREEZING hosting states are only accepted on the `dev` protocol
  *     version, and rejected on older versions.
  *
  * Setup:
  *   - 2 Participants, 1 Synchronizer.
  *   - Alice is multi-hosted on participant1 (Submission) and participant2 (Observation).
  *
  * Test: Move participant2 through the hosting states
  *   - Verify that participant2 is initially ACTIVE.
  *   - Participant 1 proposes topology deltas moving participant2 to NOMINATED, FREEZING, back to
  *     ACTIVE and finally to ONBOARDING.
  *   - Observe each hosting state on participant2.
  *
  * Test: Reject NOMINATED on non-dev protocol versions
  *   - Participant 1 proposes a topology delta moving participant2 to NOMINATED, which fails
  *     because the hosting state cannot be serialized to the v30 PartyToParticipant mapping.
  *
  * Caveat: NOMINATED and FREEZING have no effect yet; the focus is purely on setting the hosting
  * state topology-version dependent. ONBOARDING is last, because leaving it requires participant2's
  * signature.
  */
final class PartyHostingStateIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participants.local.synchronizers.connect_local(sequencer1, daName)

      PartiesAllocator(Set(participant1, participant2))(
        newParties = Seq(
          "Alice" -> participant1.id
        ),
        targetTopology = Map(
          "Alice" -> Map(
            daId -> (PositiveInt.one, Set(
              (participant1, Submission),
              (participant2, Observation),
            ))
          )
        ),
      )
    }

  private def assertHostingStates(
      observer: ParticipantReference,
      party: Party,
      synchronizerId: SynchronizerId,
      expectedStates: Map[ParticipantId, HostingState],
  ): Unit = eventually() {
    val currentPTP = observer.topology.party_to_participant_mappings
      .list(synchronizerId, filterParty = party.filterString)
      .loneElement
      .item

    currentPTP.participants.map(p => p.participantId -> p.state).toMap shouldBe expectedStates
  }

  "PartyToParticipant mapping" should {

    "simple ping test" in { env =>
      import env.*

      participant1.health.ping(participant2)

      val aliceOnP1 = participant1.parties.hosted("Alice")
      val aliceOnP2 = participant2.parties.hosted("Alice")

      aliceOnP1 shouldBe aliceOnP2
    }

    // TODO(#35499): OffPR with Squencer Channels - Switch dev to 37
    "allow setting the hosting state of a participant" onlyRunWith ProtocolVersion.dev in {
      implicit env =>
        import env.*

        val alice: Party = participant1.parties.list("Alice").headOption.value.partyResult

        def proposeAndObserve(participant2State: HostingState): Unit = {
          participant1.topology.party_to_participant_mappings.propose_delta(
            party = alice,
            store = daId,
            participantStates = Map(participant2.id -> participant2State),
          )
          // Observe the transaction becoming effective and visible on participant2
          assertHostingStates(
            participant2,
            alice,
            daId,
            Map(participant1.id -> HostingState.Active, participant2.id -> participant2State),
          )
        }

        assertHostingStates(
          participant2,
          alice,
          daId,
          Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Active),
        )

        Seq(
          HostingState.Nominated,
          HostingState.Freezing,
          HostingState.Active,
          HostingState.Onboarding,
        ).foreach(proposeAndObserve)
    }

    // TODO(#35499): OffPR with Squencer Channels - Switch dev to 37
    "reject the NOMINATED hosting state on non-dev protocol versions" onlyRunLessThan ProtocolVersion.dev in {
      implicit env =>
        import env.*

        val alice: Party = participant1.parties.list("Alice").headOption.value.partyResult

        assertHostingStates(
          participant2,
          alice,
          daId,
          Map(participant1.id -> HostingState.Active, participant2.id -> HostingState.Active),
        )

        // The console command will refuse to submit the transaction because canBeSerializedTo(30) returns a Left
        loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
          participant1.topology.party_to_participant_mappings.propose_delta(
            party = alice,
            store = daId,
            participantStates = Map(participant2.id -> HostingState.Nominated),
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

  }
}
