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
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.{ParticipantId, Party, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion

/** Intention:
  *   - Assert that the `isOffline` flag on the `PartyToParticipant` mapping can be updated and
  *     observed.
  *   - Ensure the feature is only active on the `dev` protocol version, and gracefully ignored on
  *     older versions.
  *
  * Setup:
  *   - 2 Participants, 1 Synchronizer.
  *   - Alice is multi-hosted on participant1 (Submission) and participant2 (Observation).
  *
  * Test: Freeze and unfreeze a party
  *   - Verify the initial `isOffline` state (`Some(false)` on dev, `None` on older PVs).
  *   - Participant 1 proposes a topology delta setting `freezeParty = true`.
  *   - Observe the updated flag on Participant 2.
  *   - Participant 1 proposes a topology delta setting `freezeParty = false`.
  *   - Observe the restored flag on Participant 2.
  *
  * Caveat: `isOffline` flag is ineffective, focus is purely on toggling the flag topology-version
  * dependent.
  */
final class PartyOfflineFlagIntegrationTest
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

  private def assertOfflineFlag(
      observer: ParticipantReference,
      party: Party,
      synchronizerId: SynchronizerId,
      expectedParticipants: Seq[ParticipantId],
      expectedOfflineFlag: Boolean,
  ): Unit = eventually() {
    val currentPTP = observer.topology.party_to_participant_mappings
      .list(synchronizerId, filterParty = party.filterString)
      .loneElement
      .item

    currentPTP.participants.map(
      _.participantId
    ) should contain theSameElementsAs expectedParticipants
    currentPTP.isOffline shouldBe expectedOfflineFlag
  }

  "PartyToParticipant mapping" should {

    "simple ping test" in { env =>
      import env.*

      participant1.health.ping(participant2)

      val aliceOnP1 = participant1.parties.hosted("Alice")
      val aliceOnP2 = participant2.parties.hosted("Alice")

      aliceOnP1 shouldBe aliceOnP2
    }

    // TODO(#35499): Remove onlyRunWith ProtocolVersion.dev once the feature is stable
    "allow setting and unsetting the isOffline flag for a party" onlyRunWith ProtocolVersion.dev in {
      implicit env =>
        import env.*

        val alice: Party = participant1.parties.list("Alice").headOption.value.partyResult
        val hostedParticipants = Seq(participant1.id, participant2.id)

        // Assert initial state: Alice is hosted on both participants, isOffline = false
        assertOfflineFlag(
          participant2,
          alice,
          daId,
          hostedParticipants,
          expectedOfflineFlag = false,
        )

        participant1.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          store = daId,
          freezeParty = Some(true),
        )

        // Observe the freeze transaction becoming effective and visible on participant2
        assertOfflineFlag(participant2, alice, daId, hostedParticipants, expectedOfflineFlag = true)

        participant1.topology.party_to_participant_mappings.propose_delta(
          party = alice,
          store = daId,
          freezeParty = Some(false),
        )

        // Observe the unfreeze transaction becoming effective and visible on participant2
        assertOfflineFlag(
          participant2,
          alice,
          daId,
          hostedParticipants,
          expectedOfflineFlag = false,
        )
    }

    // TODO(#35499): Remove/revisit onlyRunLessThan ProtocolVersion.dev once the feature is stable
    "ensure that setting the isOffline flag for a party has no effect on non-dev protocol versions" onlyRunLessThan ProtocolVersion.dev in {
      implicit env =>
        import env.*

        val alice: Party = participant1.parties.list("Alice").headOption.value.partyResult
        val hostedParticipants = Seq(participant1.id, participant2.id)

        assertOfflineFlag(
          participant2,
          alice,
          daId,
          hostedParticipants,
          expectedOfflineFlag = false,
        )

        // The console command will refuse to submit the transaction because canBeSerializedTo(30) returns a Left
        loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
          participant1.topology.party_to_participant_mappings.propose_delta(
            party = alice,
            store = daId,
            freezeParty = Some(true),
          ),
          // 1. The server-side Topology Manager logs the exact serialization failure
          _.errorMessage should include(
            "Unable to serialize PartyToParticipant mapping to v30 because isOffline is true"
          ),
          // 2. The server-side gRPC interceptor logs the internal failure
          _.errorMessage should include("failed with INTERNAL/An error occurred"),
          // 3. The client-side console log receives the masked security error
          _.errorMessage should include("Request failed for participant1"),
        )
    }

  }
}
