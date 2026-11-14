// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.topology.Party

/** Setup:
  *   - Alice is hosted on participant1 (P1 - source, but becomes unusable for the ACS export)
  *   - Bank1 is hosted on participant2 (P2)
  *   - Bank2 is hosted on participant3 (P3)
  *   - Participant4 (P4 - target) is empty, and the target participant for replicating Alice
  *
  * Test Goal: Replicate Alice's state to P4 using multiple ACS snapshots exported from counterparty
  * nodes (P2 and P3) when the source node (P1) is broken/unavailable.
  */
final class ExportPartyAcsMultiSnapshotIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  protected var bank1: Party = _
  protected var bank2: Party = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)
        sequencer1.topology.synchronizer_parameters
          .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval))

        source = participant1
        target = participant4

        alice = participant1.parties.testing
          .enable("Alice", synchronizeParticipants = Seq(participant2, participant3))
        bank1 = participant2.parties.testing
          .enable("Bank1", synchronizeParticipants = Seq(participant1, participant3))
        bank2 = participant3.parties.testing
          .enable("Bank2", synchronizeParticipants = Seq(participant1, participant2))

        // Alias bob to bank1 so base class helper methods like `assertAcsAndContinuedOperation` function seamlessly
        bob = bank1
      }

  "Replicate Alice using two combined ACS snapshots from counterparty nodes instead of the source node" in {
    implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      val p1 = participant1
      val p2 = participant2
      val p3 = participant3

      // Assumption: All contracts of Alice are visible to either Bank1 or Bank2

      // Contract visible to Alice and Bank1 (P1, P2)
      IouSyntax.createIou(p1)(alice, bank1, 10.0).discard

      // Contract visible to Alice and Bank2 (P1, P3)
      IouSyntax.createIou(p1)(alice, bank2, 20.0).discard

      // Contract visible to Alice, Bank1 and Bank2 (P1, P2, P3)
      IouSyntax.createIou(p1)(alice, bank1, 30.0, observers = List(bank2.partyId)).discard

      // Ping to ensure contracts are fully sequenced and observed by P2 and P3
      p1.health.ping(p2)
      p1.health.ping(p3)

      // Capture the state off the counterparties BEFORE the target starts onboarding
      val beforeActivationP2 = p2.ledger_api.state.end()
      val beforeActivationP3 = p3.ledger_api.state.end()

      // P4 (target) authorizes hosting Alice and disconnects from the synchronizer
      // (This uses `source` / P1 under the hood to authorize the topology delta)
      targetAuthorizesHosting(alice, daId, disconnectTarget = true)

      val acsSnapshotP2 = tempDirectory.toTempFile("p2_acs_snapshot.gz").toString
      val acsSnapshotP3 = tempDirectory.toTempFile("p3_acs_snapshot.gz").toString

      // Source (P1) is now considered "broken" -> We export Alice's view from P2 and P3
      // (This works because P2 and P3 hold all of Alice's active contracts via Bank1 and Bank2)
      p2.parties.export_party_acs(
        party = alice,
        synchronizerId = daId.logical,
        targetParticipantId = target.id,
        beginOffsetExclusive = beforeActivationP2,
        exportFilePath = acsSnapshotP2,
      )

      p3.parties.export_party_acs(
        party = alice,
        synchronizerId = daId.logical,
        targetParticipantId = target.id,
        beginOffsetExclusive = beforeActivationP3,
        exportFilePath = acsSnapshotP3,
      )

      // Import the combined ACS snapshots sequentially on P4
      // (Handles deduplication for the shared contract)
      target.parties.import_party_acs(daId, Some(alice), acsSnapshotP2)
      target.parties.import_party_acs(daId, Some(alice), acsSnapshotP3)

      // P4 reconnects to the synchronizer and safely clears the onboarding flag unilaterally
      reconnectAndEnsureOnboardingClearance(clock, alice, daName)

      // Verify that P4 has correctly restored Alice's state containing all 3 contracts
      // and that normal operations (archives & creations) continue to work
      assertAcsAndContinuedOperation(
        target,
        expectedNumActiveContracts = 3,
        numOfContractCreations = 2,
      )
  }
}
