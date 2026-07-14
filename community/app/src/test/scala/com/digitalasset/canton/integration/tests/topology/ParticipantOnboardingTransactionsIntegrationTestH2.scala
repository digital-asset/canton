// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.{FeatureFlag, LocalParticipantReference}
import com.digitalasset.canton.integration.plugins.{UseH2, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.sync.SyncServiceError
import com.digitalasset.canton.participant.synchronizer.PendingOnboardingTransactions
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, OwnerToKeyMapping}

/** Checks registering and connecting with explicitly provided onboarding transactions.
  *
  * Each test uses its own participant: a register or connect attempt registers the synchronizer
  * connection and the alias for the participant, after which `connect_by_config` would reconnect
  * instead of connecting with the provided onboarding transactions.
  */
final class ParticipantOnboardingTransactionsIntegrationTestH2
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1
      .addConfigTransform(ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair))

  private def synchronizerConfig(implicit env: TestConsoleEnvironment) = {
    import env.*
    SynchronizerConnectionConfig(
      daName,
      SequencerConnections.single(sequencer1.sequencerConnection),
    )
  }

  private def onboardingTransactionsOf(participant: LocalParticipantReference) =
    participant.topology.transactions
      .list(filterMappings = TopologyStore.initialParticipantDispatchingSet.forgetNE.toSeq)
      .result
      .map(_.transaction)

  "Registering with provided onboarding transactions" should {
    "reject a provided set that does not contain all the transactions required to onboard" in {
      implicit env =>
        import env.*

        // Only the NamespaceDelegation
        val incompleteOnboardingTransactions = participant1.topology.transactions
          .list(filterMappings = Seq(NamespaceDelegation.code))
          .result
          .map(_.transaction)

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.register_by_config(
            synchronizerConfig,
            performHandshake = false,
            onboardingTransactions = incompleteOnboardingTransactions,
          ),
          entry => {
            entry.shouldBeCommandFailure(SyncServiceError.SynchronizerRegistration)
            entry.commandFailureMessage should include("exactly one owner-to-key mapping")
          },
        )

        participant1.synchronizers.list_registered() shouldBe empty
    }

    "reject a provided set that contains several owner-to-key mappings" in { implicit env =>
      import env.*

      val onboardingTransactions = onboardingTransactionsOf(participant1)
      val duplicatedOtk = onboardingTransactions.filter(_.mapping.code == OwnerToKeyMapping.code)
      duplicatedOtk should not be empty

      assertThrowsAndLogsCommandFailures(
        participant1.synchronizers.register_by_config(
          synchronizerConfig,
          performHandshake = false,
          onboardingTransactions = onboardingTransactions ++ duplicatedOtk,
        ),
        entry => {
          entry.shouldBeCommandFailure(SyncServiceError.SynchronizerRegistration)
          entry.commandFailureMessage should include("exactly one owner-to-key mapping")
        },
      )

      participant1.synchronizers.list_registered() shouldBe empty
    }

    "persist the provided transactions at registration and onboard at a later reconnect" in {
      implicit env =>
        import env.*

        participant1.topology.synchronizer_trust_certificates.propose(participant1.id, daId)

        val onboardingTransactions = onboardingTransactionsOf(participant1)
        onboardingTransactions should have size 3

        participant1.synchronizers.register(
          sequencer1,
          alias = daName,
          performHandshake = false,
          // reversed on purpose: sending the onboarding transactions in the wrong order works
          onboardingTransactions = onboardingTransactions.reverse,
        )
        participant1.synchronizers.list_connected() shouldBe empty

        // persisted at registration
        participant1.repair.pending_operations
          .list(filterOperationName = Some(PendingOnboardingTransactions.operationName.unwrap))
          .loneElement
          .operationKey shouldBe PendingOnboardingTransactions.operationKey(daName)

        // The actual onboarding happens here, using the transactions persisted at registration
        participant1.synchronizers.reconnect(daName)

        participant1.synchronizers.active(daName) shouldBe true
        participant1.health.ping(participant1)

        // cleared after onboarding
        participant1.repair.pending_operations
          .list(filterOperationName =
            Some(PendingOnboardingTransactions.operationName.unwrap)
          ) shouldBe empty
    }

    "clear the pending entry on the next reconnect when re-registering after connecting" in {
      implicit env =>
        import env.*

        participant4.topology.synchronizer_trust_certificates.propose(participant4.id, daId)

        val onboardingTransactions = onboardingTransactionsOf(participant4)
        onboardingTransactions should have size 3

        def pendingOnboardingEntries =
          participant4.repair.pending_operations
            .list(filterOperationName = Some(PendingOnboardingTransactions.operationName.unwrap))

        // register then connect: the entry is persisted then cleared once connected
        participant4.synchronizers.register(
          sequencer1,
          alias = daName,
          performHandshake = false,
          onboardingTransactions = onboardingTransactions,
        )
        pendingOnboardingEntries should have size 1

        participant4.synchronizers.reconnect(daName)
        participant4.synchronizers.active(daName) shouldBe true
        pendingOnboardingEntries shouldBe empty

        // re-registering while already connected persists a new entry ...
        participant4.synchronizers.register(
          sequencer1,
          alias = daName,
          performHandshake = false,
          onboardingTransactions = onboardingTransactions,
        )
        pendingOnboardingEntries should have size 1

        // ... which is cleared on the next reconnect
        participant4.synchronizers.disconnect(daName)
        participant4.synchronizers.reconnect(daName)
        participant4.synchronizers.active(daName) shouldBe true
        pendingOnboardingEntries shouldBe empty
    }
  }

  "Connecting with provided onboarding transactions" should {
    "connect using a full provided onboarding set" in { implicit env =>
      import env.*

      participant2.topology.synchronizer_trust_certificates.propose(participant2.id, daId)

      val onboardingTransactions = onboardingTransactionsOf(participant2)
      onboardingTransactions should have size 3

      participant2.synchronizers
        .connect_by_config(synchronizerConfig, onboardingTransactions = onboardingTransactions)

      participant2.health.ping(participant2)
    }

    "connect using a full provided onboarding set in the wrong order" in { implicit env =>
      import env.*

      participant3.topology.synchronizer_trust_certificates.propose(participant3.id, daId)

      val onboardingTransactions = onboardingTransactionsOf(participant3)
      onboardingTransactions should have size 3

      // reversed on purpose (trust certificate first, namespace delegation last): sending the
      // onboarding transactions in the wrong order works
      participant3.synchronizers
        .connect_by_config(
          synchronizerConfig,
          onboardingTransactions = onboardingTransactions.reverse,
        )

      participant3.health.ping(participant3)
    }
  }
}
