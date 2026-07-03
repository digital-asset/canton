// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.InitialOnboardingError
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.NamespaceDelegation

/** Checks connecting with explicitly provided onboarding transactions
  */
final class ParticipantOnboardingTransactionsIntegrationTestH2
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1

  "Connecting with provided onboarding transactions" should {
    "reject a provided set that does not contain all the transactions required to onboard" in {
      implicit env =>
        import env.*

        val config = SynchronizerConnectionConfig(
          daName,
          SequencerConnections.single(sequencer1.sequencerConnection),
        )

        // Only the NamespaceDelegation
        val incompleteOnboardingTransactions = participant1.topology.transactions
          .list(filterMappings = Seq(NamespaceDelegation.code))
          .result
          .map(_.transaction)

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers
            .connect_by_config(config, onboardingTransactions = incompleteOnboardingTransactions),
          entry => {
            entry.shouldBeCommandFailure(InitialOnboardingError)
            entry.commandFailureMessage should include("encryption key")
          },
        )

        participant1.synchronizers.list_connected() shouldBe empty
    }

    "connect using the full onboarding set listed from the authorized store" in { implicit env =>
      import env.*

      val config = SynchronizerConnectionConfig(
        daName,
        SequencerConnections.single(sequencer1.sequencerConnection),
      )

      val onboardingTransactions = participant1.topology.transactions
        .list(filterMappings = TopologyStore.initialParticipantDispatchingSet.forgetNE.toSeq)
        .result
        .map(_.transaction)
      onboardingTransactions should have size 3

      participant1.synchronizers
        .connect_by_config(config, onboardingTransactions = onboardingTransactions)

      participant1.health.ping(participant1)
    }
  }
}
