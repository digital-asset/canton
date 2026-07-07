// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.transaction.TopologyTransaction
import com.digitalasset.canton.version.ProtocolVersion

/** Test that a participant onboarding to a synchronizer with a different protocol version re-signs
  * its onboarding transactions to the synchronizer's protocol version at connect time.
  *
  * Setup:
  *   - synchronizer with protocol version different from the ProtocolVersion.latest
  */
final class ParticipantOnboardingResignIntegrationTestH2
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  // TODO(i33912): Once ProtocolVersion.latest is updated to v36,
  // we should change this test to connect to a synchronizer using v35 by
  private val synchronizerPv = ProtocolVersion.v36

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Config
      .addConfigTransforms(ConfigTransforms.setProtocolVersion(synchronizerPv)*)
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            overrideStaticSynchronizerParameters =
              Some(StaticSynchronizerParameters.initialValues(environment.clock, synchronizerPv)),
          )
        )
      }

  "A participant onboarding to a synchronizer with a different topology protocol version" should {
    // TODO(i33335): enable this test.
    "re-sign its onboarding transactions and become active" ignore { implicit env =>
      import env.*

      val onboardingMappings = TopologyStore.initialParticipantDispatchingSet.forgetNE.toSeq

      // Before connecting, the participant's onboarding transactions in the authorize store are
      // created with the current `latest` protocol version (v35)
      val authorizedOnboarding = participant1.topology.transactions
        .list(store = TopologyStoreId.Authorized, filterMappings = onboardingMappings)
        .result
      authorizedOnboarding should not be empty

      // once the ProtocolVersion.latest is updated to v36, we should change this test to connect to a synchronizer using v35
      // to still test that the participant resigns its transactions to the synchronizer's protocol version (v35) at connect time.
      forEvery(authorizedOnboarding) { stored =>
        val tx = stored.transaction.transaction
        tx.isEquivalentTo(synchronizerPv) shouldBe false
        tx.representativeProtocolVersion shouldBe
          TopologyTransaction.protocolVersionRepresentativeFor(ProtocolVersion.v34)
      }

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.health.ping(participant1)

      // After onboarding, the participant's transactions persisted in the synchronizer store have
      // been re-signed for the synchronizer's protocol version.
      val synchronizerOnboarding = sequencer1.topology.transactions
        .list(
          store = daId,
          filterMappings = onboardingMappings,
          filterNamespace = participant1.namespace.filterString,
        )
        .result
      synchronizerOnboarding should not be empty
      forEvery(synchronizerOnboarding) { stored =>
        val tx = stored.transaction.transaction
        tx.isEquivalentTo(synchronizerPv) shouldBe true
        tx.representativeProtocolVersion shouldBe
          TopologyTransaction.protocolVersionRepresentativeFor(synchronizerPv)
      }
    }
  }
}
