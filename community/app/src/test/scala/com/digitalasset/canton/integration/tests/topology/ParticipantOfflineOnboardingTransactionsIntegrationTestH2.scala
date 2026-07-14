// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.{UseH2, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.{NamespaceDelegation, OwnerToKeyMapping}

/** Checks onboarding with onboarding transactions that are prepared outside the participant's
  * authorized store, as in an offline / cold-key setup: the synchronizer trust certificate is built
  * in a temporary topology store (so it never lands in the authorized store) and provided
  * explicitly at registration.
  */
final class ParticipantOfflineOnboardingTransactionsIntegrationTestH2
    extends CommunityIntegrationTest
    with SharedEnvironment {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1

  "Onboarding with transactions prepared outside the authorized store" should {
    "onboard using a trust certificate built in a temporary store" in { implicit env =>
      import env.*

      // Build the synchronizer trust certificate in a temporary topology store, so it is never
      // stored in the participant's authorized store (mimicking an externally / offline prepared
      // certificate).
      val temporaryStore = participant1.topology.stores
        .create_temporary_topology_store("offline-onboarding", testedProtocolVersion)

      val identityTransactions = participant1.topology.transactions
        .list(filterMappings = Seq(NamespaceDelegation.code, OwnerToKeyMapping.code))
        .result
        .map(_.transaction)

      // seed the temporary store with the participant's identity so that the trust certificate can
      // be authorized there
      participant1.topology.transactions.load(identityTransactions, temporaryStore)

      val trustCertificate = participant1.topology.synchronizer_trust_certificates.propose(
        participant1.id,
        daId,
        store = Some(temporaryStore),
      )

      // the trust certificate is not in the authorized store
      participant1.topology.synchronizer_trust_certificates
        .list(
          store = Some(TopologyStoreId.Authorized),
          filterUid = participant1.id.filterString,
        ) shouldBe empty

      val onboardingTransactions = identityTransactions :+ trustCertificate

      participant1.synchronizers.register(
        sequencer1,
        alias = daName,
        performHandshake = false,
        onboardingTransactions = onboardingTransactions,
      )

      participant1.synchronizers.reconnect(daName)

      participant1.synchronizers.active(daName) shouldBe true
      participant1.health.ping(participant1)
    }
  }
}
