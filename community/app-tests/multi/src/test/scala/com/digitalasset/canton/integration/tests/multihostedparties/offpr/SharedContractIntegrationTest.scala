// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.config
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}

/** Consider the following setup:
  *   - Alice, Bob hosted on participant1 (source)
  *   - participant2 not hosting any party (target)
  *   - Alice and Bob are stakeholders on a contract
  *
  * We check that we can first replicate Alice and then Bob. The reason for this that is that it was
  * a limitation in 2.x
  */
final class SharedContractIntegrationTest extends OfflinePartyReplicationIntegrationTestBase {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )
      .withSetup { implicit env =>
        import env.*

        // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
        sequencer1.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(reconciliationInterval = config.PositiveDurationSeconds.ofDays(365)),
        )

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)

        source = participant1
        target = participant2

        alice = source.parties.testing.enable("Alice")
        bob = source.parties.testing.enable("Bob")
      }

  "check that stakeholders of a contract can be replicated to the same target participant" in {
    implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      IouSyntax.createIou(source)(alice, bob)

      source.ledger_api.state.acs.of_party(alice) should have size 1
      source.ledger_api.state.acs.of_party(bob) should have size 1
      target.ledger_api.state.acs.of_party(alice) should have size 0

      replicateParty(clock, alice)
      target.ledger_api.state.acs.of_party(alice) should have size 1
      target.ledger_api.state.acs.of_party(bob) should have size 1

      participants.all.foreach(_.testing.fetch_synchronizer_time(daId))

      // This is basically a no-op, because Alice and Bob share the contract.
      // After Alice has been replicated to the target participant, replicating Bob to it as well results in an
      // empty ACS snapshot since the target participant already has the active contract (by hosting Alice).
      replicateParty(clock, bob)
      target.ledger_api.state.acs.of_party(alice) should have size 1
      target.ledger_api.state.acs.of_party(bob) should have size 1
  }
}
