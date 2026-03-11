// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.version.ProtocolVersion

final class FindPartyActivationOffsetsIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  "Alice has 2 activations on P1 and P3" in { implicit env =>
    import env.*

    source = participant1
    target = participant3

    IouSyntax.createIou(source)(alice, bob, 9999.95)

    val ledgerEndP1 = source.ledger_api.state.end()

    targetAuthorizesHosting(alice, daId, disconnectTarget = false)

    clue(
      "Finds 1 topology transaction for Alice on P1 for when she has been authorized on P3"
    ) {
      source.parties
        .find_party_max_activation_offset(
          partyId = alice,
          participantId = target.id,
          synchronizerId = daId,
          onboarding = testedProtocolVersion >= ProtocolVersion.v35,
          beginOffsetExclusive = ledgerEndP1,
          completeAfter = PositiveInt.one,
        ) should be > 0L
    }
  }
}
