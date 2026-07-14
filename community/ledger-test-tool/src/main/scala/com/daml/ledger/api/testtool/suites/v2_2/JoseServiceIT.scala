// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_2

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.{ExternalPartyKeySpec, LedgerTestSuite}
import com.daml.ledger.api.v2.jose_service.GetJwksRequest
import com.digitalasset.canton.crypto.JwksTestHelper
import org.scalatest.matchers.should.Matchers.*

class JoseServiceIT extends LedgerTestSuite {

  for (keySpec <- ExternalPartyKeySpec.supported) {
    test(
      s"GetJwksBasic${keySpec.name}",
      s"Allocate an external party using ${keySpec.name} and retrieve JWKS",
      allocate(NoParties),
    )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
      for {
        party <- ledger.allocateExternalPartyFromHint(
          partyIdHint = Some("alice"),
          keySpec = keySpec,
        )
        partyId = party.getValue()
        synchronizerIds <- ledger.getConnectedSynchronizers(
          party = Some(party),
          participantId = None,
          identityProviderId = None,
        )
        synchronizerId = synchronizerIds.headOption.getOrElse(
          throw new Exception("No synchronizer connected")
        )
        jwks <- ledger.getJwks(
          GetJwksRequest(
            partyId = partyId,
            synchronizerId = synchronizerId,
          )
        )
        jwk = JwksTestHelper.toAuth0(jwks.keys(0))
      } yield {
        jwk.getPublicKey() shouldBe party.signingKeyPair.getPublic()
      }
    })
  }
}
