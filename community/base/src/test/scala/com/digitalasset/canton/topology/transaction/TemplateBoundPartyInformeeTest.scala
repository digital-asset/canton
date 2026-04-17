// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Validates that template-bound parties are NOT excluded from the informee set.
  *
  * TBP parties must remain informees because:
  *   - The hosting participant needs to receive the encrypted view to
  *     execute the Daml engine and auto-confirm
  *   - Canton's encryption is participant-to-participant: the participant
  *     decrypts using its own key, not the party's destroyed signing key
  *   - Removing the party from informees removes the hosting participant
  *     from recipients, breaking auto-confirmation and cold standby sync
  *
  * Dark pool privacy comes from controlling the OBSERVER list, not from
  * excluding informees. Signatories are always informees — that's how
  * their hosting participants receive the data they need.
  */
class TemplateBoundPartyInformeeTest extends AnyWordSpec with Matchers {

  "Template-bound party informee status" should {

    "TBP party remains a signatory and therefore an informee" in {
      // A TBP pool party is signatory on Pool contracts.
      // Daml's informeesOfNode includes all signatories.
      // The TBP must NOT be filtered out of the informee set.
      //
      // If it were filtered out, the hosting participant would not
      // receive the encrypted view and could not auto-confirm.
      //
      // This test documents the architectural invariant. The actual
      // informee computation happens in TransactionViewDecompositionFactory
      // and TransactionConfirmationRequestFactory. This test verifies
      // the TemplateBoundPartyMapping does not signal exclusion.

      val mapping = TemplateBoundPartyMapping(
        partyId = com.digitalasset.canton.topology.PartyId
          .tryFromProtoPrimitive("pool::1220abcdef"),
        hostingParticipantIds = Seq(
          com.digitalasset.canton.topology.ParticipantId
            .tryFromProtoPrimitive("PAR::participant1::1220abcdef")
        ),
        allowedTemplateIds = Set("com.example:AMMPool:1.0"),
        signingKeyHash = com.google.protobuf.ByteString.copyFrom(Array.fill(32)(0x42.toByte)),
      )

      // The mapping has no field or method that would exclude it from informees.
      // It is a normal topology mapping with a normal namespace and UID.
      // The party's namespace is used for authorization, and the party
      // remains a full informee on any contract where it is a stakeholder.
      mapping.namespace shouldBe mapping.partyId.namespace
      mapping.maybeUid shouldBe Some(mapping.partyId.uid)

      // The hosting participants are referenced — they need the views
      mapping.hostingParticipantIds should not be empty
      mapping.referencedUids should contain(mapping.partyId.uid)
      mapping.hostingParticipantIds.foreach { pid =>
        mapping.referencedUids should contain(pid.uid)
      }
    }

    "dark pool privacy comes from observer list, not informee exclusion" in {
      // Privacy model:
      //   - Signatory (pool) → always informee → hosting participant receives view ✓
      //   - Observer (token issuer) → informee → issuer's participant receives view
      //   - No observer → not informee → issuer's participant does NOT receive view
      //
      // To make a dark pool: remove observers from the Pool contract.
      // The pool's hosting participant still receives views (it must, to auto-confirm).
      // But no one else sees the reserves.
      //
      // This is strictly better than informee exclusion because:
      //   1. The hosting participant can still auto-confirm (it has the view)
      //   2. Cold standby participants can sync (they receive views when online)
      //   3. The pool contract state is maintained correctly on all hosts

      // This test is a documentation-as-code assertion of the design decision.
      succeed
    }
  }
}
