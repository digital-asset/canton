// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.protocol.{ExternalAuthorization, TransactionProtocolLimits, v30, v32}
import com.digitalasset.canton.topology.{PartyId, UniqueIdentifier}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.ProtoUnvalidatedSeq
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
import org.scalatest.wordspec.AnyWordSpec

class ExternalAuthorizationTest extends AnyWordSpec with BaseTest {

  // The newest supported version on which the bounds of the next protocol version do not apply yet
  private lazy val pvBeforeNextValidationBound: ProtocolVersion =
    ProtocolVersion.supported.forgetNE.filter(_ < ProtocolVersion.nextValidationBoundPV).max

  private val limits: TransactionProtocolLimits =
    TransactionProtocolLimits.default.copy(maxActAs = PositiveInt.two)
  private val max = limits.maxActAs.unwrap

  private def partyN(i: Int): PartyId =
    PartyId(UniqueIdentifier.tryFromProtoPrimitive(s"party$i::default"))
  private lazy val party1: PartyId = partyN(1)

  private def authorizationP(parties: Seq[PartyId]): v32.ExternalAuthorization =
    v32.ExternalAuthorization(
      authentications = ProtoUnvalidatedSeq(
        parties.map(p =>
          v30.ExternalPartyAuthorization(
            party = p.toProtoPrimitive.toProtoUnvalidated,
            signatures = ProtoUnvalidatedSeq(Seq(Signature.noSignature.toProtoV30)),
          )
        )
      ),
      hashingSchemeVersion =
        v32.ExternalAuthorization.HashingSchemeVersion.HASHING_SCHEME_VERSION_V2,
      maxRecordTime = None,
    )

  private def parse(pvv: ProtocolVersionValidation, parties: Seq[PartyId]) =
    ExternalAuthorization.fromProtoV32(pvv, limits, authorizationP(parties))

  private def distinctParties(n: Int): Seq[PartyId] = (1 to n).map(partyN)

  private def tooMany(count: Int) =
    ProtoDeserializationError.InvariantViolation(
      "authentications",
      s"repeated or map field has $count elements, exceeding the maximum of $max",
    )

  "ExternalAuthorization.authentications" should {

    "accept an authorization at the limit" in {
      parse(
        ProtocolVersionValidation.AlwaysValidation,
        distinctParties(max),
      ).value.signatures.keySet shouldBe distinctParties(max).toSet
    }

    "reject an authorization above the limit, naming the field" in {
      parse(
        ProtocolVersionValidation.AlwaysValidation,
        distinctParties(max + 1),
      ).left.value shouldBe tooMany(max + 1)
    }

    "count entries before the conversion to a map, so duplicate parties count" in {
      parse(
        ProtocolVersionValidation.AlwaysValidation,
        Seq.fill(max + 1)(party1),
      ).left.value shouldBe tooMany(max + 1)

      // at the limit, the duplicates collapse into a single map entry
      parse(
        ProtocolVersionValidation.AlwaysValidation,
        Seq.fill(max)(party1),
      ).value.signatures.keySet shouldBe Set(party1)
    }

    "apply the limit from ProtocolVersion.nextValidationBoundPV" in {
      parse(
        ProtocolVersionValidation(ProtocolVersion.nextValidationBoundPV),
        distinctParties(max + 1),
      ).left.value shouldBe tooMany(max + 1)
    }

    "not apply the limit before ProtocolVersion.nextValidationBoundPV" in {
      parse(
        ProtocolVersionValidation(pvBeforeNextValidationBound),
        distinctParties(max + 1),
      ).value.signatures should have size (max + 1).toLong
    }
  }
}
