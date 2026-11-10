// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Fingerprint, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.BytestringWithCryptographicEvidence
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString

final class AggregatedSignedContentTest extends BaseTestWordSpec {

  private val content = BytestringWithCryptographicEvidence(ByteString.copyFromUtf8("content"))

  private def signature(signer: String): Signature =
    SymbolicCrypto.signature(ByteString.copyFromUtf8(signer), Fingerprint.tryFromString(signer))

  private def signed(signer: String): SignedContent[BytestringWithCryptographicEvidence] =
    SignedContent(content, signature(signer), None, testedProtocolVersion)

  "apply" should {
    "keep the content and wrap a single signature" in {
      val aggregated = AggregatedSignedContent(content, signature("alice"), testedProtocolVersion)

      aggregated.content shouldBe content
      aggregated.signatures shouldBe NonEmpty(Seq, signature("alice"))
      aggregated.representativeProtocolVersion shouldBe
        AggregatedSignedContent.protocolVersionRepresentativeFor(testedProtocolVersion)
    }
  }

  "deserialization" should {
    "read a serialized SignedContent" in {
      AggregatedSignedContent
        .fromByteString(testedProtocolVersion, signed("alice").toByteString)
        .value shouldBe AggregatedSignedContent(content, signature("alice"), testedProtocolVersion)
    }

    "reject a timestamp of the signing key" in {
      val withTimestamp =
        SignedContent(
          content,
          signature("alice"),
          Some(CantonTimestamp.Epoch),
          testedProtocolVersion,
        )

      AggregatedSignedContent
        .fromByteString(testedProtocolVersion, withTimestamp.toByteString)
        .left
        .value
        .message should include("timestamp_of_signing_key")
    }
  }
}
