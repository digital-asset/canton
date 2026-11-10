// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{Fingerprint, Signature}
import com.digitalasset.canton.serialization.BytestringWithCryptographicEvidence
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString

final class SignedContentTest extends BaseTestWordSpec {

  private val content = BytestringWithCryptographicEvidence(ByteString.copyFromUtf8("content"))

  private def signature(signer: String): Signature =
    SymbolicCrypto.signature(ByteString.copyFromUtf8(signer), Fingerprint.tryFromString(signer))

  private def signed(
      signer: String,
      protocolVersion: ProtocolVersion,
  ): SignedContent[BytestringWithCryptographicEvidence] =
    SignedContent(content, signature(signer), None, protocolVersion)

  /** [[AggregatedSignedContent]] shares the wire format, so it can produce bytes with several
    * signatures.
    */
  private def twoSignatures(protocolVersion: ProtocolVersion): ByteString =
    AggregatedSignedContent(
      content,
      NonEmpty(Seq, signature("alice"), signature("bob")),
      protocolVersion,
    ).toByteString

  private def parse(
      protocolVersion: ProtocolVersion,
      check: SignatureCountCheck,
      bytes: ByteString,
  ) = SignedContent.fromByteString(protocolVersion, check)(bytes)

  "deserialization" should {
    "accept a single signature" in {
      val signedContent = signed("alice", testedProtocolVersion)

      forEvery(Seq(SignatureCountCheck.Strict, SignatureCountCheck.ByProtocolVersion)) { check =>
        parse(testedProtocolVersion, check, signedContent.toByteString).value shouldBe signedContent
      }
    }

    "reject several signatures on every protocol version when strict" in {
      forEvery(Seq(ProtocolVersion.latest, ProtocolVersion.dev)) { protocolVersion =>
        parse(
          protocolVersion,
          SignatureCountCheck.Strict,
          twoSignatures(protocolVersion),
        ).left.value.message should include("exceeding the maximum of 1")
      }
    }

    "reject several signatures from the validation bound on" in {
      parse(
        ProtocolVersion.dev,
        SignatureCountCheck.ByProtocolVersion,
        twoSignatures(ProtocolVersion.dev),
      ).left.value.message should include("exceeding the maximum of 1")
    }

    "keep the last of several signatures before the validation bound" in {
      parse(
        ProtocolVersion.latest,
        SignatureCountCheck.ByProtocolVersion,
        twoSignatures(ProtocolVersion.latest),
      ).value.signature shouldBe signature("bob")
    }
  }
}
