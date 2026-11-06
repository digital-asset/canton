// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  EncryptionAlgorithmSpec,
  Fingerprint,
  SecureRandomness,
  SymmetricKeyScheme,
  TestHash,
}
import com.digitalasset.canton.data.ViewType.TransactionViewType
import com.digitalasset.canton.protocol.{TransactionProtocolLimits, ViewHash, v32}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.validation.ProtoUnvalidatedSeq
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class EncryptedViewMessageTest extends AnyWordSpec with BaseTest {

  // The newest supported version on which the bounds of the next protocol version do not apply yet
  private lazy val pvBeforeNextValidationBound: ProtocolVersion =
    ProtocolVersion.supported.forgetNE.filter(_ < ProtocolVersion.nextValidationBoundPV).max

  "session key lookup bounded by the synchronizer's transaction protocol limits" should {

    val limits: TransactionProtocolLimits =
      testedTransactionLimits.copy(maxRecipientsPerEnvelope = PositiveInt.three)
    val max = limits.maxRecipientsPerEnvelope.unwrap

    val encrypted = AsymmetricEncrypted[SecureRandomness](
      ByteString.copyFromUtf8("ciphertext"),
      EncryptionAlgorithmSpec.RsaOaepSha256,
      Fingerprint.tryFromString("fingerprint"),
    )

    // Repeats the same entry, because the limit counts the entries of the field
    def messageP(entryCount: Int): v32.EncryptedMultipleViewsMessage = {
      val message = EncryptedViewMessageUtils.exampleEncryptedSingleView(TransactionViewType)(
        None,
        ViewHash(TestHash.digest(1)),
        com.digitalasset.nonempty.NonEmpty.mk(Seq, encrypted),
        ByteString.copyFromUtf8("view"),
        DefaultTestIdentities.physicalSynchronizerId,
        SymmetricKeyScheme.Aes128Gcm,
        testedProtocolVersion,
      )
      message.toProtoSomeEnvelopeContentV32 match {
        case v32.EnvelopeContent.SomeEnvelopeContent.EncryptedMultipleViewsMessage(proto) =>
          proto.copy(sessionKeyLookup =
            ProtoUnvalidatedSeq(
              Seq.fill(entryCount)(EncryptedViewMessage.serializeEncryptedRandomness(encrypted))
            )
          )
        case other => fail(s"Unexpected content $other")
      }
    }

    def parse(pvv: ProtocolVersionValidation, entryCount: Int) =
      EncryptedViewMessage.fromProtoV32(pvv, limits, messageP(entryCount))

    "accept the entries at the limit" in {
      parse(
        ProtocolVersionValidation.AlwaysValidation,
        max,
      ).value.viewEncryptionKeyRandomness.forgetNE should have size max.toLong
    }

    "reject the entries above it, naming the field" in {
      parse(ProtocolVersionValidation.AlwaysValidation, max + 1).left.value shouldBe
        ProtoDeserializationError.InvariantViolation(
          "session_key_lookup",
          s"repeated or map field has ${max + 1} elements, exceeding the maximum of $max",
        )
    }

    "count duplicate entries" in {
      // all entries are equal, so they are duplicates
      parse(ProtocolVersionValidation.AlwaysValidation, max + 1).left.value shouldBe a[
        ProtoDeserializationError.InvariantViolation
      ]
    }

    "apply the limit from ProtocolVersion.nextValidationBoundPV" in {
      parse(
        ProtocolVersionValidation(ProtocolVersion.nextValidationBoundPV),
        max + 1,
      ).left.value shouldBe a[ProtoDeserializationError.InvariantViolation]
    }

    "not apply the limit before ProtocolVersion.nextValidationBoundPV" in {
      parse(
        ProtocolVersionValidation(pvBeforeNextValidationBound),
        max + 1,
      ).value.viewEncryptionKeyRandomness.forgetNE should have size (max + 1).toLong
    }
  }
}
