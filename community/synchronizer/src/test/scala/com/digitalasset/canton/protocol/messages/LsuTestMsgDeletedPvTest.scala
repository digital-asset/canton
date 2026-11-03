// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.v30 as cryptoV30
import com.digitalasset.canton.protocol.{SynchronizerLimits, v30, v32}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  ClosedUncompressedEnvelope,
  MediatorGroupRecipient,
  MemberRecipient,
  Recipients,
}
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  OpaquePhysicalSynchronizerId,
  PhysicalSynchronizerId,
}
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import com.google.protobuf.ByteString

class LsuTestMsgDeletedPvTest extends BaseTestWordSpec with HasExecutionContext {

  private val sender = DefaultTestIdentities.participant1

  // Simulates an attacker forging a PhysicalSynchronizerId string with a malicious protocol version.
  // By doing this manually, the attacker bypasses any client-side safety checks that
  // normally prevent the creation of IDs with deleted or unsupported versions.
  private def psidString(pv: Int): String =
    OpaquePhysicalSynchronizerId(
      DefaultTestIdentities.synchronizerId,
      NonNegativeInt.zero,
      pv,
    ).toProtoPrimitive

  // Simulates the attacker generating the raw gRPC/Protobuf payload directly.
  // It packs the forged PSID string and the attacker's own participant ID into the
  // inner message, exactly as it would be transmitted over the wire.
  private def contentProto(pv: Int): v30.LsuSequencingTestMessageContent =
    v30.LsuSequencingTestMessageContent(psidString(pv), sender.toProtoPrimitive)

  "PhysicalSynchronizerId string parsing" should {
    "accept the psid string of a deleted protocol version at the psid parse layer" in {
      val pv35 =
        PhysicalSynchronizerId.fromProtoPrimitive(psidString(35), "psid").value.protocolVersion
      val pv2 =
        PhysicalSynchronizerId.fromProtoPrimitive(psidString(2), "psid").value.protocolVersion
      pv35.isDeleted shouldBe true
      pv35.isSupported shouldBe false
      pv35.toProtoPrimitiveS shouldBe "35"
      pv2.isDeleted shouldBe true
      pv2.isSupported shouldBe false
      pv2.toProtoPrimitiveS shouldBe "2"
    }

    "accept the psid string of a valid protocol version at the psid parse layer" in {
      val pv36 =
        PhysicalSynchronizerId.fromProtoPrimitive(psidString(36), "psid").value.protocolVersion
      pv36.isSupported shouldBe true
      pv36.toProtoPrimitiveS shouldBe "36"
      pv36.isDeleted shouldBe false
    }
  }

  // Agnostic to receiving nodes (PN/MN)
  "LsuSequencingTestMessageContent deserialization" should {
    "not throw (not return Left) from fromProtoV30 for a PV 35 psid" in {
      val proto = contentProto(35)
      val value = LsuSequencingTestMessageContent
        .fromProtoV30(
          ProtocolVersionValidation.NoValidation,
          proto,
        )(proto.toByteString)
        .value

      value.representativeProtocolVersion.representative shouldBe (ProtocolVersion.v36)
    }

    "not throw from fromProtoV30 for a PV 2 psid" in {
      val proto = contentProto(2)

      val value = LsuSequencingTestMessageContent
        .fromProtoV30(
          ProtocolVersionValidation.NoValidation,
          proto,
        )(proto.toByteString)
        .value

      value.representativeProtocolVersion.representative shouldBe (ProtocolVersion.v36)
    }

    "not throw from fromByteString for a PV 35 psid" in {
      val proto = contentProto(35)
      val wire = UntypedVersionedMessage(
        UntypedVersionedMessage.Wrapper.Data(proto.toByteString),
        30,
      ).toByteString

      val value = LsuSequencingTestMessageContent
        .fromByteString(
          ProtocolVersionValidation.NoValidation,
          wire: ByteString,
        )
        .value

      value.representativeProtocolVersion.representative shouldBe (ProtocolVersion.v36)
    }
  }

  "Batch.openEnvelopes" should {
    "not throw instead of reporting an opening error, regardless of the recipient (Mediator or Participant)" in {
      val contentWire = UntypedVersionedMessage(
        UntypedVersionedMessage.Wrapper.Data(contentProto(35).toByteString),
        30,
      ).toByteString
      val signature = cryptoV30.Signature(
        cryptoV30.SignatureFormat.SIGNATURE_FORMAT_RAW,
        ByteString.copyFromUtf8("malicious signature"),
        signingAlgorithmSpec = cryptoV30.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_UNSPECIFIED,
        signatureDelegation = None,
        signedBy = DefaultTestIdentities.participant1.uid.namespace.toProtoPrimitive,
      )
      val envelopeContent = v32.EnvelopeContent(
        v32.EnvelopeContent.SomeEnvelopeContent.LsuSequencingTestMessage(
          v30.LsuSequencingTestMessage(contentWire, Some(signature))
        )
      )

      val targets = List(
        Recipients.cc(MediatorGroupRecipient(MediatorGroupIndex.zero)),
        Recipients.cc(MemberRecipient(DefaultTestIdentities.participant1)),
      )

      targets.foreach { recipient =>
        val closed = ClosedUncompressedEnvelope.create(
          UntypedVersionedMessage(
            UntypedVersionedMessage.Wrapper.Data(envelopeContent.toByteString),
            32,
          ).toByteString,
          recipient,
          Seq.empty,
          testedProtocolVersion,
        )

        val (values, errors) =
          Batch.openEnvelopes(Batch(List[ClosedEnvelope](closed), testedProtocolVersion))(
            testedProtocolVersion,
            SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory).pureCrypto,
            SynchronizerLimits.default,
          )
        values.envelopes should not be empty
        values.envelopes.headOption.value.protocolMessage.representativeProtocolVersion.representative shouldBe (ProtocolVersion.v36)
        errors shouldBe empty
      }
    }
  }
}
