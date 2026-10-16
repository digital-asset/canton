// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{
  AcsCommitmentLimits,
  SynchronizerLimits,
  TopologyLimits,
  TransactionProtocolLimits,
  v32,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  participant1,
  participant2,
  physicalSynchronizerId,
}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation, v1}
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class AcsCommitmentLimitsTest extends AnyWordSpec with BaseTest with AcsCommitmentLimitsFixtures {

  "ACS commitments" should {
    // AcsCommitmentSummary and v32 proto messages were introduced in PV36
    if (testedProtocolVersion >= ProtocolVersion.v36) {
      s"${if (limitsEnforced) "enforce" else "not enforce"} AcsCommitment.digest limits on $testedProtocolVersion" in {
        // Inbound
        val protoMsg = v32.AcsCommitment(
          physicalSynchronizerId = psidPrimitive,
          sendingParticipantUid = p1PrimitiveUnvalidated,
          counterparticipantUid = p2PrimitiveUnvalidated,
          period = Some(validPeriodProto),
          digest = oversizedDigestBytes,
        )

        val inboundResult =
          AcsCommitment.fromByteString(
            testedProtocolVersion,
            toEnvelope(protoVersion, protoMsg.toByteString),
          )

        // Outbound
        val outboundResult = AcsCommitment.create(
          synchronizerId = psidDomain,
          sender = participant1.toLf,
          counterparticipant = participant2.toLf,
          period = CommitmentPeriod
            .create(CantonTimestamp.Epoch, CantonTimestamp.Epoch.plusSeconds(1))
            .value,
          digest = oversizedDigestBytes,
          protocolVersion = testedProtocolVersion,
        )

        // Assertions
        if (limitsEnforced) {
          val error = ProtoDeserializationError.InvariantViolation(
            field = "digest",
            error = s"Byte length for digest (${oversizedDigestBytes
                .size()} bytes) exceeds maximum limit of ${AcsCommitment.MaxDigestSizeBytes} bytes",
          )
          inboundResult.left.value shouldBe error
          outboundResult.left.value shouldBe error.message
        } else {
          withClue(s"Inbound result: $inboundResult: ")(inboundResult.isRight shouldBe true)
          withClue(s"Outbound result: $outboundResult: ")(outboundResult.isRight shouldBe true)
        }
      }

      s"${if (limitsEnforced) "enforce" else "not enforce"} AcsCommitmentProtocolMessage.acs_commitment limits on $testedProtocolVersion" in {
        val oversizedAcs = AcsCommitment.createUnsafe(
          synchronizerId = psidDomain,
          sender = participant1.toLf,
          counterparticipant = participant2.toLf,
          period = CommitmentPeriod
            .create(CantonTimestamp.Epoch, CantonTimestamp.Epoch.plusSeconds(1))
            .value,
          digest = ByteString.copyFrom(
            new Array[Byte](defaultMaxCommitmentSize.value + 1)
          ),
          protocolVersion = testedProtocolVersion,
        )

        // Inbound: construct the proto message
        val protoMsg = v32.AcsCommitmentProtocolMessage(
          acsCommitment = oversizedAcs.toByteString,
          signature = Some(dummySignature.toProtoV30),
        )

        val inboundResult =
          AcsCommitmentProtocolMessage.fromByteString(testedProtocolVersion, testContext)(
            toEnvelope(protoVersion, protoMsg.toByteString)
          )

        // Outbound
        val outboundResult = AcsCommitmentProtocolMessage.create(
          acsCommitment = oversizedAcs,
          signature = dummySignature,
          sizeLimit = defaultMaxCommitmentSize,
        )

        // Assertions
        if (limitsEnforced) {
          val error = ProtoDeserializationError.InvariantViolation(
            field = "acs_commitment",
            error = s"Byte length for acs_commitment (${oversizedAcs.toByteString
                .size()} bytes) exceeds maximum limit of ${defaultMaxCommitmentSize.value} bytes",
          )
          inboundResult.left.value shouldBe error
          outboundResult.left.value shouldBe error.message
        } else {
          withClue(s"Inbound result: $inboundResult: ")(inboundResult.isRight shouldBe true)
          withClue(s"Outbound result: $outboundResult: ")(outboundResult.isRight shouldBe true)
          oversizedAcs.toByteString.size() shouldBe >(defaultMaxCommitmentSize.value)
        }
      }

      s"${if (limitsEnforced) "enforce" else "not enforce"} DigestForCounterparticipant.digest limits on $testedProtocolVersion" in {
        // Inbound
        val protoMsg = v32.DigestForCounterparticipant(
          digest = oversizedDigestBytes,
          counterparticipant = p1PrimitiveUnvalidated,
        )
        // Note: DigestForCounterparticipant is not a top-level versioned message, so
        // it does not implement HasProtocolVersionedWrapper and has no fromByteString method.
        // Thus, we use fromProtoV32 directly as the parsing entry point.
        val inboundResult = DigestForCounterparticipant.fromProtoV32(pvv, protoMsg)

        // Outbound
        val outboundResult =
          DigestForCounterparticipant.create(pvv, oversizedDigestBytes, participant1.toLf)

        // Assertions
        if (limitsEnforced) {
          val error = ProtoDeserializationError.InvariantViolation(
            field = "digest",
            error = s"Byte length for digest (${oversizedDigestBytes
                .size()} bytes) exceeds maximum limit of ${AcsCommitment.MaxDigestSizeBytes} bytes",
          )
          inboundResult.left.value shouldBe error
          outboundResult.left.value shouldBe error.message
        } else {
          withClue(s"Inbound result: $inboundResult: ")(inboundResult.isRight shouldBe true)
          withClue(s"Outbound result: $outboundResult: ")(outboundResult.isRight shouldBe true)

        }
      }

      s"${if (limitsEnforced) "enforce" else "not enforce"} AcsCommitmentSummary.addressed_counterparticipants limits on $testedProtocolVersion" in {
        // Inbound
        val tooManyProtoCounterparticipants =
          Seq.fill(defaultMaxAddressedCounterparticipants.value + 1)(p1PrimitiveUnvalidated)
        val protoMsg = v32.AcsCommitmentSummary(
          physicalSynchronizerId = psidPrimitive,
          commitmentTick = CantonTimestamp.Epoch.toProtoPrimitive,
          addressedCounterparticipants = tooManyProtoCounterparticipants,
          unsentDigests = Seq.empty[v32.DigestForCounterparticipant],
          batchIndex = 0,
          lastBatch = true,
        )

        val inboundResult =
          AcsCommitmentSummary.fromByteString(
            pvv,
            testContext,
            toEnvelope(protoVersion, protoMsg.toByteString),
          )

        // Outbound
        val tooManyCounterparticipants =
          Seq.fill(defaultMaxAddressedCounterparticipants.value + 1)(participant1.toLf)

        val outboundResult = AcsCommitmentSummary.create(
          psid = psidDomain,
          commitmentTick = CantonTimestamp.Epoch,
          addressedCounterparticipants = tooManyCounterparticipants,
          unsentDigests = Seq.empty[DigestForCounterparticipant],
          batchIndex = NonNegativeInt.zero,
          lastBatch = true,
          protocolVersion = testedProtocolVersion,
          limits = testLimits.acsCommitmentLimits,
        )

        // Assertions
        if (limitsEnforced) {
          val error = ProtoDeserializationError.InvariantViolation(
            field = "addressed_counterparticipants",
            error =
              s"repeated field has ${defaultMaxAddressedCounterparticipants.value + 1} elements, exceeding the maximum of $defaultMaxAddressedCounterparticipants",
          )
          inboundResult.left.value shouldBe error
          outboundResult.left.value shouldBe error.message
        } else {
          withClue(s"Inbound result: $inboundResult: ")(inboundResult.isRight shouldBe true)
          withClue(s"Outbound result: $outboundResult: ")(outboundResult.isRight shouldBe true)
        }
      }

      s"${if (limitsEnforced) "enforce" else "not enforce"} AcsCommitmentSummary.unsent_digests limits on $testedProtocolVersion" in {
        // Inbound
        val tooManyProtoDigests = Seq.fill(defaultMaxUnsentDigests.value + 1)(defaultProtoDigest)
        val protoMsg = v32.AcsCommitmentSummary(
          physicalSynchronizerId = psidPrimitive,
          commitmentTick = CantonTimestamp.Epoch.toProtoPrimitive,
          addressedCounterparticipants = Seq(p1PrimitiveUnvalidated),
          unsentDigests = tooManyProtoDigests,
          batchIndex = 0,
          lastBatch = true,
        )
        val inboundResult =
          AcsCommitmentSummary.fromByteString(
            pvv,
            testContext,
            toEnvelope(protoVersion, protoMsg.toByteString),
          )

        // Outbound
        val defaultDigestEntry =
          DigestForCounterparticipant.create(pvv, validDigest, participant1.toLf).value

        val tooManyDigests = Seq.fill(defaultMaxUnsentDigests.value + 1)(defaultDigestEntry)
        val outboundResult = AcsCommitmentSummary.create(
          psid = psidDomain,
          commitmentTick = CantonTimestamp.Epoch,
          addressedCounterparticipants = Seq(participant1.toLf),
          unsentDigests = tooManyDigests,
          batchIndex = NonNegativeInt.zero,
          lastBatch = true,
          protocolVersion = testedProtocolVersion,
          limits = testLimits.acsCommitmentLimits,
        )

        // Assertions
        if (limitsEnforced) {
          val error = ProtoDeserializationError.InvariantViolation(
            field = "unsent_digests",
            error =
              s"repeated field has ${defaultMaxUnsentDigests.value + 1} elements, exceeding the maximum of $defaultMaxUnsentDigests",
          )
          inboundResult.left.value shouldBe error
          outboundResult.left.value shouldBe error.message
        } else {
          withClue(s"Inbound result: $inboundResult: ")(inboundResult.isRight shouldBe true)
          withClue(s"Outbound result: $outboundResult: ")(outboundResult.isRight shouldBe true)
        }
      }

      s"${if (limitsEnforced) "enforce" else "not enforce"} AcsCommitmentSummaryProtocolMessage.acs_commitment_summary limits on $testedProtocolVersion" in {

        val defaultDigestEntry =
          DigestForCounterparticipant.create(pvv, validDigest, participant1.toLf).value

        // Max out both arrays to stay within element limits (10,000 max each).
        // 10,000 participants (~420 KB) + 10,000 digests (~760 KB) = ~1.18 MB payload.
        // This exceeds the 1 MB (1,048,576 bytes) boundary limit organically.
        val oversizedSummary = AcsCommitmentSummary
          .create(
            psid = psidDomain,
            commitmentTick = CantonTimestamp.Epoch,
            addressedCounterparticipants =
              Seq.fill(defaultMaxAddressedCounterparticipants.value)(participant1.toLf),
            unsentDigests = Seq.fill(defaultMaxUnsentDigests.value)(defaultDigestEntry),
            batchIndex = NonNegativeInt.zero,
            lastBatch = true,
            protocolVersion = testedProtocolVersion,
            limits = testLimits.acsCommitmentLimits,
          )
          .value

        // Inbound
        val oversizedSummaryBytes =
          toEnvelope(protoVersion, oversizedSummary.toProtoV32.toByteString)
        val protoMsg = v32.AcsCommitmentSummaryProtocolMessage(
          acsCommitmentSummary = oversizedSummaryBytes,
          signature = Some(dummySignature.toProtoV30),
        )
        val inboundResult =
          AcsCommitmentSummaryProtocolMessage.fromByteString(testedProtocolVersion, testContext)(
            toEnvelope(protoVersion, protoMsg.toByteString)
          )

        // Outbound
        val outboundResult = AcsCommitmentSummaryProtocolMessage.create(
          acsCommitmentSummary = oversizedSummary,
          signature = dummySignature,
          sizeLimit = defaultMaxCommitmentSummarySize,
        )

        if (limitsEnforced) {
          val error = ProtoDeserializationError.InvariantViolation(
            field = "acs_commitment_summary",
            error = s"Byte length for acs_commitment_summary (${oversizedSummary.toByteString
                .size()} bytes) exceeds maximum limit of ${defaultMaxCommitmentSummarySize.value} bytes",
          )
          inboundResult.left.value shouldBe error
          outboundResult.left.value shouldBe error.message
        } else {
          withClue(s"Inbound result: $inboundResult: ")(inboundResult.isRight shouldBe true)
          withClue(s"Outbound result: $outboundResult: ")(outboundResult.isRight shouldBe true)
        }
      }
    }
  }
}

trait AcsCommitmentLimitsFixtures { this: BaseTest =>

  def limitsEnforced = testedProtocolVersion >= ProtocolVersion.nextValidationBoundPV
  def pvv = ProtocolVersionValidation(testedProtocolVersion)
  def psidDomain = physicalSynchronizerId.copy(protocolVersion = testedProtocolVersion)
  def psidPrimitive = psidDomain.toProtoPrimitive

  def protoVersion = AcsCommitmentSummary.protoVersionFor(testedProtocolVersion).value.v

  val testLimits = SynchronizerLimits(
    transactionProtocolLimits = TransactionProtocolLimits.default,
    topologyLimits = TopologyLimits.default,
    acsCommitmentLimits = AcsCommitmentLimits.default,
  )
  val testContext = AcsCommitmentDeserializationContext(testLimits)

  val defaultMaxCommitmentSize = testLimits.acsCommitmentLimits.maxCommitmentSizeBytes
  val defaultMaxCommitmentSummarySize =
    testLimits.acsCommitmentLimits.maxCommitmentSummarySizeBytes
  val defaultMaxUnsentDigests = testLimits.acsCommitmentLimits.maxUnsentDigests
  val defaultMaxAddressedCounterparticipants =
    testLimits.acsCommitmentLimits.maxAddressedCounterparticipants

  val dummySignature = Signature.noSignature
  val validDigest = ByteString.copyFrom(new Array[Byte](32))
  val oversizedDigestBytes =
    ByteString.copyFrom(new Array[Byte](AcsCommitment.MaxDigestSizeBytes + 1))
  val validPeriodProto = v32.CommitmentPeriod(
    fromExclusive = CantonTimestamp.Epoch.toProtoPrimitive,
    toInclusive = CantonTimestamp.Epoch.plusSeconds(1).toProtoPrimitive,
  )
  val canonicalParticipantId =
    ProtoConverter.parseLfParticipantId(participant1.toProtoPrimitive, "test_field").value
  val p1PrimitiveUnvalidated = participant1.toProtoPrimitive.toProtoUnvalidated
  val p2PrimitiveUnvalidated = participant2.toProtoPrimitive.toProtoUnvalidated

  val defaultProtoDigest = v32.DigestForCounterparticipant(
    digest = validDigest,
    counterparticipant = p1PrimitiveUnvalidated,
  )

  /** Helper to construct a valid UntypedVersionedMessage envelope for inbound tests */
  def toEnvelope(protoVersion: Int, payload: ByteString): ByteString =
    v1.UntypedVersionedMessage(
      version = protoVersion,
      wrapper = v1.UntypedVersionedMessage.Wrapper.Data(payload),
    ).toByteString
}
