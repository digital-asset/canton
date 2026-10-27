// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.protocol.{SynchronizerLimits, v31}
import com.digitalasset.canton.util.{CompressionAlgo, MaxBytesToDecompress}
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.{
  CommonGenerators,
  ProtocolVersion,
  ProtocolVersionValidation,
}
import com.digitalasset.canton.{BaseTestWordSpec, ProtoDeserializationError}
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

final class EnvelopeOperationsTest extends BaseTestWordSpec with ScalaCheckPropertyChecks {
  private val generators = new CommonGenerators(testedProtocolVersion)
  private val algo = CompressionAlgo(testedProtocolVersion)

  s"ClosedUncompressedEnvelope (algo=$algo)" should {
    "compress and then decompress to the same value" in {
      import generators.generatorsProtocolSeq.closedUncompressedEnvelopeArb

      forAll { (uncompressed: ClosedUncompressedEnvelope) =>
        val compressed = uncompressed.toClosedCompressedEnvelope(algo)
        val decompressed = compressed.toClosedUncompressedEnvelopeUnsafe

        uncompressed shouldBe decompressed
      }
    }

    "predict the receiver's budget draw via uncompressedByteSize" in {
      import generators.generatorsProtocolSeq.closedUncompressedEnvelopeArb

      forAll { (uncompressed: ClosedUncompressedEnvelope) =>
        val compressed = uncompressed.toClosedCompressedEnvelope(algo)
        val budget = DecompressionBudget(MaxBytesToDecompress.MaxValueUnsafe)
        val decompressed = (algo match {
          case CompressionAlgo.Gzip => budget.decompressGzip(compressed.bytes)
          case CompressionAlgo.Zstd => budget.decompressZstd(compressed.bytes)
        }).value

        decompressed.size shouldBe uncompressed.uncompressedByteSize
      }
    }
  }

  s"CloseCompressedEnvelope (algo=$algo)" should {
    "decompress and compress to the same value" in {
      implicit val arb: Arbitrary[ClosedCompressedEnvelope] =
        generators.generatorsProtocolSeq.closedCompressedEnvelopeArb

      forAll { (compressed: ClosedCompressedEnvelope) =>
        val decompressed = compressed.toClosedUncompressedEnvelopeUnsafe
        val recompressed = decompressed.toClosedCompressedEnvelope(algo)

        compressed shouldBe recompressed
      }
    }
  }

  "Batch with closed compressed envelopes" should {
    "not be created if the number of recipients is different from the number of envelopes" in {
      // Creating proto from the domain object is easier
      val batch = generators.generatorsProtocolSeq.batchArb.arbitrary
        .pureApply(Gen.Parameters.default, Seed.random())

      val protoBatch = batch.toProtoV32
      val envelopes = ProtoValidation
        .validateLength(
          protoBatch.compressedEnvelopes,
          "compressed_envelopes",
          testedProtocolVersionValidation,
          ProtoValidation.MaxCollectionSize,
        )
        .value
      val invalidProtoBatch = protoBatch.copy(compressedEnvelopes = envelopes ++ envelopes)

      Batch.fromProtoV32(
        testedProtocolVersionValidation,
        BatchDeserializationContext(
          DecompressionPolicy.HardcodedDefault,
          testedSynchronizerLimits,
        ),
        invalidProtoBatch,
      ) shouldBe Left(
        InvariantViolation(
          None,
          "The number of recipients is different from the number of envelopes.",
        )
      )
    }

    "reject an envelope whose number of signatures exceeds maxSignaturesPerEnvelope (PV >= newBoundsOnPV37)" in {
      val (batch, _) = envelopeBatchWithSignatures(3)
      val protoBatch = batch.toProtoV32
      val tightLimits = synchronizerLimitsWithMaxSignaturesPerEnvelope(2)

      Batch
        .fromProtoV32(
          ProtocolVersionValidation.PV(ProtocolVersion.newBoundsOnPV37),
          BatchDeserializationContext(DecompressionPolicy.HardcodedDefault, tightLimits),
          protoBatch,
        )
        .flatMap(_.toClosedUncompressedBatchResult) shouldBe Left(
        InvariantViolation(
          Some("signatures"),
          "repeated field has 3 elements, exceeding the maximum of 2",
        )
      )
    }

    "accept an envelope whose number of signatures is within maxSignaturesPerEnvelope (PV >= newBoundsOnPV37)" in {
      val (batch, _) = envelopeBatchWithSignatures(3)
      val protoBatch = batch.toProtoV32
      val sufficientLimits = synchronizerLimitsWithMaxSignaturesPerEnvelope(3)

      Batch
        .fromProtoV32(
          ProtocolVersionValidation.PV(ProtocolVersion.newBoundsOnPV37),
          BatchDeserializationContext(DecompressionPolicy.HardcodedDefault, sufficientLimits),
          protoBatch,
        )
        .flatMap(_.toClosedUncompressedBatchResult)
        .value
        .envelopes
        .loneElement
        .signatures should have size 3
    }

    "not enforce maxSignaturesPerEnvelope in (PV < newBoundsOnPV37)" in {
      val (batch, _) = envelopeBatchWithSignatures(3)
      val protoBatch = batch.toProtoV32
      val tightLimits = synchronizerLimitsWithMaxSignaturesPerEnvelope(2)

      Batch
        .fromProtoV32(
          ProtocolVersionValidation.PV(ProtocolVersion.v36),
          BatchDeserializationContext(DecompressionPolicy.HardcodedDefault, tightLimits),
          protoBatch,
        )
        .flatMap(_.toClosedUncompressedBatchResult)
        .value
        .envelopes
        .loneElement
        .signatures should have size 3
    }

    "bound decompression cumulatively across the batch's envelopes (cumulative, PV >= 36)" in {
      val (batch, perEnvelopeSize) = twoIdenticalEnvelopeBatch
      val protoBatch = batch.toProtoV32

      def decompressAll(limit: Int) =
        Batch
          .fromProtoV32(
            testedProtocolVersionValidation,
            BatchDeserializationContext(
              DecompressionPolicy.Cumulative(MaxBytesToDecompress(NonNegativeInt.tryCreate(limit))),
              testedSynchronizerLimits,
            ),
            protoBatch,
          )
          .flatMap(_.toClosedUncompressedBatchResult)

      // A budget that admits one envelope is not enough for two, even though each fits individually.
      decompressAll(perEnvelopeSize).left.value shouldBe a[
        ProtoDeserializationError.MaxBytesToDecompressExceeded
      ]
      // Doubling the budget admits both.
      decompressAll(2 * perEnvelopeSize).value.envelopes should have size 2
    }

    "apply a decompression bound set after parsing" in {
      val (batch, perEnvelopeSize) = twoIdenticalEnvelopeBatch
      val protoBatch = batch.toProtoV32

      def rebindAndDecompressAll(limit: Int) =
        // fromProtoV32 leaves the envelopes compressed, so the decompression bound can still be replaced afterwards.
        Batch
          .fromProtoV32(
            testedProtocolVersionValidation,
            BatchDeserializationContext(
              DecompressionPolicy.MaxValueUnsafe,
              testedSynchronizerLimits,
            ),
            protoBatch,
          )
          .map(
            Batch.withDecompressionPolicy(
              _,
              DecompressionPolicy.Cumulative(MaxBytesToDecompress(NonNegativeInt.tryCreate(limit))),
            )
          )
          .flatMap(_.toClosedUncompressedBatchResult)

      rebindAndDecompressAll(perEnvelopeSize).left.value shouldBe a[
        ProtoDeserializationError.MaxBytesToDecompressExceeded
      ]
      rebindAndDecompressAll(2 * perEnvelopeSize).value.envelopes should have size 2
    }

    "not charge the budget twice for copies of an envelope" in {
      val (batch, perEnvelopeSize) = twoIdenticalEnvelopeBatch
      val protoBatch = batch.toProtoV32

      val parsed = Batch
        .fromProtoV32(
          testedProtocolVersionValidation,
          BatchDeserializationContext(
            DecompressionPolicy.Cumulative(
              MaxBytesToDecompress(NonNegativeInt.tryCreate(2 * perEnvelopeSize))
            ),
            testedSynchronizerLimits,
          ),
          protoBatch,
        )
        .value

      // Decompressing an envelope and then a copy of it must only draw the budget once,
      // so all envelopes and their copies fit within the cumulative bound.
      parsed.envelopes.foreach { envelope =>
        envelope.toClosedUncompressedEnvelopeResult.value
        envelope
          .withRecipients(envelope.recipients)
          .toClosedUncompressedEnvelopeResult
          .value
      }
    }

    "select the policy mandated by the protocol version" in {
      val limit = MaxBytesToDecompress(NonNegativeInt.tryCreate(42))

      DecompressionPolicy.forProtocolVersion(ProtocolVersion.v35, limit) shouldBe
        DecompressionPolicy.PerEnvelope(limit)
      DecompressionPolicy.forProtocolVersion(ProtocolVersion.v36, limit) shouldBe
        DecompressionPolicy.Cumulative(limit)
      DecompressionPolicy.forProtocolVersion(ProtocolVersion.dev, limit) shouldBe
        DecompressionPolicy.Cumulative(limit)
    }
  }

  /** A batch of two identical envelopes; also returns the decompressed size of one envelope. */
  private def twoIdenticalEnvelopeBatch: (Batch[ClosedUncompressedEnvelope], Int) = {
    val template = generators.generatorsProtocolSeq.closedUncompressedEnvelopeArb.arbitrary
      .pureApply(Gen.Parameters.default, Seed.random())
    val payload = ByteString.copyFrom(new Array[Byte](64 * 1024))
    val envelope =
      ClosedUncompressedEnvelope.create(
        payload,
        template.recipients,
        Seq.empty,
        testedProtocolVersion,
      )
    val perEnvelopeSize =
      v31
        .EnvelopeWithoutRecipients(
          content = payload,
          signatures = Seq.empty[com.digitalasset.canton.crypto.v30.Signature],
        )
        .serializedSize
    (Batch.fromClosed(testedProtocolVersion, envelope, envelope), perEnvelopeSize)
  }

  /** A single-envelope batch whose envelope carries `numSignatures` signatures; also returns the
    * decompressed size of the envelope.
    */
  private def envelopeBatchWithSignatures(
      numSignatures: Int
  ): (Batch[ClosedUncompressedEnvelope], Int) = {
    val template = generators.generatorsProtocolSeq.closedUncompressedEnvelopeArb.arbitrary
      .pureApply(Gen.Parameters.default, Seed.random())
    val payload = ByteString.copyFromUtf8("payload")
    val signatures = Seq.fill(numSignatures)(Signature.noSignature)
    val envelope =
      ClosedUncompressedEnvelope.create(
        payload,
        template.recipients,
        signatures,
        testedProtocolVersion,
      )
    val envelopeSize =
      v31
        .EnvelopeWithoutRecipients(
          content = payload,
          signatures = signatures.map(_.toProtoV30),
        )
        .serializedSize
    (Batch.fromClosed(testedProtocolVersion, envelope), envelopeSize)
  }

  /** [[SynchronizerLimits]] identical to the default for [[testedProtocolVersion]], except for
    * `maxSignaturesPerEnvelope`.
    */
  private def synchronizerLimitsWithMaxSignaturesPerEnvelope(max: Int): SynchronizerLimits =
    testedSynchronizerLimits
      .focus(_.transactionProtocolLimits.maxSignaturesPerEnvelope)
      .replace(PositiveInt.tryCreate(max))
}
