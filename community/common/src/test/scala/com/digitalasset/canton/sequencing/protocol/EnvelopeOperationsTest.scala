// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.protocol.v31
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.version.{CommonGenerators, ProtocolVersion}
import com.digitalasset.canton.{BaseTestWordSpec, ProtoDeserializationError}
import com.google.protobuf.ByteString
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EnvelopeOperationsTest extends BaseTestWordSpec with ScalaCheckPropertyChecks {

  override lazy val testedProtocolVersion: ProtocolVersion = ProtocolVersion.v35

  private val generators = new CommonGenerators(testedProtocolVersion)

  "ClosedUncompressedEnvelope" should {
    "compress and then decompress to the same value" in {
      import generators.generatorsProtocolSeq.closedUncompressedEnvelopeArb

      forAll { (uncompressed: ClosedUncompressedEnvelope) =>
        val compressed = uncompressed.toClosedCompressedEnvelope
        val decompressed = compressed.toClosedUncompressedEnvelopeUnsafe

        uncompressed shouldBe decompressed
      }
    }

    "predict the receiver's budget draw via uncompressedByteSize" in {
      import generators.generatorsProtocolSeq.closedUncompressedEnvelopeArb

      forAll { (uncompressed: ClosedUncompressedEnvelope) =>
        val compressed = uncompressed.toClosedCompressedEnvelope
        val decompressed = DecompressionBudget(MaxBytesToDecompress.MaxValueUnsafe)
          .decompressGzip(compressed.bytes)
          .value

        decompressed.size shouldBe uncompressed.uncompressedByteSize
      }
    }
  }

  "CloseCompressedEnvelope" should {
    "decompress and compress to the same value" in {
      implicit val arb: Arbitrary[ClosedCompressedEnvelope] =
        generators.generatorsProtocolSeq.closedCompressedEnvelopeArb

      forAll { (compressed: ClosedCompressedEnvelope) =>
        val decompressed = compressed.toClosedUncompressedEnvelopeUnsafe
        val recompressed = decompressed.toClosedCompressedEnvelope

        compressed shouldBe recompressed
      }
    }
  }

  "Batch with closed compressed envelopes" should {
    "not be created if the number of recipients is different from the number of envelopes" in {
      // Creating proto from the domain object is easier
      val batch = generators.generatorsProtocolSeq.batchArb.arbitrary
        .pureApply(Gen.Parameters.default, Seed.random())

      val protoBatch = batch.toProtoV31
      val invalidProtoBatch = protoBatch.copy(
        compressedEnvelopes = protoBatch.compressedEnvelopes ++ protoBatch.compressedEnvelopes
      )

      Batch.fromProtoV31(DecompressionPolicy.HardcodedDefault, invalidProtoBatch) shouldBe Left(
        InvariantViolation(
          None,
          "The number of recipients is different from the number of envelopes.",
        )
      )
    }

    "bound decompression cumulatively across the batch's envelopes (cumulative, PV >= 36)" in {
      val (batch, perEnvelopeSize) = twoIdenticalEnvelopeBatch
      val protoBatch = batch.toProtoV31

      def decompressAll(limit: Int) =
        Batch
          .fromProtoV31(
            DecompressionPolicy.Cumulative(MaxBytesToDecompress(NonNegativeInt.tryCreate(limit))),
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

    "bound decompression per envelope, not cumulatively (historical, PV <= 35)" in {
      val (batch, perEnvelopeSize) = twoIdenticalEnvelopeBatch
      val protoBatch = batch.toProtoV31

      // Not cumulative: each envelope is bounded on its own, so the two together are accepted.
      Batch
        .fromProtoV31(
          DecompressionPolicy.PerEnvelope(
            MaxBytesToDecompress(NonNegativeInt.tryCreate(perEnvelopeSize))
          ),
          protoBatch,
        )
        .flatMap(_.toClosedUncompressedBatchResult)
        .value
        .envelopes should have size 2
    }

    "bound decompression of a v30 batch as a single blob" in {
      val (batch, perEnvelopeSize) = twoIdenticalEnvelopeBatch
      val protoBatch = batch.toProtoV30

      // v30 gzips the whole batch as one blob, so the bound applies to all envelopes at once.
      Batch
        .fromProtoV30(
          DecompressionPolicy.PerEnvelope(
            MaxBytesToDecompress(NonNegativeInt.tryCreate(perEnvelopeSize))
          ),
          protoBatch,
        )
        .left
        .value shouldBe a[ProtoDeserializationError.MaxBytesToDecompressExceeded]

      Batch
        .fromProtoV30(DecompressionPolicy.MaxValueUnsafe, protoBatch)
        .value
        .envelopes should have size 2
    }

    "apply a decompression bound set after parsing" in {
      val (batch, perEnvelopeSize) = twoIdenticalEnvelopeBatch
      val protoBatch = batch.toProtoV31

      def rebindAndDecompressAll(limit: Int) =
        // fromProtoV31 leaves the envelopes compressed, so the decompression bound can still be replaced afterwards.
        Batch
          .fromProtoV31(DecompressionPolicy.MaxValueUnsafe, protoBatch)
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
      val protoBatch = batch.toProtoV31

      val parsed = Batch
        .fromProtoV31(
          DecompressionPolicy.Cumulative(
            MaxBytesToDecompress(NonNegativeInt.tryCreate(2 * perEnvelopeSize))
          ),
          protoBatch,
        )
        .value

      // Decompressing an envelope and then a copy of it must only draw the budget once,
      // so all envelopes and their copies fit within the cumulative bound.
      parsed.envelopes.foreach { envelope =>
        envelope.toClosedUncompressedEnvelopeResult.value.discard
        envelope
          .withRecipients(envelope.recipients)
          .toClosedUncompressedEnvelopeResult
          .value
          .discard
      }
    }

    "select the policy mandated by the protocol version" in {
      val limit = MaxBytesToDecompress(NonNegativeInt.tryCreate(42))

      DecompressionPolicy.forProtocolVersion(ProtocolVersion.v34, limit) shouldBe
        DecompressionPolicy.PerEnvelope(limit)
      DecompressionPolicy.forProtocolVersion(ProtocolVersion.v35, limit) shouldBe
        DecompressionPolicy.PerEnvelope(limit)
      DecompressionPolicy.forProtocolVersion(ProtocolVersion.v36, limit) shouldBe
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
      v31.EnvelopeWithoutRecipients(content = payload, signatures = Seq.empty).serializedSize
    (Batch.fromClosed(testedProtocolVersion, envelope, envelope), perEnvelopeSize)
  }
}
