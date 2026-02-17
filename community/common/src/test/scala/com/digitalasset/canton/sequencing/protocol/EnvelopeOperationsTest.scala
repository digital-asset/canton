// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.version.{CommonGenerators, ProtocolVersion}
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

      Batch.fromProtoV31(MaxBytesToDecompress.HardcodedDefault, invalidProtoBatch) shouldBe Left(
        InvariantViolation(
          None,
          "The number of recipients is different from the number of envelopes.",
        )
      )
    }
  }
}
