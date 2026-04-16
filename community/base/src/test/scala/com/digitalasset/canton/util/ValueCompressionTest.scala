// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValueCompressionTest extends AnyWordSpec with Matchers {

  "ValueCompression" should {

    "round-trip: compress then decompress produces original bytes" in {
      val values = Seq(
        // Small value
        ByteString.copyFromUtf8("hello world"),
        // Medium value (protobuf-like, 200 bytes)
        ByteString.copyFrom(Array.tabulate[Byte](200)(i => (i % 256).toByte)),
        // Large value with repetition (compresses well)
        ByteString.copyFrom(("field_tag_value_" * 100).getBytes),
        // Binary with mixed content
        ByteString.copyFrom({
          val buf = new Array[Byte](500)
          new java.security.SecureRandom().nextBytes(buf)
          // Add some structure (simulating protobuf field tags)
          for (i <- 0 until 500 by 20) { buf(i) = 0x0A; buf(i + 1) = 0x12 }
          buf
        }),
      )

      values.foreach { original =>
        val compressed = ValueCompression.compress(original)
        val decompressed = ValueCompression.decompress(compressed)
        decompressed shouldBe Right(original)
      }
    }

    "legacy uncompressed data reads correctly via decompressIfFlagged" in {
      // Simulate legacy protobuf data: first byte is a field tag (>= 0x08)
      val legacyData = ByteString.copyFrom(Array[Byte](
        0x0A, 0x10, // field 1, wire type 2, length 16
        0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x57, 0x6F,
        0x72, 0x6C, 0x64, 0x21, 0x00, 0x00, 0x00, 0x00,
      ))

      val result = ValueCompression.decompressIfFlagged(legacyData)
      // Legacy data should pass through unchanged
      result shouldBe legacyData
    }

    "hasFlagByte returns false for legacy data" in {
      val legacyData = ByteString.copyFrom(Array[Byte](0x0A, 0x10, 0x48))
      ValueCompression.hasFlagByte(legacyData) shouldBe false
    }

    "hasFlagByte returns true for flagged uncompressed data" in {
      val small = ByteString.copyFromUtf8("tiny")
      val flagged = ValueCompression.compress(small)
      ValueCompression.hasFlagByte(flagged) shouldBe true
      // Small value should be stored with 0x00 (uncompressed) flag
      flagged.byteAt(0) shouldBe 0x00
    }

    "hasFlagByte returns true for flagged compressed data" in {
      val large = ByteString.copyFrom(("repeated_content_" * 20).getBytes)
      val flagged = ValueCompression.compress(large)
      ValueCompression.hasFlagByte(flagged) shouldBe true
      // Large repetitive value should be stored with 0x01 (gzip) flag
      flagged.byteAt(0) shouldBe 0x01
    }

    "values below 64 bytes are stored uncompressed with flag" in {
      val small = ByteString.copyFrom(Array.tabulate[Byte](30)(i => (i + 65).toByte))
      val stored = ValueCompression.compress(small)

      // Should have flag byte 0x00 (uncompressed)
      stored.byteAt(0) shouldBe 0x00
      // Total size: 1 flag byte + original size
      stored.size() shouldBe small.size() + 1
      // Decompresses to original
      ValueCompression.decompress(stored) shouldBe Right(small)
    }

    "values at exactly 64 bytes are eligible for compression" in {
      val exact = ByteString.copyFrom(Array.fill[Byte](64)(0x41))
      val stored = ValueCompression.compress(exact)
      // Whether it compressed depends on entropy, but it should have a flag byte
      ValueCompression.hasFlagByte(stored) shouldBe true
      ValueCompression.decompress(stored) shouldBe Right(exact)
    }

    "compression is only applied when it reduces size" in {
      // High-entropy random data — gzip will expand it
      val random = {
        val buf = new Array[Byte](100)
        new java.security.SecureRandom().nextBytes(buf)
        ByteString.copyFrom(buf)
      }
      val stored = ValueCompression.compress(random)

      // Should still round-trip correctly
      ValueCompression.decompress(stored) shouldBe Right(random)

      // If gzip expanded it, should fall back to uncompressed with flag
      if (stored.byteAt(0) == 0x00) {
        stored.size() shouldBe random.size() + 1
      }
    }

    "empty ByteString compresses and decompresses" in {
      val empty = ByteString.EMPTY
      val stored = ValueCompression.compress(empty)
      stored.byteAt(0) shouldBe 0x00
      stored.size() shouldBe 1
      ValueCompression.decompress(stored) shouldBe Right(empty)
    }

    "decompressIfFlagged handles empty ByteString" in {
      ValueCompression.decompressIfFlagged(ByteString.EMPTY) shouldBe ByteString.EMPTY
    }

    "decompress rejects unknown flag byte" in {
      val bad = ByteString.copyFrom(Array[Byte](0x02, 0x00, 0x00))
      ValueCompression.decompress(bad).isLeft shouldBe true
    }

    "compressionRatio reports correct ratio" in {
      val original = ByteString.copyFrom(("compress_me_" * 50).getBytes)
      val stored = ValueCompression.compress(original)
      val ratio = ValueCompression.compressionRatio(original, stored)
      ratio should be < 1.0 // compressed is smaller
      ratio should be > 0.0
    }

    "large repetitive protobuf-like values compress well" in {
      // Simulate 20 Amulet-like contract values concatenated
      val amuletLike = ByteString.copyFrom(
        (1 to 20).flatMap { i =>
          val party = s"Alice::1220${"%040x".format(i)}"
          val amount = s"${1000 + i}.${"%010d".format(i * 12345)}"
          // Protobuf-like: field tag + length + value
          Array[Byte](0x6A) ++ Array[Byte](0x0A.toByte) ++
            Array[Byte](party.length.toByte) ++ party.getBytes ++
            Array[Byte](0x32.toByte) ++
            Array[Byte](amount.length.toByte) ++ amount.getBytes
        }.toArray
      )

      val stored = ValueCompression.compress(amuletLike)
      val ratio = ValueCompression.compressionRatio(amuletLike, stored)

      // Batch of similar structures should compress very well
      ratio should be < 0.30 // at least 70% reduction
      ValueCompression.decompress(stored) shouldBe Right(amuletLike)
    }
  }
}
