// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.serialization.DeserializationError
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

import java.nio.charset.Charset

// Reused among compression methods that work on arrays and byte strings
trait GzipCompressionTests extends AnyWordSpec with BaseTest {

  def compressGzip(str: ByteString): ByteString
  def decompressGzip(str: ByteString): Either[DeserializationError, ByteString]

  "compress and decompress Bytestrings" in {
    val tests = Table[String, String](
      ("uncompressed-utf8", "compressed-hex"),
      ("test", "1f8b08000000000000002b492d2e01000c7e7fd804000000"),
      ("", "1f8b080000000000000003000000000000000000"),
      (
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "1f8b08000000000000004b4ca41a0000a0ec9d324b000000",
      ),
    )

    tests.forEvery { (uncompressedUtf8, compressedHex) =>
      val inputUncompressed = ByteString.copyFromUtf8(uncompressedUtf8)
      val inputCompressed = HexString.parseToByteString(compressedHex).value

      val compressed = compressGzip(inputUncompressed)
      compressed shouldBe inputCompressed

      val uncompressed = decompressGzip(inputCompressed)
      uncompressed shouldBe Right(inputUncompressed)
    }
  }

  "decompress works if timestamp is set" in {
    val tests = Table[String, String, String](
      ("name", "compressed-hex", "uncompressed"),
      ("Epoch", "1f8b080000000000000003000000000000000000", ""),
      ("non-Epoch", "1f8b0800FFFFFFFF000003000000000000000000", ""),
    )

    tests.forEvery { (name, compressedHex, uncompressedUtf8) =>
      val outputUncompressed = ByteString.copyFromUtf8(uncompressedUtf8)
      val inputCompressed = HexString.parseToByteString(compressedHex).value

      val uncompressed = decompressGzip(inputCompressed)
      uncompressed shouldBe Right(outputUncompressed)
    }

  }

  "decompress fails for bad inputs" in {
    val tests = Table[String, String, String](
      ("name", "compressed-hex", "error message"),
      ("bad prefix", "1f8a08000000000000004b4ca41a0000a0ec9d324b000000", "Not in GZIP format"),
      (
        "bad compression method",
        "1f8b05000000000000004b4ca41a0000a0ec9d324b000000",
        "Unsupported compression method",
      ),
      ("bad flags", "1f8a08080000000000004b4ca41a0000a0ec9d324b000000", "Not in GZIP format"),
      (
        "bad block length",
        "1f8b080000000000000002000000000000000000",
        "invalid stored block lengths",
      ),
      (
        "truncated",
        "1f8b08000000000000002b492d2e01000c7e7fd8040000",
        "Compressed byte input ended too early",
      ),
    )

    tests.forEvery { (name, compressedHex, expectedError) =>
      val inputCompressed = HexString.parseToByteString(compressedHex).value
      val uncompressed = decompressGzip(inputCompressed)

      inside(uncompressed) { case Left(DeserializationError(err)) =>
        err should include(expectedError)
      }
    }
  }
}

class ByteStringUtilTest extends AnyWordSpec with BaseTest with GzipCompressionTests {
  override def compressGzip(str: ByteString): ByteString = ByteStringUtil.compressGzip(str)

  override def decompressGzip(str: ByteString): Either[DeserializationError, ByteString] =
    ByteStringUtil.decompressGzip(str, maxBytesLimit = None)

  "ByteStringUtilTest" should {

    "order ByteStrings lexicographically" in {
      val order = ByteStringUtil.orderByteString

      def less(cmp: Int): Boolean = cmp < 0
      def equal(cmp: Int): Boolean = cmp == 0
      def greater(cmp: Int): Boolean = cmp > 0
      def dual(f: Int => Boolean)(cmp: Int): Boolean = f(-cmp)

      val tests =
        Table[String, String, String, Int => Boolean](
          ("name", "first", "second", "outcome"),
          ("empty", "", "", equal),
          ("empty least", "", "a", less),
          ("equal", "abc", "abc", equal),
          ("longer", "abc", "abcde", less),
          ("shorter", "abcd", "ab", greater),
          ("common prefix", "abcdf", "abced", less),
          ("no common prefix", "def", "abc", greater),
        )

      tests.forEvery { (name, left, right, result) =>
        val bs1 = ByteString.copyFromUtf8(left)
        val bs2 = ByteString.copyFromUtf8(right)
        assert(result(order.compare(bs1, bs2)), name)
        assert(dual(result)(order.compare(bs2, bs1)), name + " dual")
      }
    }
    "decompress with max bytes to read" in {
      val uncompressed = "a" * 1000000
      val uncompressedByteString = ByteString.copyFrom(uncompressed, Charset.defaultCharset())
      val compressed = compressGzip(uncompressedByteString)

      val res1 = ByteStringUtil.decompressGzip(compressed, maxBytesLimit = Some(1000000))
      res1 shouldBe Right(uncompressedByteString)
      val res2 = ByteStringUtil.decompressGzip(compressed, maxBytesLimit = Some(777))
      res2 shouldBe Left(
        DeserializationError("Max bytes to decompress is exceeded. The limit is 777 bytes.")
      )
    }
  }
}
