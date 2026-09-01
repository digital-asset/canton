// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  MaxByteToDecompressExceeded,
}
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalactic.Uniformity
import org.scalatest.wordspec.AnyWordSpec

import java.io.{ByteArrayInputStream, IOException}
import java.nio.charset.Charset

// Herein contained compressed test data conforms to pre-Java 16
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
      inputCompressed should equal(compressed)(after being OsHeaderFieldIgnored)

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

    tests.forEvery { (_, compressedHex, uncompressedUtf8) =>
      val outputUncompressed = ByteString.copyFromUtf8(uncompressedUtf8)
      val inputCompressed = HexString.parseToByteString(compressedHex).value

      val uncompressed = decompressGzip(inputCompressed)

      uncompressed shouldBe Right(outputUncompressed)
    }

  }

  "decompress fails for bad inputs" in {
    val tests = Table[String, String, String](
      ("name", "compressed-hex", "error message"),
      (
        "bad prefix",
        "1f8a08000000000000004b4ca41a0000a0ec9d324b000000",
        "Input is not in the .gz format",
      ),
      (
        "bad compression method",
        "1f8b05000000000000004b4ca41a0000a0ec9d324b000000",
        "Unsupported compression method",
      ),
      (
        "bad flags",
        "1f8a08080000000000004b4ca41a0000a0ec9d324b000000",
        "Input is not in the .gz format",
      ),
      (
        "bad block length",
        "1f8b080000000000000002000000000000000000",
        "Gzip-compressed data is corrupt",
      ),
      (
        "truncated",
        "1f8b08000000000000002b492d2e01000c7e7fd8040000",
        "Compressed byte input ended too early",
      ),
    )

    tests.forEvery { (_, compressedHex, expectedError) =>
      val inputCompressed = HexString.parseToByteString(compressedHex).value
      val uncompressed = decompressGzip(inputCompressed)

      inside(uncompressed) { case Left(DefaultDeserializationError(err)) =>
        err should include(expectedError)
      }
    }
  }
}

// Herein contained compressed test data was produced by the zstd CLI (v1.5.7) from files, so the
// frames declare the content size in the frame header (required by single-call decompression)
// Reused among compression methods that work on arrays and byte strings
trait ZstdCompressionTests extends AnyWordSpec with BaseTest {

  def compressZstd(str: ByteString): ByteString
  def decompressZstd(str: ByteString): Either[DeserializationError, ByteString]

  "zstd compress and decompress Bytestrings" in {
    val tests = Table[String, String](
      ("uncompressed-utf8", "compressed-hex"),
      ("test", "28b52ffd240421000074657374398167db"),
      (
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "28b52ffd244b3d00000861010007ea84ecc1e611",
      ),
    )

    tests.forEvery { (uncompressedUtf8, compressedHex) =>
      val inputUncompressed = ByteString.copyFromUtf8(uncompressedUtf8)
      val inputCompressed = HexString.parseToByteString(compressedHex).value

      // interop: a frame produced elsewhere must decode
      decompressZstd(inputCompressed) shouldBe Right(inputUncompressed)

      // round-trip: our own output must decode back
      decompressZstd(compressZstd(inputUncompressed)) shouldBe Right(inputUncompressed)
    }
  }

  "zstd decompress fails for bad inputs" in {
    val tests = Table[String, String, String](
      ("name", "compressed-hex", "error message"),
      (
        "bad magic number",
        "28b52fff00000000000000000000",
        "Unknown frame descriptor",
      ),
      (
        "truncated frame header",
        "28b52ffd",
        "Src size is incorrect",
      ),
      (
        "bad block size / corrupt block header",
        "28b52ffda00100000000000000000000",
        "Src size is incorrect",
      ),
      (
        "corrupted data payload",
        "28b52ffd0000000000009999999999999999", // Valid magic but completely corrupted frame data
        "Src size is incorrect",
      ),
      (
        "no declared content size", // Streamed frames without content size in the header are rejected
        "28b52ffd005821000074657374",
        "Destination buffer is too small",
      ),
    )

    tests.forEvery { (_, compressedHex, expectedError) =>
      val inputCompressed = HexString.parseToByteString(compressedHex).value
      val uncompressed = decompressZstd(inputCompressed)

      inside(uncompressed) { case Left(DefaultDeserializationError(err)) =>
        err should include(expectedError)
      }
    }
  }
}

/** Ignores the 'os id' value, the 10th byte in the gzip file format header because it changed from
  * 0x00 to 0xFF in Java 16 and later (https://bugs.openjdk.org/browse/JDK-8244706); enables
  * seamless test execution on Java 11 and 17.
  */
private object OsHeaderFieldIgnored extends Uniformity[ByteString] {

  private val osHeaderFieldAt10thBytePosition = 9

  override def normalized(data: ByteString): ByteString = {
    require(data.size() >= 10, "Gzip compressed data is expected to contain a 10 bytes long header")
    if (data.byteAt(osHeaderFieldAt10thBytePosition) == 0) {
      data
    } else {
      val array = data.toByteArray
      array(osHeaderFieldAt10thBytePosition) = 0
      ByteString.readFrom(new ByteArrayInputStream(array))
    }
  }

  override def normalizedOrSame(o: Any): Any =
    o match {
      case data: ByteString => normalized(data)
      case _ => o
    }

  override def normalizedCanHandle(o: Any): Boolean = o.isInstanceOf[ByteString]
}

class ByteStringUtilTest
    extends AnyWordSpec
    with BaseTest
    with GzipCompressionTests
    with ZstdCompressionTests {
  override def compressGzip(str: ByteString): ByteString = ByteStringUtil.compressGzip(str)

  override def decompressGzip(str: ByteString): Either[DeserializationError, ByteString] =
    ByteStringUtil.decompressGzip(str, MaxBytesToDecompress.MaxValueUnsafe)

  override def compressZstd(str: ByteString): ByteString = ByteStringUtil.compressZstd(str)

  override def decompressZstd(str: ByteString): Either[DeserializationError, ByteString] =
    ByteStringUtil.decompressZstd(str, MaxBytesToDecompress.MaxValueUnsafe)

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

    "copyNBuffered" in {
      def newInput10 = ByteString.copyFrom("." * 10, Charset.defaultCharset()).newInput()
      val buf4 = new Array[Byte](4)

      case class Example(n: Int, numBytesCopied: Int, wasCompleteInput: Boolean)

      Seq(
        Example(-1, 0, false),
        Example(0, 1, false), // If n < input length, we actually copy (n+1) looking for the end.
        Example(1, 2, false),
        Example(9, 10, false),
        Example(10, 10, true),
        Example(11, 10, true),
      ).foreach { eg =>
        val out = ByteString.newOutput()
        ByteStringUtil.copyNBuffered(eg.n, buf4, newInput10, out) shouldBe eg.wasCompleteInput
        withClue(eg.toString) {
          out.toByteString.size shouldBe eg.numBytesCopied
        }
      }
    }

    "boundedInputStream" should {
      def input(n: Int) = new ByteArrayInputStream(Array.fill[Byte](n)(46))

      "pass through reads within the limit" in {
        val in = ByteStringUtil.boundedInputStream(input(10), maxBytes = 10)
        ByteString.readFrom(in).size shouldBe 10
      }

      "throw when buffered reads exceed the limit" in {
        val in = ByteStringUtil.boundedInputStream(input(10), maxBytes = 9)
        val ex = the[IOException] thrownBy ByteString.readFrom(in)
        ex.getMessage should include("Decompressed size exceeds the limit of 9 bytes")
      }

      "throw when single-byte reads exceed the limit" in {
        val in = ByteStringUtil.boundedInputStream(input(3), maxBytes = 2)
        in.read() shouldBe 46
        in.read() shouldBe 46
        an[IOException] should be thrownBy in.read()
      }

      "not count end-of-stream as bytes read" in {
        val in = ByteStringUtil.boundedInputStream(input(2), maxBytes = 2)
        ByteString.readFrom(in).size shouldBe 2
        in.read() shouldBe -1
      }
    }

    "decompress with max bytes to read" in {
      val uncompressed = "a" * 1000000
      val uncompressedByteString = ByteString.copyFrom(uncompressed, Charset.defaultCharset())
      val compressed = compressGzip(uncompressedByteString)

      val res1 = ByteStringUtil.decompressGzip(
        compressed,
        MaxBytesToDecompress(NonNegativeInt.tryCreate(1000000)),
      )
      res1 shouldBe Right(uncompressedByteString)
      val res2 = ByteStringUtil.decompressGzip(
        compressed,
        MaxBytesToDecompress(NonNegativeInt.tryCreate(777)),
      )
      res2 shouldBe Left(
        MaxByteToDecompressExceeded("Max bytes to decompress is exceeded. The limit is 777 bytes.")
      )
    }

    "decompress zstd with max bytes to read" in {
      val uncompressed = "a" * 1000000
      val uncompressedByteString = ByteString.copyFrom(uncompressed, Charset.defaultCharset())
      val compressed = compressZstd(uncompressedByteString)

      val res1 = ByteStringUtil.decompressZstd(
        compressed,
        MaxBytesToDecompress(NonNegativeInt.tryCreate(1000000)),
      )
      res1 shouldBe Right(uncompressedByteString)
      val res2 = ByteStringUtil.decompressZstd(
        compressed,
        MaxBytesToDecompress(NonNegativeInt.tryCreate(777)),
      )
      res2 shouldBe Left(
        MaxByteToDecompressExceeded("Max bytes to decompress is exceeded. The limit is 777 bytes.")
      )
    }

    "compress and decompress using CompressionAlgo" in {
      val input = ByteString.copyFromUtf8("generic-compression-roundtrip")

      Seq(
        CompressionAlgo.Gzip -> ProtocolVersion.v35,
        CompressionAlgo.Zstd -> ProtocolVersion.v36,
      ).foreach { case (algo, pv) =>
        withClue(s"algorithm=$algo, protocolVersion=$pv") {
          CompressionAlgo(pv) shouldBe algo

          val compressed = ByteStringUtil.compress(input, algo)
          ByteStringUtil
            .decompress(compressed, algo, MaxBytesToDecompress.MaxValueUnsafe) shouldBe Right(input)
        }
      }
    }

    "correctly pad or truncate a ByteString" in {
      val aByteStr = ByteString.copyFrom("abcdefghij", Charset.defaultCharset())

      // padded to 20
      val padSize = NonNegativeInt.tryCreate(20)
      val toPad = ByteString.copyFrom(new Array[Byte](padSize.value - aByteStr.size()))
      val padded = ByteStringUtil
        .padOrTruncate(aByteStr, padSize)
      padded.size() shouldBe padSize.value
      padded.substring(0, aByteStr.size()) == aByteStr shouldBe true
      padded.substring(aByteStr.size()) == toPad shouldBe true

      // truncate to 5
      val truncateSize = NonNegativeInt.tryCreate(5)
      val expected = ByteString.copyFrom("abcde", Charset.defaultCharset())
      val truncated = ByteStringUtil
        .padOrTruncate(aByteStr, truncateSize)
      truncated.size() shouldBe truncateSize.value
      truncated == expected shouldBe true

      // truncate to 0
      val truncateSize_2 = NonNegativeInt.zero
      val empty = ByteString.EMPTY
      val truncated_2 = ByteStringUtil
        .padOrTruncate(aByteStr, truncateSize_2)
      truncated_2.size() shouldBe truncateSize_2.value
      truncated_2 == empty shouldBe true
    }
  }
}
