// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.HexString
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait HkdfTest {
  this: AsyncWordSpec with BaseTest =>

  def hkdfProvider(hkdfF: => Future[HkdfOps]): Unit = {
    "HKDF provider" should {
      "produce an output of the specified length" in {
        val algo = HmacAlgorithm.HmacSha256
        val algoLen = algo.hashAlgorithm.length
        val secret = SecureRandomness.secureRandomness(algoLen.toInt)
        hkdfF.map { hkdf =>
          // Test a few key sizes that we might need
          forAll(0L until (5L * algoLen)) { i =>
            val expanded =
              hkdf
                .hkdfExpand(
                  secret,
                  i.toInt,
                  algorithm = algo,
                  info = HkdfInfo.testOnly(ByteString.EMPTY),
                )
                .valueOrFail(s"Failed to compute hkdfExpand with length $i")
            expanded.unwrap.size shouldBe i
          }
        }
      }

      "pass golden tests from RFC 5869" in {
        val vectors = List(
          (
            "077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5",
            "f0f1f2f3f4f5f6f7f8f9",
            42,
            "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865",
          ),
          (
            "06a6b88c5853361a06104c9ceb35b45cef760014904671014a193f40c15fc244",
            """b0b1b2b3b4b5b6b7b8b9babbbcbdbebf
             |c0c1c2c3c4c5c6c7c8c9cacbcccdcecf
             |d0d1d2d3d4d5d6d7d8d9dadbdcdddedf
             |e0e1e2e3e4e5e6e7e8e9eaebecedeeef
             |f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff""".stripMargin.filter(_ != '\n'),
            82,
            """b11e398dc80327a1c8e7f78c596a4934
            |4f012eda2d4efad8a050cc4c19afa97c
            |59045a99cac7827271cb41c65e590e09
            |da3275600c2f09b8367793a9aca3db71
            |cc30c58179ec3e87c14c01d5c1f3434f
            |1d87""".stripMargin.filter(_ != '\n'),
          ),
          (
            "19ef24a32c717b167f33a91d6f648bdf96596776afdb6377ac434c1c293ccb04",
            "",
            42,
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8",
          ),
        )
        val algo = HmacAlgorithm.HmacSha256
        hkdfF.map { hkdf =>
          forAll(vectors) { case (keyString, infoString, len, expectedOut) =>
            val key = HexString
              .parseToByteString(keyString)
              .getOrElse(throw new IllegalStateException(s"Wrong test vector key bytes $keyString"))
            val info = HexString
              .parseToByteString(infoString)
              .getOrElse(throw new IllegalStateException(s"Wrong test vector info $infoString"))
            val expanded =
              hkdf
                .hkdfExpand(SecureRandomness(key), len, HkdfInfo.testOnly(info), algo)
                .valueOrFail("Could not compute the HMAC for test vector")
            val expandedHex = HexString.toHexString(expanded.unwrap)
            expandedHex shouldBe expectedOut
          }
        }
      }
    }
  }
}
