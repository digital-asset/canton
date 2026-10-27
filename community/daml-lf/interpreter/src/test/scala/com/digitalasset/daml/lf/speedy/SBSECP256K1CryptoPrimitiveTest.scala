// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.data.support.crypto.MessageSignatureUtil
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.SBuiltinFun.{
  ECPublicKeyUtil,
  SBSECP256K1ValidateKey,
  SBSECP256K1WithEcdsaBool,
}
import com.digitalasset.daml.lf.speedy.SValue.{SBool, SText}
import com.digitalasset.daml.lf.speedy.Speedy.Control
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.security.interfaces.ECPublicKey
import java.security.spec.{ECGenParameterSpec, InvalidKeySpecException}
import java.security.{KeyPairGenerator, Security}
import scala.collection.immutable.ArraySeq

class SBSECP256K1CryptoPrimitiveTest
    extends AnyFreeSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with SuppressingLogging {
  import DERTestLib.*

  Security.addProvider(new BouncyCastleProvider)

  private val compilerConfig = Compiler.Config.Default
  private val dummyMachine = Speedy.Machine.fromPureExpr(
    PureCompiledPackages.Empty(compilerConfig),
    Ast.EBuiltinCon(Ast.BCUnit),
  )

  // secp256k1 generator point as a DER encoded SubjectPublicKeyInfo (88 bytes, short form length).
  private val validKey: Array[Byte] = Ref.HexString
    .decode(
      Ref.HexString.assertFromString(
        "3056301006072a8648ce3d020106052b8104000a034200" +
          "0479be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798" +
          "483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8"
      )
    )
    .toByteArray

  private def hex(bytes: Array[Byte]): Ref.HexString =
    Ref.HexString.encode(Bytes.fromByteArray(bytes))

  private val nonCanonicalCases: Map[String, Array[Byte]] = Map(
    "trailing-zero" -> (validKey :+ 0x00.toByte),
    "trailing-garbage" -> (validKey ++ Array(0xde, 0xad, 0xbe, 0xef).map(_.toByte)),
    "outer-length-long-form" -> (Array(0x30, 0x81).map(_.toByte) ++ validKey.drop(1)),
    "outer-length-padded" -> (Array(0x30, 0x82, 0x00).map(_.toByte) ++ validKey.drop(1)),
  )

  "SBSECP256K1WithEcdsaBool" - {
    "PublicKeys are correctly built from hex encoded public key strings" in {
      val actualPublicKey = MessageSignatureUtil.generateKeyPair.getPublic
      val hexEncodedPublicKey = Bytes.fromByteArray(actualPublicKey.getEncoded).toHexString

      ECPublicKeyUtil.extractPublicKey(hexEncodedPublicKey) shouldBe actualPublicKey
    }

    "PublicKeys fail to be built from invalid hex encoded public key strings" in {
      val invalidHexEncodedPublicKey = Ref.HexString.assertFromString("deadbeef")

      assertThrows[InvalidKeySpecException](
        ECPublicKeyUtil.extractPublicKey(invalidHexEncodedPublicKey)
      )
    }

    "PublicKeys fail to be built from incorrectly formatted hex encoded public key strings" in {
      val keyPairGen = KeyPairGenerator.getInstance("RSA")
      keyPairGen.initialize(1024)
      val badFormatPublicKey = keyPairGen.generateKeyPair().getPublic
      val invalidHexEncodedPublicKey =
        Ref.HexString.encode(Bytes.fromByteArray(badFormatPublicKey.getEncoded))

      assertThrows[InvalidKeySpecException](
        ECPublicKeyUtil.extractPublicKey(invalidHexEncodedPublicKey)
      )
    }

    "correctly identify invalid DER encoded public keys during decoding" in {
      val keyPair = MessageSignatureUtil.generateKeyPair
      val privateKey = keyPair.getPrivate
      val message = Ref.HexString.assertFromString("deadbeef")
      val signature = MessageSignatureUtil.sign(message, privateKey)

      forAll(derPublicKeyHexStringGen) { (invalidPublicKey: Ref.HexString) =>
        SBSECP256K1WithEcdsaBool.execute(
          ArraySeq(SText(signature), SText(message), SText(invalidPublicKey)),
          dummyMachine,
        ) match {
          case Control.Error(IE.Crypto(IE.Crypto.MalformedByteEncoding(`invalidPublicKey`, _))) =>
            succeed
          case Control.Error(IE.Crypto(IE.Crypto.MalformedKey(`invalidPublicKey`, _))) =>
            succeed
          case _ =>
            fail()
        }
      }
    }

    "correctly identify invalid DER encoded signatures" in {
      val keyPair = MessageSignatureUtil.generateKeyPair
      val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString
      val message = Ref.HexString.assertFromString("deadbeef")

      forAll(derSignatureHexStringGen) { (invalidSignature: Ref.HexString) =>
        SBSECP256K1WithEcdsaBool.execute(
          ArraySeq(SText(invalidSignature), SText(message), SText(publicKey)),
          dummyMachine,
        ) match {
          case Control.Value(SBool(false)) =>
            // May have a valid DER format that otherwise will fail verification
            succeed
          case Control.Error(IE.Crypto(IE.Crypto.MalformedSignature(`invalidSignature`, _))) =>
            succeed
          case _ =>
            fail()
        }
      }
    }
  }

  "SBSECP256K1ValidateKey" - {
    "correctly validate ECDSA keys" in {
      val keyPair = MessageSignatureUtil.generateKeyPair
      val publicKey = Bytes.fromByteArray(keyPair.getPublic.getEncoded).toHexString

      SBSECP256K1ValidateKey.execute(
        ArraySeq(SText(publicKey)),
        dummyMachine,
      ) match {
        case Control.Value(SBool(true)) =>
          succeed
        case _ =>
          fail()
      }
    }

    "correctly identify non-ECDSA public keys" in {
      val invalidKeyPairGen = KeyPairGenerator.getInstance("RSA")
      invalidKeyPairGen.initialize(1024)
      val invalidPublicKey =
        Bytes.fromByteArray(invalidKeyPairGen.generateKeyPair().getPublic.getEncoded).toHexString

      SBSECP256K1ValidateKey.execute(ArraySeq(SText(invalidPublicKey)), dummyMachine) match {
        case Control.Error(IE.Crypto(IE.Crypto.MalformedKey(`invalidPublicKey`, _))) =>
          succeed
        case _ =>
          fail()
      }
    }

    "correctly identify off curve ECDSA public keys" in {
      val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
      // Use a different EC curve to generate a ECDSA public key that is not on the secp256k1 curve
      // - we do this as raw ECPoints are checked as being on the secp256k1 curve during key generation!
      keyPairGen.initialize(new ECGenParameterSpec("secp256r1"))
      val fakePublicKey = keyPairGen.generateKeyPair().getPublic.asInstanceOf[ECPublicKey]

      // Ensure we do not have POINT_INFINITY
      fakePublicKey.getW.getAffineX should not be null
      fakePublicKey.getW.getAffineY should not be null

      SBSECP256K1ValidateKey.execute(
        ArraySeq(SText(Bytes.fromByteArray(fakePublicKey.getEncoded).toHexString)),
        dummyMachine,
      ) match {
        case Control.Value(SBool(false)) =>
          succeed
        case _ =>
          fail()
      }
    }

    "correctly identify invalid DER encoded public keys during decoding" in {
      forAll(derPublicKeyHexStringGen) { (invalidPublicKey: Ref.HexString) =>
        SBSECP256K1ValidateKey.execute(
          ArraySeq(SText(invalidPublicKey)),
          dummyMachine,
        ) match {
          case Control.Error(IE.Crypto(IE.Crypto.MalformedByteEncoding(`invalidPublicKey`, _))) =>
            succeed
          case Control.Error(IE.Crypto(IE.Crypto.MalformedKey(`invalidPublicKey`, _))) =>
            succeed
          case _ =>
            fail()
        }
      }
    }
  }

  // Non canonical DER encodings of an EC public key (trailing bytes, non minimal outer length) must
  // be rejected before reaching the JCE providers.
  "ECPublicKey" - {
    "accepts a valid canonical key" in {
      noException should be thrownBy ECPublicKeyUtil.checkDerHeader(validKey)
      ECPublicKeyUtil.extractPublicKey(hex(validKey)) should not be null
    }

    nonCanonicalCases.foreach { case (name, bytes) =>
      s"rejects $name" in {
        an[InvalidKeySpecException] should be thrownBy ECPublicKeyUtil.checkDerHeader(bytes)
        an[InvalidKeySpecException] should be thrownBy
          ECPublicKeyUtil.extractPublicKey(hex(bytes))
      }
    }

    val malformedCases: Map[String, Array[Byte]] = nonCanonicalCases ++ List(
      "empty" -> Array.emptyByteArray,
      "single byte" -> Array(0x30.toByte),
      "wrong tag" -> (0x31.toByte +: validKey.drop(1)),
      "indefinite length" -> (Array(0x30, 0x80).map(_.toByte) ++ validKey.drop(2)),
      "length of length too large" ->
        (Array(0x30, 0x84, 0x00, 0x00, 0x00, 0x56).map(_.toByte) ++ validKey.drop(2)),
      "truncated long form header" -> Array(0x30, 0x82, 0x01).map(_.toByte),
      "truncated key" -> validKey.dropRight(1),
      "three length bytes with leading zero" ->
        (Array(0x30, 0x83, 0x00, 0x01, 0x00).map(_.toByte) ++ Array.fill(256)(0.toByte)),
    )

    "checkDerHeader" - {
      "accepts a valid key" in {
        noException should be thrownBy ECPublicKeyUtil.checkDerHeader(validKey)
      }

      "accepts canonical long form lengths" - {
        "one length byte (0x81 0x80)" in {
          val bytes = Array(0x30, 0x81, 0x80).map(_.toByte) ++ Array.fill(128)(0.toByte)
          noException should be thrownBy ECPublicKeyUtil.checkDerHeader(bytes)
        }
        "two length bytes (0x82 0x01 0x00)" in {
          val bytes = Array(0x30, 0x82, 0x01, 0x00).map(_.toByte) ++ Array.fill(256)(0.toByte)
          noException should be thrownBy ECPublicKeyUtil.checkDerHeader(bytes)
        }
        "three length bytes (0x83 0x01 0x00 0x00)" in {
          val bytes =
            Array(0x30, 0x83, 0x01, 0x00, 0x00).map(_.toByte) ++ Array.fill(65536)(0.toByte)
          noException should be thrownBy ECPublicKeyUtil.checkDerHeader(bytes)
        }
      }

      malformedCases.foreach { case (name, bytes) =>
        s"rejects $name" in {
          an[InvalidKeySpecException] should be thrownBy ECPublicKeyUtil.checkDerHeader(bytes)
        }
      }
    }

    "decodePublicKey" - {
      "decodes a valid key" in {
        val key = ECPublicKeyUtil.decodePublicKey(validKey)
        key.getAlgorithm shouldBe "EC"
        key.getEncoded shouldBe validKey
      }

      malformedCases.foreach { case (name, bytes) =>
        s"rejects $name" in {
          an[InvalidKeySpecException] should be thrownBy ECPublicKeyUtil.decodePublicKey(bytes)
        }
      }

      "rejects a well formed DER header with an invalid body" in {
        val garbage = Array(0x30, 0x03, 0x01, 0x02, 0x03).map(_.toByte)
        an[InvalidKeySpecException] should be thrownBy ECPublicKeyUtil.decodePublicKey(garbage)
      }
    }
  }
}
