// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.crypto.CryptoKeyFormat.DerX509Spki
import com.digitalasset.canton.crypto.provider.jce.{JcePureCrypto, JceSecurityProvider}
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import org.bouncycastle.asn1.x9.{ECNamedCurveTable, X9ObjectIdentifiers}
import org.bouncycastle.asn1.{ASN1Integer, BERTags, DERSequence}
import org.bouncycastle.jcajce.spec.MLDSAParameterSpec
import org.scalatest.wordspec.AsyncWordSpec

import java.security.spec.{ECGenParameterSpec, RSAKeyGenParameterSpec}
import java.security.{KeyPairGenerator, SecureRandom}
import scala.concurrent.Future

trait PublicKeyValidationTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>
  import PublicKeyValidationTest.invalidPublicKeys

  private def modifyPublicKeyFormat(
      publicKey: PublicKey,
      newFormat: CryptoKeyFormat,
  ): PublicKey =
    publicKey match {
      case epk: EncryptionPublicKey =>
        epk.replaceFormat(newFormat)
      case spk: SigningPublicKey =>
        spk.replaceFormat(newFormat)
      case _ => fail(s"unsupported key type")
    }

  private def publicKeyValidationTest(
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      name: String,
      newCrypto: => Future[Crypto],
      newPublicKey: Crypto => Future[PublicKey],
  ): Unit =
    // Modify the public key to have a different encoding format than expected,
    // to verify that parseAndValidatePublicKey correctly rejects mismatched formats.
    forAll(supportedCryptoKeyFormats) { format =>
      s"Validate $name public key with format \"$format\"" in {
        for {
          crypto <- newCrypto
          publicKey <- newPublicKey(crypto)
          newPublicKeyWithTargetFormat = modifyPublicKeyFormat(publicKey, format)
          validationRes = CryptoKeyValidation.parseAndValidatePublicKey(
            newPublicKeyWithTargetFormat,
            errString => errString,
            cacheValidation = false,
          )
        } yield
          if (format == publicKey.format || format == CryptoKeyFormat.Symbolic)
            validationRes shouldEqual Either.unit
          else
            validationRes.left.value should include(
              s"Failed to deserialize or validate $format public key [${publicKey.id}]: KeyParseAndValidateError"
            )
      }
    }

  def publicKeyValidationProvider(
      supportedSigningKeySpecs: Set[SigningKeySpec],
      supportedEncryptionKeySpecs: Set[EncryptionKeySpec],
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      newCrypto: => Future[Crypto],
      javaPrivateKeyRetentionTime: PositiveFiniteDuration,
  ): Unit =
    "Validate public keys" should {
      forAll(supportedSigningKeySpecs) { signingKeySpec =>
        val keySpecName =
          if (signingKeySpec.toString == "EC-P256") "EC-P256-Signing" else signingKeySpec.toString

        publicKeyValidationTest(
          supportedCryptoKeyFormats,
          keySpecName,
          newCrypto,
          crypto =>
            getSigningPublicKey(
              crypto,
              SigningKeyUsage.ProtocolOnly,
              signingKeySpec,
            ).failOnShutdown,
        )
      }

      forAll(supportedEncryptionKeySpecs) { encryptionKeySpec =>
        val keySpecName =
          if (encryptionKeySpec.toString == "EC-P256") "EC-P256-Encryption"
          else encryptionKeySpec.toString

        publicKeyValidationTest(
          supportedCryptoKeyFormats,
          keySpecName,
          newCrypto,
          crypto => getEncryptionPublicKey(crypto, encryptionKeySpec).failOnShutdown,
        )
      }

      "fail if EC public key not on the curve" in {
        for {
          crypto <- newCrypto
          publicKeyEcP256 <- getSigningPublicKey(
            crypto,
            SigningKeyUsage.ProtocolOnly,
            SigningKeySpec.EcP256,
          ).failOnShutdown
          // use a different curve (P-384) compared to the one on which the public key was generated (P-256)
          validationRes =
            SigningPublicKey.create(
              publicKeyEcP256.format,
              publicKeyEcP256.key,
              SigningKeySpec.EcP384,
              publicKeyEcP256.usage,
            )
        } yield validationRes.left.value.toString should include(
          s"EC key not in curve"
        )
      }

      "fail if a cached EC public key declares a different key spec" in {
        for {
          crypto <- newCrypto
          publicKeyEcP256 <- getSigningPublicKey(
            crypto,
            SigningKeyUsage.ProtocolOnly,
            SigningKeySpec.EcP256,
          ).failOnShutdown
          // caches the validation and the Java key conversion of the EC-P256 key
          signature <- crypto.privateCrypto
            .signBytes(
              ByteString.copyFromUtf8("test"),
              publicKeyEcP256.id,
              SigningKeyUsage.ProtocolOnly,
            )
            .valueOrFail("sign message")
            .failOnShutdown
          _ = crypto.pureCrypto
            .verifySignature(
              ByteString.copyFromUtf8("test"),
              publicKeyEcP256,
              signature,
              SigningKeyUsage.ProtocolOnly,
            )
            .valueOrFail("verify signature")
          // the same key bytes, and so the same fingerprint, declared as an ML-DSA-65 key
          validationRes = SigningPublicKey.create(
            publicKeyEcP256.format,
            publicKeyEcP256.key,
            SigningKeySpec.MlDsa65,
            publicKeyEcP256.usage,
          )
        } yield validationRes.left.value.toString should include(
          s"Failed to deserialize or validate ${publicKeyEcP256.format} public key"
        )
      }

      "fail if an EC public key uses a larger curve than its key spec" in {
        val kpGen = KeyPairGenerator.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
        kpGen.initialize(new ECGenParameterSpec("secp384r1"))
        val publicKeyP384 = ByteString.copyFrom(kpGen.generateKeyPair().getPublic.getEncoded)
        val validationRes =
          SigningPublicKey.create(
            DerX509Spki,
            publicKeyP384,
            SigningKeySpec.EcP256,
            SigningKeyUsage.ProtocolOnly,
          )
        validationRes.left.value.toString should include(
          "Failed to create point on curve 'secp256r1'"
        )
      }

      "fail if RSA public key is invalid" in {
        val kpGen = KeyPairGenerator.getInstance("RSA", JceSecurityProvider.bouncyCastleProvider)
        kpGen.initialize(new RSAKeyGenParameterSpec(4096, RSAKeyGenParameterSpec.F4))
        val invalidPublicKey = ByteString.copyFrom(kpGen.generateKeyPair().getPublic.getEncoded)
        // use a different modulus length
        val validationRes =
          EncryptionPublicKey.create(
            DerX509Spki,
            invalidPublicKey,
            EncryptionKeySpec.Rsa2048,
          )
        validationRes.left.value.toString should include(
          s"RSA key modulus size ${4096} does not match expected " +
            s"size ${EncryptionKeySpec.Rsa2048.keySizeInBits}"
        )
      }

      "fail if Ed25519 public key is invalid" in {
        val invalidPublicKey = ByteString.copyFrom(
          new SubjectPublicKeyInfo(
            new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519),
            Array.fill[Byte](32)(0),
          ).getEncoded
        )
        val validationRes =
          SigningPublicKey.create(
            DerX509Spki,
            invalidPublicKey,
            SigningKeySpec.EcCurve25519,
            SigningKeyUsage.ProtocolOnly,
          )
        validationRes.left.value.toString should include(
          s"invalid public key"
        )
      }

      forAll(invalidPublicKeys(supportedSigningKeySpecs, supportedEncryptionKeySpecs)) {
        invalidPublicKey =>
          s"fail on an invalid public key: ${invalidPublicKey.description}" in {
            val keyBytes = ByteString.copyFrom(invalidPublicKey.bytes)
            val acceptingSigningKeySpecs = supportedSigningKeySpecs.filter { keySpec =>
              SigningPublicKey
                .create(DerX509Spki, keyBytes, keySpec, SigningKeyUsage.ProtocolOnly)
                .isRight
            }
            val acceptingEncryptionKeySpecs = supportedEncryptionKeySpecs.filter { keySpec =>
              EncryptionPublicKey.create(DerX509Spki, keyBytes, keySpec).isRight
            }
            acceptingSigningKeySpecs shouldBe empty
            acceptingEncryptionKeySpecs shouldBe empty
          }
      }

      "fail if ML-DSA-65 public key is invalid" in {
        val kpGen = KeyPairGenerator.getInstance("MLDSA", JceSecurityProvider.bouncyCastleProvider)
        // Generate a ML-DSA-44 key and claim it's a ML-DSA-65 key
        kpGen.initialize(MLDSAParameterSpec.ml_dsa_44)
        val invalidPublicKey = ByteString.copyFrom(kpGen.generateKeyPair().getPublic.getEncoded)
        val validationRes =
          SigningPublicKey.create(
            DerX509Spki,
            invalidPublicKey,
            SigningKeySpec.MlDsa65,
            SigningKeyUsage.ProtocolOnly,
          )
        validationRes.left.value.toString should include(
          s"ML-DSA public key does not have the correct parameter spec for ML-DSA-65"
        )
      }

      "retain parsed Java keys in cache for the correct amount of time" in {
        for {
          crypto <- newCrypto
          _ = assume(
            crypto.pureCrypto.isInstanceOf[JcePureCrypto],
            "Test only runs with JcePureCrypto",
          )
          jcePureCrypto = crypto.pureCrypto.asInstanceOf[JcePureCrypto]
          privateStore = crypto.cryptoPrivateStore match {
            case extended: CryptoPrivateStoreExtended => extended
            case _ => fail("incorrect crypto private store type")
          }
          publicKey <- getSigningPublicKey(
            crypto,
            SigningKeyUsage.ProtocolOnly,
            supportedSigningKeySpecs.head,
          ).failOnShutdown
          privateKey <- privateStore
            .exportPrivateKey(publicKey.id)
            .valueOrFail("export private key")
            .failOnShutdown
          signingPrivateKey = privateKey
            .valueOrFail("no private key")
            .asInstanceOf[SigningPrivateKey]

          // indirectly converts the private key to a Java key and stores it in the cache
          signature = jcePureCrypto
            .signBytes(
              ByteString.copyFromUtf8("test"),
              signingPrivateKey,
              SigningKeyUsage.ProtocolOnly,
            )
            .valueOrFail("sign message")

          // indirectly converts the public key to a Java key and stores it in the cache
          _ = crypto.pureCrypto
            .verifySignature(
              ByteString.copyFromUtf8("test"),
              publicKey,
              signature,
              SigningKeyUsage.ProtocolOnly,
            )
            .valueOrFail("verify signature")

          inCache = jcePureCrypto.isJavaPublicKeyInCache(publicKey) &&
            jcePureCrypto.isJavaPrivateKeyInCache(publicKey.id)

          // ensure enough time has passed for the Java keys to be removed from their respective caches
          _ = Threading.sleep(javaPrivateKeyRetentionTime.underlying.toMillis + 1000)

          inPublicCache = jcePureCrypto.isJavaPublicKeyInCache(publicKey)
          inPrivateCache = jcePureCrypto.isJavaPrivateKeyInCache(publicKey.id)

        } yield {
          inCache shouldBe true
          inPublicCache shouldBe false
          inPrivateCache shouldBe false
        }
      }
    }

}

object PublicKeyValidationTest {

  /** A public key encoding that every key spec must reject.
    *
    * @param description
    *   names the key in the test name
    * @param bytes
    *   the encoding
    */
  private final case class InvalidPublicKey(description: String, bytes: Array[Byte])

  /** The invalid public keys that every supported key spec must reject.
    *
    * @param signingKeySpecs
    *   the supported signing key specs
    * @param encryptionKeySpecs
    *   the supported encryption key specs
    */
  private def invalidPublicKeys(
      signingKeySpecs: Set[SigningKeySpec],
      encryptionKeySpecs: Set[EncryptionKeySpec],
  ): Seq[InvalidPublicKey] = {
    val kpGen = KeyPairGenerator.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
    kpGen.initialize(new ECGenParameterSpec("secp256r1"))
    val validKey = kpGen.generateKeyPair().getPublic.getEncoded

    val longFormLengthOf4Bytes = (0x80 | 4).toByte

    val ecCurveNames =
      (signingKeySpecs.toSeq.collect {
        case keySpec: EcKeySpec if keySpec != SigningKeySpec.EcCurve25519 =>
          keySpec.jcaCurveName
      } ++ encryptionKeySpecs.toSeq.collect { case keySpec: EcKeySpec =>
        keySpec.jcaCurveName
      }).distinct.sorted

    val ed25519IdentityPoint = BigInt(1).toByteArray.reverse.padTo(32, 0.toByte)

    val randomBytes = new Array[Byte](validKey.length)
    new SecureRandom().nextBytes(randomBytes)

    Seq(
      InvalidPublicKey("empty", Array.emptyByteArray),
      InvalidPublicKey("random bytes", randomBytes),
      InvalidPublicKey("truncated", validKey.take(validKey.length / 2)),
      InvalidPublicKey(
        "length beyond content",
        Array[Byte](
          (BERTags.CONSTRUCTED | BERTags.SEQUENCE).toByte,
          longFormLengthOf4Bytes,
        ) ++
          BigInt(Int.MaxValue).toByteArray,
      ),
      InvalidPublicKey(
        "not a SubjectPublicKeyInfo",
        new DERSequence(new ASN1Integer(1)).getEncoded,
      ),
      InvalidPublicKey(
        "EC without curve",
        new SubjectPublicKeyInfo(
          new AlgorithmIdentifier(X9ObjectIdentifiers.id_ecPublicKey),
          SubjectPublicKeyInfo.getInstance(validKey).getPublicKeyData.getBytes,
        ).getEncoded,
      ),
      InvalidPublicKey(
        "Ed25519 identity point",
        new SubjectPublicKeyInfo(
          new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519),
          ed25519IdentityPoint,
        ).getEncoded,
      ),
    ) ++ ecCurveNames.map { curveName =>
      InvalidPublicKey(
        s"point at infinity on $curveName",
        new SubjectPublicKeyInfo(
          new AlgorithmIdentifier(
            X9ObjectIdentifiers.id_ecPublicKey,
            ECNamedCurveTable.getOID(curveName),
          ),
          ECNamedCurveTable.getByName(curveName).getCurve.getInfinity.getEncoded(false),
        ).getEncoded,
      )
    }
  }
}
