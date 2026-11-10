// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CryptoProvider.Jce
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  CryptoConfig,
  PositiveFiniteDuration,
  SessionEncryptionKeyCacheConfig,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.CryptoKeyFormat.Raw
import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.digitalasset.canton.crypto.SigningKeySpec.{EcSecp256k1, MlDsa65}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.google.protobuf.ByteString
import monocle.macros.syntax.lens.*
import org.bouncycastle.crypto.DefaultBufferedBlockCipher
import org.bouncycastle.crypto.agreement.ECDHBasicAgreement
import org.bouncycastle.crypto.digests.SHA256Digest
import org.bouncycastle.crypto.engines.{AESEngine, IESEngine}
import org.bouncycastle.crypto.generators.{
  ECKeyPairGenerator,
  EphemeralKeyPairGenerator,
  KDF2BytesGenerator,
}
import org.bouncycastle.crypto.macs.HMac
import org.bouncycastle.crypto.modes.SICBlockCipher
import org.bouncycastle.crypto.params.{
  AsymmetricKeyParameter,
  ECKeyGenerationParameters,
  ECPublicKeyParameters,
  IESWithCipherParameters,
  ParametersWithIV,
}
import org.bouncycastle.jcajce.provider.asymmetric.util.ECUtil
import org.scalatest.wordspec.AsyncWordSpec

import java.security.spec.X509EncodedKeySpec
import java.security.{KeyFactory, SecureRandom}
import java.util.Base64

class JceCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with HmacTest
    with PrivateKeySerializationTest
    with PasswordBasedEncryptionTest
    with RandomTest
    with PublicKeyValidationTest
    with PrivateKeyValidationTest
    with CryptoKeyFormatMigrationTest
    with JwksTest {

  "JceCrypto" must {

    // use a short duration to verify that a Java key is removed from the cache promptly
    lazy val javaKeyCacheDuration = PositiveFiniteDuration.ofSeconds(4)

    def jceCrypto(): FutureUnlessShutdown[Crypto] =
      Crypto
        .create(
          CryptoConfig(provider = Jce, enableExperimental = true),
          CachingConfigs.defaultKmsMetadataCache,
          SessionEncryptionKeyCacheConfig()
            .focus(_.senderCache.expireAfterTimeout)
            .replace(javaKeyCacheDuration),
          CachingConfigs.defaultPublicKeyConversionCache.copy(expireAfterAccess =
            config.NonNegativeFiniteDuration(javaKeyCacheDuration.underlying)
          ),
          new MemoryStorage(loggerFactory, timeouts),
          Option.empty[ReplicaManager],
          testedReleaseProtocolVersion,
          futureSupervisor,
          wallClock,
          CommonMockMetrics.cryptoMetrics,
          executionContext,
          timeouts,
          BatchingConfig(),
          loggerFactory,
          NoReportingTracerProvider,
          new ExecutorServiceMetrics(NoOpMetricsFactory),
        )
        .valueOrFail("failed to create crypto")

    lazy val jceCryptoF: FutureUnlessShutdown[Crypto] = jceCrypto()

    behave like migrationTest(
      // No legacy keys for secp256k1
      Jce.signingKeys.supported.filterNot(_ == EcSecp256k1),
      Jce.encryptionKeys.supported,
      jceCrypto(),
    )

    behave like signingProvider(
      Jce.signingKeys.supported,
      Jce.signingAlgorithms.supported,
      Jce.supportedSignatureFormats,
      jceCrypto(),
    )
    behave like encryptionProvider(
      Jce.encryptionAlgorithms.supported,
      Jce.symmetric.supported,
      jceCrypto(),
    )

    /* Checks that the JCE crypto provider can still symmetrically decrypt legacy ciphertexts.
     * Fixed symmetric keys and ciphertexts are used as reference values, so
     * any changes in the provider that break compatibility will be detected.
     */
    forAll(
      Jce.symmetric.supported.forgetNE
    ) { symmetricKeyScheme =>
      s"${symmetricKeyScheme.name} should decrypt a legacy symmetric encrypted ciphertext" in {
        symmetricKeyScheme match {
          case scheme @ SymmetricKeyScheme.Aes128Gcm =>
            val symmetricKey =
              ByteString.copyFrom(Base64.getDecoder.decode("VZyHjKJHGWlwZU4d+9DFLw=="))

            val message = ByteString.copyFromUtf8("foobar")
            val ciphertext =
              ByteString.copyFrom(
                Base64.getDecoder.decode("J6fAsMnQGJ+Ca+mIKKQngybDYnneySmBJUHe/PJRtX4xXQ==")
              )

            for {
              crypto <- jceCrypto()
              plaintext = crypto.pureCrypto
                .decryptWith(
                  Encrypted[ByteString].apply(ciphertext),
                  SymmetricKey.create(Raw, symmetricKey, scheme).valueOrFail("create symmetric key"),
                )(plaintext => Right(plaintext))
                .valueOrFail("symmetric decrypt")
            } yield plaintext shouldBe message
        }
      }
    }

    forAll(
      Jce.encryptionAlgorithms.supported.filter(_.supportDeterministicEncryption)
    ) { encryptionAlgorithmSpec =>
      forAll(encryptionAlgorithmSpec.supportedEncryptionKeySpecs.forgetNE) { keySpec =>
        s"Deterministic hybrid encrypt " +
          s"with $encryptionAlgorithmSpec and a $keySpec key" should {

            val newCrypto = jceCrypto()

            behave like hybridEncrypt(
              keySpec,
              (message, publicKey) =>
                newCrypto.map(crypto =>
                  crypto.pureCrypto.encryptDeterministicWith(
                    message,
                    publicKey,
                    encryptionAlgorithmSpec,
                  )
                ),
              newCrypto,
            )

            "yield the same ciphertext for the same encryption" in {
              val message = TestMessage(ByteString.copyFromUtf8("foobar"))
              for {
                crypto <- jceCrypto()
                publicKey <- getEncryptionPublicKey(crypto, keySpec)
                encrypted1 = crypto.pureCrypto
                  .encryptDeterministicWith(
                    message,
                    publicKey,
                    encryptionAlgorithmSpec,
                  )
                  .valueOrFail("encrypt")
                _ = assert(message.bytes != encrypted1.ciphertext)
                encrypted2 = crypto.pureCrypto
                  .encryptDeterministicWith(
                    message,
                    publicKey,
                    encryptionAlgorithmSpec,
                  )
                  .valueOrFail("encrypt")
                _ = assert(message.bytes != encrypted2.ciphertext)
              } yield encrypted1.ciphertext shouldEqual encrypted2.ciphertext
            }

            /* Ensures that changes in the JCE crypto provider (e.g. a different version) do not break deterministic
             * encryption. A fixed set of public keys is embedded in the code, along with the ciphertexts obtained
             * by deterministically encrypting a test string with those keys. This test compares the provider’s output
             * against these reference values. If a newer version or provider produces different ciphertexts,
             * the mismatch will be detected.
             */
            "generate the same ciphertext as the reference value" in {
              val message = TestMessage(ByteString.copyFromUtf8("foobar"))

              val (publicKeyDer, expectedCiphertext) = keySpec match {
                case EncryptionKeySpec.EcP256 =>
                  val publicKeyEcP256Base64 =
                    "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEoArYy5JnyIzu4iYdGmnLDOeDY9bSqtHDYFJFnMQXQXqfAErcD/b6ObfaM+neZcNo/ejLu5GwAZWFYMiQ5z7dQQ=="
                  val expectedCiphertextBase64 =
                    "XoQZEdUTCcf3B5xAkwgJZARfG+TrRn8hK+UbKvhhwlWx3jfgTvNwPkQDn2qs/bn4Glu0lKrb1pzofxc0ZZ6SY5JU1Mr1Q0P38ZTqaY2STz3rSew000qt8mCBS1HyJgjC0DAH4kt62NFex/xKcWjA9iktRPqaPfwgjfsnFqaaDvNk"
                  (
                    ByteString.copyFrom(Base64.getDecoder.decode(publicKeyEcP256Base64)),
                    ByteString.copyFrom(Base64.getDecoder.decode(expectedCiphertextBase64)),
                  )
                case EncryptionKeySpec.Rsa2048 =>
                  val publicKeyRsa2048 =
                    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyIv/MMU8KfhQSZZwmg++01ZT5Kc8jYO99LoMei2DhFa6tcoPbvvcEWdm6bdmdR4Leampe0ZO7RWIexK/hsSTQhFVJAPngxGB0ESicbfg858+S3q4FFixqJm1A3NGwBWWUnjUwMuJ+yQkXdaeQu4hMRtNYZ/QpptXjXIy4KbyFb6YeP+FOscY/LTAGAinfgGy4/uSeoldHDUxkcfrSbesbLOAbiHmMhB12+Ayx7X8Rh5loakqWpjrF4l+wwegT9dapDTrwKFzUUTnXMX8Y0aTooIy/JWjdSF3nG8ftOYkD8mq1uXwuevN7T8se5L+aq/rKED0AibnwSDfehaKMjmbPQIDAQAB"
                  val expectedCiphertextBase64 =
                    "wvpK3CLngaeai6QQZGvhczdzW/b8Emg6HT+NdB1lzJvLHaJnUFrG5oXn9ty/GzbYPOvWdFXVHqBfwwxircyWsXYN2+s2Q57Zzcok/nRoJ/l/PfgPi3UPy34fxxrA1n007imTNmOpkhoUu+PxfTfPbcFg01XH5sla8hg58MgkQaW5v1ahXPW0aOOq6N4JYztwu476/vqHys6wFy+m+8PHSZJoTbalbSsDUhbolPs2jkBrpbEEU0sJ6Bd1jTvNCADVsEMP+Va7rOPCYMmcIbMc0+fyKxJBm9EPHXNuO93S4pGyhrqiRiGBhFrbCCKzt93bhRanaqH+MfqkQ1d77UkaiQ=="
                  (
                    ByteString.copyFrom(Base64.getDecoder.decode(publicKeyRsa2048)),
                    ByteString.copyFrom(Base64.getDecoder.decode(expectedCiphertextBase64)),
                  )
              }
              val publicKey = EncryptionPublicKey
                .create(
                  CryptoKeyFormat.DerX509Spki,
                  publicKeyDer,
                  keySpec,
                )
                .valueOrFail("create encryption public key")
              for {
                crypto <- jceCrypto()
                encrypted = crypto.pureCrypto
                  .encryptDeterministicWith(
                    message,
                    publicKey,
                    encryptionAlgorithmSpec,
                  )
                  .valueOrFail("encrypt")
              } yield encrypted.ciphertext shouldBe expectedCiphertext
            }
          }
      }
    }

    "fail to decrypt an ECIES ciphertext with a valid MAC and a body that is not block aligned" in {
      for {
        crypto <- jceCryptoF
        publicKey <- getEncryptionPublicKey(crypto, EncryptionKeySpec.EcP256)
        ecPublicKey = ECUtil.generatePublicKeyParameter(
          KeyFactory
            .getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
            .generatePublic(new X509EncodedKeySpec(publicKey.key.toByteArray))
        ) match {
          case params: ECPublicKeyParameters => params
          case other => fail(s"Unexpected public key parameters $other")
        }
        // ECIES as in `ECIESwithSHA256andAES-CBC`, but with AES-CTR, so that the body can be
        // 17 bytes long while the key derivation and the MAC stay the same
        engine = new IESEngine(
          new ECDHBasicAgreement(),
          new KDF2BytesGenerator(new SHA256Digest()),
          new HMac(new SHA256Digest()),
          new DefaultBufferedBlockCipher(SICBlockCipher.newInstance(AESEngine.newInstance())),
        )
        keyPairGenerator = new ECKeyPairGenerator()
        _ = keyPairGenerator.init(
          new ECKeyGenerationParameters(ecPublicKey.getParameters, new SecureRandom())
        )
        jcePureCrypto = crypto.pureCrypto match {
          case jcePureCrypto: JcePureCrypto => jcePureCrypto
          case other => fail(s"Unexpected pure crypto $other")
        }
        iv = new Array[Byte](jcePureCrypto.EciesHmacSha256Aes128CbcParams.ivSizeForAesCbcInBytes)
        parameterSpec = jcePureCrypto.EciesHmacSha256Aes128CbcParams.parameterSpec(iv)
        _ = engine.init(
          ecPublicKey,
          new ParametersWithIV(
            new IESWithCipherParameters(
              parameterSpec.getDerivationV,
              parameterSpec.getEncodingV,
              parameterSpec.getMacKeySize,
              parameterSpec.getCipherKeySize,
            ),
            iv,
          ),
          new EphemeralKeyPairGenerator(
            keyPairGenerator,
            (ephemeralKey: AsymmetricKeyParameter) =>
              ephemeralKey match {
                case ecKey: ECPublicKeyParameters => ecKey.getQ.getEncoded(false)
                case other => fail(s"Unexpected ephemeral public key $other")
              },
          ),
        )
        // one byte longer than an AES block, so not block aligned
        body = new Array[Byte](AESEngine.newInstance().getBlockSize + 1)
        ciphertext = engine.processBlock(body, 0, body.length)
        encrypted = new AsymmetricEncrypted[ByteString](
          ByteString.copyFrom(iv ++ ciphertext),
          EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
          publicKey.id,
        )
        result <- crypto.privateCrypto
          .decrypt(encrypted)(bytes => Right(bytes))
          .value
      } yield inside(result.left.value) { case DecryptionError.FailedToDecrypt(error) =>
        // the MAC is valid, so the decryption fails on the length of the body
        error should include("DataLengthException")
      }
    }.failOnShutdown

    forAll(Jce.signingAlgorithms.supported.forgetNE) { algorithmSpec =>
      forAll(algorithmSpec.supportedSigningKeySpecs.forgetNE) { keySpec =>
        s"fail to verify an empty signature with $algorithmSpec and a $keySpec key" in {
          val message = ByteString.copyFromUtf8("test")
          for {
            crypto <- jceCryptoF
            publicKey <- getSigningPublicKey(crypto, SigningKeyUsage.ProtocolOnly, keySpec)
            signature <- crypto.privateCrypto
              .signBytes(message, publicKey.id, SigningKeyUsage.ProtocolOnly, algorithmSpec)
              .valueOrFail("sign message")
            emptySignature = Signature.create(
              signature.format,
              ByteString.EMPTY,
              signature.signedBy,
              signature.signingAlgorithmSpec,
            )
          } yield forAll(Seq(message, ByteString.EMPTY)) { bytes =>
            crypto.pureCrypto
              .verifySignature(bytes, publicKey, emptySignature, SigningKeyUsage.ProtocolOnly)
              .left
              .value shouldBe a[SignatureCheckError]
          }
        }.failOnShutdown
      }
    }

    forAll(Jce.encryptionAlgorithms.supported.forgetNE) { algorithmSpec =>
      forAll(algorithmSpec.supportedEncryptionKeySpecs.forgetNE) { keySpec =>
        s"fail to decrypt an empty ciphertext with $algorithmSpec and a $keySpec key" in {
          for {
            crypto <- jceCryptoF
            publicKey <- getEncryptionPublicKey(crypto, keySpec)
            result <- crypto.privateCrypto
              .decrypt(
                new AsymmetricEncrypted[ByteString](ByteString.EMPTY, algorithmSpec, publicKey.id)
              )(bytes => Right(bytes))
              .value
          } yield result.left.value shouldBe a[DecryptionError]
        }.failOnShutdown
      }
    }

    forAll(Jce.symmetric.supported.forgetNE) { scheme =>
      s"fail to decrypt an empty symmetric ciphertext with ${scheme.name}" in {
        for {
          crypto <- jceCryptoF
          key = crypto.pureCrypto.generateSymmetricKey(scheme).valueOrFail("generate key")
        } yield crypto.pureCrypto
          .decryptWith(Encrypted[ByteString](ByteString.EMPTY), key)(bytes => Right(bytes))
          .left
          .value shouldBe a[DecryptionError]
      }.failOnShutdown
    }

    behave like privateKeySerializerProvider(
      Jce.signingKeys.supported,
      Jce.encryptionKeys.supported,
      jceCrypto(),
    )

    behave like hmacProvider(
      Set(HmacAlgorithm.HmacSha256),
      jceCrypto().map(_.pureCrypto),
    )

    behave like randomnessProvider(jceCrypto().map(_.pureCrypto))

    behave like pbeProvider(
      Jce.pbkdf.valueOrFail("no PBKDF schemes configured").supported,
      Jce.symmetric.supported,
      jceCrypto().map(_.pureCrypto),
    )

    behave like publicKeyValidationProvider(
      Jce.signingKeys.supported,
      Jce.encryptionKeys.supported,
      Jce.supportedCryptoKeyFormats,
      jceCrypto().failOnShutdown,
      javaKeyCacheDuration,
    )

    behave like privateKeyValidationProvider(
      Jce.signingKeys.supported,
      Jce.encryptionKeys.supported,
      Jce.supportedCryptoKeyFormats,
      jceCrypto().failOnShutdown,
    )

    behave like jwksProvider(
      supportedSigningKeySpecs = Jce.signingKeys.supported.filter(_ != MlDsa65),
      unsupportedSigningKeySpecs = Set(MlDsa65),
      newCrypto = jceCrypto(),
    )
  }
}
