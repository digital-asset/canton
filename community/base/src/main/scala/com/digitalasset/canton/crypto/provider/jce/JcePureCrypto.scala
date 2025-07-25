// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config
import com.digitalasset.canton.config.{
  CacheConfig,
  CryptoConfig,
  CryptoProvider,
  SessionEncryptionKeyCacheConfig,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.CryptoPureApiError.KeyParseAndValidateError
import com.digitalasset.canton.crypto.deterministic.encryption.DeterministicRandom
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  DeterministicEncoding,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, ShowUtil}
import com.digitalasset.canton.version.HasToByteString
import com.github.blemale.scaffeine.Cache
import com.google.common.annotations.VisibleForTesting
import com.google.crypto.tink.hybrid.subtle.AeadOrDaead
import com.google.crypto.tink.internal.EllipticCurvesUtil
import com.google.crypto.tink.subtle.*
import com.google.crypto.tink.subtle.EllipticCurves.EcdsaEncoding
import com.google.crypto.tink.subtle.Enums.HashType
import com.google.crypto.tink.{Aead, PublicKeySign, PublicKeyVerify}
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.ASN1OctetString
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.crypto.DataLengthException
import org.bouncycastle.crypto.generators.Argon2BytesGenerator
import org.bouncycastle.crypto.params.Argon2Parameters
import org.bouncycastle.jcajce.provider.asymmetric.edec.BCEdDSAPublicKey
import org.bouncycastle.jce.spec.IESParameterSpec

import java.security.interfaces.*
import java.security.{
  GeneralSecurityException,
  PrivateKey as JPrivateKey,
  PublicKey as JPublicKey,
  SecureRandom,
  Security,
  Signature as JSignature,
}
import javax.crypto.Cipher
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object JceSecureRandom {

  /** Uses [[ThreadLocal]] here to reduce contention and improve performance. */
  private[crypto] val random: ThreadLocal[SecureRandom] = new ThreadLocal[SecureRandom] {
    override def initialValue(): SecureRandom = newSecureRandom()
  }

  private def newSecureRandom() = {
    val rand = new SecureRandom()
    rand.nextLong()
    rand
  }

  private[jce] def generateRandomBytes(length: Int): Array[Byte] = {
    val randBytes = new Array[Byte](length)
    random.get().nextBytes(randBytes)
    randBytes
  }
}

/** @param publicKeyConversionCacheConfig
  *   the configuration to use for the Java public key conversion cache.
  * @param privateKeyConversionCacheTtl
  *   the eviction timeout for the Java private key conversion cache.
  */
class JcePureCrypto(
    override val defaultSymmetricKeyScheme: SymmetricKeyScheme,
    override val signingAlgorithmSpecs: CryptoScheme[SigningAlgorithmSpec],
    override val encryptionAlgorithmSpecs: CryptoScheme[EncryptionAlgorithmSpec],
    override val defaultHashAlgorithm: HashAlgorithm,
    override val defaultPbkdfScheme: PbkdfScheme,
    publicKeyConversionCacheConfig: CacheConfig,
    privateKeyConversionCacheTtl: Option[FiniteDuration],
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends CryptoPureApi
    with ShowUtil
    with NamedLogging {

  // Caches for the java key conversion results
  private val javaPublicKeyCache: Cache[Fingerprint, Either[KeyParseAndValidateError, JPublicKey]] =
    publicKeyConversionCacheConfig
      .buildScaffeine()
      // allow the JVM garbage collector to remove entries from it when there is pressure on memory
      .softValues()
      .build()
  // We must ensure that private key conversions are retained in memory no longer than the session
  // encryption/signing keys. We store an `Either` because for Ed25519 the parsing is done directly
  // into the raw key byte representation, not into a JPrivateKey, since the Tink signing primitive
  // `Ed25519Sign` expects a raw key.
  private val javaPrivateKeyCache
      : Cache[Fingerprint, Either[KeyParseAndValidateError, Either[ByteString, JPrivateKey]]] =
    (privateKeyConversionCacheTtl match {
      case Some(expire) =>
        publicKeyConversionCacheConfig.copy(
          expireAfterAccess = config.NonNegativeFiniteDuration(expire)
        )
      case None => publicKeyConversionCacheConfig
    }).buildScaffeine()
      // allow the JVM garbage collector to remove entries from it when there is pressure on memory
      .softValues()
      .build()

  @VisibleForTesting
  private[crypto] def isJavaPublicKeyInCache(keyId: Fingerprint): Boolean =
    javaPublicKeyCache.getIfPresent(keyId).exists(_.isRight)

  @VisibleForTesting
  private[crypto] def isJavaPrivateKeyInCache(keyId: Fingerprint): Boolean =
    javaPrivateKeyCache.getIfPresent(keyId).exists(_.isRight)

  /* security parameters for EciesP256HmacSha256Aes128Cbc encryption scheme,
    in particular for the HMAC and symmetric crypto algorithms.
   */
  private object EciesHmacSha256Aes128CbcParams {
    // the internal jce designation for this scheme
    val jceInternalName: String = "ECIESwithSHA256andAES-CBC"
    // the key size in bits for HMACSHA256 is 64bytes (recommended)
    private val macKeySizeInBits: Int = 512
    // the key size in bits for AES-128-CBC is 16 bytes
    private val cipherKeySizeInBits: Int = 128
    // the IV for AES-128-CBC is 16 bytes
    val ivSizeForAesCbcInBytes: Int = 16
    // the parameter specification for this scheme.
    def parameterSpec(iv: Array[Byte]): IESParameterSpec = new IESParameterSpec(
      // we do not use any encoding or derivation vector for the KDF.
      Array[Byte](),
      Array[Byte](),
      macKeySizeInBits,
      cipherKeySizeInBits,
      iv,
    )
  }

  private object RsaOaepSha256Params {
    // the internal jce designation for this scheme
    val jceInternalName: String = "RSA/NONE/OAEPWithSHA256AndMGF1Padding"
  }

  /** Converts a public key to a java public key. We store the deserialization result in a cache.
    *
    * @return
    *   Either an error or the converted java private key
    */
  private def toJavaPublicKey[E, T <: JPublicKey](
      publicKey: PublicKey,
      typeMatcher: PartialFunction[JPublicKey, Either[E, T]],
      errFn: String => E,
  ): Either[E, T] =
    for {
      publicKey <- javaPublicKeyCache
        .get(
          publicKey.id,
          _ =>
            JceJavaKeyConverter
              .toJava(publicKey)
              .leftMap(err =>
                KeyParseAndValidateError(s"Failed to convert public key to java key: $err")
              ),
        )
        .leftMap(err => errFn(s"Failed to deserialize ${publicKey.format} public key: $err"))
      checkedPublicKey <- typeMatcher(publicKey)
    } yield checkedPublicKey

  /** Parses and converts a private key. We store the deserialization result in a cache.
    *
    * For Ed25519 private keys, the raw key bytes are extracted directly from the PKCS #8 encoding
    * because Tink's `Ed25519Sign` primitive expects raw private keys rather than java private keys.
    * For other key types, the private key is converted to a JPrivateKey.
    *
    * @return
    *   Either an error or the converted key
    */
  private def parseAndGetPrivateKey[E, T](
      privateKey: PrivateKey,
      checker: PartialFunction[Either[ByteString, JPrivateKey], Either[E, T]],
      errFn: String => E,
  ): Either[E, T] =
    for {
      privateKey <- javaPrivateKeyCache
        .get(
          privateKey.id,
          _ =>
            privateKey match {
              case ed25519Key @ SigningPrivateKey(_, _, _, SigningKeySpec.EcCurve25519, _) =>
                Either
                  .catchOnly[IllegalArgumentException] {
                    val privateKeyInfo = PrivateKeyInfo.getInstance(ed25519Key.key.toByteArray)
                    Left[ByteString, JPrivateKey](
                      ByteString.copyFrom(
                        ASN1OctetString
                          .getInstance(privateKeyInfo.getPrivateKey.getOctets)
                          .getOctets
                      )
                    )
                  }
                  .leftMap(err =>
                    KeyParseAndValidateError(show"Failed to parse PKCS #8 format: $err")
                  )
              case _ =>
                JceJavaKeyConverter
                  .toJava(privateKey)
                  .map(Right[ByteString, JPrivateKey])
                  .leftMap(err =>
                    KeyParseAndValidateError(
                      s"Failed to convert private key to java key: ${err.show}"
                    )
                  )
            },
        )
        .leftMap(err => errFn(s"Failed to deserialize ${privateKey.format} private key: $err"))
      checkedPrivateKey <- checker(privateKey)
    } yield checkedPrivateKey

  private def encryptAes128Gcm(
      plaintext: ByteString,
      symmetricKey: ByteString,
  ): Either[EncryptionError, ByteString] =
    for {
      encrypter <- Either
        .catchOnly[GeneralSecurityException](new AesGcmJce(symmetricKey.toByteArray))
        .leftMap(err => EncryptionError.InvalidSymmetricKey(err.toString))
      ciphertext <- Either
        .catchOnly[GeneralSecurityException](
          encrypter.encrypt(plaintext.toByteArray, Array[Byte]())
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(err.toString))
    } yield ByteString.copyFrom(ciphertext)

  private def decryptAes128Gcm(
      ciphertext: ByteString,
      symmetricKey: ByteString,
  ): Either[DecryptionError, ByteString] =
    for {
      decrypter <- Either
        .catchOnly[GeneralSecurityException](new AesGcmJce(symmetricKey.toByteArray))
        .leftMap(err => DecryptionError.InvalidSymmetricKey(err.toString))
      plaintext <- Either
        .catchOnly[GeneralSecurityException](
          decrypter.decrypt(ciphertext.toByteArray, Array[Byte]())
        )
        .leftMap(err => DecryptionError.FailedToDecrypt(err.toString))
    } yield ByteString.copyFrom(plaintext)

  // Internal helper class for the symmetric encryption as part of the hybrid encryption scheme.
  private object Aes128GcmDemHelper extends EciesAeadHkdfDemHelper {

    override def getSymmetricKeySizeInBytes: Int = SymmetricKeyScheme.Aes128Gcm.keySizeInBytes

    override def getAeadOrDaead(symmetricKeyValue: Array[Byte]): AeadOrDaead = new AeadOrDaead(
      new Aead {
        override def encrypt(plaintext: Array[Byte], associatedData: Array[Byte]): Array[Byte] = {
          val encrypter = new AesGcmJce(symmetricKeyValue)
          encrypter.encrypt(plaintext, associatedData)
        }

        override def decrypt(ciphertext: Array[Byte], associatedData: Array[Byte]): Array[Byte] = {
          val decrypter = new AesGcmJce(symmetricKeyValue)
          decrypter.decrypt(ciphertext, associatedData)
        }
      }
    )
  }

  /** Produces an EC-DSA signature with the given private signing key.
    *
    * NOTE: `signingKey` must be an EC-DSA signing key, not an Ed-DSA key.
    */
  private def ecDsaSigner(
      signingKey: SigningPrivateKey,
      hashType: HashType,
  )(implicit traceContext: TraceContext): Either[SigningError, PublicKeySign] =
    for {
      ecPrivateKey <- parseAndGetPrivateKey(
        signingKey,
        { case Right(k: ECPrivateKey) => Right(k) },
        SigningError.InvalidSigningKey.apply,
      )
      signer <- {
        signingKey.keySpec match {
          case SigningKeySpec.EcP256 | SigningKeySpec.EcP384 =>
            Either
              .catchOnly[GeneralSecurityException](
                new EcdsaSignJce(ecPrivateKey, hashType, EcdsaEncoding.DER)
              )
              .leftMap(err =>
                SigningError.InvalidSigningKey(show"Failed to get signer for EC-DSA: $err")
              )
          case SigningKeySpec.EcSecp256k1 =>
            // Use BC and not Tink as Tink rejects keys from the curve secp256k1
            Right {
              new PublicKeySign {
                override def sign(data: Array[Byte]): Array[Byte] = {
                  val signer = JSignature.getInstance(
                    "SHA256withECDSA",
                    JceSecurityProvider.bouncyCastleProvider,
                  )
                  signer.initSign(ecPrivateKey)
                  signer.update(data)
                  signer.sign()
                }
              }
            }

          case SigningKeySpec.EcCurve25519 =>
            ErrorUtil.invalidArgument(
              s"Private key ${signingKey.id} must be EC-DSA but is for Ed-DSA."
            )
        }
      }
    } yield signer

  /** Verifies an EC-DSA signature with the given public signing key. Only supports signatures
    * encoded as DER.
    *
    * NOTE: `publicKey` must be an EC-DSA public key, not an Ed-DSA key.
    */
  private def ecDsaVerifier(
      publicKey: SigningPublicKey,
      hashType: HashType,
  )(implicit traceContext: TraceContext): Either[SignatureCheckError, PublicKeyVerify] =
    for {
      ecPublicKey <- toJavaPublicKey(
        publicKey,
        { case k: ECPublicKey => Right(k) },
        SignatureCheckError.InvalidKeyError.apply,
      )
      verifier <- {
        publicKey.keySpec match {
          case SigningKeySpec.EcP256 | SigningKeySpec.EcP384 =>
            Either
              .catchOnly[GeneralSecurityException](
                new EcdsaVerifyJce(ecPublicKey, hashType, EcdsaEncoding.DER)
              )
              .leftMap(err =>
                SignatureCheckError.InvalidKeyError(s"Failed to get verifier for EC-DSA: $err")
              )

          case SigningKeySpec.EcSecp256k1 =>
            // Use BC and not Tink as Tink rejects keys from the curve secp256k1
            for {
              _ <- Either
                .catchOnly[GeneralSecurityException](
                  EllipticCurvesUtil
                    .checkPointOnCurve(ecPublicKey.getW, ecPublicKey.getParams.getCurve)
                )
                .leftMap(err =>
                  SignatureCheckError.InvalidKeyError(
                    s"EC point of public key ${publicKey.id} is not on curve `secp256k1`: $err"
                  )
                )
            } yield {
              new PublicKeyVerify {
                override def verify(signature: Array[Byte], data: Array[Byte]): Unit = {
                  // Ensure signature is in DER
                  if (!EllipticCurves.isValidDerEncoding(signature))
                    throw new GeneralSecurityException("Invalid signature")

                  val verifier = JSignature.getInstance(
                    "SHA256withECDSA",
                    JceSecurityProvider.bouncyCastleProvider,
                  )
                  verifier.initVerify(ecPublicKey)
                  verifier.update(data)
                  val verified = verifier.verify(signature)
                  if (!verified) throw new GeneralSecurityException("Invalid signature")
                }
              }
            }

          case SigningKeySpec.EcCurve25519 =>
            ErrorUtil.invalidArgument(
              s"Public key ${publicKey.id} must be EC-DSA but is for Ed-DSA."
            )
        }

      }
    } yield verifier

  private def edDsaSigner(signingKey: SigningPrivateKey): Either[SigningError, PublicKeySign] =
    for {
      edPrivateKey <- parseAndGetPrivateKey(
        signingKey,
        { case Left(k) => Right(k) },
        SigningError.InvalidSigningKey.apply,
      )
      signer <- Either
        .catchOnly[GeneralSecurityException](new Ed25519Sign(edPrivateKey.toByteArray))
        .leftMap(err =>
          SigningError.InvalidSigningKey(show"Failed to get signer for Ed25519: $err")
        )
    } yield signer

  private def edDsaVerifier(
      publicKey: SigningPublicKey
  ): Either[SignatureCheckError, PublicKeyVerify] =
    for {
      ed25519PublicKey <- toJavaPublicKey(
        publicKey,
        { case k: BCEdDSAPublicKey => Right(k) },
        SignatureCheckError.InvalidKeyError.apply,
      )
      verifier <- Either
        .catchOnly[GeneralSecurityException] {
          new PublicKeyVerify {
            override def verify(signature: Array[Byte], data: Array[Byte]): Unit = {
              val verifier = JSignature.getInstance(
                "Ed25519",
                JceSecurityProvider.bouncyCastleProvider,
              )
              verifier.initVerify(ed25519PublicKey)
              verifier.update(data)
              val verified = verifier.verify(signature)
              if (!verified) throw new GeneralSecurityException("Invalid signature")
            }
          }
        }
        .leftMap(err =>
          SignatureCheckError.InvalidKeyError(show"Failed to get verifier for Ed25519: $err")
        )
    } yield verifier

  override def generateSymmetricKey(
      scheme: SymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey] =
    scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        val key128 = generateRandomByteString(scheme.keySizeInBytes)
        Right(SymmetricKey(CryptoKeyFormat.Raw, key128, scheme))
    }

  override def createSymmetricKey(
      bytes: SecureRandomness,
      scheme: SymmetricKeyScheme,
  ): Either[EncryptionKeyCreationError, SymmetricKey] = {
    val randomnessLength = bytes.unwrap.size()
    val keyLength = scheme.keySizeInBytes

    for {
      _ <- Either.cond(
        randomnessLength == keyLength,
        (),
        EncryptionKeyCreationError.InvalidRandomnessLength(randomnessLength, keyLength),
      )
      key = scheme match {
        case SymmetricKeyScheme.Aes128Gcm =>
          SymmetricKey(CryptoKeyFormat.Raw, bytes.unwrap, scheme)
      }
    } yield key
  }

  override protected[crypto] def signBytes(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec = signingAlgorithmSpecs.default,
  )(implicit traceContext: TraceContext): Either[SigningError, Signature] = {

    def signWithSigner(signer: PublicKeySign): Either[SigningError, Signature] =
      Either
        .catchOnly[GeneralSecurityException](signer.sign(bytes.toByteArray))
        .bimap(
          err => SigningError.FailedToSign(show"$err"),
          signatureBytes =>
            Signature.create(
              SignatureFormat.fromSigningAlgoSpec(signingAlgorithmSpec),
              ByteString.copyFrom(signatureBytes),
              signingKey.id,
              Some(signingAlgorithmSpec),
            ),
        )

    for {
      _ <- CryptoKeyValidation
        .ensureUsage(
          usage,
          signingKey.usage,
          signingKey.id,
          SigningError.InvalidKeyUsage.apply,
        )
      _ <- CryptoKeyValidation.ensureFormat(
        signingKey.format,
        Set(CryptoKeyFormat.DerPkcs8Pki),
        SigningError.UnsupportedKeyFormat.apply,
      )
      algoSpec <- CryptoKeyValidation
        .selectSigningAlgorithmSpec(
          signingKey.keySpec,
          signingAlgorithmSpec,
          signingAlgorithmSpecs.allowed,
          algorithmSpec =>
            SigningError.UnsupportedAlgorithmSpec(algorithmSpec, signingAlgorithmSpecs.allowed),
        )
      _ <- CryptoKeyValidation.ensureCryptoSpec(
        signingKey.keySpec,
        signingAlgorithmSpec,
        signingAlgorithmSpec.supportedSigningKeySpecs,
        signingAlgorithmSpecs.allowed,
        SigningError.KeyAlgoSpecsMismatch(_, signingAlgorithmSpec, _),
        SigningError.UnsupportedAlgorithmSpec.apply,
      )
      signer <- algoSpec match {
        case SigningAlgorithmSpec.Ed25519 => edDsaSigner(signingKey)
        case SigningAlgorithmSpec.EcDsaSha256 => ecDsaSigner(signingKey, HashType.SHA256)
        case SigningAlgorithmSpec.EcDsaSha384 => ecDsaSigner(signingKey, HashType.SHA384)
      }
      signature <- signWithSigner(signer)
    } yield signature
  }

  override def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): Either[SignatureCheckError, Unit] = {

    def verify(verifier: PublicKeyVerify): Either[SignatureCheckError, Unit] =
      Either
        .catchOnly[GeneralSecurityException](
          verifier.verify(signature.unwrap.toByteArray, bytes.toByteArray)
        )
        .leftMap(err =>
          SignatureCheckError
            .InvalidSignature(signature, bytes, s"Failed to verify signature: $err")
        )

    for {
      _ <- Either.cond(
        signature.signedBy == publicKey.id,
        (),
        SignatureCheckError.SignatureWithWrongKey(
          s"Signature signed by ${signature.signedBy} instead of ${publicKey.id}"
        ),
      )

      /* To ensure backwards compatibility and handle signatures that lack a 'signingAlgorithmSpec',
       * we check the key specification and derive the algorithm based on it. This approach works
       * because there is currently a one-to-one mapping between key and algorithm specifications.
       * If this one-to-one mapping is ever broken, this derivation must be revisited.
       */
      signingAlgorithmSpec <- signature.signingAlgorithmSpec match {
        case Some(spec) => Right(spec)
        case None =>
          signingAlgorithmSpecs.allowed
            .find(_.supportedSigningKeySpecs.contains(publicKey.keySpec))
            .toRight(
              SignatureCheckError
                .NoMatchingAlgorithmSpec(
                  "No matching algorithm spec for key spec " + publicKey.keySpec
                )
            )
      }

      _ <- CryptoKeyValidation.ensureUsage(
        usage,
        publicKey.usage,
        publicKey.id,
        SignatureCheckError.InvalidKeyUsage.apply,
      )
      _ <- CryptoKeyValidation.ensureFormat(
        publicKey.format,
        Set(CryptoKeyFormat.DerX509Spki),
        SignatureCheckError.UnsupportedKeyFormat.apply,
      )
      _ <- CryptoKeyValidation.ensureSignatureFormat(
        signature.format,
        signingAlgorithmSpec.supportedSignatureFormats,
        SignatureCheckError.UnsupportedSignatureFormat.apply,
      )
      _ <- CryptoKeyValidation.ensureCryptoSpec(
        publicKey.keySpec,
        signingAlgorithmSpec,
        signingAlgorithmSpec.supportedSigningKeySpecs,
        signingAlgorithmSpecs.allowed,
        SignatureCheckError.KeyAlgoSpecsMismatch(_, signingAlgorithmSpec, _),
        SignatureCheckError.UnsupportedAlgorithmSpec.apply,
      )
      verifier <- signingAlgorithmSpec match {
        case SigningAlgorithmSpec.Ed25519 => edDsaVerifier(publicKey)
        case SigningAlgorithmSpec.EcDsaSha256 => ecDsaVerifier(publicKey, HashType.SHA256)
        case SigningAlgorithmSpec.EcDsaSha384 => ecDsaVerifier(publicKey, HashType.SHA384)
      }
      _ <- verify(verifier)
    } yield ()
  }

  private def checkEcKeyInCurve[K <: ECKey](key: K, keyId: Fingerprint): Either[String, K] = {
    val curve = EllipticCurves.getNistP256Params.getCurve
    Either.cond(
      key.getParams.getCurve.equals(curve),
      key,
      s"EC key $keyId is not a key in curve $curve",
    )
  }

  private def checkRsaKeySize[K <: RSAKey](
      key: K,
      keyId: Fingerprint,
      size: Int,
  ): Either[String, K] = {
    val keySizeInBits = key.getModulus.bitLength()
    Either.cond(
      keySizeInBits == size,
      key,
      s"RSA key $keyId does not have the correct size. " +
        s"Expected: $size but got: $keySizeInBits.",
    )
  }

  private def encryptWithEciesP256HmacSha256Aes128Gcm[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    for {
      ecPublicKey <- toJavaPublicKey(
        publicKey,
        { case k: ECPublicKey => Right(k) },
        EncryptionError.InvalidEncryptionKey.apply,
      )
      encrypter <- Either
        .catchOnly[GeneralSecurityException](
          new EciesAeadHkdfHybridEncrypt(
            ecPublicKey,
            Array[Byte](),
            "HmacSha256",
            EllipticCurves.PointFormatType.UNCOMPRESSED,
            Aes128GcmDemHelper,
          )
        )
        .leftMap(err => EncryptionError.InvalidEncryptionKey(ErrorUtil.messageWithStacktrace(err)))
      ciphertext <- Either
        .catchOnly[GeneralSecurityException](
          encrypter
            .encrypt(
              message.toByteString.toByteArray,
              Array[Byte](),
            )
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(ErrorUtil.messageWithStacktrace(err)))
      encrypted = new AsymmetricEncrypted[M](
        ByteString.copyFrom(ciphertext),
        EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm,
        publicKey.fingerprint,
      )
    } yield encrypted

  private def encryptWithEciesP256HmacSha256Aes128Cbc[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      random: SecureRandom,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    for {
      ecPublicKey <- toJavaPublicKey(
        publicKey,
        { case k: ECPublicKey => Right(k) },
        EncryptionError.InvalidEncryptionKey.apply,
      )
      /* this encryption scheme makes use of AES-128-CBC as a DEM (Data Encapsulation Method)
       * and therefore we need to generate a IV/nonce of 16bytes as the IV for CBC mode.
       */
      iv = new Array[Byte](EciesHmacSha256Aes128CbcParams.ivSizeForAesCbcInBytes)
      _ = random.nextBytes(iv)
      encrypter <- Either
        .catchOnly[GeneralSecurityException] {
          val cipher = Cipher
            .getInstance(
              EciesHmacSha256Aes128CbcParams.jceInternalName,
              JceSecurityProvider.bouncyCastleProvider,
            )
          cipher.init(
            Cipher.ENCRYPT_MODE,
            ecPublicKey,
            EciesHmacSha256Aes128CbcParams.parameterSpec(iv),
            random,
          )
          cipher
        }
        .leftMap(err => EncryptionError.InvalidEncryptionKey(ErrorUtil.messageWithStacktrace(err)))
      ciphertext <- Either
        .catchOnly[GeneralSecurityException](
          encrypter.doFinal(message.toByteString.toByteArray)
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(ErrorUtil.messageWithStacktrace(err)))
    } yield new AsymmetricEncrypted[M](
      /* Prepend our IV to the ciphertext. On contrary to the Tink library, BouncyCastle's
       * ECIES encryption does not deal with the AES IV by itself and we have to randomly generate it and
       * manually prepend it to the ciphertext.
       */
      ByteString.copyFrom(iv ++ ciphertext),
      EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
      publicKey.fingerprint,
    )

  private def encryptWithRSAOaepSha256[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      random: SecureRandom,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    for {
      rsaPublicKey <- toJavaPublicKey(
        publicKey,
        { case k: RSAPublicKey =>
          for {
            size <- publicKey.keySpec match {
              case EncryptionKeySpec.Rsa2048 => Right(EncryptionKeySpec.Rsa2048.keySizeInBits)
              case wrongKeySpec =>
                Left(
                  EncryptionError.InvalidEncryptionKey(
                    s"Expected a ${EncryptionKeySpec.Rsa2048} public key, but got a $wrongKeySpec public key instead"
                  )
                )
            }
            key <- checkRsaKeySize(k, publicKey.id, size).leftMap(err =>
              EncryptionError.InvalidEncryptionKey(err)
            )
          } yield key
        },
        EncryptionError.InvalidEncryptionKey.apply,
      )
      encrypter <- Either
        .catchOnly[GeneralSecurityException] {
          val cipher = Cipher
            .getInstance(
              RsaOaepSha256Params.jceInternalName,
              JceSecurityProvider.bouncyCastleProvider,
            )
          cipher.init(Cipher.ENCRYPT_MODE, rsaPublicKey, random)
          cipher
        }
        .leftMap(err => EncryptionError.InvalidEncryptionKey(ErrorUtil.messageWithStacktrace(err)))
      ciphertext <- Either
        .catchOnly[GeneralSecurityException](
          encrypter.doFinal(message.toByteString.toByteArray)
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(ErrorUtil.messageWithStacktrace(err)))
    } yield new AsymmetricEncrypted[M](
      ByteString.copyFrom(ciphertext),
      EncryptionAlgorithmSpec.RsaOaepSha256,
      publicKey.fingerprint,
    )

  override def encryptWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = encryptionAlgorithmSpecs.default,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    CryptoKeyValidation
      .selectEncryptionAlgorithmSpec(
        publicKey.keySpec,
        encryptionAlgorithmSpec,
        encryptionAlgorithmSpecs.allowed,
        algorithmSpec =>
          EncryptionError
            .UnsupportedAlgorithmSpec(algorithmSpec, encryptionAlgorithmSpecs.allowed),
      )
      .flatMap {
        case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm =>
          encryptWithEciesP256HmacSha256Aes128Gcm(
            message,
            publicKey,
          )
        case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
          encryptWithEciesP256HmacSha256Aes128Cbc(
            message,
            publicKey,
            JceSecureRandom.random.get(),
          )
        case EncryptionAlgorithmSpec.RsaOaepSha256 =>
          encryptWithRSAOaepSha256(
            message,
            publicKey,
            JceSecureRandom.random.get(),
          )
      }

  override def encryptDeterministicWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = encryptionAlgorithmSpecs.default,
  )(implicit traceContext: TraceContext): Either[EncryptionError, AsymmetricEncrypted[M]] =
    CryptoKeyValidation.selectEncryptionAlgorithmSpec(
      publicKey.keySpec,
      encryptionAlgorithmSpec,
      encryptionAlgorithmSpecs.allowed,
      algorithmSpec =>
        EncryptionError.UnsupportedAlgorithmSpec(algorithmSpec, encryptionAlgorithmSpecs.allowed),
    ) match {
      case Right(spec) if !spec.supportDeterministicEncryption =>
        Left(
          EncryptionError.UnsupportedSchemeForDeterministicEncryption(
            s"$spec does not support deterministic asymmetric/hybrid encryption"
          )
        )
      case Right(scheme) =>
        lazy val deterministicRandomGenerator = DeterministicRandom.getDeterministicRandomGenerator(
          message.toByteString,
          publicKey.fingerprint,
          loggerFactory,
        )

        scheme match {
          case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm =>
            Left(
              EncryptionError.UnsupportedSchemeForDeterministicEncryption(
                s"${EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm.name} does not support deterministic asymmetric/hybrid encryption"
              )
            )
          case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
            encryptWithEciesP256HmacSha256Aes128Cbc(
              message,
              publicKey,
              deterministicRandomGenerator,
            )
          case EncryptionAlgorithmSpec.RsaOaepSha256 =>
            encryptWithRSAOaepSha256(
              message,
              publicKey,
              deterministicRandomGenerator,
            )
        }
      case Left(err) => Left(err)
    }

  override protected[crypto] def decryptWithInternal[M](
      encrypted: AsymmetricEncrypted[M],
      privateKey: EncryptionPrivateKey,
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] = {
    CryptoKeyValidation
      .ensureCryptoSpec(
        privateKey.keySpec,
        encrypted.encryptionAlgorithmSpec,
        encrypted.encryptionAlgorithmSpec.supportedEncryptionKeySpecs,
        encryptionAlgorithmSpecs.allowed,
        DecryptionError.KeyAlgoSpecsMismatch(_, encrypted.encryptionAlgorithmSpec, _),
        DecryptionError.UnsupportedAlgorithmSpec.apply,
      )
      .flatMap { _ =>
        encrypted.encryptionAlgorithmSpec match {
          case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm =>
            for {
              ecPrivateKey <- parseAndGetPrivateKey(
                privateKey,
                // TODO(#26423): Remove once private key validation is implemented
                { case Right(k: ECPrivateKey) =>
                  checkEcKeyInCurve(k, privateKey.id)
                    .leftMap(err => DecryptionError.InvalidEncryptionKey(err))
                },
                DecryptionError.InvalidEncryptionKey.apply,
              )
              decrypter <- Either
                .catchOnly[GeneralSecurityException](
                  new EciesAeadHkdfHybridDecrypt(
                    ecPrivateKey,
                    Array[Byte](),
                    "HmacSha256",
                    EllipticCurves.PointFormatType.UNCOMPRESSED,
                    Aes128GcmDemHelper,
                  )
                )
                .leftMap(err =>
                  DecryptionError.InvalidEncryptionKey(ErrorUtil.messageWithStacktrace(err))
                )
              plaintext <- Either
                .catchOnly[GeneralSecurityException](
                  decrypter.decrypt(encrypted.ciphertext.toByteArray, Array[Byte]())
                )
                .leftMap(err =>
                  DecryptionError.FailedToDecrypt(ErrorUtil.messageWithStacktrace(err))
                )
              message <- deserialize(ByteString.copyFrom(plaintext))
                .leftMap(DecryptionError.FailedToDeserialize.apply)
            } yield message
          case EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc =>
            for {
              ecPrivateKey <- parseAndGetPrivateKey(
                privateKey,
                // TODO(#26423): Remove once private key validation is implemented
                { case Right(k: ECPrivateKey) =>
                  checkEcKeyInCurve(k, privateKey.id)
                    .leftMap(err => DecryptionError.InvalidEncryptionKey(err))
                },
                DecryptionError.InvalidEncryptionKey.apply,
              )
              /* we split at 'ivSizeForAesCbc' (=16) because that is the size of our iv (for AES-128-CBC)
               * that gets  pre-appended to the ciphertext.
               */
              ciphertextSplit <- DeterministicEncoding
                .splitAt(
                  EciesHmacSha256Aes128CbcParams.ivSizeForAesCbcInBytes,
                  encrypted.ciphertext,
                )
                .leftMap(err =>
                  DecryptionError.FailedToDeserialize(DefaultDeserializationError(err.show))
                )
              (iv, ciphertext) = ciphertextSplit
              decrypter <- Either
                .catchOnly[GeneralSecurityException] {
                  val cipher = Cipher
                    .getInstance(
                      EciesHmacSha256Aes128CbcParams.jceInternalName,
                      JceSecurityProvider.bouncyCastleProvider,
                    )
                  cipher.init(
                    Cipher.DECRYPT_MODE,
                    ecPrivateKey,
                    EciesHmacSha256Aes128CbcParams.parameterSpec(iv.toByteArray),
                  )
                  cipher
                }
                .leftMap(err =>
                  DecryptionError.InvalidEncryptionKey(ErrorUtil.messageWithStacktrace(err))
                )
              plaintext <- Either
                .catchOnly[GeneralSecurityException](
                  decrypter.doFinal(ciphertext.toByteArray)
                )
                .leftMap(err =>
                  DecryptionError.FailedToDecrypt(ErrorUtil.messageWithStacktrace(err))
                )
              message <- deserialize(ByteString.copyFrom(plaintext))
                .leftMap(DecryptionError.FailedToDeserialize.apply)
            } yield message
          case EncryptionAlgorithmSpec.RsaOaepSha256 =>
            for {
              rsaPrivateKey <- parseAndGetPrivateKey(
                privateKey,
                { case Right(k: RSAPrivateKey) =>
                  for {
                    size <- privateKey.keySpec match {
                      case EncryptionKeySpec.Rsa2048 =>
                        Right(EncryptionKeySpec.Rsa2048.keySizeInBits)
                      case wrongKeySpec =>
                        Left(
                          DecryptionError.InvalidEncryptionKey(
                            s"Expected a ${EncryptionKeySpec.Rsa2048} private key, but got a $wrongKeySpec private key instead"
                          )
                        )
                    }
                    key <- checkRsaKeySize(k, privateKey.id, size)
                      .leftMap(err => DecryptionError.InvalidEncryptionKey(err))
                  } yield key
                },
                DecryptionError.InvalidEncryptionKey.apply,
              )
              decrypter <- Either
                .catchOnly[GeneralSecurityException] {
                  val cipher = Cipher
                    .getInstance(
                      RsaOaepSha256Params.jceInternalName,
                      JceSecurityProvider.bouncyCastleProvider,
                    )
                  cipher.init(
                    Cipher.DECRYPT_MODE,
                    rsaPrivateKey,
                  )
                  cipher
                }
                .leftMap(err => DecryptionError.InvalidEncryptionKey(err.toString))
              plaintext <- Try[Array[Byte]](
                decrypter.doFinal(encrypted.ciphertext.toByteArray)
              ).toEither.leftMap {
                case err: DataLengthException =>
                  DecryptionError
                    .FailedToDecrypt(
                      s"Most probably using a wrong secret key to decrypt the ciphertext: ${err.toString}"
                    )
                case err =>
                  DecryptionError.FailedToDecrypt(ErrorUtil.messageWithStacktrace(err))
              }
              message <- deserialize(ByteString.copyFrom(plaintext))
                .leftMap(DecryptionError.FailedToDeserialize.apply)
            } yield message
        }
      }
  }

  override private[crypto] def encryptSymmetricWith(
      data: ByteString,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, ByteString] =
    symmetricKey.scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        for {
          _ <- CryptoKeyValidation.ensureFormat(
            symmetricKey.format,
            Set(CryptoKeyFormat.Raw),
            EncryptionError.UnsupportedKeyFormat.apply,
          )
          ciphertext <- encryptAes128Gcm(data, symmetricKey.key)
        } yield ciphertext
    }

  override def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    symmetricKey.scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        for {
          _ <- CryptoKeyValidation.ensureFormat(
            symmetricKey.format,
            Set(CryptoKeyFormat.Raw),
            DecryptionError.UnsupportedKeyFormat.apply,
          )
          plaintext <- decryptAes128Gcm(encrypted.ciphertext, symmetricKey.key)
          message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize.apply)
        } yield message
    }

  override protected[crypto] def generateRandomBytes(length: Int): Array[Byte] =
    JceSecureRandom.generateRandomBytes(length)

  override def deriveSymmetricKey(
      password: String,
      symmetricKeyScheme: SymmetricKeyScheme,
      pbkdfScheme: PbkdfScheme,
      saltO: Option[SecureRandomness],
  ): Either[PasswordBasedEncryptionError, PasswordBasedEncryptionKey] =
    pbkdfScheme match {
      case mode: PbkdfScheme.Argon2idMode1.type =>
        val salt = saltO.getOrElse(generateSecureRandomness(pbkdfScheme.defaultSaltLengthInBytes))

        val params = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
          .withIterations(mode.iterations)
          .withMemoryAsKB(mode.memoryInKb)
          .withParallelism(mode.parallelism)
          .withSalt(salt.unwrap.toByteArray)
          .build()

        val argon2 = new Argon2BytesGenerator()
        argon2.init(params)

        val keyLength = symmetricKeyScheme.keySizeInBytes
        val keyBytes = new Array[Byte](keyLength)
        val keyLen = argon2.generateBytes(password.toCharArray, keyBytes)

        val key =
          SymmetricKey(CryptoKeyFormat.Raw, ByteString.copyFrom(keyBytes), symmetricKeyScheme)

        Either.cond(
          keyLen == keyLength,
          PasswordBasedEncryptionKey(key = key, salt = salt),
          PasswordBasedEncryptionError.PbkdfOutputLengthInvalid(
            expectedLength = keyLength,
            actualLength = keyLen,
          ),
        )
    }

}

object JcePureCrypto {

  def create(
      config: CryptoConfig,
      sessionEncryptionKeyCacheConfig: SessionEncryptionKeyCacheConfig,
      publicKeyConversionCacheConfig: CacheConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): Either[String, JcePureCrypto] = {

    // The retention time for the Java private key conversion cache must not be
    // longer than the minimum eviction time for the session signing/encryption private keys.

    lazy val encryptionDurationOpt: Option[FiniteDuration] =
      Option.when(sessionEncryptionKeyCacheConfig.enabled) {
        val sender = sessionEncryptionKeyCacheConfig.senderCache.expireAfterTimeout.underlying
        val receiver = sessionEncryptionKeyCacheConfig.receiverCache.expireAfterTimeout.underlying
        sender.min(receiver)
      }

    lazy val signingDurationOpt: Option[FiniteDuration] = config.kms.flatMap { kms =>
      Option.when(kms.sessionSigningKeys.enabled)(
        kms.sessionSigningKeys.keyEvictionPeriod.underlying
      )
    }

    lazy val minimumPrivateKeyCacheDuration =
      Seq(encryptionDurationOpt, signingDurationOpt).flatten.minOption

    for {
      _ <- Either
        .cond(config.provider == CryptoProvider.Jce, (), "JCE provider must be configured")
      _ = Security.addProvider(JceSecurityProvider.bouncyCastleProvider)
      schemes <- CryptoSchemes.fromConfig(config)
      pbkdfSchemes <- schemes.pbkdfSchemes.toRight("PBKDF schemes must be defined for JCE provider")
    } yield new JcePureCrypto(
      defaultSymmetricKeyScheme = schemes.symmetricKeySchemes.default,
      signingAlgorithmSpecs = schemes.signingAlgoSpecs,
      encryptionAlgorithmSpecs = schemes.encryptionAlgoSpecs,
      defaultHashAlgorithm = schemes.hashAlgorithms.default,
      defaultPbkdfScheme = pbkdfSchemes.default,
      publicKeyConversionCacheConfig = publicKeyConversionCacheConfig,
      privateKeyConversionCacheTtl = minimumPrivateKeyCacheDuration,
      loggerFactory = loggerFactory,
    )
  }
}
