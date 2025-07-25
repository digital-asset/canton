// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.serialization.{DeserializationError, DeterministicEncoding}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.version.HasToByteString
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

class SymbolicPureCrypto extends CryptoPureApi {

  /** This flag is used to control the randomness during asymmetric encryption. This is only
    * intended to be used for testing purposes and it overrides the randomization flag given to
    * [[encryptWithInternal]].
    */
  private val neverRandomizeAsymmetricEncryption = new AtomicBoolean(false)

  private val symmetricKeyCounter = new AtomicInteger
  private val randomnessCounter = new AtomicInteger
  private val signatureCounter = new AtomicInteger

  @VisibleForTesting
  def setRandomnessFlag(newValue: Boolean): Unit =
    neverRandomizeAsymmetricEncryption.set(newValue)

  // iv to pre-append to the asymmetric ciphertext
  private val ivForAsymmetricEncryptInBytes = 16

  // NOTE: The following schemes are not really used by Symbolic crypto, but we pretend to support them
  override val defaultSymmetricKeyScheme: SymmetricKeyScheme = SymmetricKeyScheme.Aes128Gcm
  override val signingAlgorithmSpecs: CryptoScheme[SigningAlgorithmSpec] =
    CryptoScheme(SigningAlgorithmSpec.Ed25519, NonEmpty.mk(Set, SigningAlgorithmSpec.Ed25519))
  override val encryptionAlgorithmSpecs: CryptoScheme[EncryptionAlgorithmSpec] =
    CryptoScheme(
      EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm,
      NonEmpty.mk(Set, EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Gcm),
    )
  override val defaultPbkdfScheme: PbkdfScheme = PbkdfScheme.Argon2idMode1

  override protected[crypto] def signBytes(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec = signingAlgorithmSpecs.default,
  )(implicit traceContext: TraceContext): Either[SigningError, Signature] =
    CryptoKeyValidation
      .ensureUsage(
        usage,
        signingKey.usage,
        signingKey.id,
        SigningError.InvalidKeyUsage.apply,
      )
      .flatMap { _ =>
        val counter = signatureCounter.getAndIncrement()
        Right(SymbolicPureCrypto.createSignature(bytes, signingKey.id, counter))
      }

  override def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): Either[SignatureCheckError, Unit] =
    for {
      _ <- Either.cond(
        publicKey.id == signature.signedBy,
        (),
        SignatureCheckError.SignatureWithWrongKey(
          s"Signature was signed by ${signature.signedBy} whereas key is ${publicKey.id}"
        ),
      )
      _ <- CryptoKeyValidation.ensureFormat(
        publicKey.format,
        Set(CryptoKeyFormat.Symbolic),
        SignatureCheckError.UnsupportedKeyFormat.apply,
      )
      _ <- CryptoKeyValidation.ensureSignatureFormat(
        signature.format,
        Set(SignatureFormat.Symbolic),
        SignatureCheckError.UnsupportedSignatureFormat.apply,
      )
      _ <- CryptoKeyValidation.ensureUsage(
        usage,
        publicKey.usage,
        publicKey.id,
        SignatureCheckError.InvalidKeyUsage.apply,
      )
      signedContent <- Either.cond(
        signature.unwrap.size() >= 4,
        signature.unwrap.substring(0, signature.unwrap.size() - 4),
        SignatureCheckError.InvalidSignature(
          signature,
          bytes,
          s"Symbolic signature ${signature.unwrap} lacks the four randomness bytes at the end",
        ),
      )
      _ <- Either.cond(
        signedContent == bytes,
        (),
        SignatureCheckError.InvalidSignature(
          signature,
          bytes,
          s"Symbolic signature with $signedContent does not match payload $bytes",
        ),
      )
    } yield ()

  override def defaultHashAlgorithm: HashAlgorithm = HashAlgorithm.Sha256

  override def generateSymmetricKey(
      scheme: SymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey] = {
    val key = ByteString.copyFromUtf8(s"key-${symmetricKeyCounter.incrementAndGet()}")
    Right(SymmetricKey(CryptoKeyFormat.Symbolic, key, scheme))
  }

  override def createSymmetricKey(
      bytes: SecureRandomness,
      scheme: SymmetricKeyScheme,
  ): Either[EncryptionKeyCreationError, SymmetricKey] =
    Right(SymmetricKey(CryptoKeyFormat.Symbolic, bytes.unwrap, scheme))

  private def encryptWithInternal[M](
      bytes: ByteString,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
      randomized: Boolean,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    for {
      _ <- Either.cond(
        publicKey.format == CryptoKeyFormat.Symbolic,
        (),
        EncryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $publicKey"),
      )
      // For a symbolic encrypted message, prepend the key id that was used to encrypt
      payload = DeterministicEncoding
        .encodeString(publicKey.id.toProtoPrimitive)
        .concat(DeterministicEncoding.encodeBytes(bytes))
      iv =
        if (randomized && !neverRandomizeAsymmetricEncryption.get())
          generateRandomByteString(ivForAsymmetricEncryptInBytes)
        else ByteString.copyFrom(new Array[Byte](ivForAsymmetricEncryptInBytes))

      encrypted = new AsymmetricEncrypted[M](
        iv.concat(payload),
        encryptionAlgorithmSpec,
        publicKey.id,
      )
    } yield encrypted

  override def encryptWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = encryptionAlgorithmSpecs.default,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    encryptWithInternal(message.toByteString, publicKey, encryptionAlgorithmSpec, randomized = true)

  def encryptDeterministicWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec = encryptionAlgorithmSpecs.default,
  )(implicit traceContext: TraceContext): Either[EncryptionError, AsymmetricEncrypted[M]] =
    encryptWithInternal(
      message.toByteString,
      publicKey,
      encryptionAlgorithmSpec,
      randomized = false,
    )

  override protected[crypto] def decryptWithInternal[M](
      encrypted: AsymmetricEncrypted[M],
      privateKey: EncryptionPrivateKey,
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    for {
      _ <- Either.cond(
        privateKey.format == CryptoKeyFormat.Symbolic,
        (),
        DecryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $privateKey"),
      )
      // Remove iv
      ciphertextWithoutIv = encrypted.ciphertext.substring(ivForAsymmetricEncryptInBytes)

      // For a symbolic encrypted message, the encryption key id is prepended before the ciphertext/plaintext
      keyIdAndCiphertext <- DeterministicEncoding
        .decodeString(ciphertextWithoutIv)
        .leftMap[DecryptionError](err =>
          DecryptionError.FailedToDecrypt(
            s"Cannot extract public key id from symbolic encrypted payload: $err"
          )
        )
      (keyId, ciphertext) = keyIdAndCiphertext
      _ <- Either.cond(
        keyId == privateKey.id.toProtoPrimitive,
        (),
        DecryptionError.FailedToDecrypt(
          s"Provided symbolic private key $privateKey does not match used public key $keyId"
        ),
      )
      plaintextAndBytes <- DeterministicEncoding
        .decodeBytes(ciphertext)
        .leftMap[DecryptionError](err =>
          DecryptionError.FailedToDecrypt(
            s"Cannot extract ciphertext from symbolic encrypted payload: $err"
          )
        )
      (plaintext, bytes) = plaintextAndBytes
      _ <- Either.cond(
        bytes.isEmpty,
        (),
        DecryptionError.FailedToDecrypt(s"Payload contains more than key id and ciphertext"),
      )
      message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize.apply)

    } yield message

  override private[crypto] def encryptSymmetricWith(
      data: ByteString,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, ByteString] =
    for {
      _ <- Either.cond(
        symmetricKey.format == CryptoKeyFormat.Symbolic,
        (),
        EncryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $symmetricKey"),
      )
      // For a symbolic symmetric encrypted message, prepend the symmetric key
      ciphertext = DeterministicEncoding
        .encodeBytes(symmetricKey.key)
        .concat(DeterministicEncoding.encodeBytes(data))
    } yield ciphertext

  override def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    for {
      _ <- Either.cond(
        symmetricKey.format == CryptoKeyFormat.Symbolic,
        (),
        DecryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $symmetricKey"),
      )
      // For a symbolic symmetric encrypted message, the encryption key is prepended before the ciphertext/plaintext
      keyAndCiphertext <- DeterministicEncoding
        .decodeBytes(encrypted.ciphertext)
        .leftMap[DecryptionError](err =>
          DecryptionError.FailedToDecrypt(
            s"Cannot extract key from symbolic encrypted payload: $err"
          )
        )
      (key, ciphertext) = keyAndCiphertext
      _ <- Either.cond(
        key == symmetricKey.key,
        (),
        DecryptionError.FailedToDecrypt(
          s"Provided symbolic key $symmetricKey does not match used key $key"
        ),
      )
      plaintextAndBytes <- DeterministicEncoding
        .decodeBytes(ciphertext)
        .leftMap[DecryptionError](err =>
          DecryptionError.FailedToDecrypt(
            s"Cannot extract ciphertext from symbolic encrypted payload: $err"
          )
        )
      (plaintext, bytes) = plaintextAndBytes
      _ <- Either.cond(
        bytes.isEmpty,
        (),
        DecryptionError.FailedToDecrypt(s"Payload contains more than key and ciphertext"),
      )
      message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize.apply)

    } yield message

  override protected[crypto] def generateRandomBytes(length: Int): Array[Byte] = {
    // Not really random
    val random =
      DeterministicEncoding.encodeInt(randomnessCounter.getAndIncrement()).toByteArray.take(length)

    // Pad the rest of the request bytes with 0 if necessary
    if (random.length < length)
      random.concat(new Array[Byte](length - random.length))
    else
      random
  }

  override def deriveSymmetricKey(
      password: String,
      symmetricKeyScheme: SymmetricKeyScheme,
      pbkdfScheme: PbkdfScheme,
      saltO: Option[SecureRandomness],
  ): Either[PasswordBasedEncryptionError, PasswordBasedEncryptionKey] = {
    val salt =
      saltO
        .getOrElse(generateSecureRandomness(pbkdfScheme.defaultSaltLengthInBytes))

    // We just hash the salt and password, then truncate/pad to desired length
    val hash = TestHash.build.addWithoutLengthPrefix(salt.unwrap).add(password).finish()
    val keyBytes =
      ByteStringUtil.padOrTruncate(
        hash.unwrap,
        NonNegativeInt.tryCreate(symmetricKeyScheme.keySizeInBytes),
      )
    val key = SymmetricKey(CryptoKeyFormat.Symbolic, keyBytes, symmetricKeyScheme)

    Right(PasswordBasedEncryptionKey(key, salt))
  }

}

object SymbolicPureCrypto {

  /** Symbolic signatures use the content as the signature, padded with a `counter` to simulate the
    * general non-determinism of signing
    */
  private[symbolic] def createSignature(
      bytes: ByteString,
      signingKey: Fingerprint,
      counter: Int,
  ): Signature =
    Signature.create(
      SignatureFormat.Symbolic,
      bytes.concat(DeterministicEncoding.encodeInt(counter)),
      signingKey,
      None,
    )
}
