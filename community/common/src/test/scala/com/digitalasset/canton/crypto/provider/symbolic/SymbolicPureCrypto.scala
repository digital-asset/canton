// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import java.util.concurrent.atomic.AtomicInteger

import cats.syntax.either._
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.serialization.{DeserializationError, DeterministicEncoding}
import com.digitalasset.canton.util.HasVersionedToByteString
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

class SymbolicPureCrypto(hkdfOps: Option[HkdfOps] = None) extends CryptoPureApi {

  private val symmetricKeyCounter = new AtomicInteger

  // NOTE: The scheme is not really used by Symbolic crypto
  override val defaultSymmetricKeyScheme: SymmetricKeyScheme = SymmetricKeyScheme.Aes128Gcm

  override protected[crypto] def sign(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
  ): Either[SigningError, Signature] = {
    Right(new Signature(SignatureFormat.Raw, bytes, signingKey.id))
  }

  override protected[crypto] def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
  ): Either[SignatureCheckError, Unit] =
    for {
      _ <- Either.cond(
        publicKey.format == CryptoKeyFormat.Symbolic,
        (),
        SignatureCheckError.InvalidKeyError(s"Public key $publicKey is not a symbolic key"),
      )
      _ <- Either.cond(
        publicKey.id == signature.signedBy,
        (),
        SignatureCheckError.SignatureWithWrongKey(
          s"Signature was signed by ${signature.signedBy} whereas key is ${publicKey.id}"
        ),
      )
      _ <- Either.cond(
        signature.unwrap == bytes,
        (),
        SignatureCheckError.InvalidSignature(
          signature,
          bytes,
          s"Symbolic signature with ${signature.unwrap} does not match payload $bytes",
        ),
      )
    } yield ()

  override def defaultHashAlgorithm: HashAlgorithm = HashAlgorithm.Sha256

  override def generateSymmetricKey(
      scheme: SymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey] = {
    val key = ByteString.copyFromUtf8(s"key-${symmetricKeyCounter.incrementAndGet()}")
    Right(SymmetricKey.create(CryptoKeyFormat.Symbolic, key, scheme))
  }

  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
  ): Either[EncryptionError, Encrypted[M]] =
    for {
      _ <- Either.cond(
        publicKey.format == CryptoKeyFormat.Symbolic,
        (),
        EncryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $publicKey"),
      )
      // For a symbolic encrypted message, prepend the key id that was used to encrypt
      payload = DeterministicEncoding
        .encodeString(publicKey.id.toProtoPrimitive)
        .concat(DeterministicEncoding.encodeBytes(message.toByteString(ProtocolVersion.default)))
      encrypted = new Encrypted[M](payload)
    } yield encrypted

  override def decryptWith[M](encrypted: Encrypted[M], privateKey: EncryptionPrivateKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    for {
      _ <- Either.cond(
        privateKey.format == CryptoKeyFormat.Symbolic,
        (),
        DecryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $privateKey"),
      )
      // For a symbolic encrypted message, the encryption key id is prepended before the ciphertext/plaintext
      keyIdAndCiphertext <- DeterministicEncoding
        .decodeString(encrypted.ciphertext)
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
      message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize)

    } yield message

  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, Encrypted[M]] =
    for {
      _ <- Either.cond(
        symmetricKey.format == CryptoKeyFormat.Symbolic,
        (),
        EncryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $symmetricKey"),
      )
      // For a symbolic symmetric encrypted message, prepend the symmetric key
      payload = DeterministicEncoding
        .encodeBytes(symmetricKey.key)
        .concat(DeterministicEncoding.encodeBytes(message.toByteString(ProtocolVersion.default)))
      encrypted = new Encrypted[M](payload)
    } yield encrypted

  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SecureRandomness,
      scheme: SymmetricKeyScheme,
  ): Either[EncryptionError, Encrypted[M]] = {
    encryptWith(message, SymmetricKey.create(CryptoKeyFormat.Symbolic, symmetricKey.unwrap, scheme))
  }

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
      message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize)

    } yield message

  override def decryptWith[M](
      encrypted: Encrypted[M],
      symmetricKey: SecureRandomness,
      scheme: SymmetricKeyScheme,
  )(deserialize: ByteString => Either[DeserializationError, M]): Either[DecryptionError, M] = {
    decryptWith(
      encrypted,
      SymmetricKey.create(CryptoKeyFormat.Symbolic, symmetricKey.unwrap, scheme),
    )(deserialize)
  }

  override def hkdfExpand(
      keyMaterial: SecureRandomness,
      outputBytes: Int,
      info: HkdfInfo,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] =
    hkdfOps.fold(super.hkdfExpand(keyMaterial, outputBytes, info, algorithm))(
      _.hkdfExpand(keyMaterial, outputBytes, info, algorithm)
    )

}
