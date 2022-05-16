// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import java.security.GeneralSecurityException
import cats.syntax.either._
import cats.syntax.foldable._
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.crypto.HkdfError.{HkdfHmacError, HkdfInternalError}
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.google.crypto.tink._
import com.google.crypto.tink.aead.AeadKeyTemplates
import com.google.crypto.tink.config.TinkConfig
import com.google.crypto.tink.subtle.{AesGcmJce, Hkdf, Random}
import com.google.protobuf.ByteString

import scala.collection.concurrent.TrieMap
import scala.reflect.{ClassTag, classTag}

class TinkPureCrypto private (
    override val defaultSymmetricKeyScheme: SymmetricKeyScheme,
    override val defaultHashAlgorithm: HashAlgorithm,
) extends CryptoPureApi {

  // Cache for the public and private key keyset deserialization result
  // Note: We do not cache symmetric keys as we generate random keys for each new view.
  private val publicKeysetCache: TrieMap[Fingerprint, Either[DeserializationError, KeysetHandle]] =
    TrieMap.empty
  private val privateKeysetCache: TrieMap[Fingerprint, Either[DeserializationError, KeysetHandle]] =
    TrieMap.empty

  private def encryptWith[M <: HasVersionedToByteString](
      message: M,
      encrypt: Array[Byte] => Array[Byte],
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]] = {
    val bytes = message.toByteString(version).toByteArray
    Either
      .catchOnly[GeneralSecurityException](encrypt(bytes))
      .bimap(
        err => EncryptionError.FailedToEncrypt(err.toString),
        enc => new Encrypted[M](ByteString.copyFrom(enc)),
      )
  }

  private def decryptWith[M](
      encrypted: Encrypted[M],
      decrypt: Array[Byte] => Array[Byte],
      deserialize: ByteString => Either[DeserializationError, M],
  ): Either[DecryptionError, M] =
    Either
      .catchOnly[GeneralSecurityException](decrypt(encrypted.ciphertext.toByteArray))
      .leftMap(err => DecryptionError.FailedToDecrypt(err.toString))
      .flatMap(plain =>
        deserialize(ByteString.copyFrom(plain)).leftMap(DecryptionError.FailedToDeserialize)
      )

  private def ensureTinkFormat[E](format: CryptoKeyFormat, errFn: String => E): Either[E, Unit] =
    Either.cond(format == CryptoKeyFormat.Tink, (), errFn(s"Key format must be Tink"))

  private def keysetNonCached[E](key: CryptoKey, errFn: String => E): Either[E, KeysetHandle] =
    for {
      _ <- ensureTinkFormat(key.format, errFn)
      keysetHandle <- TinkKeyFormat
        .deserializeHandle(key.key)
        .leftMap(err => errFn(s"Failed to deserialize keyset: $err"))
    } yield keysetHandle

  private def keysetCached[E](key: CryptoKeyPairKey, errFn: String => E): Either[E, KeysetHandle] =
    for {
      _ <- ensureTinkFormat(key.format, errFn)
      keysetCache = if (key.isPublicKey) publicKeysetCache else privateKeysetCache
      keysetHandle <- keysetCache
        .getOrElseUpdate(key.id, TinkKeyFormat.deserializeHandle(key.key))
        .leftMap(err => errFn(s"Failed to deserialize keyset: $err"))
    } yield keysetHandle

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def getPrimitive[P: ClassTag, E](
      keysetHandle: KeysetHandle,
      errFn: String => E,
  ): Either[E, P] =
    Either
      .catchOnly[GeneralSecurityException](
        keysetHandle.getPrimitive(classTag[P].runtimeClass.asInstanceOf[Class[P]])
      )
      .leftMap(err => errFn(s"Failed to get primitive: $err"))

  /** Generates a random symmetric key */
  override def generateSymmetricKey(
      scheme: SymmetricKeyScheme = defaultSymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey] = {
    val keyTemplate = scheme match {
      case SymmetricKeyScheme.Aes128Gcm => AeadKeyTemplates.AES128_GCM
    }

    Either
      .catchOnly[GeneralSecurityException](KeysetHandle.generateNew(keyTemplate))
      .bimap(
        EncryptionKeyGenerationError.GeneralError,
        { keysetHandle =>
          val key = TinkKeyFormat.serializeHandle(keysetHandle)
          SymmetricKey.create(CryptoKeyFormat.Tink, key, scheme)
        },
      )
  }

  /** Encrypts the given bytes with the given symmetric key */
  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SymmetricKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]] =
    for {
      keysetHandle <- keysetNonCached(symmetricKey, EncryptionError.InvalidSymmetricKey)
      aead <- getPrimitive[Aead, EncryptionError](keysetHandle, EncryptionError.InvalidSymmetricKey)
      encrypted <- encryptWith(message, in => aead.encrypt(in, Array[Byte]()), version)
    } yield encrypted

  /** Decrypts a message encrypted using `encryptWith` */
  override def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    for {
      keysetHandle <- keysetNonCached(symmetricKey, DecryptionError.InvalidSymmetricKey)
      aead <- getPrimitive[Aead, DecryptionError](keysetHandle, DecryptionError.InvalidSymmetricKey)
      msg <- decryptWith(encrypted, in => aead.decrypt(in, Array[Byte]()), deserialize)
    } yield msg

  /** Encrypts the given message using the given public key. */
  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]] =
    for {
      keysetHandle <- keysetCached(publicKey, EncryptionError.InvalidEncryptionKey)
      hybrid <- getPrimitive[HybridEncrypt, EncryptionError](
        keysetHandle,
        EncryptionError.InvalidEncryptionKey,
      )
      encrypted <- encryptWith(message, in => hybrid.encrypt(in, Array[Byte]()), version)
    } yield encrypted

  override def decryptWith[M](encrypted: Encrypted[M], privateKey: EncryptionPrivateKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    for {
      keysetHandle <- keysetCached(privateKey, DecryptionError.InvalidEncryptionKey)
      hybrid <- getPrimitive[HybridDecrypt, DecryptionError](
        keysetHandle,
        DecryptionError.InvalidEncryptionKey,
      )
      msg <- decryptWith(encrypted, in => hybrid.decrypt(in, Array[Byte]()), deserialize)
    } yield msg

  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SecureRandomness,
      version: ProtocolVersion,
      scheme: SymmetricKeyScheme,
  ): Either[EncryptionError, Encrypted[M]] = {
    scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        for {
          aead <- Either
            .catchOnly[GeneralSecurityException](new AesGcmJce(symmetricKey.unwrap.toByteArray))
            .leftMap(e => EncryptionError.InvalidSymmetricKey(e.getMessage))
          res <- encryptWith(message, in => aead.encrypt(in, Array.emptyByteArray), version)
        } yield res
    }
  }

  override def decryptWith[M](
      encrypted: Encrypted[M],
      symmetricKey: SecureRandomness,
      scheme: SymmetricKeyScheme,
  )(deserialize: ByteString => Either[DeserializationError, M]): Either[DecryptionError, M] = {
    scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        try {
          val aead = new AesGcmJce(symmetricKey.unwrap.toByteArray)
          decryptWith(encrypted, in => aead.decrypt(in, Array.emptyByteArray), deserialize)
        } catch {
          case e: GeneralSecurityException =>
            Left(DecryptionError.InvalidSymmetricKey(e.getMessage))
        }
    }

  }

  override protected[crypto] def sign(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
  ): Either[SigningError, Signature] =
    for {
      keysetHandle <- keysetCached(signingKey, SigningError.InvalidSigningKey)
      verify <- getPrimitive[PublicKeySign, SigningError](
        keysetHandle,
        SigningError.InvalidSigningKey,
      )
      signatureBytes <- Either
        .catchOnly[GeneralSecurityException](verify.sign(bytes.toByteArray))
        .leftMap(err => SigningError.FailedToSign(err.toString))
      signature = new Signature(
        SignatureFormat.Raw,
        ByteString.copyFrom(signatureBytes),
        signingKey.id,
      )
    } yield signature

  /** Confirms if the provided signature is a valid signature of the payload using the public key.
    * Will always deem the signature invalid if the public key is not a signature key.
    */
  override def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
  ): Either[SignatureCheckError, Unit] =
    for {
      keysetHandle <- keysetCached(publicKey, SignatureCheckError.InvalidKeyError)
      _ <- Either.cond(
        signature.signedBy == publicKey.id,
        (),
        SignatureCheckError.SignatureWithWrongKey(
          s"Signature signed by ${signature.signedBy} instead of ${publicKey.id}"
        ),
      )
      verify <- getPrimitive[PublicKeyVerify, SignatureCheckError](
        keysetHandle,
        SignatureCheckError.InvalidKeyError,
      )
      _ <- Either
        .catchOnly[GeneralSecurityException](
          verify.verify(signature.unwrap.toByteArray, bytes.toByteArray)
        )
        .leftMap(err =>
          SignatureCheckError
            .InvalidSignature(signature, bytes, s"Failed to verify signature: $err")
        )
    } yield ()

  override protected def computeHkdfInternal(
      keyMaterial: ByteString,
      outputBytes: Int,
      info: HkdfInfo,
      salt: ByteString,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] = {
    Either
      .catchOnly[GeneralSecurityException] {
        Hkdf.computeHkdf(
          algorithm.name,
          keyMaterial.toByteArray,
          salt.toByteArray,
          info.bytes.toByteArray,
          outputBytes,
        )
      }
      .leftMap(err => show"Failed to compute HKDF with Tink: $err")
      .flatMap { hkdfOutput =>
        SecureRandomness
          .fromByteString(outputBytes)(ByteString.copyFrom(hkdfOutput))
          .leftMap(err => s"Invalid output from HKDF: $err")
      }
      .leftMap(HkdfInternalError)
  }

  override protected def hkdfExpandInternal(
      keyMaterial: SecureRandomness,
      outputBytes: Int,
      info: HkdfInfo,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] = {
    // NOTE: Tink does not expose only the expand phase, thus we have to implement it ourselves
    val hashBytes = algorithm.hashAlgorithm.length
    for {
      prk <- HmacSecret.create(keyMaterial.unwrap).leftMap(HkdfHmacError)
      nrChunks = scala.math.ceil(outputBytes.toDouble / hashBytes).toInt
      outputAndLast <- (1 to nrChunks).toList
        .foldM(ByteString.EMPTY -> ByteString.EMPTY) { case ((out, last), chunk) =>
          val chunkByte = ByteString.copyFrom(Array[Byte](chunk.toByte))
          hmacWithSecret(prk, last.concat(info.bytes).concat(chunkByte), algorithm)
            .bimap(HkdfHmacError, hmac => out.concat(hmac.unwrap) -> hmac.unwrap)
        }
      (out, _last) = outputAndLast
    } yield SecureRandomness(out.substring(0, outputBytes))
  }

  override protected def generateRandomBytes(length: Int): Array[Byte] = Random.randBytes(length)
}

object TinkPureCrypto {

  def create(
      defaultSymmetricKeyScheme: SymmetricKeyScheme,
      defaultHashAlgorithm: HashAlgorithm,
  ): Either[String, TinkPureCrypto] =
    Either
      .catchOnly[GeneralSecurityException](TinkConfig.register())
      .bimap(
        err => s"Failed to initialize tink: $err",
        _ => new TinkPureCrypto(defaultSymmetricKeyScheme, defaultHashAlgorithm),
      )

}
