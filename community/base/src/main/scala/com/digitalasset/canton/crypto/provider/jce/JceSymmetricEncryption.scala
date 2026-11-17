// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.util.EitherUtil
import com.google.protobuf.ByteString

import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}

/** Symmetric encryption with provided keys. */
trait JceSymmetricEncryption extends SymmetricEncryptionOps {

  override private[crypto] def encryptSymmetricWith(
      data: ByteString,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, ByteString] =
    symmetricKey.scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        JceSymmetricEncryption.encryptAes128Gcm(data, symmetricKey.key)
    }

  override def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    symmetricKey.scheme match {
      case SymmetricKeyScheme.Aes128Gcm =>
        for {
          plaintext <- JceSymmetricEncryption.decryptAes128Gcm(
            encrypted.ciphertext,
            symmetricKey.key,
          )
          message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize.apply)
        } yield message
    }
}

object JceSymmetricEncryption {

  private object Aes128GcmParams {
    // the internal jce designation for this scheme
    val jceInternalName: String = "AES/GCM/NoPadding"
    // the raw algorithm name for SecretKeySpec (must be "AES", not the full transformation string)
    val jceKeyAlgorithm: String = "AES"
    // the IV for AES-128-GCM is 12 bytes
    val ivSizeForAesGcmInBytes: Int = 12
    // the authentication tag for AES-128-GCM is recommended to be 128bits
    val gcmTagBits: Int = 128
  }

  private def encryptAes128Gcm(
      plaintext: ByteString,
      symmetricKey: ByteString,
  ): Either[EncryptionError, ByteString] =
    for {
      _ <- EitherUtil.condUnit(
        symmetricKey.size() == SymmetricKeyScheme.Aes128Gcm.keySizeInBytes,
        EncryptionError.InvalidSymmetricKey(
          s"AES-128 key must be ${SymmetricKeyScheme.Aes128Gcm.keySizeInBytes} bytes, " +
            s"but got ${symmetricKey.size()} bytes."
        ),
      )
      // this encryption scheme AES-128-GCM requires an IV/nonce of 12bytes.
      iv = JceSecureRandom.generateRandomBytes(Aes128GcmParams.ivSizeForAesGcmInBytes)
      encrypter <- Either
        .catchNonFatal {
          val keySpec = new SecretKeySpec(symmetricKey.toByteArray, Aes128GcmParams.jceKeyAlgorithm)
          val cipher = Cipher.getInstance(
            Aes128GcmParams.jceInternalName,
            JceSecurityProvider.bouncyCastleProvider,
          )
          cipher.init(
            Cipher.ENCRYPT_MODE,
            keySpec,
            new GCMParameterSpec(Aes128GcmParams.gcmTagBits, iv),
          )
          cipher
        }
        .leftMap(err => EncryptionError.InvalidSymmetricKey(err.toString))
      ciphertext <- Either
        .catchNonFatal(
          encrypter.doFinal(plaintext.toByteArray)
        )
        .leftMap(err => EncryptionError.FailedToEncrypt(err.toString))

      /* Prepend our IV to the ciphertext. BouncyCastle's AES-GCM encryption does not deal with the AES IV by itself,
       * and we have to randomly generate it and manually prepend it to the ciphertext.
       */
    } yield ByteString.copyFrom(iv ++ ciphertext)

  private def decryptAes128Gcm(
      ciphertext: ByteString,
      symmetricKey: ByteString,
  ): Either[DecryptionError, ByteString] =
    for {
      _ <- EitherUtil.condUnit(
        symmetricKey.size() == SymmetricKeyScheme.Aes128Gcm.keySizeInBytes,
        DecryptionError.InvalidSymmetricKey(
          s"AES-128 key must be ${SymmetricKeyScheme.Aes128Gcm.keySizeInBytes} bytes, " +
            s"but got ${symmetricKey.size()} bytes."
        ),
      )
      iv = ciphertext.toByteArray.take(Aes128GcmParams.ivSizeForAesGcmInBytes)
      rawCiphertext = ciphertext.toByteArray.drop(Aes128GcmParams.ivSizeForAesGcmInBytes)
      decrypter <- Either
        .catchNonFatal {
          val keySpec = new SecretKeySpec(symmetricKey.toByteArray, Aes128GcmParams.jceKeyAlgorithm)
          val cipher = Cipher.getInstance(
            Aes128GcmParams.jceInternalName,
            JceSecurityProvider.bouncyCastleProvider,
          )
          cipher.init(
            Cipher.DECRYPT_MODE,
            keySpec,
            new GCMParameterSpec(Aes128GcmParams.gcmTagBits, iv),
          )
          cipher
        }
        .leftMap(err => DecryptionError.InvalidSymmetricKey(err.toString))
      plaintext <- Either
        .catchNonFatal(
          decrypter.doFinal(rawCiphertext)
        )
        .leftMap(err => DecryptionError.FailedToDecrypt(err.toString))
    } yield ByteString.copyFrom(plaintext)
}
