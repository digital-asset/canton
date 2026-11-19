// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.google.protobuf.ByteString
import org.bouncycastle.crypto.generators.Argon2BytesGenerator
import org.bouncycastle.crypto.params.Argon2Parameters

/** Password-based encryption with Argon2id key derivation. */
trait JcePasswordBasedEncryption extends PasswordBasedEncryptionOps {
  this: SymmetricEncryptionOps =>

  override def deriveSymmetricKey(
      password: String,
      symmetricKeyScheme: SymmetricKeyScheme,
      pbkdfScheme: PbkdfScheme,
      saltO: Option[SecureRandomness],
  ): Either[PasswordBasedEncryptionError, PasswordBasedEncryptionKey] =
    pbkdfScheme match {
      case mode: PbkdfScheme.Argon2idMode1.type =>
        val salt = saltO.getOrElse(
          SecureRandomness(
            ByteString.copyFrom(
              JceSecureRandom.generateRandomBytes(pbkdfScheme.defaultSaltLengthInBytes)
            )
          )
        )

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

        SymmetricKey
          .create(CryptoKeyFormat.Raw, ByteString.copyFrom(keyBytes), symmetricKeyScheme)
          .leftMap(PasswordBasedEncryptionError.KeyCreationError.apply)
          .flatMap { key =>
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
}

/** Password-based encryption without any key store or configuration, e.g. for the console. */
object JcePasswordBasedEncryption extends JceSymmetricEncryption with JcePasswordBasedEncryption {

  override val defaultSymmetricKeyScheme: SymmetricKeyScheme = SymmetricKeyScheme.Aes128Gcm

  override protected[crypto] val defaultPbkdfScheme: PbkdfScheme = PbkdfScheme.Argon2idMode1
}
