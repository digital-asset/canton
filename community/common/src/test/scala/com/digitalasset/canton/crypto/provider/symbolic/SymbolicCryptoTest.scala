// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import cats.data.NonEmptySet
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoKeyFormat,
  EncryptionKeyScheme,
  EncryptionTest,
  HashAlgorithm,
  HmacPrivateTest,
  SigningKeyScheme,
  SigningTest,
  SymmetricKeyScheme,
}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

object SymbolicCryptoProvider {
  // The schemes are ignored by symbolic crypto

  val supportedSigningKeySchemes: NonEmptySet[SigningKeyScheme] =
    NonEmptySet.of(SigningKeyScheme.Ed25519)
  val supportedSymmetricKeySchemes: NonEmptySet[SymmetricKeyScheme] =
    NonEmptySet.of(SymmetricKeyScheme.Aes128Gcm)
  val supportedEncryptionKeySchemes: NonEmptySet[EncryptionKeyScheme] =
    NonEmptySet.of(EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm)
  val supportedHashAlgorithms: NonEmptySet[HashAlgorithm] = NonEmptySet.of(HashAlgorithm.Sha256)
  val supportedCryptoKeyFormats: NonEmptySet[CryptoKeyFormat] =
    NonEmptySet.of(CryptoKeyFormat.Symbolic)

}

class SymbolicCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with HmacPrivateTest {

  "SymbolicCrypto" can {

    def symbolicCrypto(): Future[Crypto] =
      Future.successful(SymbolicCrypto.create(timeouts, loggerFactory))

    behave like signingProvider(SymbolicCryptoProvider.supportedSigningKeySchemes, symbolicCrypto())
    behave like encryptionProvider(
      SymbolicCryptoProvider.supportedEncryptionKeySchemes,
      SymbolicCryptoProvider.supportedSymmetricKeySchemes,
      symbolicCrypto(),
    )
    behave like hmacProvider(symbolicCrypto().map(_.privateCrypto))

    // Symbolic crypto does not support Java key conversion, thus not tested
  }

}
