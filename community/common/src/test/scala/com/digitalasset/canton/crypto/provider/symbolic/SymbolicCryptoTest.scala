// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoKeyFormat,
  EncryptionKeyScheme,
  EncryptionTest,
  HashAlgorithm,
  RandomTest,
  SigningKeyScheme,
  SigningTest,
  SymmetricKeyScheme,
}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

object SymbolicCryptoProvider {
  // The schemes are ignored by symbolic crypto

  val supportedSigningKeySchemes: NonEmpty[Set[SigningKeyScheme]] =
    NonEmpty.mk(Set, SigningKeyScheme.Ed25519)
  val supportedSymmetricKeySchemes: NonEmpty[Set[SymmetricKeyScheme]] =
    NonEmpty.mk(Set, SymmetricKeyScheme.Aes128Gcm)
  val supportedEncryptionKeySchemes: NonEmpty[Set[EncryptionKeyScheme]] =
    NonEmpty.mk(Set, EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm)
  val supportedHashAlgorithms: NonEmpty[Set[HashAlgorithm]] = NonEmpty.mk(Set, HashAlgorithm.Sha256)
  val supportedCryptoKeyFormats: NonEmpty[Set[CryptoKeyFormat]] =
    NonEmpty.mk(Set, CryptoKeyFormat.Symbolic)

}

class SymbolicCryptoTest
    extends AsyncWordSpec
    with SigningTest
    with EncryptionTest
    with RandomTest {

  "SymbolicCrypto" can {

    def symbolicCrypto(): Future[Crypto] =
      Future.successful(
        SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)
      )

    behave like signingProvider(SymbolicCryptoProvider.supportedSigningKeySchemes, symbolicCrypto())
    behave like encryptionProvider(
      SymbolicCryptoProvider.supportedEncryptionKeySchemes,
      SymbolicCryptoProvider.supportedSymmetricKeySchemes,
      symbolicCrypto(),
    )
    behave like randomnessProvider(symbolicCrypto().map(_.pureCrypto))

    // Symbolic crypto does not support Java key conversion, thus not tested
  }

}
