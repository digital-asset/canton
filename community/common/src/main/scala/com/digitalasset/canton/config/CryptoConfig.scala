// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.data.NonEmptySet
import com.digitalasset.canton.crypto.{
  CryptoKeyFormat,
  EncryptionKeyScheme,
  HashAlgorithm,
  SigningKeyScheme,
  SymmetricKeyScheme,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

case class CryptoProviderScheme[S](default: S, supported: NonEmptySet[S]) {
  require(supported.contains(default))
}

sealed trait CryptoProvider extends PrettyPrinting {

  def name: String

  def signing: CryptoProviderScheme[SigningKeyScheme]
  def encryption: CryptoProviderScheme[EncryptionKeyScheme]
  def symmetric: CryptoProviderScheme[SymmetricKeyScheme]
  def hash: CryptoProviderScheme[HashAlgorithm]

  def supportedCryptoKeyFormats: NonEmptySet[CryptoKeyFormat]

  override def pretty: Pretty[CryptoProvider.this.type] = prettyOfString(_.name)
}

object CryptoProvider {

  case object Tink extends CryptoProvider {
    override def name: String = "Tink"

    override def signing: CryptoProviderScheme[SigningKeyScheme] =
      CryptoProviderScheme(
        SigningKeyScheme.Ed25519,
        NonEmptySet.of(
          SigningKeyScheme.Ed25519,
          SigningKeyScheme.EcDsaP256,
          SigningKeyScheme.EcDsaP384,
        ),
      )

    override def encryption: CryptoProviderScheme[EncryptionKeyScheme] =
      CryptoProviderScheme(
        EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm,
        NonEmptySet.of(EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm),
      )

    override def symmetric: CryptoProviderScheme[SymmetricKeyScheme] =
      CryptoProviderScheme(
        SymmetricKeyScheme.Aes128Gcm,
        NonEmptySet.of(SymmetricKeyScheme.Aes128Gcm),
      )

    override def hash: CryptoProviderScheme[HashAlgorithm] =
      CryptoProviderScheme(HashAlgorithm.Sha256, NonEmptySet.of(HashAlgorithm.Sha256))

    override def supportedCryptoKeyFormats: NonEmptySet[CryptoKeyFormat] =
      NonEmptySet.of(CryptoKeyFormat.Tink)
  }

  case object Jce extends CryptoProvider {
    override def name: String = "JCE"

    override def signing: CryptoProviderScheme[SigningKeyScheme] =
      CryptoProviderScheme(
        SigningKeyScheme.Ed25519,
        NonEmptySet.of(
          SigningKeyScheme.Ed25519,
          SigningKeyScheme.EcDsaP256,
          SigningKeyScheme.EcDsaP384,
          SigningKeyScheme.Sm2,
        ),
      )

    override def encryption: CryptoProviderScheme[EncryptionKeyScheme] =
      CryptoProviderScheme(
        EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm,
        NonEmptySet.of(EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm),
      )

    override def symmetric: CryptoProviderScheme[SymmetricKeyScheme] =
      CryptoProviderScheme(
        SymmetricKeyScheme.Aes128Gcm,
        NonEmptySet.of(SymmetricKeyScheme.Aes128Gcm),
      )

    override def hash: CryptoProviderScheme[HashAlgorithm] =
      CryptoProviderScheme(HashAlgorithm.Sha256, NonEmptySet.of(HashAlgorithm.Sha256))

    override def supportedCryptoKeyFormats: NonEmptySet[CryptoKeyFormat] =
      NonEmptySet.of(CryptoKeyFormat.Raw, CryptoKeyFormat.Der)
  }
}

/** Configures the optional default and allowed schemes of kind S.
  *
  * @param default The optional scheme to use. If none is specified, use the provider's default scheme of kind S.
  * @param allowed The optional allowed schemes to use. If none is specified, all the provider's supported schemes of kind S are allowed.
  */
case class CryptoSchemeConfig[S](default: Option[S] = None, allowed: Option[NonEmptySet[S]] = None)

/** Cryptography configuration.
  *
  * @param provider The crypto provider implementation to use.
  * @param signing The signing key scheme configuration.
  * @param encryption The encryption key scheme configuration.
  * @param symmetric The symmetric key scheme configuration.
  * @param hash The hash algorithm configuration.
  */
case class CryptoConfig(
    provider: CryptoProvider =
      CryptoProvider.Tink, // TODO(i5100): Choosing Tink as default, as long as JCE occasionally throws exceptions on decryption.
    signing: CryptoSchemeConfig[SigningKeyScheme] = CryptoSchemeConfig(),
    encryption: CryptoSchemeConfig[EncryptionKeyScheme] = CryptoSchemeConfig(),
    symmetric: CryptoSchemeConfig[SymmetricKeyScheme] = CryptoSchemeConfig(),
    hash: CryptoSchemeConfig[HashAlgorithm] = CryptoSchemeConfig(),
)
