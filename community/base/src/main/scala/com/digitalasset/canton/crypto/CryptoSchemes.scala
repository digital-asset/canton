// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{
  CryptoConfig,
  CryptoProvider,
  CryptoProviderScheme,
  CryptoSchemeConfig,
  EncryptionSchemeConfig,
  SigningSchemeConfig,
}
import com.digitalasset.canton.crypto.kms.Kms
import com.digitalasset.canton.util.EitherUtil
import com.google.common.annotations.VisibleForTesting

final case class CryptoSchemes private (
    signingSchemes: SigningCryptoSchemes,
    encryptionSchemes: EncryptionCryptoSchemes,
    symmetricKeySchemes: CryptoScheme[SymmetricKeyScheme],
    hashAlgorithms: CryptoScheme[HashAlgorithm],
    pbkdfSchemes: Option[CryptoScheme[PbkdfScheme]],
)

object CryptoSchemes {

  /** Check that all allowed schemes are actually supported by the KMS (i.e., driver or equivalent).
    *
    * The default scheme MUST be supported by the KMS. If an allowed scheme is not supported by the
    * KMS, the scheme will not be actively supported but only used, for example, to verify a
    * signature or perform asymmetric encryption. The driver's private cryptography does not need to
    * support them.
    */
  private def selectKmsScheme[S](
      cryptoScheme: CryptoScheme[S],
      kmsSupported: Set[S],
      description: String,
  ): Either[String, CryptoScheme[S]] =
    for {
      _ <- EitherUtil.condUnit(
        kmsSupported.contains(cryptoScheme.default),
        s"The configured default scheme ${cryptoScheme.default} not supported by the KMS: $kmsSupported",
      )
      supported = cryptoScheme.allowed.intersect(kmsSupported)
      supportedNE <- NonEmpty
        .from(supported)
        .toRight(
          s"None of the allowed $description ${cryptoScheme.allowed.mkString(", ")} are supported by" +
            s"the KMS: $kmsSupported"
        )
      selectedCryptoScheme <- CryptoScheme.create(cryptoScheme.default, supportedNE)
    } yield selectedCryptoScheme

  private[crypto] def selectKmsSchemes(
      cryptoSchemes: CryptoSchemes,
      kms: Kms.SupportedSchemes,
  ): Either[String, CryptoSchemes] =
    for {
      // use only schemes allowed by configuration and supported by the KMS
      selectedSigningKeySpecs <-
        selectKmsScheme(
          cryptoSchemes.signingSchemes.keySpecs,
          kms.supportedSigningKeySpecs,
          "signing key specs",
        )

      selectedSigningAlgoSpecs <-
        selectKmsScheme(
          cryptoSchemes.signingSchemes.algorithmSpecs,
          kms.supportedSigningAlgoSpecs,
          "signing algorithm specs",
        )

      signingSchemes <- SigningCryptoSchemes.create(
        keySpecs = selectedSigningKeySpecs,
        algorithmSpecs = selectedSigningAlgoSpecs,
      )

      selectedEncryptionKeySpecs <-
        selectKmsScheme(
          cryptoSchemes.encryptionSchemes.keySpecs,
          kms.supportedEncryptionKeySpecs,
          "encryption key specs",
        )

      selectedEncryptionAlgoSpecs <-
        selectKmsScheme(
          cryptoSchemes.encryptionSchemes.algorithmSpecs,
          kms.supportedEncryptionAlgoSpecs,
          "encryption algorithm specs",
        )

      encryptionSchemes <- EncryptionCryptoSchemes.create(
        keySpecs = selectedEncryptionKeySpecs,
        algorithmSpecs = selectedEncryptionAlgoSpecs,
      )
    } yield cryptoSchemes.copy(
      signingSchemes = signingSchemes,
      encryptionSchemes = encryptionSchemes,
    )

  @VisibleForTesting
  def tryFromConfig(config: CryptoConfig): CryptoSchemes =
    fromConfig(config)
      .getOrElse(
        throw new RuntimeException(
          "Could not validate the selected crypto schemes from the configuration file."
        )
      )

  def fromConfig(config: CryptoConfig): Either[String, CryptoSchemes] =
    for {
      symmetricKeySchemes <- CryptoScheme.create(config.symmetric, config.provider.symmetric)
      hashAlgorithms <- CryptoScheme.create(config.hash, config.provider.hash)
      pbkdfSchemesO <- config.provider.pbkdf.traverse(CryptoScheme.create(config.pbkdf, _))
      signingCryptoSchemes <- SigningCryptoSchemes.create(config.signing, config.provider)
      encryptionCryptoSchemes <- EncryptionCryptoSchemes.create(config.encryption, config.provider)
    } yield CryptoSchemes(
      signingCryptoSchemes,
      encryptionCryptoSchemes,
      symmetricKeySchemes,
      hashAlgorithms,
      pbkdfSchemesO,
    )
}

/** The default and supported key and algorithm signing specifications. */
final case class SigningCryptoSchemes private (
    keySpecs: CryptoScheme[SigningKeySpec],
    algorithmSpecs: CryptoScheme[SigningAlgorithmSpec],
)

object SigningCryptoSchemes {

  /** Validates that the default key spec is compatible with the default algorithm spec. Without
    * this check, a misconfiguration (e.g. default key = EcP384 with default algorithm = Ed25519)
    * would silently produce keys that cannot be used with the default signing algorithm, or force
    * use of a weaker algorithm than intended (cryptographic downgrade).
    */
  def create(
      keySpecs: CryptoScheme[SigningKeySpec],
      algorithmSpecs: CryptoScheme[SigningAlgorithmSpec],
  ): Either[String, SigningCryptoSchemes] = for {
    _ <- EitherUtil.condUnit(
      algorithmSpecs.default.supportedSigningKeySpecs.contains(keySpecs.default),
      s"Default signing key spec ${keySpecs.default} is not supported by default signing algorithm " +
        s"${algorithmSpecs.default}. Supported key specs: ${algorithmSpecs.default.supportedSigningKeySpecs}",
    )
  } yield SigningCryptoSchemes(keySpecs, algorithmSpecs)

  def create(
      signingSchemeConfig: SigningSchemeConfig,
      cryptoProvider: CryptoProvider,
  ): Either[String, SigningCryptoSchemes] =
    for {
      keySpecs <- CryptoScheme.create(
        signingSchemeConfig.keys,
        cryptoProvider.signingKeys,
      )
      algoSpecs <- CryptoScheme.create(
        signingSchemeConfig.algorithms,
        cryptoProvider.signingAlgorithms,
      )
      signingCryptoSchemes <- create(keySpecs, algoSpecs)
    } yield signingCryptoSchemes

  private[crypto] def tryCreate(
      keySpecs: CryptoScheme[SigningKeySpec],
      algorithmSpecs: CryptoScheme[SigningAlgorithmSpec],
  ): SigningCryptoSchemes = create(keySpecs, algorithmSpecs).valueOr { err =>
    throw new IllegalArgumentException(s"Invalid signing crypto scheme configuration: $err")
  }
}

/** The default and supported key and algorithm encryption specifications. */
final case class EncryptionCryptoSchemes private (
    keySpecs: CryptoScheme[EncryptionKeySpec],
    algorithmSpecs: CryptoScheme[EncryptionAlgorithmSpec],
)

object EncryptionCryptoSchemes {

  /** Validates that the default key spec is compatible with the default algorithm spec. Without
    * this check, a misconfiguration could silently produce keys that cannot be used with the
    * default encryption algorithm.
    */
  def create(
      keySpecs: CryptoScheme[EncryptionKeySpec],
      algorithmSpecs: CryptoScheme[EncryptionAlgorithmSpec],
  ): Either[String, EncryptionCryptoSchemes] = for {
    _ <- EitherUtil.condUnit(
      algorithmSpecs.default.supportedEncryptionKeySpecs.contains(keySpecs.default),
      s"Default encryption key spec ${keySpecs.default} is not supported by default encryption algorithm " +
        s"${algorithmSpecs.default}. Supported key specs: ${algorithmSpecs.default.supportedEncryptionKeySpecs}",
    )
  } yield EncryptionCryptoSchemes(keySpecs, algorithmSpecs)

  def create(
      encryptionSchemeConfig: EncryptionSchemeConfig,
      cryptoProvider: CryptoProvider,
  ): Either[String, EncryptionCryptoSchemes] =
    for {
      keySpecs <- CryptoScheme.create(
        encryptionSchemeConfig.keys,
        cryptoProvider.encryptionKeys,
      )
      algoSpecs <- CryptoScheme.create(
        encryptionSchemeConfig.algorithms,
        cryptoProvider.encryptionAlgorithms,
      )
      encryptionCryptoSchemes <- create(keySpecs, algoSpecs)
    } yield encryptionCryptoSchemes

  private[crypto] def tryCreate(
      keySpecs: CryptoScheme[EncryptionKeySpec],
      algorithmSpecs: CryptoScheme[EncryptionAlgorithmSpec],
  ): EncryptionCryptoSchemes = create(keySpecs, algorithmSpecs).valueOr { err =>
    throw new IllegalArgumentException(s"Invalid encryption crypto scheme configuration: $err")
  }
}

final case class CryptoScheme[S] private (default: S, allowed: NonEmpty[Set[S]])

object CryptoScheme {

  /** Creates a [[CryptoScheme]] based on the configuration and the provider's capabilities.
    *
    * Default scheme is either explicitly configured or the provider's default scheme. Allowed
    * schemes are either explicitly configured and must be supported by the provider, or all
    * supported schemes by the provider.
    */
  def create[S](
      configured: CryptoSchemeConfig[S],
      provider: CryptoProviderScheme[S],
  ): Either[String, CryptoScheme[S]] = {
    val supported = provider.supported

    // If no allowed schemes are configured, all supported schemes are allowed.
    val allowed = configured.allowed.getOrElse(supported)

    // If no scheme is configured, use the default scheme of the provider
    val default = configured.default.getOrElse(provider.default)

    // The allowed schemes that are not in the supported set
    val unsupported = allowed.diff(supported)

    for {
      _ <- EitherUtil.condUnit(
        unsupported.isEmpty,
        s"Allowed schemes $unsupported are not supported",
      )
      scheme <- CryptoScheme.create(default, allowed)
    } yield scheme
  }

  def create[S](default: S, allowed: NonEmpty[Set[S]]): Either[String, CryptoScheme[S]] =
    Either.cond(
      allowed.contains(default),
      CryptoScheme(default, allowed),
      s"Scheme $default is not allowed: $allowed",
    )

  private[crypto] def tryCreate[S](
      default: S,
      allowed: NonEmpty[Set[S]],
  ): CryptoScheme[S] = create(default, allowed).valueOr { err =>
    throw new IllegalArgumentException(s"Invalid crypto scheme configuration: $err")
  }

}
