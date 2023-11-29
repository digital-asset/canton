// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{CryptoConfig, ProtocolConfig}
import com.digitalasset.canton.crypto.CryptoFactory.{
  selectAllowedEncryptionKeyScheme,
  selectAllowedHashAlgorithms,
  selectAllowedSigningKeyScheme,
  selectAllowedSymmetricKeySchemes,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.version.{DomainProtocolVersion, ProtocolVersion}

/** Configuration of domain parameters that all members connecting to a domain must adhere to.
  *
  * To set these parameters, you need to be familiar with the Canton architecture.
  * See <a href="https://docs.daml.com/canton/architecture/overview.html">the Canton architecture overview</a>
  * for further information.
  *
  * @param uniqueContractKeys When set, participants connected to this domain will check that contract keys are unique.
  *                           When a participant is connected to a domain with unique contract keys support,
  *                           it must not connect nor have ever been connected to any other domain.
  * @param requiredSigningKeySchemes The optional required signing key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredEncryptionKeySchemes The optional required encryption key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredSymmetricKeySchemes The optional required symmetric key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredHashAlgorithms The optional required hash algorithms that a member has to support. If none is specified, all the allowed algorithms are required.
  * @param requiredCryptoKeyFormats The optional required crypto key formats that a member has to support. If none is specified, all the supported algorithms are required.
  * @param protocolVersion                        The protocol version spoken on the domain. All participants and domain nodes attempting to connect to the sequencer need to support this protocol version to connect.
  * @param dontWarnOnDeprecatedPV If true, then this domain will not emit a warning when configured to use a deprecated protocol version (such as 2.0.0).
  * @param resetStoredStaticConfig DANGEROUS: If true, then the stored static configuration parameters will be reset to the ones in the configuration file
  */
final case class DomainParametersConfig(
    uniqueContractKeys: Boolean = true,
    requiredSigningKeySchemes: Option[NonEmpty[Set[SigningKeyScheme]]] = None,
    requiredEncryptionKeySchemes: Option[NonEmpty[Set[EncryptionKeyScheme]]] = None,
    requiredSymmetricKeySchemes: Option[NonEmpty[Set[SymmetricKeyScheme]]] = None,
    requiredHashAlgorithms: Option[NonEmpty[Set[HashAlgorithm]]] = None,
    requiredCryptoKeyFormats: Option[NonEmpty[Set[CryptoKeyFormat]]] = None,
    protocolVersion: DomainProtocolVersion = DomainProtocolVersion(
      ProtocolVersion.latest
    ),
    // TODO(i15561): Revert back to `false` once there is a stable Daml 3 protocol version
    override val devVersionSupport: Boolean = true,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    resetStoredStaticConfig: Boolean = false,
) extends ProtocolConfig
    with PrettyPrinting {

  override def pretty: Pretty[DomainParametersConfig] = prettyOfClass(
    param("uniqueContractKeys", _.uniqueContractKeys),
    param("requiredSigningKeySchemes", _.requiredSigningKeySchemes),
    param("requiredEncryptionKeySchemes", _.requiredEncryptionKeySchemes),
    param("requiredSymmetricKeySchemes", _.requiredSymmetricKeySchemes),
    param("requiredHashAlgorithms", _.requiredHashAlgorithms),
    param("requiredCryptoKeyFormats", _.requiredCryptoKeyFormats),
    param("protocolVersion", _.protocolVersion.version),
    param("devVersionSupport", _.devVersionSupport),
    param("dontWarnOnDeprecatedPV", _.dontWarnOnDeprecatedPV),
    param("resetStoredStaticConfig", _.resetStoredStaticConfig),
  )

  override def initialProtocolVersion: ProtocolVersion = protocolVersion.version

  /** Converts the domain parameters config into a domain parameters protocol message.
    *
    * Sets the required crypto schemes based on the provided crypto config if they are unset in the config.
    */
  def toStaticDomainParameters(
      cryptoConfig: CryptoConfig
  ): Either[String, StaticDomainParameters] = {

    def selectSchemes[S](
        configuredRequired: Option[NonEmpty[Set[S]]],
        allowedFn: CryptoConfig => Either[String, NonEmpty[Set[S]]],
    ): Either[String, NonEmpty[Set[S]]] =
      for {
        allowed <- allowedFn(cryptoConfig)
        required = configuredRequired.getOrElse(allowed)
        // All required schemes must be allowed
        _ <- Either.cond(
          required.forall(r => allowed.contains(r)),
          (),
          s"Required schemes $required are not all allowed $allowed",
        )
      } yield required

    // Set to allowed schemes if none required schemes are specified
    for {
      newRequiredSigningKeySchemes <- selectSchemes(
        requiredSigningKeySchemes,
        selectAllowedSigningKeyScheme,
      )
      newRequiredEncryptionKeySchemes <- selectSchemes(
        requiredEncryptionKeySchemes,
        selectAllowedEncryptionKeyScheme,
      )
      newRequiredSymmetricKeySchemes <- selectSchemes(
        requiredSymmetricKeySchemes,
        selectAllowedSymmetricKeySchemes,
      )
      newRequiredHashAlgorithms <- selectSchemes(
        requiredHashAlgorithms,
        selectAllowedHashAlgorithms,
      )
      newCryptoKeyFormats = requiredCryptoKeyFormats.getOrElse(
        cryptoConfig.provider.supportedCryptoKeyFormatsForProtocol(protocolVersion.unwrap)
      )
    } yield {
      StaticDomainParameters.create(
        uniqueContractKeys = uniqueContractKeys,
        requiredSigningKeySchemes = newRequiredSigningKeySchemes,
        requiredEncryptionKeySchemes = newRequiredEncryptionKeySchemes,
        requiredSymmetricKeySchemes = newRequiredSymmetricKeySchemes,
        requiredHashAlgorithms = newRequiredHashAlgorithms,
        requiredCryptoKeyFormats = newCryptoKeyFormats,
        protocolVersion = protocolVersion.unwrap,
      )
    }
  }
}
