// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CryptoConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.version.{DomainProtocolVersion, ProtocolVersion}

/** Configuration of domain parameters that all members connecting to a domain must adhere to.
  *
  * To set these parameters, you need to be familiar with the Canton architecture.
  * See <a href="https://www.canton.io/docs/stable/user-manual/architecture/overview.html">the Canton architecture overview</a>
  * for further information.
  *
  * @param reconciliationInterval determines the time between sending two successive ACS commitments.
  *                               Must be a multiple of 1 second.
  * @param maxRatePerParticipant maximum number of messages sent per participant per second
  * @param maxInboundMessageSize maximum size of messages (in bytes) that the domain can receive through the public API
  * @param uniqueContractKeys When set, participants connected to this domain will check that contract keys are unique.
  *                           When a participant is connected to a domain with unique contract keys support,
  *                           it must not connect nor have ever been connected to any other domain.
  * @param requiredSigningKeySchemes The optional required signing key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredEncryptionKeySchemes The optional required encryption key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredSymmetricKeySchemes The optional required symmetric key schemes that a member has to support. If none is specified, all the allowed schemes are required.
  * @param requiredHashAlgorithms The optional required hash algorithms that a member has to support. If none is specified, all the allowed algorithms are required.
  * @param requiredCryptoKeyFormats The optional required crypto key formats that a member has to support. If none is specified, all the supported algorithms are required.
  * @param protocolVersion                        The protocol version spoken on the domain. All participants and domain nodes attempting to connect to the sequencer need to support this protocol version to connect.
  * @param willCorruptYourSystemDevVersionSupport If set to true, development protocol versions (and database schemas) will be supported. Do NOT use this in production, as it will break your system.
  * @param dontWarnOnDeprecatedPV If true, then this domain will not emit a warning when configured to use a deprecated protocol version (such as 2.0.0).
  */
final case class DomainParametersConfig(
    reconciliationInterval: PositiveSeconds = StaticDomainParameters.defaultReconciliationInterval,
    maxRatePerParticipant: NonNegativeInt = StaticDomainParameters.defaultMaxRatePerParticipant,
    maxInboundMessageSize: NonNegativeInt = StaticDomainParameters.defaultMaxInboundMessageSize,
    uniqueContractKeys: Boolean = true,
    requiredSigningKeySchemes: Option[NonEmpty[Set[SigningKeyScheme]]] = None,
    requiredEncryptionKeySchemes: Option[NonEmpty[Set[EncryptionKeyScheme]]] = None,
    requiredSymmetricKeySchemes: Option[NonEmpty[Set[SymmetricKeyScheme]]] = None,
    requiredHashAlgorithms: Option[NonEmpty[Set[HashAlgorithm]]] = None,
    requiredCryptoKeyFormats: Option[NonEmpty[Set[CryptoKeyFormat]]] = None,
    protocolVersion: DomainProtocolVersion = DomainProtocolVersion(
      ProtocolVersion.latest
    ),
    willCorruptYourSystemDevVersionSupport: Boolean = false,
    dontWarnOnDeprecatedPV: Boolean = false,
) {

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
        CryptoFactory.selectAllowedSigningKeyScheme,
      )
      newRequiredEncryptionKeySchemes <- selectSchemes(
        requiredEncryptionKeySchemes,
        CryptoFactory.selectAllowedEncryptionKeyScheme,
      )
      newRequiredSymmetricKeySchemes <- selectSchemes(
        requiredSymmetricKeySchemes,
        CryptoFactory.selectAllowedSymmetricKeySchemes,
      )
      newRequiredHashAlgorithms <- selectSchemes(
        requiredHashAlgorithms,
        CryptoFactory.selectAllowedHashAlgorithms,
      )
      newCryptoKeyFormats = requiredCryptoKeyFormats.getOrElse(
        cryptoConfig.provider.supportedCryptoKeyFormats
      )
    } yield {
      StaticDomainParameters(
        reconciliationInterval = reconciliationInterval,
        maxRatePerParticipant = maxRatePerParticipant,
        maxInboundMessageSize = maxInboundMessageSize,
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
