// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.config

import cats.data.NonEmptySet
import com.digitalasset.canton.config.CryptoConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.protocol.{DynamicDomainParameters, StaticDomainParameters}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.version.{DomainProtocolVersion, ProtocolVersion}

/** Configuration of domain parameters that all members connecting to a domain must adhere to.
  *
  * To set these parameters, you need to be familiar with the Canton architecture.
  * See <a href="https://www.canton.io/docs/stable/user-manual/architecture/overview.html">the Canton architecture overview</a>
  * for further information.
  *
  * @param participantResponseTimeout the amount of time (w.r.t. the sequencer clock) that a participant may take
  *                                   to validate a command and send a response.
  *                                   Once the timeout has elapsed for a request,
  *                                   the mediator will discard all responses for that request.
  *                                   Choose a lower value to reduce the time to reject a command in case one of the
  *                                   involved participants has high load / operational problems.
  *                                   Choose a higher value to reduce the likelihood of commands being rejected
  *                                   due to timeouts.
  * @param mediatorReactionTimeout the maximum amount of time (w.r.t. the sequencer clock) that the mediator may take
  *                                to validate the responses for a request and broadcast the result message.
  *                                The mediator reaction timeout starts when the confirmation response timeout has elapsed.
  *                                If the mediator does not send a result message within that timeout,
  *                                participants must rollback the transaction underlying the request.
  *                                Chooses a lower value to reduce the time to learn whether a command
  *                                has been accepted.
  *                                Choose a higher value to reduce the likelihood of commands being rejected
  *                                due to timeouts.
  * @param ledgerTimeRecordTimeTolerance the maximum absolute difference between the ledger time and the
  *                                      record time of a command.
  *                                      If the absolute difference would be larger for a command,
  *                                      then the command must be rejected.
  * @param transferExclusivityTimeout this timeout affects who can initiate a transfer-in.
  *                                   Before the timeout, only the submitter of the transfer-out can initiate the
  *                                   corresponding transfer-in.
  *                                   After the timeout, every stakeholder of the contract can initiate a transfer-in,
  *                                   if it has not yet happened.
  *                                   Moreover, if this timeout is zero, no automatic transfer-ins will occur.
  *                                   Choose a low value, if you want to lower the time that contracts can be inactive
  *                                   due to ongoing transfers.
  *                                   TODO(andreas): Choosing a high value currently has no practical benefit, but
  *                                   will have benefits in a future version.
  * @param reconciliationInterval determines the time between sending two successive ACS commitments.
  *                               Must be a multiple of 1 second.
  * @param topologyChangeDelay determines the offset applied to the topology transactions before they become active, in order to support parallel transaction processing
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
  * @param protocolVersion The protocol version spoken on the domain. All participants and domain nodes attempting to connect to the sequencer need to support this protocol version to connect.
  */
final case class DomainParametersConfig(
    participantResponseTimeout: NonNegativeFiniteDuration =
      DynamicDomainParameters.defaultParticipantResponseTimeout,
    mediatorReactionTimeout: NonNegativeFiniteDuration =
      DynamicDomainParameters.defaultMediatorReactionTimeout,
    ledgerTimeRecordTimeTolerance: NonNegativeFiniteDuration =
      DynamicDomainParameters.defaultLedgerTimeRecordTimeTolerance,
    transferExclusivityTimeout: NonNegativeFiniteDuration =
      DynamicDomainParameters.defaultTransferExclusivityTimeout,
    reconciliationInterval: PositiveSeconds = StaticDomainParameters.defaultReconciliationInterval,
    topologyChangeDelay: NonNegativeFiniteDuration =
      DynamicDomainParameters.defaultTopologyChangeDelay,
    maxRatePerParticipant: NonNegativeInt = StaticDomainParameters.defaultMaxRatePerParticipant,
    maxInboundMessageSize: NonNegativeInt = StaticDomainParameters.defaultMaxInboundMessageSize,
    uniqueContractKeys: Boolean = true,
    requiredSigningKeySchemes: Option[NonEmptySet[SigningKeyScheme]] = None,
    requiredEncryptionKeySchemes: Option[NonEmptySet[EncryptionKeyScheme]] = None,
    requiredSymmetricKeySchemes: Option[NonEmptySet[SymmetricKeyScheme]] = None,
    requiredHashAlgorithms: Option[NonEmptySet[HashAlgorithm]] = None,
    requiredCryptoKeyFormats: Option[NonEmptySet[CryptoKeyFormat]] = None,
    protocolVersion: DomainProtocolVersion = DomainProtocolVersion(ProtocolVersion.default),
) {

  /** Converts the domain parameters config into a domain parameters protocol message.
    *
    * Sets the required crypto schemes based on the provided crypto config if they are unset in the config.
    */
  def toStaticDomainParameters(
      cryptoConfig: CryptoConfig
  ): Either[String, StaticDomainParameters] = {

    def selectSchemes[S](
        configuredRequired: Option[NonEmptySet[S]],
        allowedFn: CryptoConfig => Either[String, NonEmptySet[S]],
    ): Either[String, NonEmptySet[S]] =
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

  def toDynamicDomainParameters: DynamicDomainParameters =
    DynamicDomainParameters(
      participantResponseTimeout = participantResponseTimeout,
      mediatorReactionTimeout = mediatorReactionTimeout,
      transferExclusivityTimeout = transferExclusivityTimeout,
      topologyChangeDelay = topologyChangeDelay,
      ledgerTimeRecordTimeTolerance = ledgerTimeRecordTimeTolerance,
    )

}
