// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.DomainId
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCryptoProvider
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.{SequencerId, TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.version.ProtocolVersion

/** Domain parameters used for unit testing with sane default values. */
object TestDomainParameters {
  def identityFactory(
      loggerFactory: NamedLoggerFactory,
      transformDefaults: DynamicDomainParameters => DynamicDomainParameters =
        identity[DynamicDomainParameters],
  ) = TestingIdentityFactory(
    TestingTopology(),
    loggerFactory,
    transformDefaults(TestDomainParameters.defaultDynamic),
  )

  def domainSyncCryptoApi(
      domainId: DomainId,
      loggerFactory: NamedLoggerFactory,
      transformDefaults: DynamicDomainParameters => DynamicDomainParameters =
        identity[DynamicDomainParameters],
  ): DomainSyncCryptoClient =
    identityFactory(loggerFactory, transformDefaults).forOwnerAndDomain(
      SequencerId(domainId),
      domainId,
    )

  // Uses SymbolicCrypto for the configured crypto schemes
  val defaultStatic: StaticDomainParameters = StaticDomainParameters(
    reconciliationInterval = StaticDomainParameters.defaultReconciliationInterval,
    maxRatePerParticipant = StaticDomainParameters.defaultMaxRatePerParticipant,
    maxInboundMessageSize = StaticDomainParameters.defaultMaxInboundMessageSize,
    uniqueContractKeys = false,
    requiredSigningKeySchemes = SymbolicCryptoProvider.supportedSigningKeySchemes,
    requiredEncryptionKeySchemes = SymbolicCryptoProvider.supportedEncryptionKeySchemes,
    requiredSymmetricKeySchemes = SymbolicCryptoProvider.supportedSymmetricKeySchemes,
    requiredHashAlgorithms = SymbolicCryptoProvider.supportedHashAlgorithms,
    requiredCryptoKeyFormats = SymbolicCryptoProvider.supportedCryptoKeyFormats,
    protocolVersion = ProtocolVersion.current,
  )

  val defaultDynamic: DynamicDomainParameters = DynamicDomainParameters(
    participantResponseTimeout = DynamicDomainParameters.defaultParticipantResponseTimeout,
    mediatorReactionTimeout = DynamicDomainParameters.defaultMediatorReactionTimeout,
    transferExclusivityTimeout = DynamicDomainParameters.defaultTransferExclusivityTimeout,
    topologyChangeDelay = DynamicDomainParameters.defaultTopologyChangeDelay,
    ledgerTimeRecordTimeTolerance = DynamicDomainParameters.defaultLedgerTimeRecordTimeTolerance,
  )

}
