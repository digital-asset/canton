// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.version.{GeneratorsVersion, ProtocolVersion}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsProtocol {

  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.time.GeneratorsTime.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import com.digitalasset.canton.Generators.*

  implicit val staticDomainParametersArb: Arbitrary[StaticDomainParameters] = {
    Arbitrary(for {
      uniqueContractKeys <- implicitly[Arbitrary[Boolean]].arbitrary

      requiredSigningKeySchemes <- nonEmptySetGen[SigningKeyScheme]
      requiredEncryptionKeySchemes <- nonEmptySetGen[EncryptionKeyScheme]
      requiredSymmetricKeySchemes <- nonEmptySetGen[SymmetricKeyScheme]
      requiredHashAlgorithms <- nonEmptySetGen[HashAlgorithm]
      requiredCryptoKeyFormats <- nonEmptySetGen[CryptoKeyFormat]

      protocolVersion <- protocolVersionGen

      reconciliationInterval <- defaultValueGen(
        protocolVersion,
        StaticDomainParameters.defaultReconciliationIntervalFrom,
      )

      maxRatePerParticipant <- defaultValueGen(
        protocolVersion,
        StaticDomainParameters.defaultMaxRatePerParticipantFrom,
      )

      maxRequestSize <- defaultValueGen(
        protocolVersion,
        StaticDomainParameters.defaultMaxRequestSizeFrom,
      )

      parameters = StaticDomainParameters.create(
        maxRequestSize,
        uniqueContractKeys,
        requiredSigningKeySchemes,
        requiredEncryptionKeySchemes,
        requiredSymmetricKeySchemes,
        requiredHashAlgorithms,
        requiredCryptoKeyFormats,
        protocolVersion,
        reconciliationInterval,
        maxRatePerParticipant,
      )

    } yield parameters)
  }

  implicit val dynamicDomainParametersArb: Arbitrary[DynamicDomainParameters] = Arbitrary(for {
    participantResponseTimeout <- nonNegativeFiniteDurationGen
    mediatorReactionTimeout <- nonNegativeFiniteDurationGen
    transferExclusivityTimeout <- nonNegativeFiniteDurationGen
    topologyChangeDelay <- nonNegativeFiniteDurationGen

    mediatorDeduplicationMargin <- nonNegativeFiniteDurationGen
    // Because of the potential multiplication by 2 below, we want a reasonably small value
    ledgerTimeRecordTimeTolerance <- Gen
      .choose(0L, 10000L)
      .map(NonNegativeFiniteDuration.tryOfMicros)

    representativePV <- GeneratorsVersion.representativeProtocolVersionGen(DynamicDomainParameters)

    reconciliationInterval <- defaultValueArb(
      representativePV,
      DynamicDomainParameters.defaultReconciliationIntervalUntil,
    )

    maxRatePerParticipant <- defaultValueArb(
      representativePV,
      DynamicDomainParameters.defaultMaxRatePerParticipantUntil,
    )

    maxRequestSize <- defaultValueArb(
      representativePV,
      DynamicDomainParameters.defaultMaxRequestSizeUntil,
    )

    // Starting from pv=4, there is an additional constraint on the mediatorDeduplicationTimeout
    updatedMediatorDeduplicationTimeout =
      if (representativePV.representative > ProtocolVersion.v3)
        ledgerTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2) + mediatorDeduplicationMargin
      else
        ledgerTimeRecordTimeTolerance * NonNegativeInt.tryCreate(2)

    dynamicDomainParameters = DynamicDomainParameters.tryCreate(
      participantResponseTimeout,
      mediatorReactionTimeout,
      transferExclusivityTimeout,
      topologyChangeDelay,
      ledgerTimeRecordTimeTolerance,
      updatedMediatorDeduplicationTimeout,
      reconciliationInterval,
      maxRatePerParticipant,
      maxRequestSize,
    )(representativePV)

  } yield dynamicDomainParameters)

}
