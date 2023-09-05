// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.transaction.Versioned
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.util.LfTransactionBuilder
import com.digitalasset.canton.version.{GeneratorsVersion, ProtocolVersion}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsProtocol {

  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.data.GeneratorsData.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.time.GeneratorsTime.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import com.digitalasset.canton.Generators.*
  import org.scalatest.EitherValues.*

  implicit val staticDomainParametersArb: Arbitrary[StaticDomainParameters] = {
    Arbitrary(for {
      uniqueContractKeys <- implicitly[Arbitrary[Boolean]].arbitrary

      requiredSigningKeySchemes <- nonEmptySetGen[SigningKeyScheme]
      requiredEncryptionKeySchemes <- nonEmptySetGen[EncryptionKeyScheme]
      requiredSymmetricKeySchemes <- nonEmptySetGen[SymmetricKeyScheme]
      requiredHashAlgorithms <- nonEmptySetGen[HashAlgorithm]
      requiredCryptoKeyFormats <- nonEmptySetGen[CryptoKeyFormat]

      protocolVersion <- protocolVersionArb.arbitrary

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
    participantResponseTimeout <- nonNegativeFiniteDurationArb.arbitrary
    mediatorReactionTimeout <- nonNegativeFiniteDurationArb.arbitrary
    transferExclusivityTimeout <- nonNegativeFiniteDurationArb.arbitrary
    topologyChangeDelay <- nonNegativeFiniteDurationArb.arbitrary

    mediatorDeduplicationMargin <- nonNegativeFiniteDurationArb.arbitrary
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

  implicit val confirmationPolicyArb: Arbitrary[ConfirmationPolicy] = genArbitrary

  implicit val serializableRawContractInstanceArb: Arbitrary[SerializableRawContractInstance] =
    Arbitrary(
      for {
        agreementText <- Gen.asciiPrintableStr.map(AgreementText(_))
        contractInstance = ExampleTransactionFactory.contractInstance()
      } yield SerializableRawContractInstance.create(contractInstance, agreementText).value
    )

  implicit val serializableContractArb: Arbitrary[SerializableContract] = Arbitrary(
    for {
      contractId <- implicitly[Arbitrary[LfContractId]].arbitrary
      rawContractInstance <- implicitly[Arbitrary[SerializableRawContractInstance]].arbitrary
      metadata <- implicitly[Arbitrary[ContractMetadata]].arbitrary
      ledgerCreateTime <- implicitly[Arbitrary[CantonTimestamp]].arbitrary
      contractSalt <- Gen.option(implicitly[Arbitrary[Salt]].arbitrary)
    } yield SerializableContract(
      contractId,
      rawContractInstance,
      metadata,
      ledgerCreateTime,
      contractSalt,
    )
  )

  implicit val globalKeyWithMaintainersArb: Arbitrary[Versioned[LfGlobalKeyWithMaintainers]] =
    Arbitrary(
      for {
        maintainers <- Gen.containerOf[Set, LfPartyId](implicitly[Arbitrary[LfPartyId]].arbitrary)
      } yield ExampleTransactionFactory.globalKeyWithMaintainers(
        LfTransactionBuilder.defaultGlobalKey,
        maintainers,
      )
    )

  implicit val contractMetadataArb: Arbitrary[ContractMetadata] = Arbitrary(
    for {
      maybeKeyWithMaintainers <- Gen.option(globalKeyWithMaintainersArb.arbitrary)
      maintainers = maybeKeyWithMaintainers.fold(Set.empty[LfPartyId])(_.unversioned.maintainers)

      signatories <- Gen.containerOf[Set, LfPartyId](implicitly[Arbitrary[LfPartyId]].arbitrary)
      observers <- Gen.containerOf[Set, LfPartyId](implicitly[Arbitrary[LfPartyId]].arbitrary)

      allSignatories = maintainers ++ signatories
      allStakeholders = allSignatories ++ observers

      // Required invariant: maintainers \subset signatories \subset stakeholders
    } yield ContractMetadata.tryCreate(
      signatories = allSignatories,
      stakeholders = allStakeholders,
      maybeKeyWithMaintainers,
    )
  )

  implicit val lfContractIdArb: Arbitrary[LfContractId] = Arbitrary(
    for {
      index <- Gen.posNum[Int]
      contractIdDiscriminator = ExampleTransactionFactory.lfHash(index)

      suffix <- Gen.posNum[Int]
      contractIdSuffix = Unicum(
        Hash.build(TestHash.testHashPurpose, HashAlgorithm.Sha256).add(suffix).finish()
      )
    } yield AuthenticatedContractIdVersion.fromDiscriminator(
      contractIdDiscriminator,
      contractIdSuffix,
    )
  )

  implicit val lfTemplateIdArb: Arbitrary[LfTemplateId] = Arbitrary(for {
    packageName <- Gen.stringOfN(8, Gen.alphaChar)
    moduleName <- Gen.stringOfN(8, Gen.alphaChar)
    scriptName <- Gen.stringOfN(8, Gen.alphaChar)
  } yield LfTemplateId.assertFromString(s"$packageName:$moduleName:$scriptName"))

  implicit val requestIdArb: Arbitrary[RequestId] = genArbitrary
}
