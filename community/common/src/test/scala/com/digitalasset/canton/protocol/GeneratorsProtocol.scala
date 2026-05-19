// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{CantonTimestamp, ContractsReassignmentBatch}
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.pruning.CounterParticipantIntervalsBehind
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.transaction.ParticipantSynchronizerLimits
import com.digitalasset.canton.topology.{GeneratorsTopology, ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.canton.{GeneratorsLf, LfPartyId}
import com.digitalasset.daml.lf.transaction.{CreationTime, Versioned}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

import scala.jdk.CollectionConverters.*

final class GeneratorsProtocol(
    protocolVersion: ProtocolVersion,
    generatorsLf: GeneratorsLf,
    generatorsTopology: GeneratorsTopology,
) {
  import com.digitalasset.canton.Generators.*
  import generatorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.time.GeneratorsTime.*
  import generatorsTopology.*
  import org.scalatest.EitherValues.*

  implicit val staticSynchronizerParametersArb: Arbitrary[StaticSynchronizerParameters] =
    Arbitrary(for {
      requiredSigningAlgorithmSpecs <- nonEmptySetGen[SigningAlgorithmSpec]
      requiredSigningKeySpecs <- nonEmptySetGen[SigningKeySpec]
      requiredEncryptionAlgorithmSpecs <- nonEmptySetGen[EncryptionAlgorithmSpec]
      requiredEncryptionKeySpecs <- nonEmptySetGen[EncryptionKeySpec]
      requiredSymmetricKeySchemes <- nonEmptySetGen[SymmetricKeyScheme]
      requiredHashAlgorithms <- nonEmptySetGen[HashAlgorithm]
      requiredCryptoKeyFormats <- nonEmptySetGen[CryptoKeyFormat]
      requiredSignatureFormats <- nonEmptySetGen[SignatureFormat]
      topologyChangeDelay <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
      enableTransparencyChecks <- Arbitrary.arbitrary[Boolean]
      serial <- Arbitrary.arbitrary[NonNegativeInt]

      parameters = StaticSynchronizerParameters(
        RequiredSigningSpecs(requiredSigningAlgorithmSpecs, requiredSigningKeySpecs),
        RequiredEncryptionSpecs(requiredEncryptionAlgorithmSpecs, requiredEncryptionKeySpecs),
        requiredSymmetricKeySchemes,
        requiredHashAlgorithms,
        requiredCryptoKeyFormats,
        requiredSignatureFormats,
        topologyChangeDelay,
        enableTransparencyChecks,
        protocolVersion,
        serial,
      )

    } yield parameters)

  implicit val dynamicSynchronizerParametersArb: Arbitrary[DynamicSynchronizerParameters] =
    Arbitrary(
      for {
        confirmationResponseTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        mediatorReactionTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        assignmentExclusivityTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]

        mediatorDeduplicationMargin <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        // Because of the potential multiplication by 2 below, we want a reasonably small value
        ledgerTimeRecordTimeTolerance <- Gen
          .choose(0L, 10000L)
          .map(NonNegativeFiniteDuration.tryOfMicros)

        representativePV = DynamicSynchronizerParameters.protocolVersionRepresentativeFor(
          protocolVersion
        )

        reconciliationInterval <- Arbitrary.arbitrary[PositiveSeconds]
        maxRequestSize <- Arbitrary.arbitrary[MaxRequestSize]

        trafficControlConfig <- Gen.option(Arbitrary.arbitrary[TrafficControlParameters])

        updatedMediatorDeduplicationTimeout = ledgerTimeRecordTimeTolerance * NonNegativeInt
          .tryCreate(2) + mediatorDeduplicationMargin

        sequencerAggregateSubmissionTimeout <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        onboardingRestriction <- Arbitrary.arbitrary[OnboardingRestriction]

        participantSynchronizerLimits <- Arbitrary.arbitrary[ParticipantSynchronizerLimits]

        acsCommitmentsCatchupConfig <-
          for {
            isNone <- Gen.oneOf(true, false)
            skip <- Gen.choose(1, Math.sqrt(PositiveInt.MaxValue.value.toDouble).intValue)
            trigger <- Gen.choose(1, Math.sqrt(PositiveInt.MaxValue.value.toDouble).intValue)
          } yield {
            if (!isNone)
              Some(
                new AcsCommitmentsCatchUpParameters(
                  PositiveInt.tryCreate(skip),
                  PositiveInt.tryCreate(trigger),
                )
              )
            else None
          }

        // Because of the potential multiplication by 2 below, we want a reasonably small value
        preparationTimeRecordTimeTolerance <- Gen
          .choose(0L, 10000L)
          .map(NonNegativeFiniteDuration.tryOfMicros)

        dynamicSynchronizerParameters = DynamicSynchronizerParameters.tryCreate(
          confirmationResponseTimeout,
          mediatorReactionTimeout,
          assignmentExclusivityTimeout,
          ledgerTimeRecordTimeTolerance,
          updatedMediatorDeduplicationTimeout,
          reconciliationInterval,
          maxRequestSize,
          sequencerAggregateSubmissionTimeout,
          trafficControlConfig,
          onboardingRestriction,
          acsCommitmentsCatchupConfig,
          participantSynchronizerLimits,
          preparationTimeRecordTimeTolerance,
        )(representativePV)

      } yield dynamicSynchronizerParameters
    )

  implicit val counterParticipantIntervalsBehindArb: Arbitrary[CounterParticipantIntervalsBehind] =
    Arbitrary(
      for {
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        participantId <- Arbitrary.arbitrary[ParticipantId]
        intervalsBehind <- Arbitrary.arbitrary[NonNegativeLong]
        timeBehind <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
        asOfSequencingTime <- Arbitrary.arbitrary[CantonTimestamp]
      } yield CounterParticipantIntervalsBehind(
        synchronizerId,
        participantId,
        intervalsBehind,
        timeBehind,
        asOfSequencingTime,
      )
    )

  implicit val dynamicSequencingParametersArb: Arbitrary[SequencingParameters] = Arbitrary(
    for {
      payload <- Arbitrary.arbitrary[Option[ByteString]]
      representativePV = SequencingParameters.protocolVersionRepresentativeFor(
        protocolVersion
      )
      dynamicSequencingParameters = SequencingParameters(payload)(representativePV)
    } yield dynamicSequencingParameters
  )

  implicit val rootHashArb: Arbitrary[RootHash] = Arbitrary(
    Arbitrary.arbitrary[Hash].map(RootHash(_))
  )
  implicit val viewHashArb: Arbitrary[ViewHash] = Arbitrary(
    Arbitrary.arbitrary[Hash].map(ViewHash(_))
  )

  def contractInstanceArb[Time <: CreationTime](
      canHaveEmptyKey: Boolean,
      genTime: Gen[Time],
      overrideContractId: Option[LfContractId] = None,
  ): Arbitrary[GenContractInstance { type InstCreatedAtTime <: Time }] = Arbitrary(
    for {
      metadata <- contractMetadataArb(canHaveEmptyKey).arbitrary
      createdAt <- genTime
    } yield ExampleContractFactory.build[Time](
      createdAt = createdAt,
      signatories = metadata.signatories,
      stakeholders = metadata.stakeholders,
      keyOpt = metadata.maybeKeyWithMaintainers,
      overrideContractId = overrideContractId,
    )
  )

  def contractInstanceWithMetadataArb[Time <: CreationTime](
      metadataList: List[ContractMetadata],
      genTime: Gen[Time],
  ): Arbitrary[GenContractInstance { type InstCreatedAtTime <: Time }] = Arbitrary(
    for {
      metadata <- Gen.oneOf(metadataList)
      createdAt <- genTime
    } yield ExampleContractFactory.build[Time](
      createdAt = createdAt,
      signatories = metadata.signatories,
      stakeholders = metadata.stakeholders,
      keyOpt = metadata.maybeKeyWithMaintainers,
    )
  )

  def contractInstanceWithGivenMetadataArb(
      metadata: ContractMetadata
  ): Arbitrary[ContractInstance] = Arbitrary(
    for {
      createdAt <- Arbitrary.arbitrary[CreationTime.CreatedAt]
    } yield ExampleContractFactory.build(
      createdAt = createdAt,
      signatories = metadata.signatories,
      stakeholders = metadata.stakeholders,
      keyOpt = metadata.maybeKeyWithMaintainers,
    )
  )

  implicit val globalKeyWithMaintainersArb: Arbitrary[Versioned[LfGlobalKeyWithMaintainers]] =
    Arbitrary(
      for {
        maintainers <- nonEmptySetGen[LfPartyId]
        key <- Arbitrary.arbitrary[LfGlobalKey]
      } yield ExampleTransactionFactory.globalKeyWithMaintainers(
        key,
        maintainers,
      )
    )

  def contractMetadataArb(canHaveEmptyKey: Boolean): Arbitrary[ContractMetadata] = Arbitrary(
    for {
      maybeKeyWithMaintainers <-
        if (canHaveEmptyKey) Gen.option(globalKeyWithMaintainersArb.arbitrary)
        else Gen.some(globalKeyWithMaintainersArb.arbitrary)
      maintainers = maybeKeyWithMaintainers.fold(Set.empty[LfPartyId])(_.unversioned.maintainers)

      signatories <- nonEmptySetGen[LfPartyId]
      observers <- boundedSetGen[LfPartyId]

      allSignatories = maintainers ++ signatories
      allStakeholders = allSignatories ++ observers

      // Required invariant: maintainers \subset signatories \subset stakeholders
    } yield ContractMetadata.tryCreate(
      signatories = allSignatories,
      stakeholders = allStakeholders,
      maybeKeyWithMaintainers,
    )
  )

  implicit val stakeholdersArb: Arbitrary[Stakeholders] = Arbitrary(
    for {
      signatories <- boundedSetGen[LfPartyId]
      observers <- boundedSetGen[LfPartyId]
    } yield Stakeholders.withSignatoriesAndObservers(
      signatories = signatories,
      observers = observers,
    )
  )

  implicit val requestIdArb: Arbitrary[RequestId] = genArbitrary

  implicit val rollbackContextArb: Arbitrary[RollbackContext] =
    Arbitrary(boundedListGen[PositiveInt].map(RollbackContext.apply))

  implicit val createdContractArb: Arbitrary[CreatedContract] = Arbitrary(
    for {
      contract <- contractInstanceArb(
        canHaveEmptyKey = true,
        genTime = Arbitrary.arbitrary[CreationTime.CreatedAt],
      ).arbitrary
      consumedInCore <- Gen.oneOf(true, false)
      rolledBack <- Gen.oneOf(true, false)
    } yield CreatedContract.create(contract, consumedInCore, rolledBack).value
  )

  implicit val contractReassignmentBatch: Arbitrary[ContractsReassignmentBatch] = {

    def tuple(metadata: ContractMetadata) = Arbitrary(
      for {
        contract <- Arbitrary(contractInstanceWithGivenMetadataArb(metadata).arbitrary).arbitrary
        // TODO(#26468): Use lfPackageId.arbitrary for PV>=35
        sourceValidationId = contract.templateId.packageId // lfPackageId.arbitrary
        targetValidationId = contract.templateId.packageId // lfPackageId.arbitrary
        counter <- reassignmentCounterGen
      } yield (contract, Source(sourceValidationId), Target(targetValidationId), counter)
    )

    Arbitrary(
      for {
        metadata <- contractMetadataArb(canHaveEmptyKey = true).arbitrary
        contractCounters <- nonEmptyListGen(tuple(metadata))
      } yield ContractsReassignmentBatch.create(contractCounters).value
    )
  }

  implicit val hashingSchemeVersionArb: Arbitrary[HashingSchemeVersion] = Arbitrary(
    Gen.oneOf(HashingSchemeVersion.getHashingSchemeVersionsForProtocolVersion(protocolVersion))
  )

  implicit val externalAuthorizationArb: Arbitrary[ExternalAuthorization] = Arbitrary(
    for {
      parties <- boundedListGen[PartyId]
      signatures <- Gen.sequence(
        parties.map(p => boundedListGen[Signature].map(p -> _))
      )
      maxRecordTime <-
        if (protocolVersion >= ProtocolVersion.v35)
          Gen.option(Arbitrary.arbitrary[CantonTimestamp])
        else
          Gen.const[Option[CantonTimestamp]](None)
      hashingSchemeVersion <- hashingSchemeVersionArb.arbitrary
    } yield ExternalAuthorization.create(
      signatures.asScala.toMap,
      hashingSchemeVersion,
      maxRecordTime,
      protocolVersion,
    )
  )

  implicit val protocolSymmetricKeyArb: Arbitrary[ProtocolSymmetricKey] =
    Arbitrary(
      for {
        key <- Arbitrary.arbitrary[SymmetricKey]
      } yield ProtocolSymmetricKey(key, protocolVersion)
    )
}
