// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ContractReassignment,
  ContractsReassignmentBatch,
}
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.pruning.CounterParticipantIntervalsBehind
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.transaction.ParticipantSynchronizerLimits
import com.digitalasset.canton.topology.{GeneratorsTopology, ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.{GeneratorsVersion, HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.canton.{GeneratorsLf, LfPackageId, LfPartyId, LfVersioned}
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

      allLimits <- GeneratorsVersion.defaultValueGen(
        protocolVersion,
        StaticSynchronizerParameters.defaultSynchronizerLimitsUntil,
      )
      maxSignaturesPerEnvelope =
        StaticSynchronizerParameters.defaultMaxSignaturesPerEnvelopeLimitUntil
          .orValue(allLimits.transactionProtocolLimits.maxSignaturesPerEnvelope, protocolVersion)
      maxQuorumSize =
        StaticSynchronizerParameters.defaultMaxQuorumSizeUntil
          .orValue(allLimits.transactionProtocolLimits.maxQuorumSize, protocolVersion)
      transactionProtocolLimits = allLimits.transactionProtocolLimits.copy(
        maxSignaturesPerEnvelope = maxSignaturesPerEnvelope,
        maxQuorumSize = maxQuorumSize,
      )
      topologyLimitsV32 = StaticSynchronizerParameters.defaultTopologyLimitsUntil
        .orValue(allLimits.topologyLimits, protocolVersion)
      topologyLimits = topologyLimitsV32.copy(
        maxVettedPackagesPerParticipant =
          StaticSynchronizerParameters.defaultMaxVettedPackagesPerParticipantUntil
            .orValue(topologyLimitsV32.maxVettedPackagesPerParticipant, protocolVersion),
        maxSequencersPerSynchronizer =
          StaticSynchronizerParameters.defaultMaxSequencersPerSynchronizerUntil
            .orValue(topologyLimitsV32.maxSequencersPerSynchronizer, protocolVersion),
        maxMediatorsPerGroup = StaticSynchronizerParameters.defaultMaxMediatorsPerGroupUntil
          .orValue(topologyLimitsV32.maxMediatorsPerGroup, protocolVersion),
        maxSignaturesPerTransaction =
          StaticSynchronizerParameters.defaultMaxSignaturesPerTransactionUntil
            .orValue(topologyLimitsV32.maxSignaturesPerTransaction, protocolVersion),
        maxTransactionHashesPerMultiSignature =
          StaticSynchronizerParameters.defaultMaxTransactionHashesPerMultiSignatureUntil
            .orValue(topologyLimitsV32.maxTransactionHashesPerMultiSignature, protocolVersion),
        maxTransactionsPerBroadcast =
          StaticSynchronizerParameters.defaultMaxTransactionsPerBroadcastUntil
            .orValue(topologyLimitsV32.maxTransactionsPerBroadcast, protocolVersion),
        maxEndpointsPerSequencerConnection =
          StaticSynchronizerParameters.defaultMaxEndpointsPerSequencerConnectionUntil
            .orValue(topologyLimitsV32.maxEndpointsPerSequencerConnection, protocolVersion),
      )

      acsCommitmentLimits = StaticSynchronizerParameters.defaultAcsCommitmentLimitsUntil
        .orValue(allLimits.acsCommitmentLimits, protocolVersion)

      synchronizerLimits = allLimits.copy(
        transactionProtocolLimits = transactionProtocolLimits,
        topologyLimits = topologyLimits,
        acsCommitmentLimits = acsCommitmentLimits,
      )
      parameters = StaticSynchronizerParameters
        .create(
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
          synchronizerLimits,
        )
        .value

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
                AcsCommitmentsCatchUpParameters
                  .create(
                    PositiveInt.tryCreate(skip),
                    PositiveInt.tryCreate(trigger),
                  )
                  .value
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
      genTime: Gen[Time]
  ): Arbitrary[GenContractInstance { type InstCreatedAtTime <: Time }] = Arbitrary(
    for {

      byKey <- byKeyArb.arbitrary

      maybeKeyWithMaintainers <-
        if (byKey) Gen.some(globalKeyWithMaintainersArb.arbitrary) else Gen.const(None)

      maintainers = maybeKeyWithMaintainers.fold(Set.empty[LfPartyId])(_.unversioned.maintainers)
      nonMaintainerSignatories <- nonEmptySetGen[LfPartyId]
      observers <- boundedSetGen[LfPartyId]

      signatories = maintainers ++ nonMaintainerSignatories
      stakeholders = signatories ++ observers

      createdAt <- genTime
    } yield ExampleContractFactory.build[Time](
      createdAt = createdAt,
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = maybeKeyWithMaintainers.map(_.unversioned),
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
      } yield LfVersioned(LfSerializationVersion.V2, LfGlobalKeyWithMaintainers(key, maintainers))
    )

  val byKeyArb: Arbitrary[Boolean] = Arbitrary(Gen.oneOf(true, false))

  def contractMetadataArb: Arbitrary[ContractMetadata] = Arbitrary(
    for {
      byKey <- byKeyArb.arbitrary

      maybeKeyWithMaintainers <-
        if (byKey) Gen.some(globalKeyWithMaintainersArb.arbitrary)
        else Gen.const(None)

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
    Arbitrary[RollbackContext](
      if (protocolVersion >= ProtocolVersion.v36) {
        Gen.oneOf(true, false).map(inRollback => NoPathRollbackContext(inRollback))
      } else {
        boundedListGen[PositiveInt].map(l =>
          PathRollbackContext(l.toVector, PathRollbackContext.firstChild)
        )
      }
    )

  val createdContractRolledBackArb: Arbitrary[Boolean] =
    Arbitrary(
      if (protocolVersion >= ProtocolVersion.v36) Gen.const(false)
      else Gen.oneOf(true, false)
    )

  implicit val contractReassignmentBatch: Arbitrary[ContractsReassignmentBatch] = {

    def validationPackageIdArb(contractInstance: ContractInstance): Gen[LfPackageId] =
      if (protocolVersion >= ProtocolVersion.v37) {
        lfPackageId.arbitrary
      } else {
        Gen.const(contractInstance.templateId.packageId)
      }

    def reassignment(metadata: ContractMetadata) = Arbitrary(
      for {
        contract <- Arbitrary(contractInstanceWithGivenMetadataArb(metadata).arbitrary).arbitrary
        sourceValidationId <- validationPackageIdArb(contract)
        targetValidationId <- validationPackageIdArb(contract)
        counter <- reassignmentCounterGen
      } yield ContractReassignment(
        contract,
        Source(sourceValidationId),
        Target(targetValidationId),
        counter,
      )
    )

    Arbitrary(
      for {
        metadata <- contractMetadataArb.arbitrary
        contractCounters <- nonEmptyListGen(reassignment(metadata))
      } yield ContractsReassignmentBatch.create(contractCounters.toSeq).value
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
      maxRecordTime <- Gen.option(Arbitrary.arbitrary[CantonTimestamp])

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
