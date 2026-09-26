// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.config.GeneratorsConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{Hash, Signature}
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Update}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.AgreementStatus.{
  Exists,
  NotNeeded,
  NotProposed,
  Proposed,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AcsIndexingProgress,
  AcsReplicationProgress,
  AgreementStatus,
  Disconnected,
  PartyReplicationAuthorization,
  PartyReplicationError,
  PartyReplicationFailed,
  PersistentProgress,
  ReplicationParams,
}
import com.digitalasset.canton.participant.protocol.party.OnboardingClearanceOperation
import com.digitalasset.canton.participant.protocol.party.acsreplication.{
  AcsReplicationSourceParticipantMessage,
  AcsReplicationTargetParticipantMessage,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionSubmissionTrackingData.{
  CauseWithTemplate,
  RejectionCause,
  TimeoutCause,
}
import com.digitalasset.canton.participant.protocol.submission.{
  SubmissionTrackingData,
  TransactionSubmissionTrackingData,
}
import com.digitalasset.canton.participant.synchronizer.{
  PendingLsuOperation,
  PendingOnboardingTransactions,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{GeneratorsTransaction, ParticipantPermission}
import com.digitalasset.canton.topology.{
  GeneratorsTopology,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SequencerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  GeneratorsLf,
  LedgerUserId,
  LfPartyId,
  ReassignmentCounter,
  RepairCounter,
}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsParticipant(
    generatorsTopology: GeneratorsTopology,
    generatorsTransaction: GeneratorsTransaction,
    generatorsLf: GeneratorsLf,
    version: ProtocolVersion,
) {

  import GeneratorsConfig.*
  import com.digitalasset.canton.Generators.*
  import generatorsTopology.*
  import generatorsTransaction.*
  import generatorsLf.*
  import com.digitalasset.canton.ledger.api.GeneratorsApi.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*

  implicit val completionInfoArb: Arbitrary[CompletionInfo] = Arbitrary {
    for {
      actAs <- boundedListGen[LfPartyId]
      userId <- Arbitrary.arbitrary[LedgerUserId]
      commandId <- ledgerSubmissionIdArb.arbitrary
      optDedupPeriod <- Gen.option(Arbitrary.arbitrary[DeduplicationPeriod])
      submissionId <- Gen.option(lfSubmissionIdArb.arbitrary)
      trafficCost <- Arbitrary.arbitrary[NonNegativeLong]
    } yield CompletionInfo(actAs, userId, commandId, optDedupPeriod, submissionId, trafficCost)
  }

  implicit val finalReasonArb: Arbitrary[Update.CommandRejected.FinalReason] =
    Arbitrary(
      for {
        statusCode <- Gen.oneOf(
          com.google.rpc.Code.INVALID_ARGUMENT_VALUE,
          com.google.rpc.Code.CANCELLED_VALUE,
        )
      } yield Update.CommandRejected.FinalReason(com.google.rpc.status.Status(statusCode))
    )

  val causeWithTemplateGen: Gen[CauseWithTemplate] = for {
    template <- Arbitrary.arbitrary[Update.CommandRejected.FinalReason]
  } yield CauseWithTemplate(template)
  val timeoutCauseGen: Gen[TimeoutCause.type] = Gen.const(TimeoutCause)
  implicit val rejectionCauseArb: Arbitrary[RejectionCause] =
    arbitraryForAllSubclasses(classOf[RejectionCause])(
      GeneratorForClass(causeWithTemplateGen, classOf[CauseWithTemplate]),
      GeneratorForClass(timeoutCauseGen, classOf[TimeoutCause.type]),
    )

  val transactionSubmissionTrackingDataGen: Gen[TransactionSubmissionTrackingData] =
    for {
      completionInfo <- Arbitrary.arbitrary[CompletionInfo]
      rejectionCause <- Arbitrary.arbitrary[RejectionCause]
      physicalSynchronizerId <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      transactionHash <- Gen.option(Arbitrary.arbitrary[Hash])
    } yield TransactionSubmissionTrackingData(
      completionInfo,
      rejectionCause,
      physicalSynchronizerId,
      transactionHash = transactionHash,
    )

  implicit val submissionTrackingDataArg: Arbitrary[SubmissionTrackingData] =
    arbitraryForAllSubclasses(classOf[SubmissionTrackingData])(
      GeneratorForClass(
        transactionSubmissionTrackingDataGen,
        classOf[TransactionSubmissionTrackingData],
      )
    )

  implicit val reassignmentCounterArb: Arbitrary[ReassignmentCounter] =
    Arbitrary(Gen.chooseNum(0L, Long.MaxValue).map(ReassignmentCounter(_)))

  import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
  implicit val activeContractArb: Arbitrary[ActiveContract] =
    Arbitrary(
      for {
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        reassignmentCounter <- Arbitrary.arbitrary[ReassignmentCounter]
        // TODO(#26599): Add generator for LapiActiveContract
        lapiActiveContract <- Gen.const(
          LapiActiveContract(None, synchronizerId.toProtoPrimitive, reassignmentCounter.unwrap)
        )
      } yield ActiveContract.create(lapiActiveContract)(version)
    )

  // If this pattern match is not exhaustive anymore, update the message generator below
  {
    ((_: AcsReplicationSourceParticipantMessage.DataOrStatus) match {
      case _: AcsReplicationSourceParticipantMessage.AcsBatch => ()
      case _: AcsReplicationSourceParticipantMessage.EndOfAcs => ()
    }).discard
  }

  implicit val replicationParamsArb: Arbitrary[ReplicationParams] =
    Arbitrary(
      for {
        requestId <- Arbitrary.arbitrary[Hash]
        partyId <- Arbitrary.arbitrary[PartyId]
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        sourceParticipantId <- Arbitrary.arbitrary[ParticipantId]
        targetParticipantId <- Arbitrary.arbitrary[ParticipantId]
        serial <- Arbitrary.arbitrary[PositiveInt]
        participantPermission <- Arbitrary.arbitrary[ParticipantPermission]
      } yield ReplicationParams(
        requestId,
        partyId,
        synchronizerId,
        sourceParticipantId,
        targetParticipantId,
        serial,
        participantPermission,
      )
    )

  implicit val existingSequencerChannelAgreementArb: Arbitrary[Exists] =
    Arbitrary(
      for {
        damlAgreementContractId <- Arbitrary.arbitrary[LfContractId]
        agreedAt <- Arbitrary.arbitrary[CantonTimestamp]
        sequencerId <- Arbitrary.arbitrary[SequencerId]
      } yield Exists(damlAgreementContractId, agreedAt, sequencerId)
    )

  implicit val sequencerChannelAgreementStatusArb: Arbitrary[AgreementStatus] =
    Arbitrary(
      Gen.oneOf[AgreementStatus](
        Gen.const(NotProposed),
        Gen.const(Proposed),
        Gen.const(NotNeeded),
        existingSequencerChannelAgreementArb.arbitrary,
      )
    )

  implicit val partyReplicationAuthorizationArb: Arbitrary[PartyReplicationAuthorization] =
    Arbitrary(
      for {
        onboardingAt <- Arbitrary.arbitrary[CantonTimestamp]
        fullyOnboarded <- Arbitrary.arbitrary[Boolean]
      } yield PartyReplicationAuthorization(EffectiveTime(onboardingAt), fullyOnboarded)
    )

  implicit val repairCounterArb: Arbitrary[RepairCounter] =
    Arbitrary(Gen.chooseNum(0L, Long.MaxValue).map(RepairCounter(_)))

  implicit val acsReplicationProgressArb: Arbitrary[AcsReplicationProgress] =
    Arbitrary(
      for {
        replicatedContractCount <- Arbitrary.arbitrary[NonNegativeLong]
        nextPersistenceCounter <- Arbitrary.arbitrary[RepairCounter]
        acsHashO <- Arbitrary.arbitrary[Option[ByteString]]
        fullyReplicatedAcs <- Arbitrary.arbitrary[Boolean]
      } yield {
        // Alternative AcsReplicationProgressRuntime is not serializable (due to processor field)
        PersistentProgress(
          replicatedContractCount,
          nextPersistenceCounter,
          acsHashO,
          fullyReplicatedAcs,
        )
      }
    )

  implicit val acsIndexingProgressArb: Arbitrary[AcsIndexingProgress] =
    Arbitrary(
      for {
        indexedContractActivationChangeCount <- Arbitrary.arbitrary[NonNegativeLong]
        nextIndexingCounter <- Arbitrary.arbitrary[NonNegativeLong]
        indexingAlmostDoneWatermarkO <- Gen.option(Arbitrary.arbitrary[NonNegativeLong])
      } yield AcsIndexingProgress(
        indexedContractActivationChangeCount,
        nextIndexingCounter,
        indexingAlmostDoneWatermarkO,
      )
    )

  implicit val partyReplicationErrorArb: Arbitrary[PartyReplicationError] =
    Arbitrary(
      for {
        message <- Arbitrary.arbitrary[String]
        error <- Gen
          .oneOf[PartyReplicationError](Disconnected(message), PartyReplicationFailed(message))
      } yield error
    )

  implicit val partyReplicationStatusArb: Arbitrary[PartyReplicationStatus] =
    Arbitrary(
      for {
        params <- Arbitrary.arbitrary[ReplicationParams]
        authorizationO <- Gen.option(Arbitrary.arbitrary[PartyReplicationAuthorization])
        agreementO <- Arbitrary.arbitrary[AgreementStatus]
        replicationO <- Gen.option(Arbitrary.arbitrary[AcsReplicationProgress])
        // TODO (#35267): change this once AcsReplicationStatus is a separate class
        acsReplicationO <- Gen.const(None)
        indexingO <- Gen.option(Arbitrary.arbitrary[AcsIndexingProgress])
        hasCompleted <- Arbitrary.arbitrary[Boolean]
        errorO <- Gen.option(Arbitrary.arbitrary[PartyReplicationError])
      } yield PartyReplicationStatus.apply(
        params,
        version,
        authorizationO.map(_ => agreementO).getOrElse(AgreementStatus.NotProposed),
        authorizationO,
        authorizationO.flatMap(_ => replicationO),
        acsReplicationO,
        // Can only have indexing status if we have authorization and replication status
        authorizationO.flatMap(_ => replicationO).flatMap(_ => indexingO),
        hasCompleted,
        errorO,
      )
    )

  implicit val acsReplicationAcsBatchArb
      : Arbitrary[AcsReplicationSourceParticipantMessage.AcsBatch] =
    Arbitrary(
      for {
        acsBatch <- nonEmptyListGen[ActiveContract]
      } yield AcsReplicationSourceParticipantMessage.AcsBatch(
        acsBatch
      )
    )

  implicit val acsReplicationGetAcsArgumentsArb
      : Arbitrary[AcsReplicationSourceParticipantMessage.GetAcsArguments] =
    Arbitrary(
      for {
        partyId <- Arbitrary.arbitrary[PartyId]
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        asOf <- Arbitrary.arbitrary[CantonTimestamp]
        excludedStakeholders <- boundedListGen[PartyId]
      } yield AcsReplicationSourceParticipantMessage.GetAcsArguments(
        partyId,
        synchronizerId,
        asOf,
        excludedStakeholders.toSet,
      )
    )

  implicit val acsReplicationAcsDigestArb
      : Arbitrary[AcsReplicationSourceParticipantMessage.AcsDigest] =
    Arbitrary(
      for {
        acsHash <- Arbitrary.arbitrary[ByteString]
        getAcsArgs <- Arbitrary.arbitrary[AcsReplicationSourceParticipantMessage.GetAcsArguments]
        sourceParticipantUid <- Arbitrary.arbitrary[UniqueIdentifier]
        agreedAt <- Arbitrary.arbitrary[CantonTimestamp]
      } yield AcsReplicationSourceParticipantMessage.AcsDigest(
        acsHash,
        getAcsArgs,
        sourceParticipantUid,
        agreedAt,
        version,
      )
    )

  implicit val acsReplicationEndOfAcsArb
      : Arbitrary[AcsReplicationSourceParticipantMessage.EndOfAcs] =
    Arbitrary(
      for {
        acsDigest <- Arbitrary.arbitrary[AcsReplicationSourceParticipantMessage.AcsDigest]
        acsDigestByteString = acsDigest.toByteString
        signature <- Arbitrary.arbitrary[Signature]
      } yield AcsReplicationSourceParticipantMessage.EndOfAcs(
        acsDigest,
        acsDigestByteString,
        signature,
      )
    )

  implicit val acsReplicationSourceParticipantMessageArb
      : Arbitrary[AcsReplicationSourceParticipantMessage] =
    Arbitrary(
      for {
        message <- Gen
          .oneOf[AcsReplicationSourceParticipantMessage.DataOrStatus](
            Arbitrary.arbitrary[AcsReplicationSourceParticipantMessage.AcsBatch],
            Arbitrary.arbitrary[AcsReplicationSourceParticipantMessage.EndOfAcs],
          )
      } yield AcsReplicationSourceParticipantMessage.apply(
        message,
        version,
      )
    )

  // If this pattern match is not exhaustive anymore, update the instruction generator below
  {
    ((_: AcsReplicationTargetParticipantMessage.Instruction) match {
      case _: AcsReplicationTargetParticipantMessage.Initialize => ()
      case _: AcsReplicationTargetParticipantMessage.SendAcsUpTo => ()
    }).discard
  }

  implicit val acsReplicationTargetParticipantMessageArb
      : Arbitrary[AcsReplicationTargetParticipantMessage] = Arbitrary(
    for {
      contractOrdinal <- nonNegativeLongArb.arbitrary
      instruction <- Gen
        .oneOf[AcsReplicationTargetParticipantMessage.Instruction](
          AcsReplicationTargetParticipantMessage.Initialize(contractOrdinal),
          AcsReplicationTargetParticipantMessage.SendAcsUpTo(contractOrdinal),
        )
    } yield AcsReplicationTargetParticipantMessage.apply(instruction, version)
  )

  implicit val pendingLsuOperationArb: Arbitrary[PendingLsuOperation] =
    Arbitrary(
      for {
        psid <- Arbitrary.arbitrary[PhysicalSynchronizerId]
        rpv = PendingLsuOperation.protocolVersionRepresentativeFor(version)
      } yield PendingLsuOperation(psid)(rpv)
    )

  implicit val pendingOnboardingTransactionsArb: Arbitrary[PendingOnboardingTransactions] =
    Arbitrary(
      for {
        transactions <- nonEmptyListGen[GenericSignedTopologyTransaction]
        rpv = PendingOnboardingTransactions.protocolVersionRepresentativeFor(version)
      } yield PendingOnboardingTransactions(transactions)(rpv)
    )

  implicit val onboardingClearanceOperationArb: Arbitrary[OnboardingClearanceOperation] =
    Arbitrary(
      Gen
        .option(Arbitrary.arbitrary[EffectiveTime])
        .map(
          OnboardingClearanceOperation(_)(
            OnboardingClearanceOperation.protocolVersionRepresentativeFor(version)
          )
        )
    )
}
