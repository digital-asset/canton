// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.syntax.traverse.*
import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyReplicationStatus as LapiPartyReplicationStatus
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.admin.data.PartyReplicationStatus.{
  AcsIndexingProgress,
  AcsReplicationProgress,
  PartyReplicationAuthorization,
  PartyReplicationError,
  ReplicationParameters,
  SequencerChannelAgreement,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus as InternalStatus
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*

import scala.annotation.unused

/** External console representation of the party replication process. Refer to
  * party_management_service.proto PartyReplicationStatus for the semantics.
  */
final case class PartyReplicationStatus(
    parameters: ReplicationParameters,
    agreementStatusO: Option[SequencerChannelAgreement],
    authorizationO: Option[PartyReplicationAuthorization],
    replicationO: Option[AcsReplicationProgress],
    indexingO: Option[AcsIndexingProgress.type],
    hasCompleted: Boolean,
    errorO: Option[PartyReplicationError],
) extends PrettyPrintingFromCompanion {

  require(
    indexingO.isEmpty || replicationO.nonEmpty,
    s"cannot begin indexing $indexingO before replication has started",
  )

  def toProtoV30: v30.PartyReplicationStatus = v30.PartyReplicationStatus(
    Some(parameters.toProtoV30),
    agreementStatusO.map(_.toProtoV30),
    authorizationO.map(_.toProtoV30),
    replicationO.map(_.toProtoV30),
    indexingO.map(_.toProtoV30),
    hasCompleted = hasCompleted,
    errorO.map(_.toProtoV30),
  )

  def toLapiProto: LapiPartyReplicationStatus = {
    val state =
      if (errorO.isDefined) LapiPartyReplicationStatus.State.STATE_FAILED
      else if (hasCompleted) LapiPartyReplicationStatus.State.STATE_COMPLETED
      else LapiPartyReplicationStatus.State.STATE_IN_PROGRESS

    LapiPartyReplicationStatus(
      current = state,
      error = errorO.map(err => LapiPartyReplicationStatus.ErrorDetails(err.message)),
    )
  }

  override def prettyCompanion: PrettyPrintingCompanion[PartyReplicationStatus] =
    PartyReplicationStatus
}

object PartyReplicationStatus extends PrettyPrintingCompanion[PartyReplicationStatus] {
  override protected val pretty: Pretty[PartyReplicationStatus] =
    prettyOfClass(
      param("parameters", _.parameters),
      paramIfDefined("agreementStatus", _.agreementStatusO),
      paramIfDefined("authorization", _.authorizationO),
      paramIfDefined("replication", _.replicationO),
      paramIfDefined("indexing", _.indexingO),
      paramIfDefined("error", _.errorO),
      paramIfTrue("complete", _.hasCompleted),
    )

  def fromInternal: InternalStatus => PartyReplicationStatus = {
    // When new fields are added, adapt this deciding which fields may need to be exposed externally erring
    // on the side of cautious not exposing fields that don't have clear external utility and can be preserved
    // in a backward compatible way.
    case InternalStatus(
          params,
          agreementStatus,
          authorizationO,
          replicationO,
          indexingO,
          hasCompleted,
          errorO,
        ) =>
      PartyReplicationStatus(
        ReplicationParameters.fromInternal(params),
        SequencerChannelAgreement.fromInternal(agreementStatus),
        authorizationO.map(PartyReplicationAuthorization.fromInternal),
        replicationO.map(AcsReplicationProgress.fromInternal),
        indexingO.map(AcsIndexingProgress.fromInternal),
        hasCompleted,
        errorO.map(PartyReplicationError.fromInternal),
      )
  }

  def fromProtoV30(proto: v30.PartyReplicationStatus): ParsingResult[PartyReplicationStatus] =
    for {
      paramsP <- ProtoConverter.required("parameters", proto.parameters)
      params <- ReplicationParameters.fromProtoV30(paramsP)
      agreementO <- proto.agreement.traverse(SequencerChannelAgreement.fromProtoV30)
      authorizationO <- proto.authorization.traverse(PartyReplicationAuthorization.fromProtoV30)
      replicationO <- proto.replication.traverse(AcsReplicationProgress.fromProtoV30)
      indexingO <- proto.indexing.traverse(AcsIndexingProgress.fromProtoV30)
      hasCompleted = proto.hasCompleted
      errorO <- proto.errorMessage.traverse(PartyReplicationError.fromProtoV30)
    } yield PartyReplicationStatus(
      params,
      agreementO,
      authorizationO,
      replicationO,
      indexingO,
      hasCompleted,
      errorO,
    )

  final case class ReplicationParameters(
      requestId: String,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
  ) extends PrettyPrintingFromCompanion {

    def toProtoV30: v30.PartyReplicationStatus.ReplicationParameters =
      v30.PartyReplicationStatus.ReplicationParameters(
        requestId,
        partyId.toProtoPrimitive,
        synchronizerId.uid.toProtoPrimitive,
        sourceParticipantId.uid.toProtoPrimitive,
        targetParticipantId.uid.toProtoPrimitive,
        serial.unwrap,
      )

    override def prettyCompanion: PrettyPrintingCompanion[ReplicationParameters] =
      ReplicationParameters
  }

  private object ReplicationParameters extends PrettyPrintingCompanion[ReplicationParameters] {
    def fromInternal: InternalStatus.ReplicationParams => ReplicationParameters = {
      case InternalStatus.ReplicationParams(
            requestId,
            partyId,
            synchronizerId,
            sourceParticipantId,
            targetParticipantId,
            serial,
            _,
          ) =>
        ReplicationParameters(
          requestId.toHexString,
          partyId,
          synchronizerId,
          sourceParticipantId,
          targetParticipantId,
          serial,
        )
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.ReplicationParameters
    ): ParsingResult[ReplicationParameters] = for {
      partyId <- PartyId.fromProtoPrimitive(proto.partyId, "party_id")
      synchronizerId <- SynchronizerId.fromProtoPrimitive(
        proto.synchronizerId,
        "synchronizer_id",
      )
      sourceParticipantId <- UniqueIdentifier
        .fromProtoPrimitive(
          proto.sourceParticipantUid,
          "source_participant_uid",
        )
        .map(ParticipantId(_))
      targetParticipantId <- UniqueIdentifier
        .fromProtoPrimitive(
          proto.targetParticipantUid,
          "target_participant_uid",
        )
        .map(ParticipantId(_))
      topologySerial <- ProtoConverter.parsePositiveInt(
        "topology_serial",
        proto.topologySerial,
      )
    } yield ReplicationParameters(
      proto.requestId,
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      topologySerial,
    )

    override protected val pretty: Pretty[ReplicationParameters] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("request", _.requestId.doubleQuoted),
        param("party", _.partyId),
        param("synchronizer", _.synchronizerId),
        param("source participant", _.sourceParticipantId),
        param("target participant", _.targetParticipantId),
        param("serial", _.serial),
      )
    }
  }

  final case class SequencerChannelAgreement(sequencerId: SequencerId)
      extends PrettyPrintingFromCompanion {
    def toProtoV30: v30.PartyReplicationStatus.SequencerChannelAgreement =
      v30.PartyReplicationStatus.SequencerChannelAgreement(sequencerId.uid.toProtoPrimitive)

    override def prettyCompanion: PrettyPrintingCompanion[SequencerChannelAgreement] =
      SequencerChannelAgreement
  }

  private object SequencerChannelAgreement
      extends PrettyPrintingCompanion[SequencerChannelAgreement] {
    val fromInternal: InternalStatus.AgreementStatus => Option[SequencerChannelAgreement] = {
      case InternalStatus.AgreementStatus.Exists(_, sequencerId) =>
        Some(SequencerChannelAgreement(sequencerId))
      case _ => None
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.SequencerChannelAgreement
    ): ParsingResult[SequencerChannelAgreement] =
      for {
        sequencerId <- UniqueIdentifier
          .fromProtoPrimitive(proto.sequencerUid, "sequencer_uid")
          .map(SequencerId(_))
      } yield SequencerChannelAgreement(sequencerId)

    override protected val pretty: Pretty[SequencerChannelAgreement] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(param("sequencer", _.sequencerId))
    }
  }

  final case class PartyReplicationAuthorization(
      onboardingAt: CantonTimestamp,
      isOnboardingFlagCleared: Boolean,
  ) extends PrettyPrintingFromCompanion {

    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationAuthorization =
      v30.PartyReplicationStatus.PartyReplicationAuthorization(
        Some(onboardingAt.toProtoTimestamp),
        isOnboardingFlagCleared,
      )

    override def prettyCompanion: PrettyPrintingCompanion[PartyReplicationAuthorization] =
      PartyReplicationAuthorization
  }

  private object PartyReplicationAuthorization
      extends PrettyPrintingCompanion[PartyReplicationAuthorization] {
    def fromInternal
        : InternalStatus.PartyReplicationAuthorization => PartyReplicationAuthorization = {
      case InternalStatus.PartyReplicationAuthorization(onboardingAt, isOnboardingFlagCleared) =>
        PartyReplicationAuthorization(onboardingAt.value, isOnboardingFlagCleared)
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.PartyReplicationAuthorization
    ): ParsingResult[PartyReplicationAuthorization] = for {
      onboardingAtP <- ProtoConverter.required("onboarding_at", proto.onboardingAt)
      onboardingAt <- CantonTimestamp.fromProtoTimestamp(onboardingAtP)
    } yield PartyReplicationAuthorization(onboardingAt, proto.isOnboardingFlagCleared)

    override protected val pretty: Pretty[PartyReplicationAuthorization] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("onboarding at", _.onboardingAt),
        paramIfTrue("onboarding cleared", _.isOnboardingFlagCleared),
      )
    }
  }

  final case class AcsReplicationProgress(
      processedContractCount: NonNegativeLong,
      fullyProcessedAcs: Boolean,
  ) extends PrettyPrintingFromCompanion {

    def toProtoV30: v30.PartyReplicationStatus.AcsReplicationProgress =
      v30.PartyReplicationStatus.AcsReplicationProgress(
        processedContractCount.unwrap,
        fullyProcessedAcs,
      )

    override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationProgress] =
      AcsReplicationProgress
  }

  private object AcsReplicationProgress extends PrettyPrintingCompanion[AcsReplicationProgress] {
    def fromInternal(internal: InternalStatus.AcsReplicationProgress): AcsReplicationProgress = {
      val (processedContractCount, fullyProcessedAcs) = internal match {
        case InternalStatus.PersistentProgress(count, _, done) => (count, done)
        case InternalStatus.EphemeralSequencerChannelProgress(count, _, done, _) => (count, done)
        case InternalStatus.EphemeralFileImporterProgress(count, _, done, _) => (count, done)
      }
      AcsReplicationProgress(processedContractCount, fullyProcessedAcs)
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.AcsReplicationProgress
    ): ParsingResult[AcsReplicationProgress] = for {
      replicatedContractCount <- ProtoConverter.parseNonNegativeLong(
        "replicated_contract_count",
        proto.processedContractCount,
      )
    } yield AcsReplicationProgress(replicatedContractCount, proto.fullyProcessedAcs)

    override protected val pretty: Pretty[AcsReplicationProgress] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("contracts", _.processedContractCount),
        paramIfTrue("fully replicated", _.fullyProcessedAcs),
      )
    }
  }

  case object AcsIndexingProgress extends PrettyPrintingFromCompanion {
    override def prettyCompanion: PrettyPrintingCompanion[AcsIndexingProgress.this.type] =
      AcsIndexingProgressPrettyPrintingCompanion

    def fromInternal: InternalStatus.AcsIndexingProgress => AcsIndexingProgress.type = {
      case InternalStatus.AcsIndexingProgress(_indexedContractCount, _nextIndexingCounter, _done) =>
        AcsIndexingProgress
    }

    def toProtoV30: v30.PartyReplicationStatus.AcsIndexingProgress =
      v30.PartyReplicationStatus.AcsIndexingProgress()

    def fromProtoV30(
        @unused _proto: v30.PartyReplicationStatus.AcsIndexingProgress
    ): ParsingResult[AcsIndexingProgress.type] = Right(AcsIndexingProgress)
  }

  private object AcsIndexingProgressPrettyPrintingCompanion
      extends PrettyPrintingCompanion[AcsIndexingProgress.type] {
    override protected val pretty: Pretty[AcsIndexingProgress.type] =
      prettyOfObject[AcsIndexingProgress.type]
  }

  final case class PartyReplicationError(message: String) extends PrettyPrintingFromCompanion {

    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationError =
      v30.PartyReplicationStatus.PartyReplicationError(message)

    override def prettyCompanion: PrettyPrintingCompanion[PartyReplicationError] =
      PartyReplicationError
  }

  private object PartyReplicationError extends PrettyPrintingCompanion[PartyReplicationError] {
    def fromInternal: InternalStatus.PartyReplicationError => PartyReplicationError = err =>
      PartyReplicationError(err.message)

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.PartyReplicationError
    ): ParsingResult[PartyReplicationError] = Right(PartyReplicationError(proto.errorMessage))

    override protected val pretty: Pretty[PartyReplicationError] = prettyOfString(_.message)
  }

}
