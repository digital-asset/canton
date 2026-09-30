// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.syntax.traverse.*
import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyReplicationStatus as LapiPartyReplicationStatus
import com.digitalasset.canton.ProtoDeserializationError
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
  ReplicationMode,
  ReplicationParameters,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus as InternalStatus
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus as InternalAcsReplicationStatus
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*

import scala.annotation.unused

/** External console representation of the party replication process. Refer to
  * party_management_service.proto PartyReplicationStatus for the semantics.
  */
final case class PartyReplicationStatus(
    parameters: ReplicationParameters,
    authorizationO: Option[PartyReplicationAuthorization],
    replicationO: Option[AcsReplicationProgress],
    acsReplicationO: Option[AcsReplicationStatus],
    indexingO: Option[AcsIndexingProgress.type],
    hasCompleted: Boolean,
    replicationMode: ReplicationMode,
    errorO: Option[PartyReplicationError],
) extends PrettyPrintingFromCompanion {

  require(
    indexingO.isEmpty || replicationO.nonEmpty,
    s"cannot begin indexing $indexingO before replication has started",
  )

  def toProtoV30: v30.PartyReplicationStatus = v30.PartyReplicationStatus(
    Some(parameters.toProtoV30),
    authorizationO.map(_.toProtoV30),
    replicationO.map(_.toProtoV30),
    indexingO.map(_.toProtoV30),
    hasCompleted = hasCompleted,
    errorO.map(_.toProtoV30),
    acsReplicationO.map(_.toProtoV30),
    replicationMode.toProtoV30,
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
      paramIfDefined("authorization", _.authorizationO),
      paramIfDefined("replication", _.replicationO),
      paramIfDefined("acsReplicationStatus", _.acsReplicationO),
      paramIfDefined("indexing", _.indexingO),
      paramIfDefined("error", _.errorO),
      paramIfTrue("complete", _.hasCompleted),
      param("replicationMode", _.replicationMode),
    )

  def fromInternal: InternalStatus => PartyReplicationStatus = {
    // When new fields are added, adapt this deciding which fields may need to be exposed externally erring
    // on the side of cautious not exposing fields that don't have clear external utility and can be preserved
    // in a backward compatible way.
    case InternalStatus(
          params,
          authorizationO,
          replicationO,
          acsReplicationStatusO,
          indexingO,
          hasCompleted,
          replicationMode,
          errorO,
        ) =>
      PartyReplicationStatus(
        ReplicationParameters.fromInternal(params),
        authorizationO.map(PartyReplicationAuthorization.fromInternal),
        replicationO.map(AcsReplicationProgress.fromInternal),
        acsReplicationStatusO.map(AcsReplicationStatus.fromInternal),
        indexingO.map(AcsIndexingProgress.fromInternal),
        hasCompleted,
        ReplicationMode.fromInternal(replicationMode),
        errorO.map(PartyReplicationError.fromInternal),
      )
  }

  def fromProtoV30(proto: v30.PartyReplicationStatus): ParsingResult[PartyReplicationStatus] =
    for {
      paramsP <- ProtoConverter.required("parameters", proto.parameters)
      params <- ReplicationParameters.fromProtoV30(paramsP)
      authorizationO <- proto.authorization.traverse(PartyReplicationAuthorization.fromProtoV30)
      replicationO <- proto.replication.traverse(AcsReplicationProgress.fromProtoV30)
      acsReplicationO <- proto.acsReplicationStatus.traverse(AcsReplicationStatus.fromProtoV30)
      indexingO <- proto.indexing.traverse(AcsIndexingProgress.fromProtoV30)
      hasCompleted = proto.hasCompleted
      errorO <- proto.errorMessage.traverse(PartyReplicationError.fromProtoV30)
      replicationMode <- ReplicationMode.fromProtoV30(proto.replicationMode)
    } yield PartyReplicationStatus(
      params,
      authorizationO,
      replicationO,
      acsReplicationO,
      indexingO,
      hasCompleted,
      replicationMode,
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
    def fromInternal(
        internal: InternalAcsReplicationStatus.AcsReplicationProgress
    ): AcsReplicationProgress = {
      val (processedContractCount, fullyProcessedAcs) = internal match {
        case InternalAcsReplicationStatus.PersistentProgress(count, _, _, done) => (count, done)
        case InternalAcsReplicationStatus.EphemeralSequencerChannelProgress(
              count,
              _,
              _,
              done,
              _,
            ) =>
          (count, done)
        case InternalAcsReplicationStatus.EphemeralFileImporterProgress(count, _, _, done, _) =>
          (count, done)
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

  sealed trait ReplicationMode extends PrettyPrintingFromCompanion with Product with Serializable {
    def toProtoV30: v30.PartyReplicationStatus.ReplicationMode
  }

  object ReplicationMode {
    case object File extends ReplicationMode {
      override def toProtoV30: v30.PartyReplicationStatus.ReplicationMode =
        v30.PartyReplicationStatus.ReplicationMode.REPLICATION_MODE_FILE

      override def prettyCompanion: PrettyPrintingCompanion[File.this.type] =
        FilePrettyPrintingCompanion
    }

    private object FilePrettyPrintingCompanion extends PrettyPrintingCompanion[File.type] {
      override protected val pretty: Pretty[File.type] = prettyOfObject[File.type]
    }

    case object SequencerChannel extends ReplicationMode {
      override def toProtoV30: v30.PartyReplicationStatus.ReplicationMode =
        v30.PartyReplicationStatus.ReplicationMode.REPLICATION_MODE_SEQUENCER_CHANNEL

      override def prettyCompanion: PrettyPrintingCompanion[SequencerChannel.type] =
        SequencerChannelPrintingCompanion
    }

    private object SequencerChannelPrintingCompanion
        extends PrettyPrintingCompanion[SequencerChannel.type] {
      override protected val pretty: Pretty[SequencerChannel.type] =
        prettyOfObject[SequencerChannel.type]
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.ReplicationMode
    ): ParsingResult[ReplicationMode] =
      ProtoConverter.parseEnum[ReplicationMode, v30.PartyReplicationStatus.ReplicationMode](
        {
          case v30.PartyReplicationStatus.ReplicationMode.REPLICATION_MODE_FILE => Right(Some(File))
          case v30.PartyReplicationStatus.ReplicationMode.REPLICATION_MODE_SEQUENCER_CHANNEL =>
            Right(Some(SequencerChannel))
          case v30.PartyReplicationStatus.ReplicationMode.REPLICATION_MODE_UNSPECIFIED =>
            Left(ProtoDeserializationError.FieldNotSet("replication_mode"))
          case v30.PartyReplicationStatus.ReplicationMode.Unrecognized(unknown) =>
            Left(ProtoDeserializationError.UnrecognizedEnum("replication_mode", unknown))
        },
        "replication_mode",
        proto,
      )

    def fromInternal(internal: InternalStatus.ReplicationMode): ReplicationMode = internal match {
      case InternalStatus.ReplicationMode.File => File
      case InternalStatus.ReplicationMode.SequencerChannel => SequencerChannel
    }

  }
}
