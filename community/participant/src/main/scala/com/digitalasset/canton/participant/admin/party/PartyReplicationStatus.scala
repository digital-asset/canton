// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AcsIndexingProgress,
  Disconnected,
  PartyReplicationAuthorization,
  PartyReplicationError,
  PartyReplicationFailed,
  ReplicationMode,
  ReplicationParams,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.{
  AcsReplicationParameters,
  AcsReplicationProgress,
  EphemeralSequencerChannelProgress,
}
import com.digitalasset.canton.participant.admin.party.acsreplication.{
  AcsReplicationAgreementParams,
  AcsReplicationStatus,
}
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationProcessor
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.participant.protocol.v30.PartyReplicationStatus.ReplicationMode as ProtoReplicationMode
import com.digitalasset.canton.protocol.v30 as v30Topology
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.version.*
import io.scalaland.chimney.dsl.*

/** Internal state representation of the party replication process. Refer to party_replication.proto
  * PartyReplicationStatus for the semantics.
  */
final case class PartyReplicationStatus(
    params: ReplicationParams,
    authorizationO: Option[PartyReplicationAuthorization],
    replicationO: Option[AcsReplicationProgress],
    acsReplicationStatusO: Option[AcsReplicationStatus],
    indexingO: Option[AcsIndexingProgress],
    hasCompleted: Boolean,
    replicationMode: ReplicationMode,
    errorO: Option[PartyReplicationError],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      PartyReplicationStatus.type
    ]
) extends HasProtocolVersionedWrapper[PartyReplicationStatus]
    with PrettyPrintingFromCompanion {
  @transient override protected lazy val companionObj: PartyReplicationStatus.type =
    PartyReplicationStatus

  require(
    indexingO.isEmpty || replicationO.nonEmpty,
    s"cannot begin indexing $indexingO before replication has started",
  )

  def setProcessor(
      processor: AcsReplicationProcessor
  ): PartyReplicationStatus = modifyReplication {
    case None => AcsReplicationProgress.initialize(Some(processor))
    case Some(previous) =>
      EphemeralSequencerChannelProgress(
        previous.processedContractCount,
        previous.nextPersistenceCounter,
        previous.acsHashO,
        previous.fullyProcessedAcs,
        Some(processor),
      )
  }

  def setAuthorization(newAuthorization: PartyReplicationAuthorization): PartyReplicationStatus =
    copy(authorizationO = Some(newAuthorization))(representativeProtocolVersion)
  def modifyReplication(
      modify: Option[AcsReplicationProgress] => AcsReplicationProgress
  ): PartyReplicationStatus =
    copy(replicationO = Some(modify(replicationO)))(representativeProtocolVersion)
  def setReplication(newReplication: Option[AcsReplicationProgress]): PartyReplicationStatus =
    copy(replicationO = newReplication)(representativeProtocolVersion)
  def setIndexing(): PartyReplicationStatus =
    copy(indexingO =
      Some(
        AcsIndexingProgress(
          indexedContractActivationChangeCount = NonNegativeLong.zero,
          nextIndexingCounter = NonNegativeLong.zero,
          indexingAlmostDoneWatermarkO = None,
        )
      )
    )(representativeProtocolVersion)
  def updateIndexing(indexingProgress: AcsIndexingProgress): PartyReplicationStatus =
    copy(indexingO = Some(indexingProgress))(representativeProtocolVersion)
  def setCompleted(): PartyReplicationStatus =
    copy(hasCompleted = true)(representativeProtocolVersion)
  def modifyErrorO(
      modify: Option[PartyReplicationError] => Option[PartyReplicationError]
  ): PartyReplicationStatus = copy(errorO = modify(errorO))(representativeProtocolVersion)
  def setTopologySerial(serial: PositiveInt): PartyReplicationStatus =
    copy(params = params.copy(serial = serial))(representativeProtocolVersion)
  def setAcsReplicationStatus(
      acsReplicationStatus: Option[AcsReplicationStatus]
  ): PartyReplicationStatus =
    copy(acsReplicationStatusO = acsReplicationStatus)(representativeProtocolVersion)

  /** Indicates whether party replication is active and expected to be progressing, i.e. whether
    * monitoring for progress and initiating state transitions are needed in contrast to having
    * completed or having failed in such a way that requires operator intervention.
    */
  def isProgressExpected: Boolean = !hasCompleted && !errorO.exists {
    case PartyReplicationFailed(_) => true
    case Disconnected(_) => false
  }

  def toProtoV30: v30.PartyReplicationStatus = v30.PartyReplicationStatus(
    Some(params.toProtoV30),
    authorizationO.map(_.toProtoV30),
    replicationO.map(_.toProtoV30),
    indexingO.map(_.toProtoV30),
    hasCompleted = hasCompleted,
    errorO.map(_.toProtoV30),
    acsReplicationStatusO.map(_.toProtoV30),
    replicationMode.toProtoV30,
  )

  override def prettyCompanion: PrettyPrintingCompanion[PartyReplicationStatus] =
    PartyReplicationStatus
}

object PartyReplicationStatus
    extends VersioningCompanion[PartyReplicationStatus]
    with PrettyPrintingCompanion[PartyReplicationStatus] {

  override val name: String = "PartyReplicationStatus"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v30.PartyReplicationStatus
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  override protected val pretty: Pretty[PartyReplicationStatus] = {
    import com.digitalasset.canton.logging.pretty.PrettyInstances.*
    prettyOfClass(
      param("params", _.params),
      paramIfDefined("authorization", _.authorizationO),
      paramIfDefined("replication", _.replicationO),
      paramIfDefined("acsReplicationStatus", _.acsReplicationStatusO),
      paramIfDefined("indexing", _.indexingO),
      paramIfDefined("error", _.errorO),
      paramIfTrue("complete", _.hasCompleted),
      param("replicationMode", _.replicationMode),
    )
  }

  def fromProtoV30(
      proto: v30.PartyReplicationStatus
  ): ParsingResult[PartyReplicationStatus] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    parametersP <- ProtoConverter.required("parameters", proto.parameters)
    parameters <- ReplicationParams.fromProtoV30(parametersP)
    authorizationO <- proto.authorization.traverse(PartyReplicationAuthorization.fromProtoV30)
    replicationO <- proto.replication.traverse(AcsReplicationProgress.fromProtoV30)
    acsReplicationO <- proto.acsReplicationStatus.traverse(AcsReplicationStatus.fromProtoV30)
    indexingO <- proto.indexing.traverse(AcsIndexingProgress.fromProtoV30)
    hasCompleted = proto.hasCompleted
    errorO <- proto.errorMessage.traverse(PartyReplicationError.fromProtoV30)
    replicationMode <- ReplicationMode.fromProtoV30(proto.replicationMode)
  } yield PartyReplicationStatus(
    parameters,
    authorizationO,
    replicationO,
    acsReplicationO,
    indexingO,
    hasCompleted,
    replicationMode,
    errorO,
  )(rpv)

  def apply(
      params: ReplicationParams,
      pv: ProtocolVersion,
      authorizationO: Option[PartyReplicationAuthorization] = None,
      replicationO: Option[AcsReplicationProgress] = None,
      acsReplicationO: Option[AcsReplicationStatus] = None,
      indexingO: Option[AcsIndexingProgress] = None,
      hasCompleted: Boolean = false,
      errorO: Option[PartyReplicationError] = None,
      replicationMode: ReplicationMode,
  ): PartyReplicationStatus = PartyReplicationStatus(
    params,
    authorizationO,
    replicationO,
    acsReplicationO,
    indexingO,
    hasCompleted,
    replicationMode,
    errorO,
  )(
    protocolVersionRepresentativeFor(pv)
  )

  def fromAcsReplicationStatus(
      acsReplicationStatus: AcsReplicationStatus
  ): PartyReplicationStatus = {
    val params = ReplicationParams.fromAcsReplicationParams(acsReplicationStatus.params)
    PartyReplicationStatus(
      params,
      authorizationO = acsReplicationStatus.authorizationO.map(
        PartyReplicationAuthorization.fromAcsReplicationAuthorization
      ),
      replicationO = acsReplicationStatus.replicationO,
      acsReplicationStatusO = Some(acsReplicationStatus),
      indexingO = None,
      hasCompleted = acsReplicationStatus.hasCompleted,
      replicationMode = ReplicationMode.SequencerChannel,
      errorO = acsReplicationStatus.errorO.map(PartyReplicationError.fromAcsReplicationError),
    )(
      protocolVersionRepresentativeFor(
        acsReplicationStatus.representativeProtocolVersion.representative
      )
    )
  }

  final case class ReplicationParams(
      requestId: AddPartyRequestId,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  ) extends PrettyPrintingFromCompanion {
    def toProtoV30: v30.PartyReplicationStatus.ReplicationParameters =
      v30.PartyReplicationStatus.ReplicationParameters(
        requestId.toHexString,
        partyId.toProtoPrimitive,
        synchronizerId.uid.toProtoPrimitive,
        sourceParticipantId.uid.toProtoPrimitive,
        targetParticipantId.uid.toProtoPrimitive,
        serial.unwrap,
        toPartyReplicationPermissionProtoV30(participantPermission),
      )

    private def toPartyReplicationPermissionProtoV30(
        permission: ParticipantPermission
    ): v30Topology.Enums.ParticipantPermission =
      permission match {
        case ParticipantPermission.Submission =>
          v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION
        case ParticipantPermission.Confirmation =>
          v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION
        case ParticipantPermission.Observation =>
          v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION
      }

    override def prettyCompanion: PrettyPrintingCompanion[ReplicationParams] = ReplicationParams
  }

  object ReplicationParams extends PrettyPrintingCompanion[ReplicationParams] {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.ReplicationParameters
    ): ParsingResult[ReplicationParams] = for {
      requestIdBytes <- HexString
        .parseToByteString(proto.requestId)
        .toRight(
          ProtoDeserializationError
            .ValueDeserializationError(s"not a hex string \"${proto.requestId}\"", "request_id")
        )
      requestId <- Hash.fromProtoPrimitive(requestIdBytes)
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
      participantPermission <- fromParticipantPermissionProtoV30(proto.participantPermission)
    } yield ReplicationParams(
      requestId,
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      topologySerial,
      participantPermission,
    )

    private def fromParticipantPermissionProtoV30(
        proto: v30Topology.Enums.ParticipantPermission
    ): ParsingResult[ParticipantPermission] =
      ProtoConverter.parseEnum[ParticipantPermission, v30Topology.Enums.ParticipantPermission](
        {
          case v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION =>
            Right(Some(ParticipantPermission.Submission))
          case v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION =>
            Right(Some(ParticipantPermission.Confirmation))
          case v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION =>
            Right(Some(ParticipantPermission.Observation))
          case v30Topology.Enums.ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED =>
            Right(None)
          case v30Topology.Enums.ParticipantPermission.Unrecognized(unknown) =>
            Left(ProtoDeserializationError.UnrecognizedEnum(proto.name, unknown))
        },
        "participant_permission",
        proto,
      )

    def fromAgreementParams(agreement: AcsReplicationAgreementParams): ReplicationParams =
      agreement.transformInto[ReplicationParams]

    def fromAcsReplicationParams(
        acsReplicationParams: AcsReplicationParameters
    ): ReplicationParams =
      acsReplicationParams.transformInto[ReplicationParams]

    override protected val pretty: Pretty[ReplicationParams] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("request", _.requestId),
        param("party", _.partyId),
        param("synchronizer", _.synchronizerId),
        param("source participant", _.sourceParticipantId),
        param("target participant", _.targetParticipantId),
        param("serial", _.serial),
        param("permission", _.participantPermission.showType),
      )
    }
  }

  final case class PartyReplicationAuthorization(
      onboardingAt: EffectiveTime,
      isOnboardingFlagCleared: Boolean,
  ) extends PrettyPrintingFromCompanion {
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationAuthorization =
      v30.PartyReplicationStatus.PartyReplicationAuthorization(
        Some(onboardingAt.value.toProtoTimestamp),
        isOnboardingFlagCleared,
      )

    override def prettyCompanion: PrettyPrintingCompanion[PartyReplicationAuthorization] =
      PartyReplicationAuthorization
  }

  object PartyReplicationAuthorization
      extends PrettyPrintingCompanion[PartyReplicationAuthorization] {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.PartyReplicationAuthorization
    ): ParsingResult[PartyReplicationAuthorization] =
      for {
        onboardingAtP <- ProtoConverter.required("onboarding_at", proto.onboardingAt)
        onboardingAt <- CantonTimestamp.fromProtoTimestamp(onboardingAtP)
      } yield PartyReplicationAuthorization(
        EffectiveTime(onboardingAt),
        proto.isOnboardingFlagCleared,
      )

    def fromAcsReplicationAuthorization(
        acsReplicationAuthorization: AcsReplicationStatus.PartyReplicationAuthorization
    ): PartyReplicationAuthorization =
      acsReplicationAuthorization.transformInto[PartyReplicationAuthorization]

    override protected val pretty: Pretty[PartyReplicationAuthorization] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("onboarding at", _.onboardingAt.value),
        paramIfTrue("onboarding cleared", _.isOnboardingFlagCleared),
      )
    }
  }

  final case class AcsIndexingProgress(
      indexedContractActivationChangeCount: NonNegativeLong,
      nextIndexingCounter: NonNegativeLong,
      indexingAlmostDoneWatermarkO: Option[NonNegativeLong],
  ) extends PrettyPrintingFromCompanion {

    /** Because completing indexing during party replication is a moving target in the face of
      * transactions running concurrently to party replication, check if the watermark tracking the
      * most recent indexedContractActivationChangeCount-"odometer" reading matches the odometer.
      * The caller can use this to decide when to clear the onboarding flag thus moving party
      * replication to the next party replication stage.
      */
    def isIndexingCurrentlyAlmostDone: Boolean =
      indexingAlmostDoneWatermarkO.contains(indexedContractActivationChangeCount)

    def toProtoV30: v30.PartyReplicationStatus.AcsIndexingProgress =
      v30.PartyReplicationStatus.AcsIndexingProgress(
        indexedContractActivationChangeCount.unwrap,
        nextIndexingCounter.unwrap,
        indexingAlmostDoneWatermarkO.map(_.unwrap),
      )

    override def prettyCompanion: PrettyPrintingCompanion[AcsIndexingProgress] = AcsIndexingProgress
  }

  object AcsIndexingProgress extends PrettyPrintingCompanion[AcsIndexingProgress] {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.AcsIndexingProgress
    ): ParsingResult[AcsIndexingProgress] =
      for {
        indexedContractCount <- ProtoConverter.parseNonNegativeLong(
          "indexed_contract_activation_change_count",
          proto.indexedContractActivationChangeCount,
        )
        nextIndexingCounter <- ProtoConverter.parseNonNegativeLong(
          "next_indexing_counter",
          proto.nextIndexingCounter,
        )
        lastFullDrainCountO <- proto.indexingAlmostDoneWatermark
          .traverse(ProtoConverter.parseNonNegativeLong("indexing_almost_done_watermark", _))
      } yield AcsIndexingProgress(
        indexedContractCount,
        nextIndexingCounter,
        lastFullDrainCountO,
      )

    override protected val pretty: Pretty[AcsIndexingProgress] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("change count", _.indexedContractActivationChangeCount),
        param("next counter", _.nextIndexingCounter),
        paramIfDefined("almost done watermark", _.indexingAlmostDoneWatermarkO),
      )
    }
  }

  sealed trait PartyReplicationError extends PrettyPrintingFromCompanion {
    def message: String
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationError
    override def prettyCompanion: PrettyPrintingCompanion[PartyReplicationError] =
      PartyReplicationError
  }

  final case class Disconnected(message: String) extends PartyReplicationError {
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationError =
      v30.PartyReplicationStatus.PartyReplicationError(
        v30.PartyReplicationStatus.PartyReplicationError.ErrorType.ERROR_TYPE_DISCONNECTED,
        message,
      )
  }

  final case class PartyReplicationFailed(message: String) extends PartyReplicationError {
    def toProtoV30: v30.PartyReplicationStatus.PartyReplicationError =
      v30.PartyReplicationStatus.PartyReplicationError(
        v30.PartyReplicationStatus.PartyReplicationError.ErrorType.ERROR_TYPE_FAILED,
        message,
      )
  }

  object PartyReplicationError extends PrettyPrintingCompanion[PartyReplicationError] {
    def fromProtoV30(
        proto: v30.PartyReplicationStatus.PartyReplicationError
    ): ParsingResult[PartyReplicationError] = {
      import v30.PartyReplicationStatus.PartyReplicationError.ErrorType
      ProtoConverter.parseEnum[PartyReplicationError, ErrorType](
        {
          case ErrorType.ERROR_TYPE_DISCONNECTED =>
            Right(Some(Disconnected(proto.errorMessage)))
          case ErrorType.ERROR_TYPE_FAILED =>
            Right(Some(PartyReplicationFailed(proto.errorMessage)))
          case ErrorType.ERROR_TYPE_UNSPECIFIED => Right(None)
          case ErrorType.Unrecognized(unknown) =>
            Left(ProtoDeserializationError.UnrecognizedEnum("error_type", unknown))
        },
        "error_type",
        proto.errorType,
      )
    }

    def fromAcsReplicationError(
        acsReplicationError: AcsReplicationStatus.AcsReplicationError
    ): PartyReplicationError =
      acsReplicationError match {
        case AcsReplicationStatus.Disconnected(message) => Disconnected(message)
        case AcsReplicationStatus.AcsReplicationFailed(message) =>
          PartyReplicationFailed(message)
      }

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
      ProtoConverter.parseEnum[ReplicationMode, ProtoReplicationMode](
        {
          case ProtoReplicationMode.REPLICATION_MODE_FILE => Right(Some(File))
          case ProtoReplicationMode.REPLICATION_MODE_SEQUENCER_CHANNEL =>
            Right(Some(SequencerChannel))
          case ProtoReplicationMode.REPLICATION_MODE_UNSPECIFIED =>
            Left(ProtoDeserializationError.FieldNotSet("replication_mode"))
          case ProtoReplicationMode.Unrecognized(unknown) =>
            Left(ProtoDeserializationError.UnrecognizedEnum("replication_mode", unknown))
        },
        "replication_mode",
        proto,
      )
  }
}
