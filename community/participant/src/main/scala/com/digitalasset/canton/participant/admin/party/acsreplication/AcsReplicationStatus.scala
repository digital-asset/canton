// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus.{
  AcsReplicationError,
  AcsReplicationFailed,
  AcsReplicationParameters,
  AcsReplicationProgress,
  AgreementStatus,
  Disconnected,
  EphemeralSequencerChannelProgress,
  PartyReplicationAuthorization,
}
import com.digitalasset.canton.participant.protocol.party.PartyReplicationFileImporter
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationProcessor
import com.digitalasset.canton.participant.protocol.v30
import com.digitalasset.canton.protocol.{LfContractId, v30 as v30Topology}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.HexString
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{ProtoDeserializationError, RepairCounter}
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*

/** Internal state representation of the ACS replication process. Refer to acs_replication.proto
  * AcsReplicationStatus for the semantics.
  */
final case class AcsReplicationStatus(
    params: AcsReplicationParameters,
    agreementStatus: AgreementStatus,
    // TODO (#35267) remove once authorization is no longer used in AcsReplicator
    authorizationO: Option[PartyReplicationAuthorization],
    replicationO: Option[AcsReplicationProgress],
    hasCompleted: Boolean,
    errorO: Option[AcsReplicationError],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      AcsReplicationStatus.type
    ]
) extends HasProtocolVersionedWrapper[AcsReplicationStatus]
    with PrettyPrintingFromCompanion {
  @transient override protected lazy val companionObj: AcsReplicationStatus.type =
    AcsReplicationStatus

  def setProcessor(
      processor: AcsReplicationProcessor
  ): AcsReplicationStatus = modifyReplication {
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

  def setAgreementStatus(newAgreementStatus: AgreementStatus): AcsReplicationStatus =
    copy(agreementStatus = newAgreementStatus)(representativeProtocolVersion)
  def setAuthorization(newAuthorization: PartyReplicationAuthorization): AcsReplicationStatus =
    copy(authorizationO = Some(newAuthorization))(representativeProtocolVersion)
  def modifyReplication(
      modify: Option[AcsReplicationProgress] => AcsReplicationProgress
  ): AcsReplicationStatus =
    copy(replicationO = Some(modify(replicationO)))(representativeProtocolVersion)
  def setReplication(newReplication: Option[AcsReplicationProgress]): AcsReplicationStatus =
    copy(replicationO = newReplication)(representativeProtocolVersion)
  def setCompleted(): AcsReplicationStatus =
    copy(hasCompleted = true)(representativeProtocolVersion)
  def modifyErrorO(
      modify: Option[AcsReplicationError] => Option[AcsReplicationError]
  ): AcsReplicationStatus = copy(errorO = modify(errorO))(representativeProtocolVersion)
  def setTopologySerial(serial: PositiveInt): AcsReplicationStatus =
    copy(params = params.copy(serial = serial))(representativeProtocolVersion)

  def ensureCanSetAgreement(paramsReceived: AcsReplicationParameters): Either[String, Unit] = for {
    _ <- Either.cond(
      agreementStatus.isEmpty,
      (),
      s"ACS replication ${params.requestId} already has an agreement $agreementStatus",
    )
    _ <- Either.cond(
      paramsReceived == params,
      (),
      s"The ACS replication ${params.requestId} agreement parameters received $paramsReceived do not match the locally stored agreement parameters $params",
    )
  } yield ()

  /** Indicates whether ACS replication is active and expected to be progressing, i.e. whether
    * monitoring for progress and initiating state transitions are needed in contrast to having
    * completed or having failed in such a way that requires operator intervention.
    */
  def isProgressExpected: Boolean = !hasCompleted && !errorO.exists {
    case AcsReplicationFailed(_) => true
    case Disconnected(_) => false
  }

  def toProtoV30: v30.AcsReplicationStatus = v30.AcsReplicationStatus(
    Some(params.toProtoV30),
    agreementStatus.toProtoV30,
    authorizationO.map(_.toProtoV30),
    replicationO.map(_.toProtoV30),
    hasCompleted = hasCompleted,
    errorO.map(_.toProtoV30),
  )

  override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationStatus] =
    AcsReplicationStatus
}

object AcsReplicationStatus
    extends VersioningCompanion[AcsReplicationStatus]
    with PrettyPrintingCompanion[AcsReplicationStatus] {

  override val name: String = "AcsReplicationStatus"

  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.dev)(
      v30.AcsReplicationStatus
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    ),
  )

  override protected val pretty: Pretty[AcsReplicationStatus] = {
    import com.digitalasset.canton.logging.pretty.PrettyInstances.*
    prettyOfClass(
      param("params", _.params),
      param("agreement", _.agreementStatus),
      paramIfDefined("authorization", _.authorizationO),
      paramIfDefined("replication", _.replicationO),
      paramIfDefined("error", _.errorO),
      paramIfTrue("complete", _.hasCompleted),
    )
  }

  def fromProtoV30(
      proto: v30.AcsReplicationStatus
  ): ParsingResult[AcsReplicationStatus] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    parametersP <- ProtoConverter.required("parameters", proto.parameters)
    parameters <- AcsReplicationParameters.fromProtoV30(parametersP)
    agreement <- AgreementStatus.fromProtoV30(proto.agreementStatus)
    authorizationO <- proto.authorization.traverse(PartyReplicationAuthorization.fromProtoV30)
    replicationO <- proto.replication.traverse(AcsReplicationProgress.fromProtoV30)
    hasCompleted = proto.hasCompleted
    errorO <- proto.errorMessage.traverse(AcsReplicationError.fromProtoV30)
  } yield AcsReplicationStatus(
    parameters,
    agreement,
    authorizationO,
    replicationO,
    hasCompleted,
    errorO,
  )(rpv)

  def apply(
      params: AcsReplicationParameters,
      pv: ProtocolVersion,
      agreementStatus: AgreementStatus = AgreementStatus.NotProposed,
      authorizationO: Option[PartyReplicationAuthorization] = None,
      replicationO: Option[AcsReplicationProgress] = None,
      hasCompleted: Boolean = false,
      errorO: Option[AcsReplicationError] = None,
  ): AcsReplicationStatus = AcsReplicationStatus(
    params,
    agreementStatus,
    authorizationO,
    replicationO,
    hasCompleted,
    errorO,
  )(
    protocolVersionRepresentativeFor(pv)
  )

  final case class AcsReplicationParameters(
      requestId: AddPartyRequestId,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  ) extends PrettyPrintingFromCompanion {
    def toProtoV30: v30.AcsReplicationStatus.AcsReplicationParameters =
      v30.AcsReplicationStatus.AcsReplicationParameters(
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

    override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationParameters] =
      AcsReplicationParameters
  }

  object AcsReplicationParameters extends PrettyPrintingCompanion[AcsReplicationParameters] {
    def fromProtoV30(
        proto: v30.AcsReplicationStatus.AcsReplicationParameters
    ): ParsingResult[AcsReplicationParameters] = for {
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
    } yield AcsReplicationParameters(
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

    def fromAgreementParams(agreement: AcsReplicationAgreementParams): AcsReplicationParameters =
      agreement.transformInto[AcsReplicationParameters]

    override protected val pretty: Pretty[AcsReplicationParameters] = {
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

  sealed trait AgreementStatus extends PrettyPrintingFromCompanion with Product with Serializable {
    def toProtoV30: v30.AcsReplicationStatus.AgreementStatus
    def isEmpty: Boolean
  }

  object AgreementStatus {
    case object NotProposed extends AgreementStatus {
      override def toProtoV30: v30.AcsReplicationStatus.AgreementStatus.NotProposed =
        v30.AcsReplicationStatus.AgreementStatus.NotProposed(
          v30.AcsReplicationStatus.AgreementNotProposed()
        )

      override val isEmpty: Boolean = true

      override def prettyCompanion: PrettyPrintingCompanion[NotProposed.this.type] =
        NotProposedPrettyPrintingCompanion
    }

    private object NotProposedPrettyPrintingCompanion
        extends PrettyPrintingCompanion[NotProposed.type] {
      override protected val pretty: Pretty[NotProposed.type] = prettyOfObject[NotProposed.type]
    }

    case object Proposed extends AgreementStatus {
      override def toProtoV30: v30.AcsReplicationStatus.AgreementStatus.Proposed =
        v30.AcsReplicationStatus.AgreementStatus.Proposed(
          v30.AcsReplicationStatus.AgreementProposed()
        )

      override val isEmpty: Boolean = true

      override def prettyCompanion: PrettyPrintingCompanion[Proposed.this.type] =
        ProposedPrettyPrintingCompanion
    }

    private object ProposedPrettyPrintingCompanion extends PrettyPrintingCompanion[Proposed.type] {
      override protected val pretty: Pretty[Proposed.type] = prettyOfObject[Proposed.type]
    }

    final case class Exists(
        // daml agreement contract id to be archived upon completion or disruptions
        damlAgreementContractId: LfContractId,
        agreedAt: CantonTimestamp,
        sequencerId: SequencerId,
    ) extends AgreementStatus {
      override def toProtoV30: v30.AcsReplicationStatus.AgreementStatus.Exists =
        v30.AcsReplicationStatus.AgreementStatus.Exists(
          v30.AcsReplicationStatus.AgreementExists(
            damlAgreementContractId.coid,
            Some(agreedAt.toProtoTimestamp),
            sequencerId.uid.toProtoPrimitive,
          )
        )

      override val isEmpty: Boolean = false

      override def prettyCompanion: PrettyPrintingCompanion[Exists] =
        Exists
    }

    object Exists extends PrettyPrintingCompanion[Exists] {
      override protected val pretty: Pretty[Exists] = {
        import com.digitalasset.canton.logging.pretty.PrettyInstances.*
        prettyOfClass(
          param("contract id", _.damlAgreementContractId),
          param("agreed at", _.agreedAt),
          param("sequencer", _.sequencerId),
        )
      }
    }

    case object Archived extends AgreementStatus {
      override def toProtoV30: v30.AcsReplicationStatus.AgreementStatus.Archived =
        v30.AcsReplicationStatus.AgreementStatus.Archived(
          v30.AcsReplicationStatus.AgreementArchived()
        )

      override val isEmpty: Boolean = true

      override def prettyCompanion: PrettyPrintingCompanion[Archived.this.type] =
        ArchivedPrettyPrintingCompanion
    }

    private object ArchivedPrettyPrintingCompanion extends PrettyPrintingCompanion[Archived.type] {
      override protected val pretty: Pretty[Archived.type] = prettyOfObject[Archived.type]
    }

    def fromProtoV30(
        proto: v30.AcsReplicationStatus.AgreementStatus
    ): ParsingResult[AgreementStatus] =
      proto match {
        case v30.AcsReplicationStatus.AgreementStatus.NotProposed(_) =>
          Right(AgreementStatus.NotProposed)
        case v30.AcsReplicationStatus.AgreementStatus.Proposed(_) =>
          Right(AgreementStatus.Proposed)
        case v30.AcsReplicationStatus.AgreementStatus.Exists(exists) =>
          for {
            contractId <- ProtoConverter.parseLfContractId(exists.contractId)
            agreedAt <- ProtoConverter.parseRequired(
              CantonTimestamp.fromProtoTimestamp,
              "agreed_at",
              exists.agreedAt,
            )
            sequencerId <- UniqueIdentifier
              .fromProtoPrimitive(exists.sequencerUid, "sequencer_uid")
              .map(SequencerId(_))
          } yield Exists(contractId, agreedAt, sequencerId)
        case v30.AcsReplicationStatus.AgreementStatus.Archived(_) =>
          Right(AgreementStatus.Archived)
        case v30.AcsReplicationStatus.AgreementStatus.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("agreement_status"))
      }

  }

  final case class PartyReplicationAuthorization(
      onboardingAt: EffectiveTime,
      isOnboardingFlagCleared: Boolean,
  ) extends PrettyPrintingFromCompanion {
    def toProtoV30: v30.AcsReplicationStatus.PartyReplicationAuthorization =
      v30.AcsReplicationStatus.PartyReplicationAuthorization(
        Some(onboardingAt.value.toProtoTimestamp),
        isOnboardingFlagCleared,
      )

    override def prettyCompanion: PrettyPrintingCompanion[PartyReplicationAuthorization] =
      PartyReplicationAuthorization
  }

  object PartyReplicationAuthorization
      extends PrettyPrintingCompanion[PartyReplicationAuthorization] {
    def fromProtoV30(
        proto: v30.AcsReplicationStatus.PartyReplicationAuthorization
    ): ParsingResult[PartyReplicationAuthorization] =
      for {
        onboardingAtP <- ProtoConverter.required("onboarding_at", proto.onboardingAt)
        onboardingAt <- CantonTimestamp.fromProtoTimestamp(onboardingAtP)
      } yield PartyReplicationAuthorization(
        EffectiveTime(onboardingAt),
        proto.isOnboardingFlagCleared,
      )

    override protected val pretty: Pretty[PartyReplicationAuthorization] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("onboarding at", _.onboardingAt.value),
        paramIfTrue("onboarding cleared", _.isOnboardingFlagCleared),
      )
    }
  }

  sealed trait AcsReplicationProgress extends PrettyPrintingFromCompanion {
    def processedContractCount: NonNegativeLong
    def nextPersistenceCounter: RepairCounter
    def acsHashO: Option[ByteString]
    def fullyProcessedAcs: Boolean
    def processorO: Option[AcsReplicationProcessor]

    def toProtoV30: v30.AcsReplicationStatus.AcsReplicationProgress =
      v30.AcsReplicationStatus.AcsReplicationProgress(
        processedContractCount.unwrap,
        nextPersistenceCounter.unwrap,
        acsHashO,
        fullyProcessedAcs,
      )

    override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationProgress] =
      AcsReplicationProgress
  }

  /** PersistentProgress contains the db-persisted portion of the ACS replication progress. Before
    * the progress needs to be updated, this case class needs to be turned into one of the ephemeral
    * AcsReplicationProgress case classes.
    *
    * @param processedContractCount
    *   how many ACS contracts have been replicated so far
    * @param nextPersistenceCounter
    *   the next unique repair counter to use for the subsequent ACS batch
    * @param acsHashO
    *   the homomorphic hash bytes of the portion of the ACS replicated so far, None if ACS
    *   replication hasn't started
    * @param fullyProcessedAcs
    *   whether the ACS has been fully replicated yet
    */
  final case class PersistentProgress(
      processedContractCount: NonNegativeLong,
      nextPersistenceCounter: RepairCounter,
      acsHashO: Option[ByteString],
      fullyProcessedAcs: Boolean,
  ) extends AcsReplicationProgress {
    override def processorO: Option[AcsReplicationProcessor] = None
  }

  /** EphemeralSequencerChannelProgress holds the ephemeral sequencer channel based ACS replication
    * status of a party replication source or target participant processor.
    */
  final case class EphemeralSequencerChannelProgress(
      processedContractCount: NonNegativeLong,
      nextPersistenceCounter: RepairCounter,
      acsHashO: Option[ByteString],
      fullyProcessedAcs: Boolean,
      processor: Option[AcsReplicationProcessor],
  ) extends AcsReplicationProgress {
    override def processorO: Option[AcsReplicationProcessor] = processor
  }

  /** EphemeralFileImporterProgress holds the ephemeral file-based ACS import status of a party
    * replication target participant.
    */
  // TODO (#35267) remove from here because it doesn't belong to SequencerChannel-based ACS replication
  final case class EphemeralFileImporterProgress(
      processedContractCount: NonNegativeLong,
      nextPersistenceCounter: RepairCounter,
      acsHashO: Option[ByteString],
      fullyProcessedAcs: Boolean,
      fileImporter: PartyReplicationFileImporter,
  ) extends AcsReplicationProgress {
    override def processorO: Option[AcsReplicationProcessor] = None
  }

  object AcsReplicationProgress extends PrettyPrintingCompanion[AcsReplicationProgress] {
    def fromProtoV30(
        proto: v30.AcsReplicationStatus.AcsReplicationProgress
    ): ParsingResult[PersistentProgress] = for {
      replicatedContractCount <- ProtoConverter.parseNonNegativeLong(
        "replicated_contract_count",
        proto.processedContractCount,
      )
      nextPersistenceCounter <- ProtoConverter.parseNonNegativeLong(
        "next_persistence_counter",
        proto.nextPersistenceCounter,
      )
    } yield PersistentProgress(
      replicatedContractCount,
      RepairCounter(nextPersistenceCounter.unwrap),
      proto.acsHash,
      proto.fullyProcessedAcs,
    )

    def initialize(processor: Option[AcsReplicationProcessor]): AcsReplicationProgress =
      EphemeralSequencerChannelProgress(
        NonNegativeLong.zero,
        RepairCounter.Genesis,
        acsHashO = None,
        fullyProcessedAcs = false,
        processor,
      )

    def initialize(fileImporter: PartyReplicationFileImporter): AcsReplicationProgress =
      EphemeralFileImporterProgress(
        NonNegativeLong.zero,
        RepairCounter.Genesis,
        acsHashO = None,
        fullyProcessedAcs = false,
        fileImporter,
      )

    override protected val pretty: Pretty[AcsReplicationProgress] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("contracts", _.processedContractCount),
        param("next counter", _.nextPersistenceCounter),
        paramIfDefined("acs hash", _.acsHashO),
        paramIfTrue("fully replicated", _.fullyProcessedAcs),
        paramIfDefined("processor", _.processorO.map(_.showType)),
      )
    }
  }

  sealed trait AcsReplicationError extends PrettyPrintingFromCompanion {
    def message: String
    def toProtoV30: v30.AcsReplicationStatus.AcsReplicationError
    override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationError] =
      AcsReplicationError
  }

  final case class Disconnected(message: String) extends AcsReplicationError {
    def toProtoV30: v30.AcsReplicationStatus.AcsReplicationError =
      v30.AcsReplicationStatus.AcsReplicationError(
        v30.AcsReplicationStatus.AcsReplicationError.ErrorType.ERROR_TYPE_DISCONNECTED,
        message,
      )
  }

  final case class AcsReplicationFailed(message: String) extends AcsReplicationError {
    def toProtoV30: v30.AcsReplicationStatus.AcsReplicationError =
      v30.AcsReplicationStatus.AcsReplicationError(
        v30.AcsReplicationStatus.AcsReplicationError.ErrorType.ERROR_TYPE_FAILED,
        message,
      )
  }

  object AcsReplicationError extends PrettyPrintingCompanion[AcsReplicationError] {
    def fromProtoV30(
        proto: v30.AcsReplicationStatus.AcsReplicationError
    ): ParsingResult[AcsReplicationError] = {
      import v30.AcsReplicationStatus.AcsReplicationError.ErrorType
      ProtoConverter.parseEnum[AcsReplicationError, ErrorType](
        {
          case ErrorType.ERROR_TYPE_DISCONNECTED =>
            Right(Some(Disconnected(proto.errorMessage)))
          case ErrorType.ERROR_TYPE_FAILED =>
            Right(Some(AcsReplicationFailed(proto.errorMessage)))
          case ErrorType.ERROR_TYPE_UNSPECIFIED => Right(None)
          case ErrorType.Unrecognized(unknown) =>
            Left(ProtoDeserializationError.UnrecognizedEnum("error_type", unknown))
        },
        "error_type",
        proto.errorType,
      )
    }

    override protected val pretty: Pretty[AcsReplicationError] = prettyOfString(_.message)
  }
}
