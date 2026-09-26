// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.syntax.traverse.*
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
  AcsReplicationProgress,
  AgreementStatus,
  Disconnected,
  EphemeralSequencerChannelProgress,
  PartyReplicationAuthorization,
  PartyReplicationError,
  PartyReplicationFailed,
  ReplicationParams,
}
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationAgreementParams
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

/** Internal state representation of the party replication process. Refer to party_replication.proto
  * PartyReplicationStatus for the semantics.
  */
// TODO(#35267) remove ACS replication specific info and encapsulate it in AcsReplicationStatus
final case class PartyReplicationStatus(
    params: ReplicationParams,
    agreementStatus: AgreementStatus,
    authorizationO: Option[PartyReplicationAuthorization],
    replicationO: Option[AcsReplicationProgress],
    // TODO(#35267) use AcsReplicationStatus
    acsReplicationStatusO: Option[PartyReplicationStatus],
    indexingO: Option[AcsIndexingProgress],
    hasCompleted: Boolean,
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

  def setAgreementStatus(newAgreementStatus: AgreementStatus): PartyReplicationStatus =
    copy(agreementStatus = newAgreementStatus)(representativeProtocolVersion)
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
      acsReplicationStatus: Option[PartyReplicationStatus]
  ): PartyReplicationStatus =
    copy(acsReplicationStatusO = acsReplicationStatus)(representativeProtocolVersion)

  def ensureCanSetAgreement(paramsReceived: ReplicationParams): Either[String, Unit] = for {
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
    agreementStatus.toProtoV30,
    authorizationO.map(_.toProtoV30),
    replicationO.map(_.toProtoV30),
    indexingO.map(_.toProtoV30),
    hasCompleted = hasCompleted,
    errorO.map(_.toProtoV30),
    acsReplicationStatusO.map(_.toProtoV30),
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
      param("agreement", _.agreementStatus),
      paramIfDefined("authorization", _.authorizationO),
      paramIfDefined("replication", _.replicationO),
      paramIfDefined("acsReplicationStatus", _.acsReplicationStatusO),
      paramIfDefined("indexing", _.indexingO),
      paramIfDefined("error", _.errorO),
      paramIfTrue("complete", _.hasCompleted),
    )
  }

  def fromProtoV30(
      proto: v30.PartyReplicationStatus
  ): ParsingResult[PartyReplicationStatus] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    parametersP <- ProtoConverter.required("parameters", proto.parameters)
    parameters <- ReplicationParams.fromProtoV30(parametersP)
    agreement <- AgreementStatus.fromProtoV30(proto.agreementStatus)
    authorizationO <- proto.authorization.traverse(PartyReplicationAuthorization.fromProtoV30)
    replicationO <- proto.replication.traverse(AcsReplicationProgress.fromProtoV30)
    acsReplicationO <- proto.acsReplicationStatus.traverse(PartyReplicationStatus.fromProtoV30)
    indexingO <- proto.indexing.traverse(AcsIndexingProgress.fromProtoV30)
    hasCompleted = proto.hasCompleted
    errorO <- proto.errorMessage.traverse(PartyReplicationError.fromProtoV30)
  } yield PartyReplicationStatus(
    parameters,
    agreement,
    authorizationO,
    replicationO,
    acsReplicationO,
    indexingO,
    hasCompleted,
    errorO,
  )(rpv)

  def apply(
      params: ReplicationParams,
      pv: ProtocolVersion,
      agreementStatus: AgreementStatus = AgreementStatus.NotProposed,
      authorizationO: Option[PartyReplicationAuthorization] = None,
      replicationO: Option[AcsReplicationProgress] = None,
      acsReplicationO: Option[PartyReplicationStatus] = None,
      indexingO: Option[AcsIndexingProgress] = None,
      hasCompleted: Boolean = false,
      errorO: Option[PartyReplicationError] = None,
  ): PartyReplicationStatus = PartyReplicationStatus(
    params,
    agreementStatus,
    authorizationO,
    replicationO,
    acsReplicationO,
    indexingO,
    hasCompleted,
    errorO,
  )(
    protocolVersionRepresentativeFor(pv)
  )

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

  sealed trait AgreementStatus extends PrettyPrintingFromCompanion with Product with Serializable {
    def toProtoV30: v30.PartyReplicationStatus.AgreementStatus
    def isEmpty: Boolean
  }

  object AgreementStatus {
    case object NotProposed extends AgreementStatus {
      override def toProtoV30: v30.PartyReplicationStatus.AgreementStatus.NotProposed =
        v30.PartyReplicationStatus.AgreementStatus.NotProposed(
          v30.PartyReplicationStatus.AgreementNotProposed()
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
      override def toProtoV30: v30.PartyReplicationStatus.AgreementStatus.Proposed =
        v30.PartyReplicationStatus.AgreementStatus.Proposed(
          v30.PartyReplicationStatus.AgreementProposed()
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
      override def toProtoV30: v30.PartyReplicationStatus.AgreementStatus.Exists =
        v30.PartyReplicationStatus.AgreementStatus.Exists(
          v30.PartyReplicationStatus.AgreementExists(
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

    case object NotNeeded extends AgreementStatus {
      override def toProtoV30: v30.PartyReplicationStatus.AgreementStatus.NotNeeded =
        v30.PartyReplicationStatus.AgreementStatus.NotNeeded(
          v30.PartyReplicationStatus.AgreementNotNeeded()
        )

      override val isEmpty: Boolean = true

      override def prettyCompanion: PrettyPrintingCompanion[NotNeeded.this.type] =
        NotNeededPrettyPrintingCompanion
    }

    private object NotNeededPrettyPrintingCompanion
        extends PrettyPrintingCompanion[NotNeeded.type] {
      override protected val pretty: Pretty[NotNeeded.type] = prettyOfObject[NotNeeded.type]
    }

    case object Archived extends AgreementStatus {
      override def toProtoV30: v30.PartyReplicationStatus.AgreementStatus.Archived =
        v30.PartyReplicationStatus.AgreementStatus.Archived(
          v30.PartyReplicationStatus.AgreementArchived()
        )

      override val isEmpty: Boolean = true

      override def prettyCompanion: PrettyPrintingCompanion[Archived.this.type] =
        ArchivedPrettyPrintingCompanion
    }

    private object ArchivedPrettyPrintingCompanion extends PrettyPrintingCompanion[Archived.type] {
      override protected val pretty: Pretty[Archived.type] = prettyOfObject[Archived.type]
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.AgreementStatus
    ): ParsingResult[AgreementStatus] =
      proto match {
        case v30.PartyReplicationStatus.AgreementStatus.NotProposed(_) =>
          Right(AgreementStatus.NotProposed)
        case v30.PartyReplicationStatus.AgreementStatus.Proposed(_) =>
          Right(AgreementStatus.Proposed)
        case v30.PartyReplicationStatus.AgreementStatus.Exists(exists) =>
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
        case v30.PartyReplicationStatus.AgreementStatus.NotNeeded(_) =>
          Right(AgreementStatus.NotNeeded)
        case v30.PartyReplicationStatus.AgreementStatus.Archived(_) =>
          Right(AgreementStatus.Archived)
        case v30.PartyReplicationStatus.AgreementStatus.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("agreement_status"))
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

    def toProtoV30: v30.PartyReplicationStatus.AcsReplicationProgress =
      v30.PartyReplicationStatus.AcsReplicationProgress(
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
        proto: v30.PartyReplicationStatus.AcsReplicationProgress
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

    override protected val pretty: Pretty[PartyReplicationError] = prettyOfString(_.message)
  }
}
