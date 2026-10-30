// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.protocol.{ReassignmentId, Stakeholders, v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.{
  ProtoUnvalidatedSeq,
  ProtoUnvalidatedString,
  ProtoValidation,
}
import com.digitalasset.canton.version.*

/** Stores the data of an unassignment that needs to be passed from the source synchronizer to the
  * target synchronizer.
  */
final case class UnassignmentData(
    // Data known by the submitter
    submitterMetadata: ReassignmentSubmitterMetadata,
    contractsBatch: ContractsReassignmentBatch,
    reassigningParticipants: Set[ParticipantId],
    sourcePsid: Source[PhysicalSynchronizerId],
    targetPsid: Target[PhysicalSynchronizerId],
    targetTimestamp: Target[CantonTimestamp],
    // Data unknown by the submitter
    unassignmentTs: CantonTimestamp,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      UnassignmentData.type
    ]
) extends HasProtocolVersionedWrapper[UnassignmentData] {
  @transient override protected lazy val companionObj: UnassignmentData.type = UnassignmentData

  lazy val reassignmentId: ReassignmentId = ReassignmentId(
    sourcePsid.map(_.logical),
    targetPsid.map(_.logical),
    unassignmentTs,
    contractsBatch.contractIdCounters,
  )

  def stakeholders: Stakeholders = contractsBatch.stakeholders

  def copy(
      submitterMetadata: ReassignmentSubmitterMetadata = this.submitterMetadata,
      contractsBatch: ContractsReassignmentBatch = this.contractsBatch,
      reassigningParticipants: Set[ParticipantId] = this.reassigningParticipants,
      sourcePsid: Source[PhysicalSynchronizerId] = this.sourcePsid,
      targetPsid: Target[PhysicalSynchronizerId] = this.targetPsid,
      targetTimestamp: Target[CantonTimestamp] = this.targetTimestamp,
      unassignmentTs: CantonTimestamp = this.unassignmentTs,
  ): UnassignmentData =
    UnassignmentData(
      submitterMetadata,
      contractsBatch,
      reassigningParticipants,
      sourcePsid,
      targetPsid,
      targetTimestamp,
      unassignmentTs,
    )(representativeProtocolVersion)

  def toProtoV30: v30.UnassignmentData = v30.UnassignmentData(
    submitterMetadata = submitterMetadata.toProtoV30.some,
    contracts = contractsBatch.contracts.map(_.toProtoV30),
    reassigningParticipantUids =
      reassigningParticipants.map(_.uid.toProtoPrimitive.toProtoUnvalidated).toSeq,
    sourcePhysicalSynchronizerId = sourcePsid.unwrap.toProtoPrimitive,
    targetPhysicalSynchronizerId = targetPsid.unwrap.toProtoPrimitive,
    targetTimestamp = targetTimestamp.unwrap.toProtoTimestamp.some,
    unassignmentTs = unassignmentTs.toProtoTimestamp.some,
  )

  def toProtoV31: v31.UnassignmentData = v31.UnassignmentData(
    submitterMetadata = submitterMetadata.toProtoV30.some,
    contracts = contractsBatch.contracts.map(_.toProtoV31),
    reassigningParticipantUids =
      reassigningParticipants.map(_.uid.toProtoPrimitive.toProtoUnvalidated).toSeq,
    sourcePhysicalSynchronizerId = sourcePsid.unwrap.toProtoPrimitive,
    targetPhysicalSynchronizerId = targetPsid.unwrap.toProtoPrimitive,
    targetTimestamp = targetTimestamp.unwrap.toProtoTimestamp.some,
    unassignmentTs = unassignmentTs.toProtoTimestamp.some,
  )
}

object UnassignmentData
    extends VersioningCompanion[UnassignmentData]
    with ProtocolVersionedCompanionDbHelpers[UnassignmentData] {

  override def name: String = "UnassignmentData"
  override val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v36), v30.UnassignmentData)(
        supportedProtoVersionPVV(_)(fromProtoV30),
        _.toProtoV30,
      ),
    ProtoVersion(31) -> VersionedProtoCodec
      .storage(ReleaseProtocolVersion(ProtocolVersion.v37), v31.UnassignmentData)(
        supportedProtoVersionPVV(_)(fromProtoV31),
        _.toProtoV31,
      ),
  )

  def apply(
      unassignmentRequest: FullUnassignmentTree,
      unassignmentTs: CantonTimestamp,
  ): UnassignmentData = UnassignmentData(
    submitterMetadata = unassignmentRequest.submitterMetadata,
    contractsBatch = unassignmentRequest.contracts,
    reassigningParticipants = unassignmentRequest.reassigningParticipants,
    sourcePsid = unassignmentRequest.sourceSynchronizer,
    targetPsid = unassignmentRequest.targetSynchronizer,
    unassignmentTs = unassignmentTs,
    targetTimestamp = unassignmentRequest.targetTimestamp,
  )(
    UnassignmentData.protocolVersionRepresentativeFor(
      unassignmentRequest.sourceSynchronizer.unwrap.protocolVersion
    )
  )

  private def fromProto[P](
      pvv: ProtocolVersionValidation,
      submitterMetadataP: Option[v30.ReassignmentSubmitterMetadata],
      contractsP: ProtoUnvalidatedSeq[P],
      reassigningParticipantUidsP: ProtoUnvalidatedSeq[ProtoUnvalidatedString],
      sourcePhysicalSynchronizerIdP: ProtoUnvalidatedString,
      targetPhysicalSynchronizerIdP: ProtoUnvalidatedString,
      targetTimestampP: Option[com.google.protobuf.timestamp.Timestamp],
      unassignmentTsP: Option[com.google.protobuf.timestamp.Timestamp],
      parseContract: P => ParsingResult[ContractReassignment],
      representativeProtocolVersion: RepresentativeProtocolVersion[UnassignmentData.type],
  ): ParsingResult[UnassignmentData] = for {
    submitterMetadata <- ProtoConverter.parseRequired(
      ReassignmentSubmitterMetadata.fromProtoV30(pvv, _),
      "submitter_metadata",
      submitterMetadataP,
    )

    contracts <- ProtoValidation
      .validateLengthThen(
        contractsP,
        "contracts",
        pvv,
        ProtoValidation.MaxCollectionSize,
      ) { case (contractP, _) => parseContract(contractP) }
      .flatMap(
        ContractsReassignmentBatch
          .create(_)
          .leftMap(err => InvariantViolation(Some("contracts"), err.toString))
      )

    reassigningParticipants <- ProtoValidation
      .validateThen(
        reassigningParticipantUidsP,
        "reassigning_participant_uids",
        pvv,
        ProtoValidation.MaxCollectionSize,
      )(
        ParticipantId.fromProtoPrimitiveUid
      )

    sourceSynchronizer <- ProtoValidation
      .validateThen(
        sourcePhysicalSynchronizerIdP,
        "source_physical_synchronizer_id",
        pvv,
      )(PhysicalSynchronizerId.fromProtoPrimitive)
      .map(Source(_))

    targetSynchronizer <- ProtoValidation
      .validateThen(
        targetPhysicalSynchronizerIdP,
        "target_physical_synchronizer_id",
        pvv,
      )(PhysicalSynchronizerId.fromProtoPrimitive)
      .map(Target(_))

    targetTimestamp <- ProtoConverter
      .parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "target_timestamp",
        targetTimestampP,
      )
      .map(Target(_))

    unassignmentTs <- ProtoConverter.parseRequired(
      CantonTimestamp.fromProtoTimestamp,
      "unassignment_ts",
      unassignmentTsP,
    )
  } yield new UnassignmentData(
    submitterMetadata = submitterMetadata,
    contractsBatch = contracts,
    reassigningParticipants = reassigningParticipants.toSet,
    sourcePsid = sourceSynchronizer,
    targetPsid = targetSynchronizer,
    targetTimestamp = targetTimestamp,
    unassignmentTs = unassignmentTs,
  )(representativeProtocolVersion)

  private def fromProtoV30(
      pvv: ProtocolVersionValidation,
      proto: v30.UnassignmentData,
  ): ParsingResult[UnassignmentData] =
    for {
      rpv <- UnassignmentData.protocolVersionRepresentativeFor(ProtoVersion(30))
      parsed <- fromProto(
        pvv,
        proto.submitterMetadata,
        proto.contracts,
        proto.reassigningParticipantUids,
        proto.sourcePhysicalSynchronizerId,
        proto.targetPhysicalSynchronizerId,
        proto.targetTimestamp,
        proto.unassignmentTs,
        ContractReassignment.fromProtoV30,
        rpv,
      )
    } yield parsed

  private def fromProtoV31(
      pvv: ProtocolVersionValidation,
      proto: v31.UnassignmentData,
  ): ParsingResult[UnassignmentData] = for {
    rpv <- UnassignmentData.protocolVersionRepresentativeFor(ProtoVersion(31))
    parsed <- fromProto(
      pvv,
      proto.submitterMetadata,
      proto.contracts,
      proto.reassigningParticipantUids,
      proto.sourcePhysicalSynchronizerId,
      proto.targetPhysicalSynchronizerId,
      proto.targetTimestamp,
      proto.unassignmentTs,
      ContractReassignment.fromProtoV31(pvv),
      rpv,
    )
  } yield parsed

  sealed trait ReassignmentGlobalOffset extends Product with Serializable {
    def merge(other: ReassignmentGlobalOffset): Either[String, ReassignmentGlobalOffset]

    def unassignment: Option[Offset]
    def assignment: Option[Offset]
  }

  object ReassignmentGlobalOffset {
    def create(
        unassignment: Option[Offset],
        assignment: Option[Offset],
    ): Either[String, Option[ReassignmentGlobalOffset]] =
      (unassignment, assignment) match {
        case (Some(unassignment), Some(assignment)) =>
          ReassignmentGlobalOffsets.create(unassignment, assignment).map(Some(_))
        case (Some(unassignment), None) => Right(Some(UnassignmentGlobalOffset(unassignment)))
        case (None, Some(assignment)) => Right(Some(AssignmentGlobalOffset(assignment)))
        case (None, None) => Right(None)
      }
  }

  final case class UnassignmentGlobalOffset(offset: Offset) extends ReassignmentGlobalOffset {
    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case UnassignmentGlobalOffset(newUnassignment) =>
          Either.cond(
            offset == newUnassignment,
            this,
            s"Unable to merge unassignment offsets $offset and $newUnassignment",
          )
        case AssignmentGlobalOffset(newAssignment) =>
          ReassignmentGlobalOffsets.create(offset, newAssignment)
        case offsets @ ReassignmentGlobalOffsets(newUnassignment, _) =>
          Either.cond(
            offset == newUnassignment,
            offsets,
            s"Unable to merge unassignment offsets $offset and $newUnassignment",
          )
      }

    override def unassignment: Option[Offset] = Some(offset)
    override def assignment: Option[Offset] = None
  }

  final case class AssignmentGlobalOffset(offset: Offset) extends ReassignmentGlobalOffset {
    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case AssignmentGlobalOffset(newAssignment) =>
          Either.cond(
            offset == newAssignment,
            this,
            s"Unable to merge assignment offsets $offset and $newAssignment",
          )
        case UnassignmentGlobalOffset(newUnassignment) =>
          ReassignmentGlobalOffsets.create(newUnassignment, offset)
        case offsets @ ReassignmentGlobalOffsets(_, newAssignment) =>
          Either.cond(
            offset == newAssignment,
            offsets,
            s"Unable to merge assignment offsets $offset and $newAssignment",
          )
      }

    override def unassignment: Option[Offset] = None
    override def assignment: Option[Offset] = Some(offset)
  }

  final case class ReassignmentGlobalOffsets private (
      unassignmentOffset: Offset,
      assignmentOffset: Offset,
  ) extends ReassignmentGlobalOffset {
    require(
      unassignmentOffset != assignmentOffset,
      s"Unassignment and assignment offsets should be different; got $unassignmentOffset",
    )

    override def merge(
        other: ReassignmentGlobalOffset
    ): Either[String, ReassignmentGlobalOffset] =
      other match {
        case UnassignmentGlobalOffset(newUnassignment) =>
          Either.cond(
            newUnassignment == unassignmentOffset,
            this,
            s"Unable to merge unassignment offsets $unassignment and $newUnassignment",
          )
        case AssignmentGlobalOffset(newAssignment) =>
          Either.cond(
            newAssignment == assignmentOffset,
            this,
            s"Unable to merge assignment offsets $assignment and $newAssignment",
          )
        case ReassignmentGlobalOffsets(newUnassignment, newAssignment) =>
          Either.cond(
            newUnassignment == unassignmentOffset && newAssignment == assignmentOffset,
            this,
            s"Unable to merge reassignment offsets ($unassignment, $assignment) and ($newUnassignment, $newAssignment)",
          )
      }

    override def unassignment: Option[Offset] = Some(unassignmentOffset)
    override def assignment: Option[Offset] = Some(assignmentOffset)
  }

  object ReassignmentGlobalOffsets {
    def create(
        unassignment: Offset,
        assignment: Offset,
    ): Either[String, ReassignmentGlobalOffsets] =
      Either.cond(
        unassignment != assignment,
        ReassignmentGlobalOffsets(unassignment, assignment),
        s"Unassignment and assignment offsets should be different but got $unassignment",
      )
  }
}
