// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrintingCompanion}
import com.digitalasset.canton.protocol.{v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.{ProtoUnvalidatedString, ProtoValidation}
import com.digitalasset.canton.version.ProtocolVersionValidation

/** Information about the submitters of the transaction in the case of a reassignment. This data
  * structure is quite similar to [[com.digitalasset.canton.data.SubmitterMetadata]] but differ on a
  * small number of fields.
  */
final case class ReassignmentSubmitterMetadata(
    submitter: LfPartyId,
    submittingParticipant: ParticipantId,
    commandId: LedgerCommandId,
    submissionId: Option[LedgerSubmissionId],
    userId: LedgerUserId,
    workflowId: Option[LfWorkflowId],
    // TODO(#37720): Make the max sequencing time mandatory once protocol version 37 is removed
    maxSequencingTime: Option[CantonTimestamp],
) extends HasSubmissionTrackerData {

  override def submissionTrackerData: Option[SubmissionTrackerData] =
    maxSequencingTime.map(SubmissionTrackerData(submittingParticipant, _))

  def toProtoV30: v30.ReassignmentSubmitterMetadata =
    v30.ReassignmentSubmitterMetadata(
      submitter = submitter,
      submittingParticipantUid = submittingParticipant.uid.toProtoPrimitive,
      commandId = commandId,
      submissionId = submissionId.getOrElse("").toProtoUnvalidated,
      userId = userId,
      workflowId = workflowId.getOrElse("").toProtoUnvalidated,
    )

  def toProtoV31: v31.ReassignmentSubmitterMetadata =
    v31.ReassignmentSubmitterMetadata(
      submitter = submitter,
      submittingParticipantUid = submittingParticipant.uid.toProtoPrimitive,
      commandId = commandId,
      submissionId = submissionId.getOrElse("").toProtoUnvalidated,
      userId = userId,
      workflowId = workflowId.getOrElse("").toProtoUnvalidated,
      maxSequencingTime = maxSequencingTime.map(_.toProtoTimestamp),
    )

  override def prettyCompanion: PrettyPrintingCompanion[ReassignmentSubmitterMetadata] =
    ReassignmentSubmitterMetadata

  def submittingAdminParty: LfPartyId = submittingParticipant.adminParty.toLf
}

object ReassignmentSubmitterMetadata
    extends PrettyPrintingCompanion[ReassignmentSubmitterMetadata] {

  override protected val pretty: Pretty[ReassignmentSubmitterMetadata] = prettyOfClass(
    param("submitter", _.submitter),
    param("submitting participant", _.submittingParticipant),
    param("command id", _.commandId),
    paramIfDefined("submission id", _.submissionId),
    param("user id", _.userId),
    param("workflow id", _.workflowId),
    paramIfDefined("max sequencing time", _.maxSequencingTime),
  )

  def fromProtoV30(
      pvv: ProtocolVersionValidation,
      reassignmentSubmitterMetadataP: v30.ReassignmentSubmitterMetadata,
  ): ParsingResult[ReassignmentSubmitterMetadata] = {
    val v30.ReassignmentSubmitterMetadata(
      submitterP,
      submittingParticipantP,
      commandIdP,
      submissionIdP,
      userIdP,
      workflowIdP,
    ) = reassignmentSubmitterMetadataP

    fromProto(
      pvv,
      submitterP,
      submittingParticipantP,
      commandIdP,
      submissionIdP,
      userIdP,
      workflowIdP,
      maxSequencingTime = None,
    )
  }

  def fromProtoV31(
      pvv: ProtocolVersionValidation,
      reassignmentSubmitterMetadataP: v31.ReassignmentSubmitterMetadata,
  ): ParsingResult[ReassignmentSubmitterMetadata] = {
    val v31.ReassignmentSubmitterMetadata(
      submitterP,
      submittingParticipantP,
      commandIdP,
      submissionIdP,
      userIdP,
      workflowIdP,
      maxSequencingTimeP,
    ) = reassignmentSubmitterMetadataP

    for {
      maxSequencingTime <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "max_sequencing_time",
        maxSequencingTimeP,
      )
      metadata <- fromProto(
        pvv,
        submitterP,
        submittingParticipantP,
        commandIdP,
        submissionIdP,
        userIdP,
        workflowIdP,
        Some(maxSequencingTime),
      )
    } yield metadata
  }

  private def fromProto(
      pvv: ProtocolVersionValidation,
      submitterP: ProtoUnvalidatedString,
      submittingParticipantP: ProtoUnvalidatedString,
      commandIdP: ProtoUnvalidatedString,
      submissionIdP: ProtoUnvalidatedString,
      userIdP: ProtoUnvalidatedString,
      workflowIdP: ProtoUnvalidatedString,
      maxSequencingTime: Option[CantonTimestamp],
  ): ParsingResult[ReassignmentSubmitterMetadata] =
    for {
      submitter <- ProtoValidation.validateThen(submitterP, "submitter", pvv)(
        ProtoConverter.parseLfPartyId
      )
      submittingParticipant <-
        ProtoValidation
          .validateThen(
            submittingParticipantP,
            "submitting_participant_uid",
            pvv,
          )(UniqueIdentifier.fromProtoPrimitive)
          .map(ParticipantId(_))
      commandId <- ProtoValidation.validateThen(commandIdP, "command_id", pvv)(
        ProtoConverter.parseCommandId
      )
      submissionId <- ProtoValidation.validateThen(submissionIdP, "submission_id", pvv)(
        ProtoConverter.parseLFSubmissionIdO
      )
      userId <- ProtoValidation.validateThen(userIdP, "user_id", pvv)(ProtoConverter.parseLFUserId)
      workflowId <- ProtoValidation.validateThen(workflowIdP, "workflow_id", pvv)(
        ProtoConverter.parseLFWorkflowIdO
      )
    } yield ReassignmentSubmitterMetadata(
      submitter,
      submittingParticipant,
      commandId,
      submissionId,
      userId,
      workflowId,
      maxSequencingTime,
    )
}
