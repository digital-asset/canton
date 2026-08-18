// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.ProtoValidation
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
) extends PrettyPrinting
    with HasSubmissionTrackerData {

  override def submissionTrackerData: Option[SubmissionTrackerData] = None

  def toProtoV30: v30.ReassignmentSubmitterMetadata =
    v30.ReassignmentSubmitterMetadata(
      submitter = submitter,
      submittingParticipantUid = submittingParticipant.uid.toProtoPrimitive,
      commandId = commandId,
      submissionId = submissionId.getOrElse("").toProtoUnvalidated,
      userId = userId,
      workflowId = workflowId.getOrElse("").toProtoUnvalidated,
    )

  override protected def pretty: Pretty[ReassignmentSubmitterMetadata] = prettyOfClass(
    param("submitter", _.submitter),
    param("submitting participant", _.submittingParticipant),
    param("command id", _.commandId),
    paramIfDefined("submission id", _.submissionId),
    param("user id", _.userId),
    param("workflow id", _.workflowId),
  )

  def submittingAdminParty: LfPartyId = submittingParticipant.adminParty.toLf
}

object ReassignmentSubmitterMetadata {
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
    )
  }
}
