// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LedgerCommandId,
  LedgerSubmissionId,
  LedgerUserId,
  LfPartyId,
  LfWorkflowId,
}

/** Information about the submitter of a reassignment, as given by the caller of the submission. The
  * [[com.digitalasset.canton.data.ReassignmentSubmitterMetadata]] of the request is built from it
  * once the max sequencing time of the request is known.
  */
final case class ReassignmentSubmissionInfo(
    submitter: LfPartyId,
    submittingParticipant: ParticipantId,
    commandId: LedgerCommandId,
    submissionId: Option[LedgerSubmissionId],
    userId: LedgerUserId,
    workflowId: Option[LfWorkflowId],
) {

  /** Starting with
    * [[com.digitalasset.canton.version.ProtocolVersion.reassignmentReplayProtection]], the
    * submitter metadata carries the max sequencing time of the request, used by the submission
    * tracker of the submitting participant.
    */
  def toSubmitterMetadata(
      maxSequencingTime: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  ): ReassignmentSubmitterMetadata =
    ReassignmentSubmitterMetadata(
      submitter = submitter,
      submittingParticipant = submittingParticipant,
      commandId = commandId,
      submissionId = submissionId,
      userId = userId,
      workflowId = workflowId,
      maxSequencingTime = Option.when(
        protocolVersion >= ProtocolVersion.reassignmentReplayProtection
      )(maxSequencingTime),
    )
}
