// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.command.submission

import com.digitalasset.canton.protocol.ReassignmentId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value

final case class SubmitReassignmentRequest(
    submitter: Ref.Party,
    userId: Ref.UserId,
    commandId: Ref.CommandId,
    submissionId: Ref.SubmissionId,
    workflowId: Option[Ref.WorkflowId],
    reassignmentCommands: Seq[Either[AssignCommand, UnassignCommand]],
)

final case class UnassignCommand(
    sourceSynchronizerId: Source[SynchronizerId],
    targetSynchronizerId: Target[SynchronizerId],
    contractId: Value.ContractId,
)
final case class AssignCommand(
    sourceSynchronizerId: Source[SynchronizerId],
    targetSynchronizerId: Target[SynchronizerId],
    reassignmentId: ReassignmentId,
)
