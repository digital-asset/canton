// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.PartyManagementServiceErrorGroup
import com.digitalasset.canton.error.{CantonBaseError, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicator.AcsReplicationRequestId
import com.digitalasset.canton.topology.SynchronizerId

sealed trait ReplicateAcsError extends CantonBaseError {
  def requestId: AcsReplicationRequestId
}

object ReplicateAcsError extends PartyManagementServiceErrorGroup {

  @Explanation(
    """|This error indicates that replicating ACS to a participant and a synchronizer
       |has been interrupted by a participant having disconnected from a synchronizer."""
  )
  @Resolution("Reconnect the participant to the synchronizer to unblock ACS replication.")
  object DisconnectedFromSynchronizer
      extends ErrorCode(
        id = "REPLICATE_ACS_DISCONNECTED_FROM_SYNCHRONIZER",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Failure(
        requestId: AcsReplicationRequestId,
        synchronizerId: SynchronizerId,
        reason: String,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = reason)
        with ReplicateAcsError
  }

  // TODO(#22136): Complete error categorization and remove "Other" error code.
  @Explanation(
    """|This error is currently used as a catch-all for a variety of failures that prevent replicating
       |ACS to a participant and synchronizer."""
  )
  @Resolution(
    "Inspect the message, potentially rebuild the target participant, and retry replicating the ACS."
  )
  object Other
      extends ErrorCode(
        id = "REPLICATE_ACS_OTHER",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Failure(requestId: AcsReplicationRequestId, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"ACS replication $requestId: $reason")
        with ReplicateAcsError
  }
}
