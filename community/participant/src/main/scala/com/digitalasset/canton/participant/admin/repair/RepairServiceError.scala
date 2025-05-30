// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.RepairServiceErrorGroup
import com.digitalasset.canton.error.{CantonBaseError, CantonError}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.participant.store.AcsInspectionError
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

sealed trait RepairServiceError extends Product with Serializable with CantonBaseError

object RepairServiceError extends RepairServiceErrorGroup {

  @Explanation("The participant does not support the requested protocol version.")
  @Resolution(
    "Specify a protocol version that the participant supports or upgrade the participant to a release that supports the requested protocol version."
  )
  object UnsupportedProtocolVersionParticipant
      extends ErrorCode(
        id = "UNSUPPORTED_PROTOCOL_VERSION_PARTICIPANT",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(
        requestedProtocolVersion: ProtocolVersion,
        supportedVersions: Seq[ProtocolVersion] = ProtocolVersion.supported,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not support the requested protocol version $requestedProtocolVersion"
        )
        with RepairServiceError
  }

  @Explanation(
    "The participant does not support serving an ACS snapshot at the requested timestamp, likely because some concurrent processing has not yet finished."
  )
  @Resolution(
    "Make sure that the specified timestamp has been obtained from the participant in some way. If so, retry after a bit (possibly repeatedly)."
  )
  object InvalidAcsSnapshotTimestamp
      extends ErrorCode(
        id = "INVALID_ACS_SNAPSHOT_TIMESTAMP",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Error(
        requestedTimestamp: CantonTimestamp,
        cleanTimestamp: CantonTimestamp,
        synchronizerId: SynchronizerId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not yet support serving an ACS snapshot at the requested timestamp $requestedTimestamp on synchronizer $synchronizerId"
        )
        with RepairServiceError
  }

  @Explanation(
    "The participant does not support serving an ACS snapshot at the requested timestamp because its database has already been pruned, e.g., by the continuous background pruning process."
  )
  @Resolution(
    "The snapshot at the requested timestamp is no longer available. Pick a more recent timestamp if possible."
  )
  object UnavailableAcsSnapshot
      extends ErrorCode(
        id = "UNAVAILABLE_ACS_SNAPSHOT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        requestedTimestamp: CantonTimestamp,
        prunedTimestamp: CantonTimestamp,
        synchronizerId: SynchronizerId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The participant does not support serving an ACS snapshot at the requested timestamp $requestedTimestamp on synchronizer $synchronizerId"
        )
        with RepairServiceError
  }

  @Explanation(
    "The ACS snapshot cannot be returned because it contains inconsistencies. This is likely due to the request happening concurrently with pruning."
  )
  @Resolution(
    "Retry the operation"
  )
  object InconsistentAcsSnapshot
      extends ErrorCode(
        id = "INCONSISTENT_ACS_SNAPSHOT",
        ErrorCategory.TransientServerFailure,
      ) {
    final case class Error(synchronizerId: SynchronizerId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The ACS snapshot for $synchronizerId cannot be returned because it contains inconsistencies."
        )
        with RepairServiceError
  }

  @Explanation(
    "A contract cannot be serialized due to an error."
  )
  @Resolution(
    "Retry after operator intervention."
  )
  object SerializationError
      extends ErrorCode(
        id = "CONTRACT_SERIALIZATION_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class Error(synchronizerId: SynchronizerId, contractId: LfContractId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Serialization for contract $contractId in synchronizer $synchronizerId failed"
        )
        with RepairServiceError
  }

  object InvalidArgument
      extends ErrorCode(
        id = "INVALID_ARGUMENT_REPAIR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with RepairServiceError
  }

  @Explanation(
    "A contract cannot be purged due to an error."
  )
  @Resolution(
    "Retry after operator intervention."
  )
  object ContractPurgeError
      extends ErrorCode(
        id = "CONTRACT_PURGE_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(synchronizerAlias: SynchronizerAlias, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = reason)
        with RepairServiceError
  }

  @Explanation(
    "The assignation of a contract cannot be changed due to an error."
  )
  @Resolution(
    "Retry after operator intervention."
  )
  object ContractAssignationChangeError
      extends ErrorCode(
        id = "CONTRACT_ASSIGNATION_CHANGE_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = reason)
        with RepairServiceError
  }

  @Explanation(
    "Import Acs has failed."
  )
  @Resolution(
    "Retry after operator intervention."
  )
  object ImportAcsError
      extends ErrorCode(
        id = "IMPORT_ACS_ERROR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = reason)
        with RepairServiceError
  }

  def fromAcsInspectionError(acsError: AcsInspectionError, logger: TracedLogger)(implicit
      traceContext: TraceContext,
      elc: ErrorLoggingContext,
  ): RepairServiceError =
    acsError match {
      case AcsInspectionError.RequestedAfterCleanTimeOfChange(synchronizerId, requested, clean) =>
        RepairServiceError.InvalidAcsSnapshotTimestamp.Error(
          requested.timestamp,
          clean.timestamp,
          synchronizerId,
        )
      case AcsInspectionError.TimestampBeforePruning(synchronizerId, requested, pruned) =>
        RepairServiceError.UnavailableAcsSnapshot.Error(
          requested,
          pruned,
          synchronizerId,
        )
      case AcsInspectionError.InconsistentSnapshot(synchronizerId, missingContract) =>
        logger.warn(
          s"Inconsistent ACS snapshot for synchronizer $synchronizerId. Contract $missingContract (and possibly others) is missing."
        )
        RepairServiceError.InconsistentAcsSnapshot.Error(synchronizerId)
      case AcsInspectionError.SerializationIssue(synchronizerId, contractId, errorMessage) =>
        logger.error(
          s"Contract $contractId for synchronizer $synchronizerId cannot be serialized due to: $errorMessage"
        )
        RepairServiceError.SerializationError.Error(synchronizerId, contractId)
      case AcsInspectionError.InvariantIssue(synchronizerId, contractId, errorMessage) =>
        logger.error(
          s"Contract $contractId for synchronizer $synchronizerId cannot be serialized due to an invariant violation: $errorMessage"
        )
        RepairServiceError.SerializationError.Error(synchronizerId, contractId)
      case AcsInspectionError.OffboardingParty(synchronizerId, error) =>
        RepairServiceError.InvalidArgument.Error(
          s"Parties offboarding on synchronizer $synchronizerId: $error"
        )
      case AcsInspectionError.ContractLookupIssue(synchronizerId, contractId, errorMessage) =>
        logger.debug(
          s"Contract $contractId for synchronizer $synchronizerId cannot be found due to: $errorMessage"
        )
        RepairServiceError.InvalidArgument.Error(errorMessage)
    }
}
