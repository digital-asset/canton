// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.RepairServiceErrorGroup
import com.digitalasset.canton.error.{CantonBaseError, CantonError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.PhysicalSynchronizerId

sealed trait RepairServiceError extends Product with Serializable with CantonBaseError

object RepairServiceError extends RepairServiceErrorGroup {

  object InvalidArgument
      extends ErrorCode(
        id = "INVALID_ARGUMENT_REPAIR",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with RepairServiceError
  }

  object InvalidState
      extends ErrorCode(
        id = "INVALID_STATE_REPAIR_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(reason)
        with RepairServiceError
  }

  object IOStream
      extends ErrorCode(
        id = "IO_STREAM_REPAIR_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
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

  @Explanation("The assignation of a contract cannot be changed due to an error.")
  @Resolution("Retry after operator intervention.")
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

  @Explanation("Import ACS has failed.")
  @Resolution("Retry after operator intervention.")
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

  @Explanation("Manual logical synchronizer upgrade failed")
  @Resolution("Retry after operator intervention.")
  object SynchronizerUpgradeError
      extends ErrorCode(
        id = "MANUAL_LOGICAL_SYNCHRONIZER_UPGRADE_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(successorPsid: PhysicalSynchronizerId, reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = reason)
        with RepairServiceError
  }
}
