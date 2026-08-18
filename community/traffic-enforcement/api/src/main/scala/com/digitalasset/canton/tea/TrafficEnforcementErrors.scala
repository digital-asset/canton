// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import com.digitalasset.base.error.{
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  Explanation,
  Resolution,
  RpcError,
}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TrafficEnforcementErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext

object TrafficEnforcementErrors extends TrafficEnforcementErrorGroup {

  sealed trait TrafficEnforcementError extends RpcError

  @Explanation(
    "This error occurs if a Ledger API command submission cannot debit from the given local traffic account."
  )
  @Resolution(
    "Inspect the error details, adapt your Ledger API submission or contact the participant operator for adjusting your account details."
  )
  object InsufficientBalance
      extends ErrorCode(
        id = "TRAFFIC_ACCOUNT_VALIDATION_FAILED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(override val cause: String)(implicit
        loggingContext: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(cause = cause)
        with TrafficEnforcementError
  }

  @Explanation(
    "This error occurs if a Ledger API command submission has more than one actAs party" +
      " and the participant is configured to reject such submissions under traffic enforcement."
  )
  @Resolution(
    "Submit the command with a single actAs party, or contact the participant operator to disable" +
      " rejection of multi-party submissions under traffic enforcement."
  )
  object MultiPartySubmissionRejected
      extends ErrorCode(
        id = "TRAFFIC_MULTI_PARTY_SUBMISSION_REJECTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Reject(override val cause: String)(implicit
        loggingContext: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(cause = cause)
        with TrafficEnforcementError
  }

  @Explanation(
    "This error indicates that a traffic delta could not be applied, as it would overflow the current credit balance."
  )
  @Resolution(
    "Use a lower (absolute) delta value."
  )
  object TrafficUpdateOutOfBound
      extends ErrorCode(
        id = "TRAFFIC_UPDATE_OUT_OF_BOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    // The account id and the delta are strings because their types live in the TEA module, which this module doesn't depend on.
    final case class Reject(accountId: String, delta: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The traffic delta $delta cannot be applied to the current balance of $accountId without the credit balance exceeding its maximum value."
        )
        with TrafficEnforcementError
  }

  @Explanation(
    "This error occurs if a traffic account operation failed for a reason expected to be transient," +
      " such as a database timeout, and the request can be retried."
  )
  @Resolution(
    "Retry the request. When retrying an account update, reuse the deduplication id of the original" +
      " request, otherwise the update may be applied a second time. If the problem persists, contact" +
      " the participant operator."
  )
  object TransientFailure
      extends ErrorCode(
        id = "TRAFFIC_TRANSIENT_FAILURE",
        ErrorCategory.TransientServerFailure,
      ) {
    // throwable is attached for the server-side log only; it is never serialized into the gRPC status.
    final case class Reject(throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The traffic account operation failed due to a transient failure.",
          throwableO = Some(throwable),
        )
        with TrafficEnforcementError
  }

  @Explanation(
    "This error occurs if a traffic account operation failed for a reason that is not expected to" +
      " resolve on retry."
  )
  @Resolution(
    "Resolution will require operator intervention, and potentially vendor support."
  )
  object FatalFailure
      extends ErrorCode(
        id = "TRAFFIC_FATAL_FAILURE",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    // throwable is attached for the server-side log only; it is never serialized into the gRPC status.
    final case class Reject(throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "The traffic account operation failed due to an internal error.",
          throwableO = Some(throwable),
        )
        with TrafficEnforcementError
  }

  /** Ids of every error in this module, so the participant can tell if the failure came from TEA or
    * maybe an intermediary. `lazy` to avoid a circular init deadlock.
    */
  lazy val allErrorIds: Set[String] = Set(
    InsufficientBalance.id,
    MultiPartySubmissionRejected.id,
    TrafficUpdateOutOfBound.id,
    TransientFailure.id,
    FatalFailure.id,
  )
}
