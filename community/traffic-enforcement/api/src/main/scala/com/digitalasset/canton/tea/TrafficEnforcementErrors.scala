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
}
