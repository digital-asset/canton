// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.LsuErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext

sealed trait LsuError extends Product with Serializable with CantonBaseError

object LsuError extends LsuErrorGroup {

  // TODO(#26113) – Ensure all non-retryable errors have a corresponding error here
  @Explanation("TODO(#31526)")
  @Resolution("TODO(#31526)")
  object FailedLsu
      extends ErrorCode(
        id = "FAILED_LSU",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Error(
        details: String
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Lsu failed: $details")
        with LsuError
  }

}
