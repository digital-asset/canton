// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.LsuErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}

sealed trait LsuError extends Product with Serializable with CantonBaseError with PrettyPrinting {

  override protected def pretty: Pretty[LsuError] =
    adHocPrettyInstanceWithName(_.code.id, showFieldNames = true)
}

object LsuError extends LsuErrorGroup {

  @Explanation(
    """An unexpected error in Canton during an LSU. May be caused by concurrent synchronizer connect or disconnect operations during the LSU or by a bug in Canton."""
  )
  @Resolution(
    "Verify and ensure no modifications are being made to the synchronizer connections during the LSU and retry the LSU. Automatic LSU can be retried by restarting the PN. For a manual LSU resubmit the LSU command. Should the error persist, please reach out to Canton support."
  )
  object Internal
      extends ErrorCode(
        id = "LSU_INTERNAL_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        details: String
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Lsu failed: $details")
        with LsuError
  }

  @Explanation(
    """Retries has been exhausted for an operation, that otherwise can intermittently return errors or fail pre-condition checks, while awaiting the synchronizer state to advance towards the upgrade."""
  )
  @Resolution(
    """Check the error message and resolve possible external causes of the error, i.e. network connectivity problems, resource starvation, misconfigurations, etc. Then retry the LSU. Automatic LSU can be retried by restarting the PN. For a manual LSU resubmit the LSU command."""
  )
  object Transient
      extends ErrorCode(
        id = "LSU_TRANSIENT_ERROR",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    final case class Error(
        details: String
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"LSU transient failure: $details")
        with LsuError
  }

  @Explanation(
    """The specified physical synchronizer id doesn't correspond to an expected one. This can be caused by specifying sequencer connections of a wrong synchronizer or by specifying a wrong physical synchronizer id in the upgrade command."""
  )
  @Resolution(
    """Check and fix the incorrect physical synchronizer id in the upgrade command or the incorrect sequencer connections (i.e. could the sequencer successor connection URL point to the old sequencer?)"""
  )
  object WrongPsid
      extends ErrorCode(
        id = "LSU_WRONG_PSID",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(
        details: String
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Wrong physical synchronizer id: $details")
        with LsuError
  }

  @Explanation(
    """The LSU request contains errors or fails preconditions for a safe upgrade procedure."""
  )
  @Resolution("""Check the error description and adjust the request accordingly""")
  object MalformedRequest
      extends ErrorCode(
        id = "LSU_MALFORMED_REQUEST",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {
    final case class Error(
        details: String
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Invalid LSU request: $details")
        with LsuError
  }

  @Explanation(
    """A failure to connect to the new synchronizer, due to connectivity issues or unsufficient trust configuration."""
  )
  @Resolution(
    """See the error description for the cause of the failure. After that follow the documentation to perform a manual LSU passing in successor sequencers configuration that overrides the failed connection configuration and fixes the cause"""
  )
  object SynchronizerConnection
      extends ErrorCode(
        id = "LSU_SYNCHRONIZER_CONNECTION_ERROR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(
        details: String
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Synchronizer connection failure: $details")
        with LsuError
  }

}
