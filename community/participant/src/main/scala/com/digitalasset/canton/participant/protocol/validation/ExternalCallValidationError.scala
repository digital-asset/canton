// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.base.error.{Alarm, AlarmErrorCode, Explanation, Resolution}
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup

object ExternalCallValidationError extends LocalRejectionGroup {

  @Explanation(
    """The participant observed external call results that record different outputs for the same
      |external call.
      |"""
  )
  @Resolution(
    "Inspect the submitting participant and the external-call service deployments/configuration for inconsistent or non-deterministic results for the same external-call identity."
  )
  object ExternalCallResultDisagreementAlarm
      extends AlarmErrorCode("EXTERNAL_CALL_RESULT_DISAGREEMENT_ALARM") {
    final case class Warn(override val cause: String) extends Alarm(cause)
  }
}
