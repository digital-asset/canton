// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error.groups

import com.digitalasset.base.error.{
  DamlErrorWithDefiniteAnswer,
  ErrorCategory,
  ErrorCode,
  Explanation,
  Resolution,
}
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup.AdminServicesErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext

@Explanation("Errors raised by Ledger API admin services.")
object AdminServiceErrors extends AdminServicesErrorGroup {

  val UserManagement: UserManagementServiceErrors.type =
    UserManagementServiceErrors
  val IdentityProviderConfig: IdentityProviderConfigServiceErrors.type =
    IdentityProviderConfigServiceErrors
  val PartyManagement: PartyManagementServiceErrors.type =
    PartyManagementServiceErrors

  @Explanation("This rejection is given when a new configuration is rejected.")
  @Resolution("Fetch newest configuration and/or retry.")
  object ConfigurationEntryRejected
      extends ErrorCode(
        id = "CONFIGURATION_ENTRY_REJECTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {

    final case class Reject(_message: String)(implicit
        loggingContext: ErrorLoggingContext
    ) extends DamlErrorWithDefiniteAnswer(
          cause = _message
        )

  }

}
