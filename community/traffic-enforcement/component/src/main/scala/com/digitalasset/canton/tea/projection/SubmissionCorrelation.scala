// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficEnforcementError

final case class CorrelationKey(changeIdHash: LfHash, submissionId: String) {
  def conflictError(implicit loggingContext: ErrorLoggingContext): TrafficEnforcementError =
    TrafficEnforcementErrors.CorrelationConflict.Reject(changeIdHash.toHexString, submissionId)
}

sealed trait CorrelationAccount {
  def toOption: Option[AccountId]
}

object CorrelationAccount {
  final case class Account(accountId: AccountId) extends CorrelationAccount {
    override def toOption: Option[AccountId] = Some(accountId)
  }

  case object NoAccountSelected extends CorrelationAccount {
    override def toOption: Option[AccountId] = None
  }

  def fromOption(accountId: Option[AccountId]): CorrelationAccount =
    accountId.fold[CorrelationAccount](NoAccountSelected)(Account.apply)
}

final case class SubmissionCorrelation(
    key: CorrelationKey,
    account: CorrelationAccount,
    insertedAt: CantonTimestamp,
)
