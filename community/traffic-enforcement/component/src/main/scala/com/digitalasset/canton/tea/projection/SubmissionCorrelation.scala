// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.LfHash

final case class CorrelationKey(changeIdHash: LfHash, submissionId: String)

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
