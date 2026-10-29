// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.LfPartyId

final case class CompletionAccountingPolicy(
    debitRejectedCompletions: Boolean,
    adminParty: LfPartyId,
)

sealed trait CompletionDecision extends Product with Serializable

object CompletionDecision {
  final case class Debit(delta: AccountDelta) extends CompletionDecision
  final case class RecordUncharged(completion: UnchargedCompletion) extends CompletionDecision
  case object NoEvent extends CompletionDecision
}

object CompletionAccounting {

  /** Decides how to account for a completion using its stored correlation. */
  def decide(
      event: ProjectionEvent.Completion,
      correlation: Option[SubmissionCorrelation],
      policy: CompletionAccountingPolicy,
  ): CompletionDecision = {
    val record = event.record
    def uncharged(reason: UnchargedReason) = CompletionDecision.RecordUncharged(
      UnchargedCompletion(
        offset = event.offset,
        changeIdHash = record.correlationKey.changeIdHash,
        submissionId = record.submissionId,
        paidTrafficCost = record.paidTrafficCost,
        reason = reason,
        actAsCategory = record.actAsCategory(policy.adminParty),
        timestamp = event.timestamp,
      )
    )

    if (record.paidTrafficCost.value == 0L) CompletionDecision.NoEvent
    else
      correlation.map(_.account) match {
        case None => uncharged(UnchargedReason.MissingCorrelation)
        case Some(CorrelationAccount.NoAccountSelected) =>
          uncharged(UnchargedReason.NoAccountSelected)
        case Some(CorrelationAccount.Account(accountId))
            if record.accepted || policy.debitRejectedCompletions =>
          CompletionDecision.Debit(
            AccountDelta(accountId, TrafficDelta.debitBalanceDelta(record.paidTrafficCost.value))
          )
        case Some(CorrelationAccount.Account(_)) =>
          uncharged(UnchargedReason.RejectedCompletionChargingDisabled)
      }
  }
}
