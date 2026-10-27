// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.daml.lf.data.Ref

/** Which traffic account a submission is charged to. */
sealed trait TrafficAccountSelection extends Product with Serializable

object TrafficAccountSelection {

  final case class Explicit(accountId: String) extends TrafficAccountSelection

  final case class FallbackParty(party: LfPartyId) extends TrafficAccountSelection

  final case class AdminFallback(adminParty: LfPartyId) extends TrafficAccountSelection

  /** Several acting parties and no account, so nobody is charged. */
  case object NoAccount extends TrafficAccountSelection

  def select(
      accountId: Option[String255],
      actAs: Set[LfPartyId],
      adminParty: LfPartyId,
  ): TrafficAccountSelection =
    accountId match {
      case Some(account) => Explicit(account.unwrap)
      case None if actAs == Set(adminParty) => AdminFallback(adminParty)
      case None =>
        actAs.toList match {
          case single :: Nil => FallbackParty(single)
          case _ => NoAccount
        }
    }
}

final case class SubmissionAccounting(selection: TrafficAccountSelection)

object SubmissionAccounting {
  val NoAccount: SubmissionAccounting = SubmissionAccounting(TrafficAccountSelection.NoAccount)

  def forSubmitter(
      accountId: Option[String255],
      actAs: Set[LfPartyId],
      adminParty: LfPartyId,
  ): SubmissionAccounting =
    SubmissionAccounting(TrafficAccountSelection.select(accountId, actAs, adminParty))

  def participantAdmin(adminParty: LfPartyId): SubmissionAccounting =
    SubmissionAccounting(TrafficAccountSelection.AdminFallback(adminParty))
}

/** What TEA stores for a submission, so the completion can be charged to the right account. */
final case class SubmissionAccountingInfo(
    changeId: ChangeId,
    submissionId: Ref.SubmissionId,
    accounting: SubmissionAccounting,
)

object SubmissionAccountingInfo {

  /** Every Ledger API path sets a submission id, so a missing one is a caller bug. */
  def create(
      changeId: ChangeId,
      submissionId: Option[Ref.SubmissionId],
      accounting: SubmissionAccounting,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Either[TrafficEnforcementErrors.FatalFailure.Reject, SubmissionAccountingInfo] =
    submissionId
      .map(SubmissionAccountingInfo(changeId, _, accounting))
      .toRight(
        TrafficEnforcementErrors.FatalFailure.Reject(
          new IllegalArgumentException(
            s"Cannot register the submission for command ${changeId.commandId}: it has no submission id"
          )
        )
      )
}
