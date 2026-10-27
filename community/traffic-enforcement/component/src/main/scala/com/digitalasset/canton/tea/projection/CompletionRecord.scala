// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.resource.DbStorage.Implicits.lfHashGetResult
import slick.jdbc.{GetResult, SetParameter}

/** The fields of a Ledger API completion that TEA needs to charge it. */
final case class CompletionRecord(
    changeId: ChangeId,
    submissionId: String,
    paidTrafficCost: NonNegativeLong,
    accepted: Boolean,
) {
  def correlationKey: CorrelationKey = CorrelationKey(changeId.hash, submissionId)

  def actAsCategory(adminParty: LfPartyId): ActAsCategory =
    if (changeId.actAs.sizeIs > 1) ActAsCategory.MultipleActingParties
    else if (changeId.actAs == Set(adminParty)) ActAsCategory.AdminActingParty
    else ActAsCategory.SingleNonAdminActingParty
}

/** Why TEA recorded a completion with a positive cost without charging it. */
sealed abstract class UnchargedReason(val code: Short, val label: String)
    extends Product
    with Serializable

object UnchargedReason {
  case object MissingCorrelation extends UnchargedReason(0, "missing_correlation")
  case object NoAccountSelected extends UnchargedReason(1, "no_account_selected")
  case object RejectedCompletionChargingDisabled
      extends UnchargedReason(2, "rejected_completion_charging_disabled")

  val values: Set[UnchargedReason] =
    Set(MissingCorrelation, NoAccountSelected, RejectedCompletionChargingDisabled)

  private def fromCode(code: Short): UnchargedReason =
    values
      .find(_.code == code)
      .getOrElse(
        throw new IllegalArgumentException(s"Unknown UnchargedReason code from DB: $code")
      )

  implicit val setUnchargedReason: SetParameter[UnchargedReason] =
    SetParameter((reason, pp) => pp.setShort(reason.code))

  implicit val getUnchargedReason: GetResult[UnchargedReason] =
    GetResult(r => fromCode(r.nextShort()))
}

/** Labels uncharged completions and metrics. Doesn't affect how a completion is charged. */
sealed abstract class ActAsCategory(val code: Short, val label: String)
    extends Product
    with Serializable

object ActAsCategory {
  case object MultipleActingParties extends ActAsCategory(0, "multiple_acting_parties")
  case object AdminActingParty extends ActAsCategory(1, "admin_acting_party")
  case object SingleNonAdminActingParty extends ActAsCategory(2, "single_non_admin_acting_party")

  val values: Set[ActAsCategory] =
    Set(MultipleActingParties, AdminActingParty, SingleNonAdminActingParty)

  private def fromCode(code: Short): ActAsCategory =
    values
      .find(_.code == code)
      .getOrElse(
        throw new IllegalArgumentException(s"Unknown ActAsCategory code from DB: $code")
      )

  implicit val setActAsCategory: SetParameter[ActAsCategory] =
    SetParameter((category, pp) => pp.setShort(category.code))

  implicit val getActAsCategory: GetResult[ActAsCategory] =
    GetResult(r => fromCode(r.nextShort()))
}

/** A completion with a positive cost that TEA didn't charge to any account. */
final case class UnchargedCompletion(
    offset: Long,
    changeIdHash: LfHash,
    submissionId: String,
    paidTrafficCost: NonNegativeLong,
    reason: UnchargedReason,
    actAsCategory: ActAsCategory,
    timestamp: CantonTimestamp,
)

object UnchargedCompletion {
  implicit val getUnchargedCompletion: GetResult[UnchargedCompletion] =
    GetResult(r => UnchargedCompletion(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))
}
