// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import cats.syntax.unorderedTraverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.auth.RequiredClaim
import com.digitalasset.canton.platform.apiserver.services.command.TrafficAccountSelection

/** Claims a submission needs to debit the traffic account it is charged to.
  *
  * @param adminParty
  *   by-name, because the traffic backend may not be initialized when the Ledger API services are
  *   built.
  */
final class TrafficAccountAuthorization(
    adminParty: => LfPartyId,
    implicitDebitFromExecuteRights: Boolean,
) {

  /** @param accountId the raw `account_id` from the request, empty if not set */
  def requiredClaims[Req](accountId: String, actAs: Set[String]): List[RequiredClaim[Req]] =
    if (accountId.nonEmpty)
      List(
        RequiredClaim.DebitTrafficFrom[Req](
          accountId,
          acceptExecuteRights = implicitDebitFromExecuteRights,
        )
      )
    else if (implicitDebitFromExecuteRights) Nil
    else
      // actAs isn't validated yet. If a party id is invalid, we skip the claim,
      // because validation will reject the request anyway.
      actAs
        .unorderedTraverse(LfPartyId.fromString(_).toOption)
        .map(TrafficAccountSelection.select(None, _, adminParty))
        .collect { case TrafficAccountSelection.FallbackParty(party) =>
          RequiredClaim.DebitTrafficFrom[Req](party, acceptExecuteRights = false)
        }
        .toList
}

object TrafficAccountAuthorization {
  def requiredClaims[Req](
      authorization: Option[TrafficAccountAuthorization],
      accountId: String,
      actAs: Set[String],
  ): List[RequiredClaim[Req]] =
    authorization.toList.flatMap(_.requiredClaims[Req](accountId, actAs))
}
