// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth

import com.daml.ledger.api.v2.transaction_filter.{EventFormat, TransactionFormat, UpdateFormat}
import com.digitalasset.canton.auth.{ClaimApiArea, RequiredClaim}
import scalapb.lenses.Lens

object RequiredClaims {

  def apply[Req](claims: RequiredClaim[Req]*): List[RequiredClaim[Req]] = claims.toList

  def submissionClaims[Req](
      actAs: Set[String],
      readAs: Set[String],
      userIdL: Lens[Req, String],
  ): List[RequiredClaim[Req]] =
    RequiredClaim.ApiArea[Req](ClaimApiArea.ledgerAct.area)
      :: RequiredClaim.MatchUserId(userIdL)
      :: actAs.view.map(RequiredClaim.ActAs[Req]).toList
      ::: readAs.view.map(RequiredClaim.ReadAs[Req]).toList

  /** Does not include any ApiArea claims, since those differ in between call sites. */
  def executionClaims[Req](
      executeAs: Set[String],
      readAs: Set[String],
      userIdL: Lens[Req, String],
  ): List[RequiredClaim[Req]] =
    RequiredClaim.MatchUserId(userIdL)
      :: executeAs.view.map(RequiredClaim.ExecuteAs[Req]).toList
      ::: readAs.view.map(RequiredClaim.ReadAs[Req]).toList

  def readAsForAllParties[Req](parties: Iterable[String]): List[RequiredClaim[Req]] =
    parties.view.map(RequiredClaim.ReadAs[Req]).toList

  def readAsForAllPartiesOrAnyPartyIfEmpty[Req](
      parties: Iterable[String]
  ): List[RequiredClaim[Req]] =
    if (parties.isEmpty) List(RequiredClaim.ReadAsAnyParty[Req]())
    else readAsForAllParties[Req](parties)

  def transactionFormatClaims[Req](transactionFormat: TransactionFormat): List[RequiredClaim[Req]] =
    transactionFormat.eventFormat.toList.flatMap(RequiredClaims.eventFormatClaims[Req])

  def eventFormatClaims[Req](eventFormat: EventFormat): List[RequiredClaim[Req]] =
    readAsForAllParties[Req](eventFormat.filtersByParty.keys)
      ::: eventFormat.filtersForAnyParty.map(_ => RequiredClaim.ReadAsAnyParty[Req]()).toList

  def updateFormatClaims[Req](updateFormat: UpdateFormat): List[RequiredClaim[Req]] =
    List(
      updateFormat.includeTransactions.toList
        .flatMap(transactionFormatClaims[Req]),
      updateFormat.includeReassignments.toList
        .flatMap(eventFormatClaims[Req]),
      updateFormat.includeTransactionsAndReassignments.toList
        .flatMap(transactionFormatClaims[Req]),
    ).flatten.distinct match {
      case Nil => List()
      case claims => claims
    }

  def usersWriteAndMatchingRequestIdpId[Req](
      identityProviderIdL: Lens[Req, String],
      mustBeParticipantAdmin: Boolean = false,
  ): List[RequiredClaim[Req]] = RequiredClaims(
    if (mustBeParticipantAdmin) RequiredClaim.Admin[Req]()
    else RequiredClaim.ApiArea[Req](ClaimApiArea.usersWrite.area),
    RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
  )

  def usersReadAndMatchingRequestIdpId[Req](
      identityProviderIdL: Lens[Req, String]
  ): List[RequiredClaim[Req]] = RequiredClaims(
    RequiredClaim.ApiArea[Req](ClaimApiArea.usersRead.area),
    RequiredClaim.MatchIdentityProviderId(identityProviderIdL),
  )
}
