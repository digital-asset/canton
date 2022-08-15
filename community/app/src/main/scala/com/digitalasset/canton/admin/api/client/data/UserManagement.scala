// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data.console

import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.ledger.api.v1.admin.user_management_service.Right.Kind
import com.daml.ledger.api.v1.admin.user_management_service.{
  ListUsersResponse => ProtoListUsersResponse,
  Right => ProtoUserRight,
  User => ProtoLedgerApiUser,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}

case class LedgerApiUser(id: String, primaryParty: Option[LfPartyId])

object LedgerApiUser {
  def fromProtoV0(
      value: ProtoLedgerApiUser
  ): ParsingResult[LedgerApiUser] = {
    val ProtoLedgerApiUser(id, primaryParty) = value
    Option
      .when(primaryParty.nonEmpty)(primaryParty)
      .traverse(LfPartyId.fromString)
      .leftMap { err =>
        ProtoDeserializationError.ValueConversionError("primaryParty", err)
      }
      .map { primaryPartyO =>
        LedgerApiUser(id, primaryPartyO)
      }
  }
}

case class UserRights(actAs: Set[LfPartyId], readAs: Set[LfPartyId], participantAdmin: Boolean)
object UserRights {
  def fromProtoV0(
      values: Seq[ProtoUserRight]
  ): ParsingResult[UserRights] = {
    Right(values.map(_.kind).foldLeft(UserRights(Set(), Set(), false)) {
      case (acc, Kind.Empty) => acc
      case (acc, Kind.ParticipantAdmin(value)) => acc.copy(participantAdmin = true)
      case (acc, Kind.CanActAs(value)) =>
        acc.copy(actAs = acc.actAs + LfPartyId.assertFromString(value.party))
      case (acc, Kind.CanReadAs(value)) =>
        acc.copy(readAs = acc.readAs + LfPartyId.assertFromString(value.party))
    })
  }
}

case class ListLedgerApiUsersResult(users: Seq[LedgerApiUser], nextPageToken: String)

object ListLedgerApiUsersResult {
  def fromProtoV0(
      value: ProtoListUsersResponse,
      filterUser: String,
  ): ParsingResult[ListLedgerApiUsersResult] = {
    val ProtoListUsersResponse(protoUsers, nextPageToken) = value
    protoUsers.traverse(LedgerApiUser.fromProtoV0).map { users =>
      ListLedgerApiUsersResult(users.filter(_.id.startsWith(filterUser)), nextPageToken)
    }
  }
}
