// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.digitalasset.canton.auth.ClaimApiArea
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.google.protobuf.field_mask.FieldMask

import java.util.UUID
import scala.concurrent.Future

final class UpdatePartyDetailsAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with ApiAreaScopeServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "PartyManagementService#UpdatePartyDetails"

  override def apiAreaScopeBaseToken = adminToken
  override def apiAreaScopeRestrictions = List(
    List(ClaimApiArea.partiesWrite.area) -> true,
    List(ClaimApiArea.partiesRead.area) -> false,
    List(ClaimApiArea.usersWrite.area) -> false,
  )

  private def updatePartyDetailsRequest(partyDetails: PartyDetails) =
    UpdatePartyDetailsRequest(
      partyDetails = Some(partyDetails),
      updateMask = Some(FieldMask(paths = scala.Seq("is_local"))),
    )

  private def updatePartyDetails(token: Option[String], partyDetails: PartyDetails) =
    stub(PartyManagementServiceGrpc.stub(channel), token)
      .updatePartyDetails(updatePartyDetailsRequest(partyDetails))

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    val partyId = UUID.randomUUID().toString
    for {
      // Note how we allocate the party using an admin context, since we are not
      // testing allocation here, we are testing the subsequent call.
      response <- allocatePartyWithDetails(
        serviceCallContext = canBeAnAdmin,
        party = partyId,
        userId = "",
        identityProviderIdOverride = Some(context.identityProviderId),
      )
      _ <- updatePartyDetails(
        context.token,
        response.partyDetails.getOrElse(
          throw new RuntimeException(s"party details for party $partyId were not found")
        ),
      )
    } yield ()
  }

}
