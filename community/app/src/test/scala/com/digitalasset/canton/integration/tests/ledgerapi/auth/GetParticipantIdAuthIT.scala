// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.digitalasset.canton.auth.ClaimApiArea
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class GetParticipantIdAuthIT
    extends PublicServiceCallAuthTests
    with ApiAreaScopeServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "PartyManagementService#GetParticipantId"

  override def apiAreaScopeBaseToken = standardToken(randomPartyReadUser)
  override def apiAreaScopeRestrictions = List(
    List(ClaimApiArea.baseRead.area) -> true,
    List(ClaimApiArea.ledgerRead.area) -> true,
    List(ClaimApiArea.partiesAllocate.area) -> false,
  )

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(PartyManagementServiceGrpc.stub(channel), context.token)
      .getParticipantId(GetParticipantIdRequest())

}
