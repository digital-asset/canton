// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.state_service.{GetActiveContractsPageRequest, StateServiceGrpc}
import com.digitalasset.canton.auth.ClaimApiArea
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class GetActiveContractsPageAuthIT
    extends SuperReaderServiceCallAuthTests
    with ApiAreaScopeServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "StateService#GetActiveContractsPage"

  override def apiAreaScopeBaseToken = standardToken(mainActorReadUser)
  override def apiAreaScopeRestrictions = List(
    List(ClaimApiArea.ledgerRead.area) -> true,
    List(ClaimApiArea.partiesAllocate.area) -> false,
  )

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(StateServiceGrpc.stub(channel), context.token)
      .getActiveContractsPage(
        GetActiveContractsPageRequest(
          activeAtOffset = None,
          eventFormat = context.eventFormat.orElse(Some(eventFormat(getMainActorId))),
          maxPageSize = None,
          pageToken = None,
          includeIncompleteReassignments = false,
        )
      )

}
