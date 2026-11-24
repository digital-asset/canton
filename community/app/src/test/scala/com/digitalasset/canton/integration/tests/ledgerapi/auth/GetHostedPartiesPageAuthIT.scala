// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.state_service.{
  GetHostedPartiesPageRequest,
  HostedPartiesFilter,
  HostedParty,
  StateServiceGrpc,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class GetHostedPartiesPageAuthIT extends HostedPartiesServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "StateService#GetHostedPartiesPage"

  override protected def hostedParties(
      context: ServiceCallContext,
      filter: Option[HostedPartiesFilter],
  )(implicit env: TestConsoleEnvironment): Future[Seq[HostedParty]] = {
    import env.*
    stub(StateServiceGrpc.stub(channel), context.token)
      .getHostedPartiesPage(
        GetHostedPartiesPageRequest(
          hostedAtOffset = None,
          filter = filter,
          pageToken = None,
          maxPageSize = None,
        )
      )
      .map(_.hostedParties)
  }
}
