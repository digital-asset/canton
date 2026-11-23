// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.state_service.{
  GetHostedPartiesRequest,
  HostedPartiesFilter,
  HostedParty,
  StateServiceGrpc,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class GetHostedPartiesAuthIT extends HostedPartiesServiceCallAuthTests {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "StateService#GetHostedParties"

  override protected def hostedParties(
      context: ServiceCallContext,
      filter: Option[HostedPartiesFilter],
  )(implicit env: TestConsoleEnvironment): Future[Seq[HostedParty]] = {
    import env.*
    val service = stub(StateServiceGrpc.stub(channel), context.token)
    new StreamConsumer(
      service.getHostedParties(
        GetHostedPartiesRequest(
          hostedAtOffset = None,
          filter = filter,
          streamContinuationToken = None,
        ),
        _,
      )
    ).all().map(_.map(_.getHostedParty))
  }
}
