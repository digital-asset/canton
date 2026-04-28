// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.grpc.test.StreamConsumer
import com.daml.ledger.api.v2.transaction_filter.UpdateFormat
import com.daml.ledger.api.v2.update_service.{
  GetUpdatesRequest,
  GetUpdatesResponse,
  UpdateServiceGrpc,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.services.SubmitAndWaitDummyCommand
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

final class GetUpdatesAuthIT
    extends ExpiringStreamServiceCallAuthTests[GetUpdatesResponse]
    with ReadOnlyServiceCallAuthTests
    with SubmitAndWaitDummyCommand
    with UpdateFormatAuthTestCases {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "UpdateService#GetUpdates"

  private def request(updateFormat: Option[UpdateFormat]) =
    new GetUpdatesRequest(
      beginExclusive = participantBegin,
      endInclusive = None,
      updateFormat = updateFormat,
      descendingOrder = false,
    )

  override protected def stream(
      context: ServiceCallContext,
      env: TestConsoleEnvironment,
  ): StreamObserver[GetUpdatesResponse] => Unit =
    observer =>
      stub(UpdateServiceGrpc.stub(channel), context.token)
        .getUpdates(request(context.updateFormat), observer)

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = {
    import env.*
    val mainActorId = getMainActorId
    submitAndWaitAsMainActor(mainActorId).flatMap { _ =>
      val contextWithUpdateFormat =
        if (context.updateFormat.isDefined) context
        else context.copy(updateFormat = updateFormat(Some(mainActorId)))
      new StreamConsumer[GetUpdatesResponse](
        stream(contextWithUpdateFormat, env)
      ).first()
    }
  }
}
