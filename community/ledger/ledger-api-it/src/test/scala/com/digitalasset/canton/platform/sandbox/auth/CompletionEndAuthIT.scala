// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox.auth

import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest,
}

import scala.concurrent.Future

final class CompletionEndAuthIT extends PublicServiceCallAuthTests {

  override def serviceCallName: String = "CommandCompletionService#CompletionEnd"

  private lazy val request = new CompletionEndRequest(unwrappedLedgerId)

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(CommandCompletionServiceGrpc.stub(channel), context.token).completionEnd(request)

}