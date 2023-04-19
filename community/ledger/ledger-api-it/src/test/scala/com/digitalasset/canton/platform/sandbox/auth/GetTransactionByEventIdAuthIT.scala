// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.sandbox.auth

import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionByEventIdRequest,
  TransactionServiceGrpc,
}
import io.grpc.Status
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.Future

final class GetTransactionByEventIdAuthIT extends ReadOnlyServiceCallAuthTests {

  override def serviceCallName: String = "TransactionService#GetTransactionByEventId"

  override def successfulBehavior: Future[Any] => Future[Assertion] =
    expectFailure(_: Future[Any], Status.Code.INVALID_ARGUMENT)

  private lazy val request =
    new GetTransactionByEventIdRequest(unwrappedLedgerId, UUID.randomUUID.toString, List(mainActor))

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    stub(TransactionServiceGrpc.stub(channel), context.token).getTransactionByEventId(request)

}