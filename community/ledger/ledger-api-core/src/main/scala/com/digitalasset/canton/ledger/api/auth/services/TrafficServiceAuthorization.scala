// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  TrafficServiceGrpc,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

// TODO(#33681): Implement authorization tests once the traffic enforcement server is implemented
//               and the Ledger API endpoints can actually serve
final class TrafficServiceAuthorization(
    protected val service: TrafficService & AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends TrafficService
    with ProxyCloseable
    with GrpcApiService {

  override def getAccount(request: GetAccountRequest): Future[GetAccountResponse] =
    authorizer.rpc(service.getAccount)(
      // TODO(#33681): In Canton 3.5, the account-id matches the act-as party.
      RequiredClaim.ExecuteAs(request.accountId)
    )(request)

  override def updateAccount(request: UpdateAccountRequest): Future[UpdateAccountResponse] =
    authorizer.rpc(service.updateAccount)(RequiredClaim.Admin())(request)

  override def bindService(): ServerServiceDefinition =
    TrafficServiceGrpc.bindService(this, executionContext)
}
