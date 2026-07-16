// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.jose_service.*
import com.daml.ledger.api.v2.jose_service.JoseServiceGrpc.JoseService
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class JoseServiceAuthorization(
    protected val service: JoseService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends JoseService
    with ProxyCloseable
    with GrpcApiService {

  override def getJwks(request: GetJwksRequest): Future[GetJwksResponse] =
    authorizer.rpc(service.getJwks)(RequiredClaim.Public())(request)

  override def bindService(): ServerServiceDefinition =
    JoseServiceGrpc.bindService(this, executionContext)
}
