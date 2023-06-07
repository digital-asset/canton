// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService as GrpcLedgerIdentityService
import com.daml.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  GetLedgerIdentityResponse,
  LedgerIdentityServiceGrpc,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.error.{CommonErrors, DamlContextualizedErrorLogger}
import io.grpc.{BindableService, ServerServiceDefinition}
import scalaz.syntax.tag.*

import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

private[apiserver] final class ApiLedgerIdentityService private (
    getLedgerId: () => Future[LedgerId]
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends GrpcLedgerIdentityService
    with GrpcApiService {
  private val logger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  val closed: AtomicBoolean = new AtomicBoolean(false)

  @nowarn("cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*")
  override def getLedgerIdentity(
      request: GetLedgerIdentityRequest
  ): Future[GetLedgerIdentityResponse] = {
    logger.info(s"Received request for ledger identity: $request")
    if (closed.get())
      Future.failed(CommonErrors.ServiceNotRunning.Reject("Ledger Identity Service").asGrpcError)
    else
      getLedgerId()
        .map(ledgerId => GetLedgerIdentityResponse(ledgerId.unwrap))
        .andThen(logger.logErrorsOnCall[GetLedgerIdentityResponse])
  }

  override def close(): Unit = closed.set(true)

  override def bindService(): ServerServiceDefinition =
    LedgerIdentityServiceGrpc.bindService(this, executionContext): @nowarn(
      "cat=deprecation&origin=com\\.daml\\.ledger\\.api\\.v1\\.ledger_identity_service\\..*"
    )
}

private[apiserver] object ApiLedgerIdentityService {
  def create(ledgerId: LedgerId)(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ApiLedgerIdentityService with BindableService = {
    new ApiLedgerIdentityService(() => Future.successful(ledgerId))
  }
}
