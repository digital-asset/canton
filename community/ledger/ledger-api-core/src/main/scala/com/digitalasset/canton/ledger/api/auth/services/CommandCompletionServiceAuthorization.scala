// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.daml.ledger.api.v2.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionStreamRequest,
  CompletionStreamResponse,
  GetCompletionByHashRequest,
  GetCompletionByHashResponse,
  GetCompletionsRequest,
}
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.auth.services.CommandCompletionServiceAuthorization.completionStreamClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.platform.apiserver.services.ApiCommandCompletionService
import com.digitalasset.canton.platform.apiserver.services.ApiCommandCompletionService.InternalGetCompletionByHashRequest
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class CommandCompletionServiceAuthorization(
    protected val service: ApiCommandCompletionService,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends CommandCompletionService
    with ProxyCloseable
    with GrpcApiService {

  override def completionStream(
      request: CompletionStreamRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit =
    authorizer.stream(service.completionStream)(completionStreamClaims(request)*)(
      request,
      responseObserver,
    )

  override def bindService(): ServerServiceDefinition =
    CommandCompletionServiceGrpc.bindService(this, executionContext)

  /** Subscribe to command completion events. This streaming endpoint provides more flexibility in
    * filtering than the predecessor ``CompletionStream``.
    */
  override def getCompletions(
      request: GetCompletionsRequest,
      responseObserver: StreamObserver[CompletionStreamResponse],
  ): Unit =
    authorizer.stream(service.getCompletions)(
      CommandCompletionServiceAuthorization.getCompletionsClaims(request)*
    )(
      request,
      responseObserver,
    )

  /** Look up a completion by its transaction hash. This is only available for completions received
    * after upgrading to Canton version 3.5. If:
    *
    *   - there is no completion with this transaction hash,
    *   - or the completion is not visible to the user,
    *   - or the completion was populated before upgrading to Canton version 3.5,
    *   - or respective completions are all pruned,
    *
    * a COMPLETION_NOT_FOUND error will be raised.
    */
  override def getCompletionByHash(
      request: GetCompletionByHashRequest
  ): Future[GetCompletionByHashResponse] =
    authorizer.rpc[InternalGetCompletionByHashRequest, GetCompletionByHashResponse](
      service.getCompletionByHash
    )(
      RequiredClaim.MatchReadAsParties(InternalGetCompletionByHashRequest.partiesLens)
    )(InternalGetCompletionByHashRequest(request.transactionHash, Set.empty))
}

object CommandCompletionServiceAuthorization {
  def completionStreamClaims(
      request: CompletionStreamRequest
  ): List[RequiredClaim[CompletionStreamRequest]] =
    RequiredClaim.MatchUserId(
      requestStringL = Lens.unit[CompletionStreamRequest].userId,
      skipUserIdValidationForAnyPartyReaders = true,
    ) :: request.parties.view.map(RequiredClaim.ReadAs[CompletionStreamRequest]).toList
  def getCompletionsClaims(
      request: GetCompletionsRequest
  ): List[RequiredClaim[GetCompletionsRequest]] =
    RequiredClaims.readAsForAllPartiesOrAnyPartyIfEmpty[GetCompletionsRequest](request.parties)
}
