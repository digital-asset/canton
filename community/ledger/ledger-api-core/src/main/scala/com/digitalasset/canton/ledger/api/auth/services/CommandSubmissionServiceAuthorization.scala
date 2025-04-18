// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.command_submission_service.*
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.digitalasset.canton.auth.Authorizer
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.RequiredClaims
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

final class CommandSubmissionServiceAuthorization(
    protected val service: CommandSubmissionService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends CommandSubmissionService
    with ProxyCloseable
    with GrpcApiService {

  override def submit(request: SubmitRequest): Future[SubmitResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.rpc(service.submit)(
      RequiredClaims.submissionClaims(
        actAs = effectiveSubmitters.actAs,
        readAs = effectiveSubmitters.readAs,
        userIdL = Lens.unit[SubmitRequest].commands.userId,
      )*
    )(request)
  }

  override def submitReassignment(
      request: SubmitReassignmentRequest
  ): Future[SubmitReassignmentResponse] =
    authorizer.rpc(service.submitReassignment)(
      RequiredClaims.submissionClaims(
        actAs = request.reassignmentCommands.map(_.submitter).toList.toSet,
        readAs = Set.empty,
        userIdL = Lens.unit[SubmitReassignmentRequest].reassignmentCommands.userId,
      )*
    )(request)

  override def bindService(): ServerServiceDefinition =
    CommandSubmissionServiceGrpc.bindService(this, executionContext)

}
