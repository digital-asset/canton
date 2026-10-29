// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.command_service.*
import com.daml.ledger.api.v2.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v2.commands.Commands
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.services.CommandServiceAuthorization.{
  getSubmitAndWaitForReassignmentClaims,
  getSubmitAndWaitForTransactionClaims,
}
import com.digitalasset.canton.ledger.api.auth.{RequiredClaims, TrafficAccountAuthorization}
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.CommandsValidator
import io.grpc.ServerServiceDefinition
import scalapb.lenses.Lens

import scala.concurrent.{ExecutionContext, Future}

/** Note: the command service internally uses calls to the CommandSubmissionService and
  * CommandCompletionService. These calls already require authentication, but it is better to check
  * authorization here as well.
  */
final class CommandServiceAuthorization(
    protected val service: CommandService with AutoCloseable,
    private val authorizer: Authorizer,
    trafficAccountAuthorization: Option[TrafficAccountAuthorization],
)(implicit executionContext: ExecutionContext)
    extends CommandService
    with ProxyCloseable
    with GrpcApiService {

  override def submitAndWaitForTransaction(
      request: SubmitAndWaitForTransactionRequest
  ): Future[SubmitAndWaitForTransactionResponse] =
    authorizer.rpc(service.submitAndWaitForTransaction)(
      (getSubmitAndWaitForTransactionClaims(request) :::
        debitClaims[SubmitAndWaitForTransactionRequest](request.commands))*
    )(request)

  override def submitAndWaitForReassignment(
      request: SubmitAndWaitForReassignmentRequest
  ): Future[SubmitAndWaitForReassignmentResponse] =
    authorizer.rpc(service.submitAndWaitForReassignment)(
      (getSubmitAndWaitForReassignmentClaims(request) :::
        TrafficAccountAuthorization.requiredClaims[SubmitAndWaitForReassignmentRequest](
          trafficAccountAuthorization,
          accountId = request.reassignmentCommands.fold("")(_.accountId),
          actAs = request.reassignmentCommands.fold(Set.empty[String])(c => Set(c.submitter)),
        ))*
    )(request)

  override def submitAndWait(
      request: SubmitAndWaitRequest
  ): Future[SubmitAndWaitResponse] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    authorizer.rpc(service.submitAndWait)(
      (RequiredClaims.submissionClaims(
        actAs = effectiveSubmitters.actAs,
        readAs = effectiveSubmitters.readAs,
        userIdL = Lens.unit[SubmitAndWaitRequest].commands.userId,
      ) ::: debitClaims[SubmitAndWaitRequest](request.commands))*
    )(request)
  }

  private def debitClaims[Req](commands: Option[Commands]): List[RequiredClaim[Req]] =
    TrafficAccountAuthorization.requiredClaims[Req](
      trafficAccountAuthorization,
      accountId = commands.fold("")(_.accountId),
      actAs = CommandsValidator.effectiveSubmitters(commands).actAs,
    )

  override def bindService(): ServerServiceDefinition =
    CommandServiceGrpc.bindService(this, executionContext)
}

object CommandServiceAuthorization {
  def getSubmitAndWaitForTransactionClaims(
      request: SubmitAndWaitForTransactionRequest
  ): List[RequiredClaim[SubmitAndWaitForTransactionRequest]] = {
    val effectiveSubmitters = CommandsValidator.effectiveSubmitters(request.commands)
    (RequiredClaims.submissionClaims(
      actAs = effectiveSubmitters.actAs,
      readAs = effectiveSubmitters.readAs,
      userIdL = userIdForTransactionL,
    ) ::: request.transactionFormat.toList
      .flatMap(
        RequiredClaims.transactionFormatClaims[SubmitAndWaitForTransactionRequest]
      )).distinct
  }

  def getSubmitAndWaitForReassignmentClaims(
      request: SubmitAndWaitForReassignmentRequest
  ): List[RequiredClaim[SubmitAndWaitForReassignmentRequest]] =
    (RequiredClaims.submissionClaims(
      actAs = request.reassignmentCommands.fold(Set.empty[String])(c => Set(c.submitter)),
      readAs = Set.empty,
      userIdL = userIdForReassignmentL,
    ) ::: request.eventFormat.toList
      .flatMap(
        RequiredClaims.eventFormatClaims[SubmitAndWaitForReassignmentRequest]
      )).distinct

  val userIdL: Lens[SubmitAndWaitRequest, String] =
    Lens.unit[SubmitAndWaitRequest].commands.userId

  val userIdForTransactionL: Lens[SubmitAndWaitForTransactionRequest, String] =
    Lens.unit[SubmitAndWaitForTransactionRequest].commands.userId

  val userIdForReassignmentL: Lens[SubmitAndWaitForReassignmentRequest, String] =
    Lens.unit[SubmitAndWaitForReassignmentRequest].reassignmentCommands.userId

}
