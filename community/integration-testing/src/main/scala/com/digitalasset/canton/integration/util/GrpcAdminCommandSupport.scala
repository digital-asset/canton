// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import cats.syntax.either.*
import com.daml.ledger.api.v2 as proto
import com.daml.ledger.api.v2.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionServiceStub
import com.daml.ledger.api.v2.state_service.StateServiceGrpc.StateServiceStub
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import com.digitalasset.canton.console.{
  ConsoleCommandResult,
  ConsoleEnvironment,
  LocalParticipantReference,
}
import io.grpc.stub.{AbstractStub, StreamObserver}
import io.grpc.{Channel, ManagedChannel}

import scala.concurrent.Future

object GrpcAdminCommandSupport {

  implicit class StubAdminCommandOps[SVC <: AbstractStub[SVC]](stub: Channel => SVC) {
    def syncCommand[Req, Resp](
        syncMethod: SVC => Req => Future[Resp]
    ): Req => GrpcAdminCommand[Req, Resp, Resp] = { request =>
      new GrpcAdminCommand[Req, Resp, Resp] {
        override type Svc = SVC
        override def createService(channel: ManagedChannel): SVC = stub(channel)
        override protected def submitRequest(service: SVC, request: Req): Future[Resp] =
          syncMethod(service)(request)
        override protected def createRequest(): Either[String, Req] = Right(request)
        override protected def handleResponse(response: Resp): Either[String, Resp] = Right(
          response
        )
      }
    }

    def streamingCommand[Req, Resp](
        streamMethod: SVC => (Req, StreamObserver[Resp]) => Unit,
        filter: Resp => Boolean = (_: Resp) => true,
    ): Req => ConsoleEnvironment => Int => GrpcAdminCommand[Req, Seq[Resp], Seq[Resp]] = {
      request => consoleEnvironment => expected =>
        new GrpcAdminCommand[Req, Seq[Resp], Seq[Resp]] {
          override type Svc = SVC
          override def createService(channel: ManagedChannel): SVC = stub(channel)
          override protected def submitRequest(service: SVC, request: Req): Future[Seq[Resp]] =
            GrpcAdminCommand.streamedResponse[Req, Resp, Resp](
              service = streamMethod(service),
              extract = Seq(_).filter(filter),
              request = request,
              expected = expected,
              timeout = consoleEnvironment.commandTimeouts.ledgerCommand.asFiniteApproximation,
              scheduler = consoleEnvironment.environment.scheduler,
            )
          override protected def createRequest(): Either[String, Req] = Right(request)
          override protected def handleResponse(response: Seq[Resp]): Either[String, Seq[Resp]] =
            Right(
              response
            )
        }
    }
  }

  implicit class ParticipantReferenceOps(
      participant: LocalParticipantReference
  ) {
    def runLapiAdminCommand[Req, Resp, Result](
        command: GrpcAdminCommand[Req, Resp, Result]
    ): ConsoleCommandResult[Result] =
      participant.consoleEnvironment.grpcLedgerCommandRunner.runCommand(
        participant.name,
        command,
        participant.config.clientLedgerApi,
        participant.adminToken,
      )

    def runStreamingLapiAdminCommand[Req, Resp, Result](
        command: ConsoleEnvironment => Int => GrpcAdminCommand[Req, Resp, Result],
        expected: Int = Int.MaxValue,
    ): ConsoleCommandResult[Result] =
      participant.consoleEnvironment.grpcLedgerCommandRunner.runCommand(
        participant.name,
        command(participant.consoleEnvironment)(expected),
        participant.config.clientLedgerApi,
        participant.adminToken,
      )
  }

  implicit class AdminCommandOps[T](val consoleCommandResult: ConsoleCommandResult[T])
      extends AnyVal {
    def tryResult: T =
      consoleCommandResult.toEither.valueOr(error => throw new RuntimeException(error))
  }
}

private[integration] object GrpcServices {
  import GrpcAdminCommandSupport.*

  object ReassignmentsService {
    val stub: Channel => CommandSubmissionServiceStub =
      proto.command_submission_service.CommandSubmissionServiceGrpc.stub
    val submit = stub.syncCommand(_.submitReassignment)
  }

  object StateService {
    val stub: Channel => StateServiceStub = proto.state_service.StateServiceGrpc.stub
    val getActiveContracts = stub.streamingCommand(_.getActiveContracts)
    val getConnectedSynchronizers = stub.syncCommand(_.getConnectedSynchronizers)
  }
}
