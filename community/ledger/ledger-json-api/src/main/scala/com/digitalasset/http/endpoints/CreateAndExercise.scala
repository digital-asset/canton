// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.http.scaladsl.model.*
import com.daml.lf.value.{Value as LfValue}
import EndpointsCompanion.*
import Endpoints.ET
import domain.{ContractTypeId, JwtPayloadTag, JwtWritePayload}
import json.*
import util.FutureUtil.{either, eitherT}
import util.Logging.{InstanceUUID, RequestID}
import util.toLedgerId
import util.JwtParties.*
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.{v1 as lav1}
import lav1.value.{Value as ApiValue, Record as ApiRecord}
import scalaz.std.scalaFuture.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json.*

import scala.concurrent.ExecutionContext
import com.daml.http.metrics.HttpApiMetrics
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Timed

private[http] final class CreateAndExercise(
    routeSetup: RouteSetup,
    decoder: DomainJsonDecoder,
    commandService: CommandService,
    contractsService: ContractsService,
)(implicit ec: ExecutionContext) {
  import CreateAndExercise.*
  import routeSetup.*, RouteSetup.*
  import json.JsonProtocol.*
  import util.ErrorOps.*

  def create(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: HttpApiMetrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.CreateCommand[ApiRecord, ContractTypeId.Template.RequiredPkg]
          ]
        _ <- EitherT.pure(parseAndDecodeTimer.stop())

        response <- eitherT(
          Timed.future(
            metrics.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(commandService.create(jwt, jwtPayload, cmd)),
          )
        ): ET[domain.CreateCommandResponse[ApiValue]]
      } yield response
    }

  def exercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: HttpApiMetrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeExerciseCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.ExerciseCommand.RequiredPkg[LfValue, domain.ContractLocator[LfValue]]
          ]
        _ <- EitherT.pure(parseAndDecodeTimer.stop())
        resolvedRef <- resolveReference(jwt, jwtPayload, cmd.meta, cmd.reference)

        apiArg <- either(lfValueToApiValue(cmd.argument)): ET[ApiValue]

        apiMeta <- either {
          import scalaz.std.option.*
          cmd.meta traverse (_ traverse lfValueToApiValue)
        }

        resolvedCmd = cmd.copy(argument = apiArg, reference = resolvedRef, meta = apiMeta)

        resp <- eitherT(
          Timed.future(
            metrics.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.exercise(jwt, jwtPayload, resolvedCmd)
            ),
          )
        ): ET[domain.ExerciseResponse[ApiValue]]

      } yield resp
    }

  def createAndExercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateAndExerciseCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.CreateAndExerciseCommand.LAVResolved
          ]
        _ <- EitherT.pure(parseAndDecodeTimer.stop())

        resp <- eitherT(
          Timed.future(
            metrics.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.createAndExercise(jwt, jwtPayload, cmd)
            ),
          )
        ): ET[domain.ExerciseResponse[ApiValue]]
      } yield resp
    }

  private def resolveReference(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      meta: Option[domain.CommandMeta.IgnoreDisclosed],
      reference: domain.ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[domain.ResolvedContractRef[ApiValue]] =
    contractsService
      .resolveContractReference(
        jwt,
        resolveRefParties(meta, jwtPayload),
        reference,
        toLedgerId(jwtPayload.ledgerId),
      )
      .flatMap {
        case -\/((tpId, key)) => EitherT.either(lfValueToApiValue(key).map(k => -\/(tpId -> k)))
        case a @ \/-((_, _)) => EitherT.pure(a)
      }
}

object CreateAndExercise {
  import util.ErrorOps.*

  private def lfValueToApiValue(a: LfValue): Error \/ ApiValue =
    JsValueToApiValueConverter.lfValueToApiValue(a).liftErr(ServerError.fromMsg)
}
