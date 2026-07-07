// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  TrafficServiceGrpc,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.ServerEndpoint

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class JsTrafficService(
    ledgerClient: LedgerClient,
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit val authInterceptor: AuthInterceptor, val executionContext: ExecutionContext)
    extends Endpoints
    with NamedLogging {

  @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
  def endpoints(): List[ServerEndpoint[Any, Future]] =
    List(
      withServerLogic(
        JsTrafficService.getAccountEndpoint,
        getAccount,
      ),
      withServerLogic(
        JsTrafficService.updateAccountEndpoint,
        updateAccount,
      ),
    )

  private def getAccount(
      callerContext: CallerContext
  ): TracedInput[String] => Future[Either[JsCantonError, GetAccountResponse]] = { req =>
    implicit val tc: TraceContext = callerContext.traceContext()
    trafficServiceClient(callerContext.token())
      .getAccount(GetAccountRequest(accountId = req.in))
      .resultToRight
  }

  private def updateAccount(
      callerContext: CallerContext
  ): TracedInput[UpdateAccountRequest] => Future[Either[JsCantonError, UpdateAccountResponse]] = {
    req =>
      implicit val tc: TraceContext = callerContext.traceContext()
      trafficServiceClient(callerContext.token())
        .updateAccount(req.in)
        .resultToRight
  }

  private def trafficServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): TrafficServiceGrpc.TrafficServiceStub =
    ledgerClient.serviceClient(TrafficServiceGrpc.stub, token)
}

object JsTrafficService extends DocumentationEndpoints {
  import Endpoints.*
  import JsTrafficServiceCodecs.*

  private val accountIdPath = "account-id"
  private val traffic = v2Endpoint.in("traffic")

  private val getAccountEndpoint =
    traffic.get
      .in("accounts")
      .in(path[String](accountIdPath))
      .out(jsonBody[GetAccountResponse])
      // TODO(#33681): Use the regular proto-ref
      .description(
        "Get the traffic account state for the given account ID (same as the party ID in Canton 3.5)."
      )

  private val updateAccountEndpoint =
    traffic.post
      .in("accounts")
      .in(jsonBody[UpdateAccountRequest])
      .out(jsonBody[UpdateAccountResponse])
      // TODO(#33681): Use the regular proto-ref
      .description("Update the traffic account state for the given account ID.")

  // TODO(#33681): Not wired in the static documentation endpoints yet
  //               because the service is not yet stable and it's disabled by default
  override def documentation: Seq[AnyEndpoint] = List(
    getAccountEndpoint,
    updateAccountEndpoint,
  )
}

object JsTrafficServiceCodecs {
  import JsSchema.config

  implicit val getAccountResponseRW: Codec[GetAccountResponse] = deriveRelaxedCodec
  implicit val updateAccountRequestRW: Codec[UpdateAccountRequest] = deriveRelaxedCodec
  implicit val updateAccountResponseRW: Codec[UpdateAccountResponse] = deriveRelaxedCodec
}
