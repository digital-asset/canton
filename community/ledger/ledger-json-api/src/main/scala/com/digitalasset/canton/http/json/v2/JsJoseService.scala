// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.jose_service
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import io.circe.{Codec, Json}
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, FieldName, Schema, SchemaType}

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class JsJoseService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    val authInterceptor: AuthInterceptor,
) extends Endpoints
    with NamedLogging {

  private def joseServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): jose_service.JoseServiceGrpc.JoseServiceStub =
    ledgerClient.serviceClient(jose_service.JoseServiceGrpc.stub, token)

  def endpoints() = List(
    withServerLogic(
      JsJoseService.getJwksEndpoint,
      getJwks,
    )
  )

  private def getJwks(
      caller: CallerContext
  ): TracedInput[jose_service.GetJwksRequest] => Future[
    Either[JsCantonError, JsJoseService.GetJwksResponse]
  ] = { req =>
    implicit val tc: TraceContext = caller.traceContext()
    joseServiceClient(caller.token())
      .getJwks(req.in)
      .flatMap(protocolConverters.GetJwksResponse.toJson)
      .resultToRight
  }
}

object JsJoseService extends DocumentationEndpoints {
  import Endpoints.*
  import JsJoseServiceCodecs.*

  private lazy val jose = v2Endpoint.in(sttp.tapir.stringToPath("jose"))

  final case class GetJwksResponse(keys: List[Json])

  // Override the schema to hint it's an array of JSON objects (rather than
  // an array of JSON anys).
  implicit val getJwksResponseSchema: Schema[GetJwksResponse] = Schema(
    SchemaType.SProduct(
      List(
        SchemaType.SProductField[GetJwksResponse, List[Json]](
          FieldName("keys"),
          Schema[List[Json]](
            SchemaType.SArray(
              Schema[Json](
                SchemaType.SProduct(List.empty)
              ).description("A JSON object representing a JWK")
            )(identity)
          ).description(
            """List of JWKs for the specified party and synchronizer.
              |Only keys of which the usage allows for Party JWT authentication will be returned.
              |
              |Optional: can be empty""".stripMargin
          ),
          response => Some(response.keys),
        )
      )
    )
  ).description("A JWKS as specified by RFC 7517")

  val getJwksEndpoint = jose.get
    .in(sttp.tapir.stringToPath("jwks"))
    .in(
      sttp.tapir.stringToPath("synchronizer") / sttp.tapir
        .path[String]("synchronizer")
        .description("synchronizer from which to read the public keys")
    )
    .in(
      sttp.tapir.stringToPath("party") / sttp.tapir
        .path[String]("party")
        .description("party for which to retrieve the public keys")
    )
    // Tapir makes up the operationId `getV2JoseJwksSynchronizerSynchronizerPartyParty` for the OpenAPI spec,
    // which is a silly name, so we override it with something shorter.
    .name("getV2JoseJwks")
    .mapIn { case (synchronizerId: String, partyId: String) =>
      jose_service.GetJwksRequest(synchronizerId = synchronizerId, partyId = partyId)
    }(req => (req.synchronizerId, req.partyId))
    .out(jsonBody[JsJoseService.GetJwksResponse])
    .description("""Retrieve public keys as JWKS for a specific party and synchronizer.
        |This endpoint is experimental / alpha, therefore no backwards compatibility is guaranteed.
        |""".stripMargin)

  override def documentation: Seq[AnyEndpoint] = List(
    getJwksEndpoint
  )
}

object JsJoseServiceCodecs {
  import JsSchema.config

  implicit val jsGetJwksResponseRW: Codec[JsJoseService.GetJwksResponse] =
    deriveConfiguredCodec
}
