// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.{experimental_features, version_service}
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.ledger.client.services.version.VersionClient
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.circe.Codec
import sttp.tapir.AnyEndpoint
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody

import scala.concurrent.{ExecutionContext, Future}

class JsVersionService(versionClient: VersionClient, val loggerFactory: NamedLoggerFactory)(implicit
    val executionContext: ExecutionContext,
    val authInterceptor: AuthInterceptor,
) extends Endpoints {

  def endpoints() = List(
    withServerLogic(
      JsVersionService.versionEndpoint,
      getVersion,
    )
  )
  private def getVersion(
      caller: CallerContext
  ): TracedInput[Unit] => Future[
    Either[JsCantonError, version_service.GetLedgerApiVersionResponse]
  ] =
    tracedInput =>
      versionClient
        .serviceStub(caller.token())(tracedInput.traceContext)
        .getLedgerApiVersion(version_service.GetLedgerApiVersionRequest())
        .resultToRight
}

object JsVersionService extends DocumentationEndpoints {
  import Endpoints.*
  import JsVersionServiceCodecs.*

  private val version = v2Endpoint.in(sttp.tapir.stringToPath("version"))

  val versionEndpoint = version.get
    .out(jsonBody[version_service.GetLedgerApiVersionResponse])
    .description("Get the version details of the participant node")

  override def documentation: Seq[AnyEndpoint] = Seq(versionEndpoint)
}

object JsVersionServiceCodecs {
  import JsSchema.config

  implicit val est: Codec[experimental_features.ExperimentalStaticTime] = deriveRelaxedCodec
  implicit val ecis: Codec[experimental_features.ExperimentalCommandInspectionService] =
    deriveRelaxedCodec
  implicit val epte: Codec[experimental_features.ExperimentalPartyTopologyEvents] =
    deriveRelaxedCodec
  implicit val ef: Codec[experimental_features.ExperimentalFeatures] = deriveRelaxedCodec
  implicit val umf: Codec[version_service.UserManagementFeature] = deriveRelaxedCodec
  implicit val pmf: Codec[version_service.PartyManagementFeature] = deriveRelaxedCodec
  implicit val ocf: Codec[version_service.OffsetCheckpointFeature] = deriveRelaxedCodec
  implicit val fd: Codec[version_service.FeaturesDescriptor] = deriveRelaxedCodec
  implicit val glavr: Codec[version_service.GetLedgerApiVersionResponse] = deriveRelaxedCodec
}
