// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.admin.package_management_service
import com.daml.ledger.api.v2.package_service
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  stringDecoderForEnum,
  stringEncoderForEnum,
}
import com.digitalasset.canton.ledger.client.services.admin.PackageManagementClient
import com.digitalasset.canton.ledger.client.services.pkg.PackageClient
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf
import io.circe.{Codec, Decoder, Encoder}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}
import org.apache.pekko.util
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.{AnyEndpoint, CodecFormat, Schema, path, streamBinaryBody}

import scala.annotation.unused
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.IteratorHasAsScala

import JsPackageCodecs.*

class JsPackageService(
    packageClient: PackageClient,
    packageManagementClient: PackageManagementClient,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    materializer: Materializer,
    val authInterceptor: AuthInterceptor,
) extends Endpoints {
  import JsPackageService.*

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  def endpoints() =
    List(
      withServerLogic(
        JsPackageService.listPackagesEndpoint,
        list,
      ),
      withServerLogic(
        JsPackageService.downloadPackageEndpoint,
        getPackage,
      ),
      withServerLogic(
        uploadDar,
        upload,
      ),
      withServerLogic(
        JsPackageService.packageStatusEndpoint,
        status,
      ),
    )
  private def list(
      caller: CallerContext
  ): TracedInput[Unit] => Future[Either[JsCantonError, package_service.ListPackagesResponse]] = {
    req =>
      packageClient.listPackages(caller.token())(req.traceContext).resultToRight
  }

  private def status(
      @unused caller: CallerContext
  ): TracedInput[String] => Future[
    Either[JsCantonError, package_service.GetPackageStatusResponse]
  ] = req => packageClient.getPackageStatus(req.in, caller.token())(req.traceContext).resultToRight

  private def upload(caller: CallerContext) = {
    (tracedInput: TracedInput[Source[util.ByteString, Any]]) =>
      implicit val traceContext: TraceContext = tracedInput.traceContext
      val inputStream = tracedInput.in.runWith(StreamConverters.asInputStream())(materializer)
      val bs = protobuf.ByteString.readFrom(inputStream)
      packageManagementClient
        .uploadDarFile(bs, caller.token())
        .map { _ =>
          package_management_service.UploadDarFileResponse()
        }
        .resultToRight

  }

  private def getPackage(caller: CallerContext) = { (tracedInput: TracedInput[String]) =>
    packageClient
      .getPackage(tracedInput.in, caller.token())(tracedInput.traceContext)
      .map(response =>
        (
          Source.fromIterator(() =>
            response.archivePayload
              .asReadOnlyByteBufferList()
              .iterator
              .asScala
              .map(org.apache.pekko.util.ByteString(_))
          ),
          response.hash,
        )
      )
      .resultToRight
  }
}

object JsPackageService extends DocumentationEndpoints {
  import Endpoints.*
  lazy val packages = v2Endpoint.in(sttp.tapir.stringToPath("packages"))
  private val packageIdPath = "package-id"

  val uploadDar =
    packages.post
      .in(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()).toEndpointIO)
      .out(jsonBody[package_management_service.UploadDarFileResponse])
      .description("Upload a DAR to the participant node")

  val listPackagesEndpoint =
    packages.get
      .out(jsonBody[package_service.ListPackagesResponse])
      .description("List all packages uploaded on the participant node")

  val downloadPackageEndpoint =
    packages.get
      .in(path[String](packageIdPath))
      .out(streamBinaryBody(PekkoStreams)(CodecFormat.OctetStream()))
      .out(
        sttp.tapir.header[String]("Canton-Package-Hash")
      ) // Non standard header used for hash output
      .description("Download the package for the requested package-id")

  val packageStatusEndpoint =
    packages.get
      .in(path[String](packageIdPath))
      .in(sttp.tapir.stringToPath("status"))
      .out(jsonBody[package_service.GetPackageStatusResponse])
      .description("Get package status")

  override def documentation: Seq[AnyEndpoint] =
    Seq(uploadDar, listPackagesEndpoint, downloadPackageEndpoint, packageStatusEndpoint)

}

object JsPackageCodecs {
  import JsSchema.config

  implicit val listPackagesResponse: Codec[package_service.ListPackagesResponse] =
    deriveRelaxedCodec
  implicit val getPackageStatusResponse: Codec[package_service.GetPackageStatusResponse] =
    deriveRelaxedCodec

  implicit val uploadDarFileResponseRW: Codec[package_management_service.UploadDarFileResponse] =
    deriveRelaxedCodec
  implicit val packageStatusEncoder: Encoder[package_service.PackageStatus] =
    stringEncoderForEnum()
  implicit val packageStatusDecoder: Decoder[package_service.PackageStatus] =
    stringDecoderForEnum()

  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs
  implicit val packageStatusRecognizedSchema: Schema[package_service.PackageStatus.Recognized] =
    Schema.oneOfWrapped

  implicit val packageStatusSchema: Schema[package_service.PackageStatus] = Schema.string

}
