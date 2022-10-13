// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.client

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SigningPublicKey
import com.digitalasset.canton.domain.admin.{v0 as domainProto}
import com.digitalasset.canton.domain.sequencing.admin.protocol.{InitRequest, InitResponse}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.http.HttpClient
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.sequencing.client.http.HttpSequencerEndpoints.endpointVersion
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.TraceContext
import sttp.client3.*
import sttp.model.Uri

import java.net.URL
import scala.concurrent.{ExecutionContext, Future}

/** HTTP Sequencer administration/privileged operations client. */
class HttpSequencerAdminClient(
    baseUrl: URL,
    httpClient: HttpClient,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerAdminClient
    with NamedLogging {

  private case class InitializeParams(domain_id: String)
  private case class InitializeResponse(public_key: String, fingerprint: String)

  private def uri(packageName: String, serviceName: String, endpointName: String): Uri =
    uri"$baseUrl/app/com.digitalasset.canton.$packageName.$endpointVersion.$serviceName/$endpointName"

  override def initialize(
      initRequest: InitRequest
  )(implicit traceContext: TraceContext): EitherT[Future, String, InitResponse] =
    for {
      response <- httpClient
        .postAsBytes(
          uri("domain.admin", "SequencerInitializationService", "Init"),
          initRequest.toByteArrayV0,
        )
        .leftMap(err => s"HTTP sequencer initialization request failed: $err")
      initResponseP <- EitherT.fromEither[Future](
        ProtoConverter
          .protoParserArray(domainProto.InitResponse.parseFrom)(response.body)
          .leftMap(err => s"Failed to deserialize initialization response: $err")
      )
      publicKey <- EitherT.fromEither[Future](
        ProtoConverter
          .parseRequired(SigningPublicKey.fromProtoV0, "public_key", initResponseP.publicKey)
          .leftMap(err => s"Failed to deserialize public key: $err")
      )
      _ <- EitherT.cond(
        publicKey.id.unwrap == initResponseP.keyId,
        (),
        s"Fingerprints of sequencer signing keys do not match: ${publicKey.id.unwrap} vs. ${initResponseP.keyId}",
      )
    } yield InitResponse(initResponseP.keyId, publicKey, replicated = true)

  override def handshake(request: HandshakeRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse] =
    for {
      response <- httpClient
        .postAsBytes(uri("protocol", "SequencerVersionService", "Handshake"), request.toByteArrayV0)
        .leftMap(err => HandshakeRequestError(s"Sending HTTP sequencer handshake failed: $err"))
      handshakeP <- ProtoConverter
        .protoParserArray(v0.Handshake.Response.parseFrom)(response.body)
        .leftMap(err => HandshakeRequestError(s"Failed to deserialize handshake response: $err"))
        .toEitherT
      handshake <- HandshakeResponse
        .fromProtoV0(handshakeP)
        .leftMap(err => HandshakeRequestError(s"Failed to deserialize handshake response: $err"))
        .toEitherT
    } yield handshake

  override protected def onClosed(): Unit =
    Lifecycle.close(
      httpClient
    )(logger)
}
