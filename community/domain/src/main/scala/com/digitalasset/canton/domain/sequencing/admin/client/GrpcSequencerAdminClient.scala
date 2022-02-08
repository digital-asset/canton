// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.admin.client

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.admin.v0.SequencerInitializationServiceGrpc
import com.digitalasset.canton.domain.api.v0.SequencerVersionServiceGrpc
import com.digitalasset.canton.domain.sequencing.admin.protocol.{InitRequest, InitResponse}
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.{neverRetry, sendGrpcRequest}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.sequencing.client.grpc.GrpcSequencerChannelBuilder
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}

import scala.concurrent.{ExecutionContextExecutor, Future}

class GrpcSequencerAdminClient(
    connection: GrpcSequencerConnection,
    domainParameters: StaticDomainParameters,
    traceContextPropagation: TracingConfig.Propagation,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContextExecutor)
    extends SequencerAdminClient
    with NamedLogging {

  private val channel =
    GrpcSequencerChannelBuilder(
      ClientChannelBuilder(loggerFactory),
      connection,
      domainParameters.maxInboundMessageSize,
      traceContextPropagation,
    )
  private val initializationServiceStub = SequencerInitializationServiceGrpc.stub(channel)
  private val versionServiceStub = SequencerVersionServiceGrpc.stub(channel)

  override def initialize(
      request: InitRequest
  )(implicit traceContext: TraceContext): EitherT[Future, String, InitResponse] =
    for {
      responseP <- sendGrpcRequest(initializationServiceStub, "sequencer-initialization")(
        _.init(request.toProtoV0),
        "initialize",
        timeouts.network.unwrap,
        logger,
      ).leftMap(err => s"Failed to call initialize endpoint: $err")
      response <- InitResponse
        .fromProtoV0(responseP)
        .leftMap(err => s"Failed to deserialize initialization response: $err")
        .toEitherT[Future]
    } yield response

  override def handshake(request: HandshakeRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse] =
    for {
      responseP <- sendGrpcRequest(versionServiceStub, "sequencer-initialization")(
        _.handshake(request.toProtoV0),
        "handshake",
        timeouts.network.unwrap,
        logger,
        retryPolicy =
          neverRetry, // never retry here, allow the caller to decide based on their retry policy
        logPolicy = err =>
          logger =>
            traceContext => logger.debug(s"Failed to handshake with sequencer: $err")(traceContext),
      ).leftMap(err =>
        HandshakeRequestError(s"Failed to call handshake operation: $err", err.retry)
      )
      response <- HandshakeResponse
        .fromProtoV0(responseP)
        .leftMap(err => HandshakeRequestError(s"Failed to deserialize handshake response: $err"))
        .toEitherT[Future]
    } yield response

  override protected def onClosed(): Unit =
    Lifecycle.close(
      Lifecycle.toCloseableChannel(channel, logger, "grpc-sequencer-admin-operations")
    )(logger)
}
