// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.data.EitherT.fromEither
import com.digitalasset.canton.domain.admin.v0.SequencerInitializationServiceGrpc.SequencerInitializationService
import com.digitalasset.canton.domain.admin.{v0, v1}
import com.digitalasset.canton.domain.sequencing.admin.protocol.{InitRequest, InitResponse}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TraceContextGrpc, Traced}
import com.digitalasset.canton.util.{EitherTUtil, SimpleExecutionQueue}
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

/** Will initialize the sequencer server based using the provided initialize function when called.
  */
class GrpcSequencerInitializationService(
    initialize: Traced[InitRequest] => EitherT[Future, String, InitResponse],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends SequencerInitializationService
    with NamedLogging
    with NoTracing {
  private val executionQueue = new SimpleExecutionQueue()

  override def init(requestP: v0.InitRequest): Future[v0.InitResponse] =
    initInternal(requestP.domainId, requestP, InitRequest.fromProtoV0)

  override def initV1(requestP: v1.InitRequest): Future[v0.InitResponse] =
    initInternal(requestP.domainId, requestP, InitRequest.fromProtoV1)

  /** Process requests sequentially */
  def initInternal[P](
      domainId: String,
      requestP: P,
      deserializer: P => ParsingResult[InitRequest],
  ): Future[v0.InitResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    // ensure here we don't process initialization requests concurrently
    executionQueue.execute(
      {
        val result = for {
          request <- fromEither[Future](deserializer(requestP))
            .leftMap(err => s"Failed to deserialize request: $err")
            .leftMap(Status.INVALID_ARGUMENT.withDescription)
          response <- initialize(Traced(request))
            .leftMap(Status.FAILED_PRECONDITION.withDescription)
          responseP = response.toProtoV0
        } yield responseP

        EitherTUtil.toFuture(result.leftMap(_.asRuntimeException()))
      },
      s"sequencer initialization for domain $domainId",
    )
  }

}
