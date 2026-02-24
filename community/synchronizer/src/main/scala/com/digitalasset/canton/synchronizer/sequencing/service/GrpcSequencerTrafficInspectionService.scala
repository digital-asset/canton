// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.*
import com.digitalasset.canton.synchronizer.sequencer.Sequencer
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.{ExecutionContext, Future}

class GrpcSequencerTrafficInspectionService(
    sequencer: Sequencer,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends v30.SequencerTrafficInspectionServiceGrpc.SequencerTrafficInspectionService
    with NamedLogging {

  /** Retrieve summaries of the traffic cost of sequenced events.
    */
  override def getTrafficSummaries(
      request: GetTrafficSummariesRequest
  ): Future[GetTrafficSummariesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    logger.debug(s"Computing traffic summaries for ${request.sequencingTimestamps.size} events")

    val result = for {
      sequencingTimestamps <- EitherT
        .fromEither[FutureUnlessShutdown](
          request.sequencingTimestamps.traverse(CantonTimestamp.fromProtoTimestamp)
        )
        .leftMap(err => ProtoDeserializationFailure.Wrap(err))
        .leftWiden[CantonBaseError]
      trafficSummaries <- sequencer
        .getTrafficSummaries(sequencingTimestamps)
        .leftWiden[CantonBaseError]
    } yield GetTrafficSummariesResponse(trafficSummaries)

    mapErrNewEUS(result.leftMap(_.toCantonRpcError))
  }
}
