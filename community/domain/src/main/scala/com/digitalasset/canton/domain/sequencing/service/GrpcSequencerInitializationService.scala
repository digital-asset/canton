// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.data.EitherT.fromEither
import cats.syntax.functor._
import cats.syntax.traverse._
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.admin.v0.SequencerInitializationServiceGrpc.SequencerInitializationService
import com.digitalasset.canton.domain.sequencing.admin.protocol.{InitRequest, InitResponse}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, Traced}
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil, SimpleExecutionQueue}
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

  /** Process requests sequentially */
  override def init(requestP: v0.InitRequest): Future[v0.InitResponse] =
    TraceContext.fromGrpcContext { implicit traceContext =>
      // ensure here we don't process initialization requests concurrently
      executionQueue.execute(
        {
          logger.info("Initializing Sequencer")
          val result = for {
            request <- fromEither[Future](InitRequest.fromProtoV0(requestP))
              .leftMap(err => s"Failed to deserialize request: $err")
              .leftMap(Status.INVALID_ARGUMENT.withDescription)
            _ <- validateSnapshot(request)
            response <- initialize(Traced(request))
              .leftMap(Status.INTERNAL.withDescription)
            responseP = response.toProtoV0
          } yield responseP

          EitherTUtil.toFuture(result.leftMap(_.asRuntimeException()))
        },
        s"sequencer initialization for domain ${requestP.domainId}",
      )
    }

  private def validateSnapshot(request: InitRequest): EitherT[Future, Status, Unit] = {
    val sequencerMember = SequencerId(request.domainId)
    EitherT.fromEither[Future] {
      request.sequencerSnapshot.traverse { snapshot =>
        val members = snapshot.status.members.map(_.member)
        EitherUtil.condUnitE(
          members.contains(sequencerMember) && snapshot.heads.contains(
            sequencerMember
          ),
          Status.INVALID_ARGUMENT.withDescription("Given snapshot must contain sequencer member"),
        )
      }.void
    }
  }
}
