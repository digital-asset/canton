// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.admin.participant.v30.{
  ParticipantStatusRequest,
  ParticipantStatusResponse,
  ParticipantStatusServiceGrpc,
}
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.health.admin.ParticipantStatus
import com.digitalasset.canton.topology.Synchronizer
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}

import scala.concurrent.Future

class GrpcParticipantStatusService(
    status: => NodeStatus[ParticipantStatus],
    val loggerFactory: NamedLoggerFactory,
) extends ParticipantStatusServiceGrpc.ParticipantStatusService
    with NamedLogging {

  override def participantStatus(
      request: ParticipantStatusRequest
  ): Future[ParticipantStatusResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val synchronizerFilterE: Either[Future[ParticipantStatusResponse], Option[Synchronizer]] =
      request.synchronizerId
        .traverse(Synchronizer.fromProtoV30)
        .leftMap(err => Future.failed(ProtoDeserializationFailure.Wrap(err).asGrpcError))

    synchronizerFilterE.map { synchronizerFilterO =>
      val responseP: ParticipantStatusResponse.Kind = status match {
        case NodeStatus.Failure(_msg) =>
          logger.warn(s"Unexpectedly found failure status: ${_msg}")
          ParticipantStatusResponse.Kind.Empty

        case notInitialized: NodeStatus.NotInitialized =>
          ParticipantStatusResponse.Kind.NotInitialized(notInitialized.toProtoV30)

        case NodeStatus.Success(status: ParticipantStatus) =>
          val filtered = synchronizerFilterO.fold(status)(status.filterBySynchronizer)
          ParticipantStatusResponse.Kind.Status(filtered.toParticipantStatusProto)
      }

      Future.successful(ParticipantStatusResponse(responseP))
    }.merge
  }
}
