// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.UniqueIdentifier
import com.digitalasset.canton.topology.admin.v30 as adminProto
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.PositiveSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.validation.ProtoUnvalidated.syntax.*
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.ProtocolVersionValidation
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}

class GrpcIdentityInitializationService(
    clock: Clock,
    bootstrap: GrpcIdentityInitializationService.Callback,
    nodeInitViaApi: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends adminProto.IdentityInitializationServiceGrpc.IdentityInitializationService
    with NamedLogging {

  override def initId(request: adminProto.InitIdRequest): Future[adminProto.InitIdResponse] = if (
    !nodeInitViaApi
  )
    Future.failed(
      io.grpc.Status.FAILED_PRECONDITION
        .withDescription(
          "Node configuration does not allow for a manual node initialization via API"
        )
        .asRuntimeException()
    )
  else {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val adminProto.InitIdRequest(identifierP, namespaceP, certificatesP) = request
    def handleProtoFailure[T](
        either: Either[ProtoDeserializationError, T]
    ): EitherT[Future, StatusRuntimeException, T] =
      EitherT.fromEither[Future](
        either.leftMap(err => ProtoDeserializationFailure.WrapNoLogging(err).toGrpcError)
      )
    val ret = for {
      identifier <- handleProtoFailure(
        ProtoValidation
          .validate(identifierP, Some("identifier"), ProtocolVersionValidation.AlwaysValidation)
      )
      namespace <- handleProtoFailure(
        ProtoValidation
          .validate(namespaceP, Some("namespace"), ProtocolVersionValidation.AlwaysValidation)
      )
      // parse topology transactions
      certificates <- handleProtoFailure(
        MonadUtil
          .sequentialTraverse(certificatesP)(
            SignedTopologyTransaction
              .fromProtoV30(ProtocolVersionValidation.AlwaysValidation, _)
              .flatMap { tx =>
                tx.selectOp[TopologyChangeOp.Replace]
                  .toRight(
                    ProtoDeserializationError
                      .OtherError("Topology transaction is not a replace but a remove")
                  )
              }
          )
      )
      _ <- bootstrap
        .initializeViaApi(identifier, namespace, certificates)
        .leftMap(err =>
          io.grpc.Status.FAILED_PRECONDITION.withDescription(err).asRuntimeException()
        )
    } yield adminProto.InitIdResponse()
    EitherTUtil.toFuture(ret)
  }

  override def getId(request: adminProto.GetIdRequest): Future[adminProto.GetIdResponse] = {
    val id = bootstrap.getId
    Future.successful(
      adminProto.GetIdResponse(
        initialized = bootstrap.isInitialized,
        uniqueIdentifier = id.map(_.toProtoPrimitive).getOrElse("").toProtoUnvalidated,
      )
    )
  }

  override def currentTime(
      request: adminProto.CurrentTimeRequest
  ): Future[adminProto.CurrentTimeResponse] =
    Future.successful(adminProto.CurrentTimeResponse(clock.now.toProtoPrimitive))
}

object GrpcIdentityInitializationService {
  trait Callback {
    def initializeViaApi(
        identifier: String,
        namespace: String,
        certificates: Seq[PositiveSignedTopologyTransaction],
    )(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, Unit]
    def getId: Option[UniqueIdentifier]
    def isInitialized: Boolean
  }
}
