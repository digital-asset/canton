// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.service

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.SequencerAggregatedInfo
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.mediator.admin.v30
import com.digitalasset.canton.mediator.admin.v30.SequencerConnectionServiceGrpc.SequencerConnectionService
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.networking.grpc.CantonMutableHandlerRegistry
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.{
  RequestSigner,
  RichSequencerClient,
  SequencerClient,
  SequencerClientTransportFactory,
}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnectionValidation,
  SequencerConnections,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{EitherTUtil, retry}
import io.grpc.{Status, StatusException}
import monocle.Lens
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

class GrpcSequencerConnectionService(
    fetchConnection: () => Future[Option[SequencerConnections]],
    setConnection: (SequencerConnectionValidation, SequencerConnections) => EitherT[
      Future,
      String,
      Unit,
    ],
    logout: () => EitherT[FutureUnlessShutdown, Status, Unit],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends v30.SequencerConnectionServiceGrpc.SequencerConnectionService
    with NamedLogging {
  override def getConnection(request: v30.GetConnectionRequest): Future[v30.GetConnectionResponse] =
    fetchConnection()
      .map {
        case Some(sequencerConnections) =>
          v30.GetConnectionResponse(Some(sequencerConnections.toProtoV30))

        case None => v30.GetConnectionResponse(None)
      }

  override def setConnection(request: v30.SetConnectionRequest): Future[v30.SetConnectionResponse] =
    EitherTUtil.toFuture(for {
      existing <- EitherT.right(getConnection)
      requestedReplacement <- parseConnection(request)
      _ <- validateReplacement(
        existing,
        requestedReplacement,
      )
      validation <- EitherT.fromEither[Future](
        SequencerConnectionValidation
          .fromProtoV30(request.sequencerConnectionValidation)
          .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.message).asException())
      )
      _ <- setConnection(validation, requestedReplacement)
        .leftMap(error => Status.FAILED_PRECONDITION.withDescription(error).asException())
    } yield v30.SetConnectionResponse())

  private def getConnection: Future[SequencerConnections] =
    fetchConnection().flatMap {
      case None =>
        Future.failed(
          Status.FAILED_PRECONDITION
            .withDescription(
              "Initialize node before attempting to change sequencer connection"
            )
            .asException()
        )
      case Some(conn) =>
        Future.successful(conn)
    }

  private def parseConnection(
      request: v30.SetConnectionRequest
  ): EitherT[Future, StatusException, SequencerConnections] = {
    val v30.SetConnectionRequest(sequencerConnectionsPO, validation) = request
    ProtoConverter
      .required("sequencerConnections", sequencerConnectionsPO)
      .flatMap(SequencerConnections.fromProtoV30)
      .leftMap(err => Status.INVALID_ARGUMENT.withDescription(err.message).asException())
      .toEitherT[Future]
  }

  private def validateReplacement(
      existing: SequencerConnections,
      requestedReplacement: SequencerConnections,
  ): EitherT[Future, StatusException, Unit] =
    (existing.default, requestedReplacement.default) match {
      // TODO(i12076): How should we be checking connetions here? what should be validated?
      case (_: GrpcSequencerConnection, _: GrpcSequencerConnection) =>
        EitherT.rightT[Future, StatusException](())
      case _ =>
        EitherT.leftT[Future, Unit](
          Status.INVALID_ARGUMENT
            .withDescription(
              "requested replacement connection info is not of the same type as the existing"
            )
            .asException()
        )
    }

  /** Revoke the authentication tokens on a sequencer and disconnect the sequencer client
    */
  override def logout(
      request: v30.LogoutRequest
  ): Future[v30.LogoutResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val ret = for {
      _ <- logout()
        .leftMap(err => err.asRuntimeException())
        .onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError))
    } yield v30.LogoutResponse()

    EitherTUtil.toFuture(ret)
  }
}

object GrpcSequencerConnectionService {

  trait UpdateSequencerClient {
    def set(client: RichSequencerClient): Unit
  }

  def setup[C](member: Member)(
      registry: CantonMutableHandlerRegistry,
      fetchConfig: () => Future[Option[C]],
      saveConfig: C => Future[Unit],
      sequencerConnectionLens: Lens[C, SequencerConnections],
      requestSigner: RequestSigner,
      transportFactory: SequencerClientTransportFactory,
      sequencerInfoLoader: SequencerInfoLoader,
      domainAlias: DomainAlias,
      domainId: DomainId,
      sequencerClient: SequencerClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionServiceFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): UpdateSequencerClient = {
    val clientO = new AtomicReference[Option[RichSequencerClient]](None)
    registry.addServiceU(
      SequencerConnectionService.bindService(
        new GrpcSequencerConnectionService(
          fetchConnection = () => fetchConfig().map(_.map(sequencerConnectionLens.get)),
          setConnection = (sequencerConnectionValidation, newSequencerConnection) =>
            for {
              currentConfig <- EitherT.right(fetchConfig())
              newConfig <- currentConfig.fold(
                EitherT.leftT[Future, C](
                  "Can't update config when none has yet been set. Please initialize node."
                )
              )(config =>
                EitherT.rightT(sequencerConnectionLens.replace(newSequencerConnection)(config))
              )

              // load and potentially validate the new connection
              newEndpointsInfo <- sequencerInfoLoader
                .loadAndAggregateSequencerEndpoints(
                  domainAlias,
                  Some(domainId),
                  newSequencerConnection,
                  sequencerConnectionValidation,
                )
                .leftMap(_.cause)

              sequencerTransportsMap = transportFactory
                .makeTransport(
                  newEndpointsInfo.sequencerConnections,
                  member,
                  requestSigner,
                )

              sequencerTransports <- EitherT.fromEither[Future](
                SequencerTransports.from(
                  sequencerTransportsMap,
                  newEndpointsInfo.expectedSequencers,
                  newEndpointsInfo.sequencerConnections.sequencerTrustThreshold,
                  newEndpointsInfo.sequencerConnections.submissionRequestAmplification,
                )
              )

              // important to only save the config and change the transport after the `makeTransport` has run and done the handshake
              _ <- EitherT.right(saveConfig(newConfig))
              _ <- EitherT.right(
                clientO
                  .get()
                  .fold {
                    // need to close here
                    sequencerTransportsMap.values.foreach(_.close())
                    Future.unit
                  }(_.changeTransport(sequencerTransports))
              )
            } yield (),
          sequencerClient.logout _,
          loggerFactory,
        ),
        executionContext,
      )
    )
    new UpdateSequencerClient {
      override def set(client: RichSequencerClient): Unit = clientO.set(Some(client))
    }
  }

  def waitUntilSequencerConnectionIsValid(
      sequencerInfoLoader: SequencerInfoLoader,
      flagCloseable: FlagCloseable,
      futureSupervisor: FutureSupervisor,
      loadConfig: => Future[Option[SequencerConnections]],
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
      executionContext: ExecutionContextExecutor,
  ): EitherT[Future, String, SequencerAggregatedInfo] = {
    val promise =
      new PromiseUnlessShutdown[Either[String, SequencerAggregatedInfo]](
        "wait-for-valid-connection",
        futureSupervisor,
      )
    flagCloseable.runOnShutdown_(promise)
    implicit val closeContext = CloseContext(flagCloseable)
    val alias = DomainAlias.tryCreate("domain")

    def tryNewConfig: EitherT[Future, String, SequencerAggregatedInfo] =
      EitherT
        .right(loadConfig)
        .flatMap {
          case Some(settings) =>
            sequencerInfoLoader
              .loadAndAggregateSequencerEndpoints(
                domainAlias = alias,
                expectedDomainId = None,
                sequencerConnections = settings,
                sequencerConnectionValidation = SequencerConnectionValidation.Active,
              )
              .leftMap { e =>
                errorLoggingContext.logger.warn(s"Waiting for valid sequencer connection $e")
                e.toString
              }
          case None => EitherT.leftT("No sequencer connection config")
        }
    import scala.concurrent.duration.*
    EitherT(
      retry
        .Pause(
          errorLoggingContext.logger,
          flagCloseable,
          maxRetries = Int.MaxValue,
          delay = 50.millis,
          operationName = "wait-for-valid-sequencer-connection",
        )
        .apply(
          tryNewConfig.value,
          NoExceptionRetryPolicy,
        )
    )
  }

}
