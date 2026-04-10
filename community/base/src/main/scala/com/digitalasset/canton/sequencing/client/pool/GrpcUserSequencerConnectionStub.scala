// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.data.EitherT
import cats.implicits.{catsSyntaxEither, toTraverseOps}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasRunOnClosing}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcLogPolicy
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.SequencerServiceGrpc.SequencerServiceStub
import com.digitalasset.canton.sequencer.api.v30.{
  AcknowledgeSignedRequest,
  AcknowledgeSignedResponse,
  SendAsyncRequest,
}
import com.digitalasset.canton.sequencing.SequencedEventHandler
import com.digitalasset.canton.sequencing.client.SequencerSubscription
import com.digitalasset.canton.sequencing.client.pool.Connection.ConnectionError
import com.digitalasset.canton.sequencing.client.transports.{
  ConsumesCancellableGrpcStreamObserver,
  GrpcSequencerSubscription,
}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitHashResponse,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.Context.CancellableContext
import io.grpc.{Channel, StatusRuntimeException}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

import SequencerConnectionStub.SequencerConnectionStubError

/** Stub for user interactions with a sequencer, specialized for gRPC transport.
  */
class GrpcUserSequencerConnectionStub(
    connection: GrpcConnection,
    sequencerSvcFactory: Channel => SequencerServiceStub,
    metricsContext: MetricsContext,
    timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
    protocolVersion: ProtocolVersion,
)(implicit
    ec: ExecutionContextExecutor,
    esf: ExecutionSequencerFactory,
    materializer: Materializer,
) extends UserSequencerConnectionStub {
  override def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError.ConnectionError, Unit] = {
    val messageId = request.content.messageId
    for {
      _ <- connection
        .sendRequest(
          requestDescription = s"send-async-versioned/$messageId",
          stubFactory = sequencerSvcFactory,
          retryPolicy = retryPolicy,
          logPolicy = logPolicy,
          timeout = timeout,
          metricsContext = metricsContext.withExtraLabels("endpoint" -> "SendAsync"),
        )(
          _.sendAsync(
            SendAsyncRequest(signedSubmissionRequest = request.toByteString)
          )
        )
        .leftMap(
          SequencerConnectionStubError.ConnectionError.apply
        )
    } yield ()
  }

  override def acknowledgeSigned(
      signedRequest: SignedContent[AcknowledgeRequest],
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, AcknowledgeSignedResponse] = {
    val acknowledgeRequest = AcknowledgeSignedRequest(signedRequest.toByteString)
    for {
      result <- connection
        .sendRequest(
          requestDescription = s"acknowledge-signed/${signedRequest.content.timestamp}",
          stubFactory = sequencerSvcFactory,
          retryPolicy = retryPolicy,
          logPolicy = logPolicy,
          timeout = timeout,
          metricsContext = metricsContext.withExtraLabels("endpoint" -> "AcknowledgeSigned"),
        )(_.acknowledgeSigned(acknowledgeRequest))
        .leftMap[SequencerConnectionStubError](
          SequencerConnectionStubError.ConnectionError.apply
        )
    } yield result
  }

  override def getTrafficStateForMember(
      request: GetTrafficStateForMemberRequest,
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionStubError,
    GetTrafficStateForMemberResponse,
  ] = for {
    responseP <- connection
      .sendRequest(
        requestDescription = s"get-traffic-state/${request.member}",
        stubFactory = sequencerSvcFactory,
        retryPolicy = retryPolicy,
        logPolicy = logPolicy,
        timeout = timeout,
        metricsContext = metricsContext.withExtraLabels("endpoint" -> "GetTrafficStateForMember"),
      )(_.getTrafficStateForMember(request.toProtoV30))
      .leftMap[SequencerConnectionStubError](
        SequencerConnectionStubError.ConnectionError.apply
      )
    responseE = GetTrafficStateForMemberResponse.fromProtoV30(responseP)
    response <- EitherT.fromEither[FutureUnlessShutdown](
      responseE.leftMap[SequencerConnectionStubError](err =>
        SequencerConnectionStubError.DeserializationError(err.message)
      )
    )
  } yield response

  override def getTime(
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, Option[CantonTimestamp]] =
    for {
      response <- connection
        .sendRequest(
          requestDescription = s"get-time",
          stubFactory = sequencerSvcFactory,
          retryPolicy = retryPolicy,
          logPolicy = logPolicy,
          timeout = timeout,
          metricsContext = metricsContext.withExtraLabels("endpoint" -> "GetTime"),
        )(_.getTime(com.digitalasset.canton.sequencer.api.v30.GetTimeRequest()))
        .leftMap[SequencerConnectionStubError](
          SequencerConnectionStubError.ConnectionError.apply
        )
      timestampO <- EitherT.fromEither[FutureUnlessShutdown](
        response.sequencingTimestamp
          .traverse(CantonTimestamp.fromProtoPrimitive)
          .leftMap(err =>
            SequencerConnectionStubError.DeserializationError(
              err.message
            ): SequencerConnectionStubError
          )
      )
    } yield timestampO

  override def downloadTopologyStateForInit(
      request: TopologyStateForInitRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionStubError,
    TopologyStateForInitResponse,
  ] =
    // TODO(i26281): Maybe use a `KillSwitch` to enforce a timeout
    for {
      source <-
        connection
          .serverStreamingRequestPekko(
            stubFactory = sequencerSvcFactory,
            metricsContext =
              metricsContext.withExtraLabels("endpoint" -> "DownloadTopologyStateForInit"),
          )(
            request = request.toProtoV30,
            send = _.downloadTopologyStateForInit,
          )
          .leftMap[SequencerConnectionStubError](
            SequencerConnectionStubError.ConnectionError.apply
          )
          .toEitherT[FutureUnlessShutdown]

      result <- EitherT[Future, SequencerConnectionStubError, TopologyStateForInitResponse](
        source
          .map(TopologyStateForInitResponse.fromProtoV30(_))
          .flatMapConcat { parsingResult =>
            parsingResult.fold(
              err => Source.failed(ProtoDeserializationFailure.Wrap(err).asGrpcError),
              Source.single,
            )
          }
          // TODO(i26281): Maybe use `.runWith(Sink.seq)` to simplify
          .runFold(Vector.empty[GenericStoredTopologyTransaction])((acc, txs) =>
            acc ++ txs.topologyTransactions.value.result
          )
          .map { accumulated =>
            val storedTxs = StoredTopologyTransactions(accumulated)
            TopologyStateForInitResponse(Traced(storedTxs))
          }
          .transformWith {
            case Success(value) => Future.successful(Right(value))

            case Failure(grpcExc: StatusRuntimeException) =>
              logger.debug(
                s"Downloading topology state for initialization failed with gRPC exception",
                grpcExc,
              )
              val grpcError =
                GrpcError("download-topology-state-for-init", connection.name, grpcExc)
              Future.successful(
                Left(
                  SequencerConnectionStubError
                    .ConnectionError(ConnectionError.TransportError(grpcError))
                )
              )

            case Failure(exc) =>
              logger.warn(
                s"Downloading topology state for initialization failed with unexpected exception",
                exc,
              )
              Future.failed(exc)
          }
      )
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield result

  override def subscribe[E](
      request: SubscriptionRequest,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerConnectionStubError, SequencerSubscription[E]] = {
    val loggerWithConnection = loggerFactory.append("connection", connection.name)

    def mkSubscription(
        context: CancellableContext,
        hasRunOnClosing: HasRunOnClosing,
    ): ConsumesCancellableGrpcStreamObserver[E, v30.SubscriptionRequest, v30.SubscriptionResponse] =
      GrpcSequencerSubscription.fromSubscriptionResponse(
        context,
        handler,
        hasRunOnClosing,
        timeouts,
        loggerWithConnection,
      )(protocolVersion)

    connection
      .serverStreamingRequest(
        stubFactory = sequencerSvcFactory,
        observerFactory = mkSubscription,
        metricsContext = metricsContext.withExtraLabels("endpoint" -> "Subscribe"),
      )(getObserver = _.observer)(_.subscribe(request.toProtoV30, _))
      .leftMap[SequencerConnectionStubError](SequencerConnectionStubError.ConnectionError.apply)
  }

  override def downloadTopologyStateForInitHash(
      request: TopologyStateForInitRequest,
      timeout: Duration,
      retryPolicy: GrpcError => Boolean,
      logPolicy: GrpcLogPolicy,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionStubError,
    TopologyStateForInitHashResponse,
  ] =
    for {
      responseP <- connection
        .sendRequest(
          requestDescription = "download-topology-state-for-init-hash",
          stubFactory = sequencerSvcFactory,
          retryPolicy = retryPolicy,
          logPolicy = logPolicy,
          timeout = timeout,
          metricsContext = metricsContext.withExtraLabels(
            "endpoint" -> "DownloadTopologyStateForInitHash"
          ),
        )(_.downloadTopologyStateForInitHash(request.toHashProtoV30))
        .leftMap[SequencerConnectionStubError](
          SequencerConnectionStubError.ConnectionError.apply
        )
      responseE = TopologyStateForInitHashResponse.fromProtoV30(responseP)
      response <- EitherT.fromEither[FutureUnlessShutdown](
        responseE.leftMap[SequencerConnectionStubError](err =>
          SequencerConnectionStubError.DeserializationError(err.message)
        )
      )
    } yield response
}
