// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.data.EitherT
import cats.implicits.catsSyntaxEither
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.connection.v30
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc.ApiInfoServiceStub
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.api.v30.SequencerConnect.GetSynchronizerParametersResponse.Parameters
import com.digitalasset.canton.sequencer.api.v30.SequencerConnectServiceGrpc.SequencerConnectServiceStub
import com.digitalasset.canton.sequencer.api.v30.{
  SequencerConnect,
  SequencerConnectServiceGrpc,
  SequencerServiceGrpc,
}
import com.digitalasset.canton.sequencing.client.SequencerConnectClientInterceptor
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.topology.{
  Member,
  PhysicalSynchronizerId,
  SequencerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.{Channel, ClientInterceptors}
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContextExecutor

import SequencerConnectionStub.SequencerConnectionStubError

/** Stub to interact with a sequencer, specialized for gRPC transport.
  */
class GrpcSequencerConnectionStub(
    member: Member,
    connection: GrpcConnection,
    apiSvcFactory: Channel => ApiInfoServiceStub,
    sequencerConnectSvcFactory: Channel => SequencerConnectServiceStub,
    metricsContext: MetricsContext,
    loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextExecutor
) extends SequencerConnectionStub {
  private val interceptor = new SequencerConnectClientInterceptor(member, loggerFactory)

  override def getApiName(
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError.ConnectionError, String] = for {
    apiName <- connection
      .sendRequest(
        requestDescription = "get API info",
        stubFactory = apiSvcFactory,
        retryPolicy = retryPolicy,
        logPolicy = logPolicy,
        metricsContext = metricsContext.withExtraLabels("endpoint" -> "GetApiInfo"),
      )(_.getApiInfo(v30.GetApiInfoRequest()).map(_.name))
      .leftMap(
        SequencerConnectionStubError.ConnectionError.apply
      )
  } yield apiName

  override def performHandshake(
      clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
      minimumProtocolVersion: Option[ProtocolVersion],
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, HandshakeResponse] = {
    val handshakeRequest = HandshakeRequest(clientProtocolVersions, minimumProtocolVersion)

    for {
      handshakeResponseP <- connection
        .sendRequest(
          requestDescription = "perform handshake",
          stubFactory = channel =>
            sequencerConnectSvcFactory(ClientInterceptors.intercept(channel, interceptor)),
          retryPolicy = retryPolicy,
          logPolicy = logPolicy,
          metricsContext = metricsContext.withExtraLabels("endpoint" -> "Handshake"),
        )(_.handshake(handshakeRequest.toProtoV30))
        .leftMap(SequencerConnectionStubError.ConnectionError.apply)
      handshakeResponse <- EitherT
        .fromEither[FutureUnlessShutdown](HandshakeResponse.fromProtoV30(handshakeResponseP))
        .leftMap[SequencerConnectionStubError](err =>
          SequencerConnectionStubError.DeserializationError(err.message)
        )
    } yield handshakeResponse
  }

  override def getSynchronizerAndSequencerIds(
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionStubError,
    (PhysicalSynchronizerId, SequencerId),
  ] =
    for {
      synchronizerIdP <- connection
        .sendRequest(
          requestDescription = "get synchronizer ID",
          stubFactory = sequencerConnectSvcFactory,
          retryPolicy = retryPolicy,
          logPolicy = logPolicy,
          metricsContext = metricsContext.withExtraLabels("endpoint" -> "GetSynchronizerId"),
        )(_.getSynchronizerId(SequencerConnect.GetSynchronizerIdRequest()))
        .leftMap(SequencerConnectionStubError.ConnectionError.apply)

      psid <- EitherT.fromEither[FutureUnlessShutdown](
        PhysicalSynchronizerId
          .fromProtoPrimitive(synchronizerIdP.physicalSynchronizerId, "physical_synchronizer_id")
          .leftMap(err => SequencerConnectionStubError.DeserializationError(err.message))
      )

      sequencerId <- EitherT.fromEither[FutureUnlessShutdown](
        UniqueIdentifier
          .fromProtoPrimitive(synchronizerIdP.sequencerUid, "sequencer_uid")
          .map(SequencerId(_))
          .leftMap[SequencerConnectionStubError](err =>
            SequencerConnectionStubError.DeserializationError(err.message)
          )
      )
    } yield (psid, sequencerId)

  override def getStaticSynchronizerParameters(
      retryPolicy: GrpcError => Boolean,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionStubError, StaticSynchronizerParameters] =
    for {
      synchronizerParametersP <- connection
        .sendRequest(
          requestDescription = "get static synchronizer parameters",
          stubFactory = sequencerConnectSvcFactory,
          retryPolicy = retryPolicy,
          logPolicy = logPolicy,
          metricsContext = metricsContext.withExtraLabels("endpoint" -> "GetSynchronizerParameters"),
        )(_.getSynchronizerParameters(SequencerConnect.GetSynchronizerParametersRequest()))
        .leftMap(SequencerConnectionStubError.ConnectionError.apply)

      synchronizerParametersE = synchronizerParametersP.parameters match {
        case Parameters.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("GetSynchronizerParameters.parameters"))
        case Parameters.ParametersV1(parametersV1) =>
          StaticSynchronizerParameters.fromProtoV30(parametersV1)
      }
      synchronizerParameters <- EitherT.fromEither[FutureUnlessShutdown](
        synchronizerParametersE
          .leftMap[SequencerConnectionStubError](err =>
            SequencerConnectionStubError.DeserializationError(err.message)
          )
      )
    } yield synchronizerParameters
}

class SequencerConnectionStubFactoryImpl(member: Member, loggerFactory: NamedLoggerFactory)
    extends SequencerConnectionStubFactory {
  override def createStub(connection: Connection, metricsContext: MetricsContext)(implicit
      ec: ExecutionContextExecutor
  ): SequencerConnectionStub = connection match {
    case grpcConnection: GrpcConnection =>
      new GrpcSequencerConnectionStub(
        member,
        grpcConnection,
        ApiInfoServiceGrpc.stub,
        SequencerConnectServiceGrpc.stub,
        metricsContext,
        loggerFactory,
      )

    case _ => throw new IllegalStateException(s"Connection type not supported: $connection")
  }

  def createUserStub(
      connection: Connection,
      clientAuth: GrpcSequencerClientAuth,
      metricsContext: MetricsContext,
      timeouts: ProcessingTimeout,
      protocolVersion: ProtocolVersion,
  )(implicit
      ec: ExecutionContextExecutor,
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
  ): UserSequencerConnectionStub =
    connection match {
      case grpcConnection: GrpcConnection =>
        new GrpcUserSequencerConnectionStub(
          grpcConnection,
          channel => clientAuth(SequencerServiceGrpc.stub(channel)),
          metricsContext,
          timeouts,
          loggerFactory,
          protocolVersion,
        )

      case _ => throw new IllegalStateException(s"Connection type not supported: $connection")
    }
}
