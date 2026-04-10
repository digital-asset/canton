// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.GrpcServiceInvocationMethod
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, HasRunOnClosing, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.metrics.SequencerConnectionPoolMetrics
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  ClientChannelBuilder,
  ClientChannelParams,
  GrpcClient,
  GrpcError,
  GrpcManagedChannel,
  ManagedChannelBuilderProxy,
}
import com.digitalasset.canton.sequencing.client.pool.Connection.{
  ConnectionConfig,
  ConnectionError,
  ConnectionHealth,
  ConnectionState,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex
import io.grpc.Channel
import io.grpc.Context.CancellableContext
import io.grpc.stub.{AbstractStub, StreamObserver}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Future}

/** Connection specialized for gRPC transport.
  */
final case class GrpcConnection(
    config: ConnectionConfig,
    params: ClientChannelParams,
    metrics: SequencerConnectionPoolMetrics,
    override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends Connection
    with PrettyPrinting {

  private val channelRef = new AtomicReference[Option[GrpcManagedChannel]](None)
  private val lock = new Mutex()

  override val health: ConnectionHealth = new ConnectionHealth(
    name = name,
    associatedHasRunOnClosing = this,
    logger = logger,
  )

  private[sequencing] def channel: Option[GrpcManagedChannel] = channelRef.get

  override def name: String = s"connection-${config.name}"

  override def start()(implicit traceContext: TraceContext): Unit =
    lock.exclusive {
      channelRef.get match {
        case Some(_) => logger.warn("Starting an already-started connection. Ignoring.")

        case None =>
          val builder = mkChannelBuilder()
          val channel = GrpcManagedChannel(
            s"GrpcConnection-$name",
            builder.build(),
            associatedHasRunOnClosing = this,
            logger,
          )

          channelRef.set(Some(channel))
          health.reportHealthState(ConnectionState.Started)
      }
    }

  override def stop()(implicit traceContext: TraceContext): Unit = {
    lock.exclusive {
      channelRef.get match {
        case Some(_) =>
          closeChannel()
          Some(ConnectionState.Stopped)
        // Not logging at WARN level because concurrent calls may happen (e.g. at closing time)
        case None =>
          logger.info("Stopping an already-stopped connection. Ignoring.")
          None
      }
    }
  }.foreach(state => health.reportHealthState(state))

  override def onClosed(): Unit = closeChannel()

  private def closeChannel(): Unit = {
    lock.exclusive {
      channelRef.getAndSet(None)
    }
  }.foreach(LifeCycle.close(_)(logger))

  @GrpcServiceInvocationMethod
  def sendRequest[Svc <: AbstractStub[Svc], Res](
      requestDescription: String,
      stubFactory: Channel => Svc,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
      retryPolicy: GrpcError => Boolean,
      timeout: Duration = timeouts.network.unwrap,
      metricsContext: MetricsContext,
  )(
      send: Svc => Future[Res]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, ConnectionError, Res] =
    // We don't need to add synchronization in this method, because:
    // 1. If the channel gets closed during the call, the call will fail with an appropriate gRPC error.
    // 2. The gRPC channels and stubs are thread-safe.
    channelRef.get() match {
      case Some(channel) =>
        val client = GrpcClient.create(channel, stubFactory)

        metrics.connectionRequests.inc()(metricsContext)
        CantonGrpcUtil
          .sendGrpcRequest(client, s"server-${config.name}")(
            send = send,
            requestDescription = requestDescription,
            timeout = timeout,
            logger = logger,
            logPolicy = logPolicy,
            retryPolicy = retryPolicy,
          )
          .leftMap[ConnectionError](ConnectionError.TransportError.apply)

      case None =>
        EitherT.leftT[FutureUnlessShutdown, Res](
          ConnectionError.InvalidStateError("Connection is not started")
        )
    }

  def serverStreamingRequest[Svc <: AbstractStub[Svc], HasObserver, Res](
      stubFactory: Channel => Svc,
      observerFactory: (CancellableContext, HasRunOnClosing) => HasObserver,
      metricsContext: MetricsContext,
  )(getObserver: HasObserver => StreamObserver[Res])(
      send: (Svc, StreamObserver[Res]) => Unit
  )(implicit traceContext: TraceContext): Either[ConnectionError.InvalidStateError, HasObserver] =
    channelRef.get() match {
      case Some(channel) =>
        metrics.connectionRequests.inc()(metricsContext)
        val client = GrpcClient.create(channel, stubFactory)
        val result =
          CantonGrpcUtil.serverStreamingRequest(client, observerFactory)(getObserver)(send)
        Right(result)

      case None =>
        Left(ConnectionError.InvalidStateError("Connection is not started"))
    }

  def serverStreamingRequestPekko[Svc <: AbstractStub[Svc], Req, Res](
      stubFactory: Channel => Svc,
      metricsContext: MetricsContext,
  )(request: Req, send: Svc => (Req, StreamObserver[Res]) => Unit)(implicit
      esf: ExecutionSequencerFactory
  ): Either[ConnectionError.InvalidStateError, Source[Res, NotUsed]] =
    channelRef.get() match {
      case Some(channel) =>
        metrics.connectionRequests.inc()(metricsContext)
        val client = GrpcClient.create(channel, stubFactory)
        val source = ClientAdapter.serverStreaming(request, send(client.service))
        Right(source)

      case None =>
        Left(ConnectionError.InvalidStateError("Connection is not started"))
    }

  private def mkChannelBuilder()(implicit
      executor: Executor
  ): ManagedChannelBuilderProxy = {
    val clientChannelBuilder = ClientChannelBuilder(loggerFactory)

    ManagedChannelBuilderProxy(
      clientChannelBuilder
        .create(
          endpoint = config.endpoint,
          useTls = config.transportSecurity,
          executor = executor,
          trustCertificate = config.customTrustCertificates,
          // TODO(i30502): Limit maxInboundMessageSize to `DynamicSynchronizerParameters.maxRequestSize`
          // maybe you don't need it but you can just document that if the max request size of the synchronizer is
          // larger then this parameter needs to be adjusted in the sequencer client.
          // the default value here is 128MB and we are really just dealing with "malicious sequencer node scenarios".
          params,
        )
    )
  }

  override protected def pretty: Pretty[GrpcConnection] =
    prettyOfString(conn => s"Connection ${conn.name.singleQuoted}")
}
