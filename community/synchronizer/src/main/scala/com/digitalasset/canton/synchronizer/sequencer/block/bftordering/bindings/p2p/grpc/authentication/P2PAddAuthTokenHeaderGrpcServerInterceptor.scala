// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.{ClientChannelBuilder, GrpcManagedChannel}
import com.digitalasset.canton.sequencing.authentication.grpc.SequencerClientTokenAuthentication
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
}
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth.ChannelTokenFetcher
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  createNettyClientChannelBuilder,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication.P2PAddAuthTokenHeaderGrpcServerInterceptor.P2PAuthenticatorServerCall
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall as GrpcSimpleForwardingServerCall
import io.grpc.{ServerInterceptor as GrpcServerInterceptor, *}

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.util.Try

private[bftordering] class P2PAddAuthTokenHeaderGrpcServerInterceptor(
    synchronizerId: PhysicalSynchronizerId,
    member: Member,
    crypto: SynchronizerCrypto,
    supportedProtocolVersions: Seq[ProtocolVersion],
    config: AuthenticationTokenManagerConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContextExecutor: ExecutionContextExecutor)
    extends GrpcServerInterceptor
    with NamedLogging
    with FlagCloseableAsync {

  private val tokenProvider =
    new AuthenticationTokenProvider(
      synchronizerId,
      member,
      crypto,
      supportedProtocolVersions,
      config,
      metricsO = None,
      metricsContext = MetricsContext.Empty,
      timeouts,
      loggerFactory,
    )

  private val liveChannels =
    Collections
      .newSetFromMap(new ConcurrentHashMap[GrpcManagedChannel, java.lang.Boolean]())
      .asScala

  private val clientChannelBuilder = ClientChannelBuilder(loggerFactory)

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      requestHeaders: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    Option(
      requestHeaders.get(P2PAddEndpointHeaderGrpcClientInterceptor.ENDPOINT_METADATA_KEY)
    ) match {
      case Some(p2pEndpoint) =>
        val p2pEndpointId = p2pEndpoint.id
        logger.info(
          s"Found endpoint header and adding it to the gRPC server context: $p2pEndpointId"
        )
        synchronizeWithClosingSync("P2PAddAuthTokenHeaderGrpcServerInterceptor.interceptCall") {
          val channel =
            createNettyClientChannelBuilder(clientChannelBuilder, p2pEndpoint.endpointConfig)
              .build()

          val authenticationServiceChannel =
            GrpcManagedChannel(
              s"server-authenticationServiceChannel-$p2pEndpoint",
              channel,
              this,
              loggerFactory.getTracedLogger(getClass),
            )
          liveChannels.add(authenticationServiceChannel).discard
          authenticationServiceChannel
        } match {
          case UnlessShutdown.Outcome(authenticationServiceChannel) =>
            // Add the endpoint info sent by the client (`externalAddress`) to the context, so we can use it to find
            //  a potentially existing outgoing connection rather than accepting the incoming one;
            //  if not found, we'll accept the incoming one, associate it with the endpoint
            //  and won't create an outgoing one when we need to send.
            val contextWithEndpoint =
              Context
                .current()
                .withValue(
                  P2PAddAuthTokenHeaderGrpcServerInterceptor.peerEndpointContextKey,
                  Some(p2pEndpoint),
                )
            logger.info(
              s"Intercepting incoming P2P call: authenticating this P2P server to the client reachable at $p2pEndpointId " +
                s"on newly created channel $authenticationServiceChannel"
            )
            Contexts.interceptCall(
              contextWithEndpoint,
              new P2PAuthenticatorServerCall(
                call,
                tokenProvider,
                p2pEndpointId,
                authenticationServiceChannel,
                liveChannels,
                synchronizerId,
                member,
                timeouts,
                loggerFactory,
              ),
              requestHeaders,
              next,
            )
          case UnlessShutdown.AbortedDueToShutdown =>
            logger.info(s"Not intercepting incoming P2P call as the interceptor is shutting down")
            next.startCall(call, requestHeaders)
        }

      case _ =>
        logger.error("No authenticated endpoint header found")
        call.close(Status.INTERNAL, new Metadata())
        new ServerCall.Listener[ReqT] {}
    }
  }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq(
      SyncCloseable("tokenProvider.close()", tokenProvider.close()),
      SyncCloseable(
        "liveChannels.close()",
        { liveChannels.foreach(_.close()); liveChannels.clear() },
      ),
    )
}

object P2PAddAuthTokenHeaderGrpcServerInterceptor {

  val peerEndpointContextKey: Context.Key[Option[P2PEndpoint]] =
    Context
      .keyWithDefault[Option[P2PEndpoint]]("bft-orderer-p2p-peer-endpoint", None)

  private class P2PAuthenticatorServerCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      tokenProvider: AuthenticationTokenProvider,
      p2pEndpointId: P2PEndpoint.Id,
      authenticationServiceChannel: GrpcManagedChannel,
      liveChannels: scala.collection.mutable.Set[GrpcManagedChannel],
      synchronizerId: PhysicalSynchronizerId,
      member: Member,
      timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext)
      extends GrpcSimpleForwardingServerCall[ReqT, RespT](call)
      with NamedLogging {

    private val closed = new java.util.concurrent.atomic.AtomicBoolean(false)

    private def cleanupChannel(): Unit =
      if (closed.compareAndSet(false, true)) {
        logger.debug(
          s"Closing the authentication service channel $authenticationServiceChannel " +
            s"for authenticating this P2P server to $p2pEndpointId"
        )
        liveChannels.remove(authenticationServiceChannel).discard
        authenticationServiceChannel.close()
      }

    override def sendHeaders(responseHeaders: Metadata): Unit = {
      Try {
        logger.info(
          s"Retrieving sequencer client authentication token to authenticate this P2P server to $p2pEndpointId"
        )

        val tokenFetcher =
          new ChannelTokenFetcher(
            tokenProvider,
            Endpoint(p2pEndpointId.address, p2pEndpointId.port),
            authenticationServiceChannel,
          )
        timeouts.network
          .awaitUS("tokenFetcher")( // Unfortunately, headers must be set synchronously
            tokenFetcher.apply
              .fold(
                error =>
                  logger.warn(
                    s"Failed to fetch P2P server authentication token to $p2pEndpointId: $error"
                  ),
                { tokenWithExpiry =>
                  logger.debug(
                    s"Setting P2P server authentication token to $p2pEndpointId into response headers"
                  )
                  SequencerClientTokenAuthentication
                    .authenticationMetadata(
                      synchronizerId,
                      member,
                      tokenWithExpiry.token,
                      into = responseHeaders,
                    )
                    .discard
                },
              )
          ) match {
          case UnlessShutdown.AbortedDueToShutdown =>
            logger.info(
              s"Aborted due to shutdown while trying to fetch P2P server authentication token to $p2pEndpointId"
            )
          case _ =>
            logger.info(
              s"Successfully retrieved token to authenticate P2P server to $p2pEndpointId " +
                "and added authentication headers"
            )
        }
      }.fold(
        logger.warn(
          s"Timed out while trying to fetch P2P server authentication token to $p2pEndpointId",
          _,
        ),
        _ => (),
      )

      cleanupChannel()

      logger.debug(s"Sending server response headers to $p2pEndpointId")
      super.sendHeaders(responseHeaders)
    }

    override def close(status: Status, trailers: Metadata): Unit =
      try {
        super.close(status, trailers)
      } finally {
        cleanupChannel() // Ensures channel is closed if sendHeaders was never called
      }
  }
}
