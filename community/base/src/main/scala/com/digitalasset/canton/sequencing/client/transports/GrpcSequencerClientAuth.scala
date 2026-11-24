// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SynchronizerCrypto
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerConnectionPoolMetrics
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.GrpcManagedChannel
import com.digitalasset.canton.sequencing.authentication.grpc.{
  AuthenticationTokenWithExpiry,
  SequencerClientTokenAuthentication,
}
import com.digitalasset.canton.sequencing.authentication.{
  AuthenticationTokenManagerConfig,
  AuthenticationTokenProvider,
  P2PAuthenticationTokenProvider,
}
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth.ChannelTokenFetcher
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.stub.AbstractStub
import io.grpc.{ManagedChannel, Status}

import scala.concurrent.ExecutionContext

/** Auth helpers when dealing with our custom authentication tokens.
  */
class GrpcSequencerClientAuth(
    synchronizerId: PhysicalSynchronizerId,
    member: Member,
    crypto: SynchronizerCrypto,
    endpoint: Endpoint,
    channel: ManagedChannel,
    supportedProtocolVersions: Seq[ProtocolVersion],
    tokenManagerConfig: AuthenticationTokenManagerConfig,
    clock: Clock,
    metricsO: Option[SequencerConnectionPoolMetrics],
    metricsContext: MetricsContext,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  private val grpcChannel = GrpcManagedChannel(
    s"grpc-client-auth-$endpoint",
    channel,
    this,
    logger,
  )

  private val tokenProvider =
    AuthenticationTokenProvider(
      synchronizerId,
      member,
      crypto,
      supportedProtocolVersions,
      tokenManagerConfig,
      metricsO,
      metricsContext,
      timeouts,
      loggerFactory,
    )

  // This is an outgoing connection, not a callback to an authenticated caller,
  //  so no verifier identity is known yet. Discover it from the challenge and bind the proof
  //  to the dialed endpoint.
  private val tokenFetcher = new ChannelTokenFetcher(tokenProvider, endpoint, grpcChannel)

  private val clientAuthentication =
    SequencerClientTokenAuthentication(
      synchronizerId,
      member,
      tokenFetcher,
      tokenProvider.isClosing,
      tokenManagerConfig,
      clock,
      loggerFactory,
    )

  def logout()(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit] =
    tokenProvider.logout(endpoint, grpcChannel)

  /** Wrap a grpc client with components to appropriately perform authentication */
  def apply[S <: AbstractStub[S]](client: S): S =
    clientAuthentication(client)

  override protected def onClosed(): Unit =
    LifeCycle.close(tokenProvider)(logger)
}

object GrpcSequencerClientAuth {

  trait TokenFetcher {
    def apply(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry]
  }

  /** @param expectedVerifierO
    *   For sequencers, pass the token-authenticated caller's sequencer ID when fetching a token
    *   through a P2P reverse-authentication callback. From PV38, the provider checks this identity
    *   before signing, preventing the caller from redirecting authentication to another sequencer
    *   and receiving its token. Leave unset for ordinary outgoing connections, which discover the
    *   verifier identity automatically. The legacy handshake below PV38 does not enforce this
    *   check.
    */
  final class ChannelTokenFetcher(
      tokenProvider: AuthenticationTokenProvider,
      endpoint: Endpoint,
      channel: GrpcManagedChannel,
      expectedVerifierO: Option[SequencerId] = None,
  ) extends TokenFetcher {

    override def apply(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, Status, AuthenticationTokenWithExpiry] =
      (tokenProvider, expectedVerifierO) match {
        case (provider: P2PAuthenticationTokenProvider, Some(expectedVerifier)) =>
          provider.generateTokenForPeer(endpoint, channel, expectedVerifier)
        case _ => tokenProvider.generateToken(endpoint, channel)
      }
  }
}
