// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{SynchronizerCrypto, SynchronizerCryptoClient}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientConfig}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.ManagedChannel

import scala.concurrent.ExecutionContextExecutor

/** The SequencerChannelClientFactory creates a SequencerChannelClient and its embedded GRPC channel
  * transports
  */
final class SequencerChannelClientFactory(
    synchronizerId: PhysicalSynchronizerId,
    synchronizerCryptoApi: SynchronizerCryptoClient,
    crypto: SynchronizerCrypto,
    config: SequencerClientConfig,
    traceContextPropagation: TracingConfig.Propagation,
    synchronizerParameters: StaticSynchronizerParameters,
    processingTimeout: ProcessingTimeout,
    clock: Clock,
    override protected val loggerFactory: NamedLoggerFactory,
    supportedProtocolVersions: Seq[ProtocolVersion],
) extends NamedLogging {
  def create(
      member: Member,
      sequencerConnections: SequencerConnections,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
  )(implicit
      executionContext: ExecutionContextExecutor,
      traceContext: TraceContext,
  ): Either[String, SequencerChannelClient] =
    makeChannelTransports(
      sequencerConnections,
      member,
      expectedSequencers,
    ).map(transportMap =>
      new SequencerChannelClient(
        member,
        new SequencerChannelClientState(transportMap, processingTimeout, loggerFactory),
        synchronizerCryptoApi,
        synchronizerParameters,
        processingTimeout,
        loggerFactory,
      )
    )

  private def makeChannelTransports(
      sequencerConnections: SequencerConnections,
      member: Member,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContextExecutor,
  ): Either[String, NonEmpty[Map[SequencerId, SequencerChannelClientTransport]]] = for {
    _ <- {
      val unexpectedSequencers = sequencerConnections.connections.collect {
        case conn if !expectedSequencers.contains(conn.sequencerAlias) =>
          conn.sequencerAlias
      }
      Either.cond(
        unexpectedSequencers.isEmpty,
        (),
        s"Missing sequencer id for alias(es): ${unexpectedSequencers.mkString(", ")}",
      )
    }
    transportsMap = sequencerConnections.connections.map { conn =>
      val sequencerId =
        expectedSequencers.getOrElse(
          conn.sequencerAlias,
          throw new IllegalStateException(
            s"Coding bug: Missing sequencer id for alias ${conn.sequencerAlias} should have been caught above"
          ),
        )
      sequencerId -> makeChannelTransport(conn, sequencerId, member)
    }.toMap
  } yield transportsMap

  private def makeChannelTransport(
      conn: SequencerConnection,
      sequencerId: SequencerId,
      member: Member,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContextExecutor,
  ): SequencerChannelClientTransport = {
    val loggerFactoryWithSequencerId =
      SequencerClient.loggerFactoryWithSequencerId(loggerFactory, sequencerId)
    conn match {
      case connection: GrpcSequencerConnection =>
        // TODO(i31759): Remove multiple endpoints from `SequencerConnection`
        if (connection.endpoints.sizeIs > 1) {
          logger.warn(
            s"Configuration for sequencer ${connection.sequencerAlias} defines more than one endpoint. " ++
              "This is deprecated and not supported. Only the first endpoint will be used."
          )
        }

        val channel = createChannel(connection)
        val auth = grpcSequencerClientAuth(
          connection.sequencerAlias,
          connection.endpoints.head1,
          channel,
          member,
        )
        new SequencerChannelClientTransport(
          channel,
          auth,
          processingTimeout,
          loggerFactoryWithSequencerId,
        )
    }
  }

  private def grpcSequencerClientAuth(
      sequencerAlias: SequencerAlias,
      endpoint: Endpoint,
      channel: ManagedChannel,
      member: Member,
  )(implicit executionContext: ExecutionContextExecutor): GrpcSequencerClientAuth =
    new GrpcSequencerClientAuth(
      synchronizerId,
      member,
      crypto,
      endpoint = endpoint,
      channel = channel,
      supportedProtocolVersions,
      config.authToken,
      clock,
      metricsO = None,
      metricsContext = MetricsContext.Empty,
      processingTimeout,
      SequencerClient.loggerFactoryWithSequencerAlias(
        loggerFactory,
        sequencerAlias,
      ),
    )

  /** Creates a GRPC-level managed channel (not to be confused with a sequencer channel).
    */
  private def createChannel(connection: GrpcSequencerConnection)(implicit
      executionContext: ExecutionContextExecutor
  ): ManagedChannel = {
    val channelBuilder = ClientChannelBuilder(
      SequencerClient.loggerFactoryWithSequencerAlias(loggerFactory, connection.sequencerAlias)
    )
    channelBuilder
      .create(
        connection.endpoints.head1,
        connection.transportSecurity,
        executionContext,
        connection.customTrustCertificates,
        config.clientChannelParams(traceContextPropagation),
      )
      .build()
  }
}
