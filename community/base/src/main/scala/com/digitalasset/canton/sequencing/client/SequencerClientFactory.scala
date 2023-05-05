// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import akka.stream.Materializer
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.{Crypto, SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLoggingContext,
}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.protocol.{DomainParametersLookup, StaticDomainParameters}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.ReplayAction.{SequencerEvents, SequencerSends}
import com.digitalasset.canton.sequencing.client.grpc.GrpcSequencerChannelBuilder
import com.digitalasset.canton.sequencing.client.transports.*
import com.digitalasset.canton.sequencing.client.transports.replay.{
  ReplayingEventsSequencerClientTransport,
  ReplayingSendsSequencerClientTransport,
}
import com.digitalasset.canton.sequencing.handshake.SequencerHandshake
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.CallOptions
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.*

trait SequencerClientFactory {
  def create(
      member: Member,
      sequencedEventStore: SequencedEventStore,
      sendTrackerStore: SendTrackerStore,
      requestSigner: RequestSigner,
      connection: SequencerConnection,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClient]

}

object SequencerClientFactory {
  def apply(
      domainId: DomainId,
      sequencerId: SequencerId,
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      crypto: Crypto,
      agreedAgreementId: Option[ServiceAgreementId],
      config: SequencerClientConfig,
      traceContextPropagation: TracingConfig.Propagation,
      testingConfig: TestingConfigInternal,
      domainParameters: StaticDomainParameters,
      processingTimeout: ProcessingTimeout,
      clock: Clock,
      topologyClient: DomainTopologyClient,
      futureSupervisor: FutureSupervisor,
      recordingConfigForMember: Member => Option[RecordingConfig],
      replayConfigForMember: Member => Option[ReplayConfig],
      metrics: SequencerClientMetrics,
      loggingConfig: LoggingConfig,
      loggerFactory: NamedLoggerFactory,
      supportedProtocolVersions: Seq[ProtocolVersion],
      minimumProtocolVersion: Option[ProtocolVersion],
  ): SequencerClientFactory with SequencerClientTransportFactory =
    new SequencerClientFactory with SequencerClientTransportFactory {

      override def create(
          member: Member,
          sequencedEventStore: SequencedEventStore,
          sendTrackerStore: SendTrackerStore,
          requestSigner: RequestSigner,
          connection: SequencerConnection,
      )(implicit
          executionContext: ExecutionContextExecutor,
          materializer: Materializer,
          tracer: Tracer,
          traceContext: TraceContext,
      ): EitherT[Future, String, SequencerClient] = {
        // initialize recorder if it's been configured for the member (should only be used for testing)
        val recorderO = recordingConfigForMember(member).map { recordingConfig =>
          new SequencerClientRecorder(
            recordingConfig.fullFilePath,
            processingTimeout,
            loggerFactory,
          )
        }
        val sequencerDomainParamsLookup = DomainParametersLookup.forSequencerDomainParameters(
          domainParameters,
          config.overrideMaxRequestSize,
          topologyClient,
          futureSupervisor,
          loggerFactory,
        )

        for {
          transport <- makeTransport(
            connection,
            member,
            requestSigner,
          )
          // fetch the initial set of pending sends to initialize the client with.
          // as it owns the client that should be writing to this store it should not be racy.
          initialPendingSends <- EitherT.right(sendTrackerStore.fetchPendingSends)
          sendTracker = new SendTracker(
            initialPendingSends,
            sendTrackerStore,
            metrics,
            loggerFactory,
            processingTimeout,
          )
          // pluggable send approach to support transitioning to the new async sends
          validatorFactory = new SequencedEventValidatorFactory {
            override def create(
                initialLastEventProcessedO: Option[PossiblyIgnoredSerializedEvent],
                unauthenticated: Boolean,
            )(implicit loggingContext: NamedLoggingContext): SequencedEventValidator =
              if (config.skipSequencedEventValidation) {
                SequencedEventValidator.noValidation(domainId, sequencerId, processingTimeout)(
                  NamedLoggingContext(loggerFactory, TraceContext.empty)
                )
              } else {
                new SequencedEventValidatorImpl(
                  initialLastEventProcessedO,
                  unauthenticated,
                  config.optimisticSequencedEventValidation,
                  domainId,
                  sequencerId,
                  domainParameters.protocolVersion,
                  syncCryptoApi,
                  loggerFactory,
                  processingTimeout,
                )
              }
          }
        } yield new SequencerClientImpl(
          domainId,
          member,
          transport,
          config,
          testingConfig,
          domainParameters.protocolVersion,
          sequencerDomainParamsLookup,
          processingTimeout,
          validatorFactory,
          clock,
          requestSigner,
          sequencedEventStore,
          sendTracker,
          metrics,
          recorderO,
          replayConfigForMember(member).isDefined,
          syncCryptoApi.pureCrypto,
          loggingConfig,
          loggerFactory,
        )
      }

      override def makeTransport(
          connection: SequencerConnection,
          member: Member,
          requestSigner: RequestSigner,
      )(implicit
          executionContext: ExecutionContextExecutor,
          materializer: Materializer,
          traceContext: TraceContext,
      ): EitherT[Future, String, SequencerClientTransport] = {
        def mkRealTransport: EitherT[Future, String, SequencerClientTransport] =
          connection match {
            case grpc: GrpcSequencerConnection => grpcTransport(grpc, member).toEitherT
          }

        val transportEitherT: EitherT[Future, String, SequencerClientTransport] =
          replayConfigForMember(member) match {
            case None => mkRealTransport
            case Some(ReplayConfig(recording, SequencerEvents)) =>
              EitherT.rightT(
                new ReplayingEventsSequencerClientTransport(
                  domainParameters.protocolVersion,
                  recording.fullFilePath,
                  metrics,
                  processingTimeout,
                  loggerFactory,
                )
              )
            case Some(ReplayConfig(recording, replaySendsConfig: SequencerSends)) =>
              for {
                underlyingTransport <- mkRealTransport
              } yield new ReplayingSendsSequencerClientTransport(
                domainParameters.protocolVersion,
                recording.fullFilePath,
                replaySendsConfig,
                member,
                underlyingTransport,
                requestSigner,
                metrics,
                processingTimeout,
                loggerFactory,
              )
          }

        for {
          transport <- transportEitherT
          // handshake to check that sequencer client supports the protocol version required by the sequencer
          _ <- SequencerHandshake
            .handshake(
              supportedProtocolVersions,
              minimumProtocolVersion,
              transport,
              config,
              processingTimeout,
              loggerFactory,
            )
            .leftMap { error =>
              // make sure to close transport in case of handshake failure
              transport.close()
              error
            }
        } yield transport
      }

      private def grpcTransport(connection: GrpcSequencerConnection, member: Member)(implicit
          executionContext: ExecutionContextExecutor
      ): Either[String, SequencerClientTransport] = {
        def createChannel(conn: GrpcSequencerConnection) = {
          val channelBuilder = ClientChannelBuilder(loggerFactory)
          GrpcSequencerChannelBuilder(
            channelBuilder,
            conn,
            NonNegativeInt.maxValue, // we set this limit only on the sequencer node, to avoid restarting the client if this value is changed
            traceContextPropagation,
            config.keepAliveClient,
          )
        }

        val channel = createChannel(connection)
        val auth = {
          val channelPerEndpoint = connection.endpoints.map { endpoint =>
            val subConnection = connection.copy(endpoints = NonEmpty.mk(Seq, endpoint))
            endpoint -> createChannel(subConnection)
          }.toMap
          new GrpcSequencerClientAuth(
            domainId,
            member,
            crypto,
            agreedAgreementId,
            channelPerEndpoint,
            supportedProtocolVersions,
            config.authToken,
            clock,
            processingTimeout,
            loggerFactory,
          )
        }
        val callOptions = {
          // the wait-for-ready call option is added for when round-robin-ing through connections
          // so that if one of them gets closed, we try the next one instead of unnecessarily failing.
          // wait-for-ready semantics: https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md
          // this is safe for non-idempotent RPCs.
          if (connection.endpoints.length > 1) CallOptions.DEFAULT.withWaitForReady()
          else CallOptions.DEFAULT
        }
        Right(
          new GrpcSequencerClientTransport(
            channel,
            callOptions,
            auth,
            metrics,
            processingTimeout,
            loggerFactory,
            domainParameters.protocolVersion,
          )
        )
      }

      def validateTransport(
          connection: SequencerConnection,
          logWarning: Boolean,
      )(implicit
          executionContext: ExecutionContextExecutor,
          errorLoggingContext: ErrorLoggingContext,
          closeContext: CloseContext,
      ): EitherT[FutureUnlessShutdown, String, Unit] =
        SequencerClientTransportFactory.validateTransport(
          connection,
          traceContextPropagation,
          config,
          logWarning,
          loggerFactory,
        )

    }
}
