// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.traverse.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerPredecessor}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.{StaticSynchronizerParameters, SynchronizerParametersLookup}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.ReplayAction.SequencerSends
import com.digitalasset.canton.sequencing.client.SequencerClient.SequencerTransports
import com.digitalasset.canton.sequencing.client.pool.SequencerConnectionPool
import com.digitalasset.canton.sequencing.client.transports.replay.ReplayClientImpl
import com.digitalasset.canton.sequencing.protocol.{GetTrafficStateForMemberRequest, TrafficState}
import com.digitalasset.canton.sequencing.traffic.{EventCostCalculator, TrafficStateController}
import com.digitalasset.canton.store.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.SynchronizerTopologyClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer
import org.slf4j.event.Level

import scala.concurrent.*

trait SequencerClientFactory {
  def create(
      member: Member,
      sequencedEventStore: SequencedEventStore,
      sendTrackerStore: SendTrackerStore,
      requestSigner: RequestSigner,
      sequencerConnections: SequencerConnections,
      synchronizerPredecessor: Option[SynchronizerPredecessor],
      connectionPool: SequencerConnectionPool,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, SequencerClientFactory.CreateError, RichSequencerClient]

}

object SequencerClientFactory {
  // Left holds a traffic state error, which can be retried. Right d
  sealed trait CreateError
  final case class RetryableError(message: String) extends CreateError
  final case class NonRetryableError(message: String) extends CreateError

  def apply(
      psid: PhysicalSynchronizerId,
      syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
      config: SequencerClientConfig,
      testingConfig: TestingConfigInternal,
      synchronizerParameters: StaticSynchronizerParameters,
      processingTimeout: ProcessingTimeout,
      clock: Clock,
      topologyClient: SynchronizerTopologyClient,
      futureSupervisor: FutureSupervisor,
      recordingConfigForMember: Member => Option[RecordingConfig],
      replayConfigForMember: Member => Option[ReplayConfig],
      metrics: SequencerClientMetrics,
      loggingConfig: LoggingConfig,
      exitOnFatalErrors: Boolean,
      namedLoggerFactory: NamedLoggerFactory,
  ): SequencerClientFactory =
    new SequencerClientFactory with NamedLogging {
      override protected def loggerFactory: NamedLoggerFactory = namedLoggerFactory

      override def create(
          member: Member,
          sequencedEventStore: SequencedEventStore,
          sendTrackerStore: SendTrackerStore,
          requestSigner: RequestSigner,
          sequencerConnections: SequencerConnections,
          synchronizerPredecessor: Option[SynchronizerPredecessor],
          connectionPool: SequencerConnectionPool,
      )(implicit
          executionContext: ExecutionContextExecutor,
          executionSequencerFactory: ExecutionSequencerFactory,
          materializer: Materializer,
          tracer: Tracer,
          traceContext: TraceContext,
          closeContext: CloseContext,
      ): EitherT[FutureUnlessShutdown, CreateError, RichSequencerClient] = {
        // initialize recorder if it's been configured for the member (should only be used for testing)
        val recorderO = recordingConfigForMember(member).map { recordingConfig =>
          new SequencerClientRecorder(
            recordingConfig.fullFilePath,
            processingTimeout,
            loggerFactory,
          )
        }
        val sequencerSynchronizerParamsLookup =
          SynchronizerParametersLookup.forSequencerSynchronizerParameters(
            config.overrideMaxRequestSize,
            topologyClient,
            loggerFactory,
          )

        replayConfigForMember(member) match {
          case Some(ReplayConfig(recording, replaySendsConfig: SequencerSends)) =>
            logger.debug(s"Building replay client with config $replaySendsConfig")
            syncCryptoApi.currentSnapshotApproximation.map { currentSnapshotApproximation =>
              // The replay client registers itself with the config
              new ReplayClientImpl(
                synchronizerParameters.protocolVersion,
                recording.fullFilePath,
                replaySendsConfig,
                member,
                connectionPool,
                requestSigner,
                currentSnapshotApproximation,
                clock,
                metrics,
                processingTimeout,
                loggerFactory,
              )
            }.discard
          case _ =>
        }

        def getTrafficStateWithConnectionPool(
            ts: CantonTimestamp
        ): EitherT[FutureUnlessShutdown, CreateError, Option[TrafficState]] =
          for {
            connections <- EitherT.fromEither[FutureUnlessShutdown](
              NonEmpty
                .from(connectionPool.getOneConnectionPerSequencer("get-traffic-state"))
                .toRight(
                  NonRetryableError(
                    s"No connection available to retrieve traffic state from synchronizer for $member"
                  )
                )
            )
            result <- BftSender
              .makeRequest(
                s"Retrieving traffic state from synchronizer for $member at $ts",
                futureSupervisor,
                logger,
                operators = connections,
                threshold = sequencerConnections.sequencerTrustThreshold,
              )(
                performRequest = _.getTrafficStateForMember(
                  // Request the traffic state at the timestamp immediately following the last sequenced event timestamp
                  // That's because we will not re-process that event, but if it was a traffic purchase, the sequencer
                  // would return a state with the previous extra traffic value, because traffic purchases only become
                  // valid _after_ they've been sequenced. This ensures the participant doesn't miss a traffic purchase
                  // if it gets disconnected just after reading one.
                  GetTrafficStateForMemberRequest(
                    member,
                    ts.immediateSuccessor,
                    synchronizerParameters.protocolVersion,
                  ),
                  timeout = processingTimeout.network.duration,
                  // On LSU these calls may fail and be noisy. We rather let the caller decide what to do with the error.
                  logPolicy = CantonGrpcUtil.SilentLogPolicy,
                ).map(_.trafficState)
              )(identity)
              .leftMap[CreateError] { err =>
                RetryableError(
                  s"Failed to retrieve traffic state from synchronizer for $member: $err"
                )
              }

          } yield result

        val sequencerTransports = SequencerTransports.from(
          sequencerConnections.sequencerTrustThreshold,
          sequencerConnections.sequencerLivenessMargin,
          sequencerConnections.submissionRequestAmplification,
          sequencerConnections.sequencerConnectionPoolDelays,
        )
        for {
          // Reinitialize the sequencer counter allocator to ensure that passive->active replica transitions
          // correctly track the counters produced by other replicas
          _ <- EitherT.right(
            sequencedEventStore.reinitializeFromDbOrSetLowerBound()
          )
          // Find the timestamp of the last known sequenced event, we'll use that timestamp to initialize
          // the traffic state
          latestSequencedTimestampO <- EitherT.right(
            sequencedEventStore
              .find(SequencedEventStore.LatestUpto(CantonTimestamp.MaxValue))
              .toOption
              .value
              .map(_.map(_.timestamp))
          )

          // Use the current snapshot approximation to determine if traffic control is enabled on the synchronizer
          // at this time
          trafficControlEnabled <- EitherT[FutureUnlessShutdown, CreateError, Boolean](
            syncCryptoApi.currentSnapshotApproximation
              .flatMap(
                _.ipsSnapshot.findDynamicSynchronizerParameters().map {
                  case Left(failure) =>
                    // This happens the first time a node connects to a synchronizer and has not synced its topology yet
                    // In that case assume traffic control is enabled, so that we initialize our traffic state in case it is in fact enabled
                    logger.info(
                      s"Failed to retrieve dynamic synchronizer parameters during sequencer client initialization. Assuming traffic control is enabled. $failure"
                    )
                    Right(true)
                  case Right(value) =>
                    Right(value.parameters.trafficControl.isDefined)
                }
              )
          )

          getTrafficStateFromSynchronizerFn =
            // If traffic control is not enabled, there's no need to even ask the sequencers to initialize the traffic state
            // If it gets enabled later, the node will become aware by receiving the topology change
            // and start receiving traffic receipts from the sequencer for messages it sends
            if (!trafficControlEnabled) { (_: CantonTimestamp) =>
              EitherT.pure[FutureUnlessShutdown, CreateError](None)
            } else getTrafficStateWithConnectionPool _

          // Make a BFT call to all the transports to retrieve the current traffic state from the synchronizer
          // and initialize the trafficStateController with it
          trafficInitTimestampO = latestSequencedTimestampO
            .orElse(
              synchronizerPredecessor.map(
                _.upgradeTime
              )
            )

          _ = logger.info(
            s"Initializing traffic state at timestamp: $trafficInitTimestampO"
          )

          getTrafficStateFromSynchronizerWithRetryFn = { (ts: CantonTimestamp) =>
            EitherT[FutureUnlessShutdown, CreateError, Option[TrafficState]](
              retry
                .Backoff(
                  logger,
                  closeContext.context,
                  maxRetries = retry.Forever,
                  initialDelay = config.startupConnectionRetryDelay.asFiniteApproximation,
                  maxDelay = config.maxConnectionRetryDelay.asFiniteApproximation,
                  "Traffic State Initialization",
                  s"Initialize traffic state from a BFT read with threshold ${sequencerConnections.sequencerTrustThreshold} from ${sequencerConnections.connections.length} total connections",
                  retryLogLevel = Some(Level.INFO),
                )
                .unlessShutdown(
                  getTrafficStateFromSynchronizerFn(ts).value,
                  AllExceptionRetryPolicy,
                )
            )
          }

          trafficStateO <- trafficInitTimestampO
            .flatTraverse(getTrafficStateFromSynchronizerWithRetryFn)

          // fetch the initial set of pending sends to initialize the client with.
          // as it owns the client that should be writing to this store it should not be racy.
          initialPendingSends = sendTrackerStore.fetchPendingSends
          trafficStateController = new TrafficStateController(
            member,
            loggerFactory,
            syncCryptoApi,
            trafficStateO.getOrElse(TrafficState.empty(CantonTimestamp.Epoch)),
            synchronizerParameters.protocolVersion,
            new EventCostCalculator(loggerFactory),
            metrics.trafficConsumption,
            psid,
          )
          sendTracker = new SendTracker(
            initialPendingSends,
            sendTrackerStore,
            metrics,
            loggerFactory,
            processingTimeout,
            Some(trafficStateController),
          )
          // pluggable send approach to support transitioning to the new async sends
          validatorFactory = new SequencedEventValidatorFactory {
            override def create(loggerFactory: NamedLoggerFactory)(implicit
                traceContext: TraceContext
            ): SequencedEventValidator =
              if (config.skipSequencedEventValidation) {
                SequencedEventValidator.noValidation(psid)(
                  NamedLoggingContext(loggerFactory, traceContext)
                )
              } else {
                new SequencedEventValidatorImpl(
                  psid,
                  syncCryptoApi,
                  loggerFactory,
                  processingTimeout,
                )
              }
          }
        } yield new RichSequencerClientImpl(
          psid,
          synchronizerPredecessor,
          member,
          sequencerTransports,
          connectionPool,
          config,
          testingConfig,
          sequencerSynchronizerParamsLookup,
          processingTimeout,
          validatorFactory,
          clock,
          requestSigner,
          sequencedEventStore,
          sendTracker,
          metrics,
          recorderO,
          replayConfigForMember(member),
          syncCryptoApi,
          loggingConfig,
          Some(trafficStateController),
          exitOnFatalErrors,
          loggerFactory,
          futureSupervisor,
        )
      }
    }
}
