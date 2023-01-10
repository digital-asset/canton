// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.{EitherT, NonEmptySeq}
import cats.instances.future.*
import cats.syntax.bifunctor.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.Mediator.PruningError
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  Lifecycle,
  StartAndCloseable,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.protocol.{DomainParameters, DynamicDomainParameters}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.handlers.{DiscardIgnoredEvents, EnvelopeOpener}
import com.digitalasset.canton.sequencing.protocol.Envelope
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.{SequencedEventStore, SequencerCounterTrackerStore}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, DomainTimeTrackerConfig}
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

/** The Mediator that acts as transaction coordinator. */
private[mediator] class Mediator(
    val domain: DomainId,
    val mediatorId: MediatorId,
    @VisibleForTesting
    val sequencerClient: SequencerClient,
    val topologyClient: DomainTopologyClientWithInit,
    private[canton] val syncCrypto: DomainSyncCryptoClient,
    topologyTransactionProcessor: TopologyTransactionProcessor,
    timeTrackerConfig: DomainTimeTrackerConfig,
    state: MediatorState,
    private[canton] val sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    sequencedEventStore: SequencedEventStore,
    parameters: CantonNodeParameters,
    protocolVersion: ProtocolVersion,
    clock: Clock,
    metrics: MediatorMetrics,
    readyCheck: MediatorReadyCheck,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with StartAndCloseable[Unit]
    with NoTracing {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  private val delayLogger =
    new DelayLogger(
      clock,
      logger,
      parameters.delayLoggingThreshold,
      metrics.sequencerClient.delay,
    )

  val timeTracker = DomainTimeTracker(timeTrackerConfig, clock, sequencerClient, loggerFactory)

  private val verdictSender =
    VerdictSender(sequencerClient, syncCrypto, protocolVersion, loggerFactory)

  private val processor = new ConfirmationResponseProcessor(
    domain,
    mediatorId,
    verdictSender,
    syncCrypto,
    timeTracker,
    state,
    protocolVersion,
    loggerFactory,
  )

  private val deduplicator = MediatorEventDeduplicator.create(
    state.deduplicationStore,
    verdictSender,
    syncCrypto.ips,
    protocolVersion,
    loggerFactory,
  )

  private val eventsProcessor = MediatorEventsProcessor(
    state,
    syncCrypto,
    topologyTransactionProcessor.createHandler(domain),
    processor,
    deduplicator,
    protocolVersion,
    readyCheck,
    loggerFactory,
  )

  val stateInspection: MediatorStateInspection = new MediatorStateInspection(state)

  override protected def startAsync(): Future[Unit] = for {

    preheadO <- sequencerCounterTrackerStore.preheadSequencerCounter
    nextTs = preheadO.fold(CantonTimestamp.MinValue)(_.timestamp.immediateSuccessor)
    _ <- state.deduplicationStore.initialize(nextTs)

    _ <- sequencerClient.subscribeTracking(
      sequencerCounterTrackerStore,
      DiscardIgnoredEvents(loggerFactory)(
        EnvelopeOpener[OrdinaryEnvelopeBox](protocolVersion, syncCrypto.crypto.pureCrypto)(handler)
      ),
      timeTracker,
      onCleanHandler = onCleanSequencerCounterHandler,
    )
  } yield ()

  private def onCleanSequencerCounterHandler(
      newTracedPrehead: Traced[SequencerCounterCursorPrehead]
  ): Future[Unit] = newTracedPrehead.withTraceContext { implicit traceContext => newPrehead =>
    performUnlessClosingF("prune mediator deduplication store")(
      state.deduplicationStore.prune(newPrehead.timestamp)
    ).onShutdown(logger.info("Not pruning the mediator deduplication store due to shutdown"))
  }

  /** Prune all unnecessary data from the mediator state and sequenced events store.
    * Will validate the provided timestamp is before the prehead position of the sequenced events store,
    * meaning that all events up until this point have completed processing and can be safely removed.
    */
  def prune(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, PruningError, Unit] =
    for {
      preHeadCounterO <- EitherT.right(sequencerCounterTrackerStore.preheadSequencerCounter)
      preHeadTsO = preHeadCounterO.map(_.timestamp)
      cleanTimestamp <- EitherT
        .fromOption(preHeadTsO, PruningError.NoDataAvailableForPruning)
        .leftWiden[PruningError]

      _ <- EitherT.cond(
        timestamp <= cleanTimestamp,
        (),
        PruningError.CannotPruneAtTimestamp(timestamp, cleanTimestamp),
      )

      domainParametersChanges <- EitherT.right(
        topologyClient.awaitSnapshot(timestamp).flatMap(_.listDynamicDomainParametersChanges())
      )

      _ <- NonEmptySeq.fromSeq(domainParametersChanges) match {
        case Some(domainParametersChangesNes) =>
          prune(
            pruneAt = timestamp,
            cleanTimestamp = cleanTimestamp,
            domainParametersChanges = domainParametersChangesNes,
          )

        case None =>
          logger.info(
            s"No domain parameters found for pruning at $timestamp. This is likely due to $timestamp being before domain bootstrapping. Will not prune."
          )
          EitherT.pure[Future, PruningError](())
      }

    } yield ()

  private def prune(
      pruneAt: CantonTimestamp,
      cleanTimestamp: CantonTimestamp,
      domainParametersChanges: NonEmptySeq[DomainParameters.WithValidity[DynamicDomainParameters]],
  ): EitherT[Future, PruningError, Unit] = {
    val latestSafePruningTsO = Mediator.latestSafePruningTsBefore(
      domainParametersChanges,
      cleanTimestamp,
    )

    for {
      _ <- EitherT.fromEither {
        latestSafePruningTsO
          .toRight(PruningError.MissingDomainParametersForValidPruningTsComputation(pruneAt))
          .flatMap { latestSafePruningTs =>
            Either.cond[PruningError, Unit](
              pruneAt <= latestSafePruningTs,
              (),
              PruningError.CannotPruneAtTimestamp(pruneAt, latestSafePruningTs),
            )
          }
      }

      _ = logger.debug(show"Pruning finalized responses up to [$pruneAt]")
      _ <- EitherT.right(state.prune(pruneAt))
      _ = logger.debug(show"Pruning sequenced event up to [$pruneAt]")
      _ <- EitherT.right(sequencedEventStore.prune(pruneAt).merge)
    } yield ()
  }

  private def handler: ApplicationHandler[Lambda[
    `+X <: Envelope[_]` => Traced[Seq[OrdinarySequencedEvent[X]]]
  ], DefaultOpenEnvelope] =
    new ApplicationHandler[Lambda[
      `+X <: Envelope[_]` => Traced[Seq[OrdinarySequencedEvent[X]]]
    ], DefaultOpenEnvelope] {
      override def name: String = s"mediator-${mediatorId}"

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          domainTimeTracker: DomainTimeTracker,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        topologyTransactionProcessor.subscriptionStartsAt(start, domainTimeTracker)

      override def apply(tracedEvents: Traced[Seq[OrdinaryProtocolEvent]]): HandlerResult = {
        tracedEvents.withTraceContext { implicit traceContext => events =>
          // update the delay logger using the latest event we've been handed
          events.lastOption.foreach(e => delayLogger.checkForDelay(e))

          logger.trace(s"Processing ${events.size} events for the mediator")
          eventsProcessor.handle(events)
        }
      }
    }

  override def closeAsync() =
    Seq(
      SyncCloseable(
        "mediator",
        Lifecycle.close(
          topologyTransactionProcessor,
          syncCrypto.ips,
          timeTracker,
          sequencerClient,
          syncCrypto.ips,
          topologyClient,
          sequencerCounterTrackerStore,
          state,
        )(logger),
      )
    )
}

private[mediator] object Mediator {
  sealed trait PruningError {
    def message: String
  }
  object PruningError {

    /** The mediator has not yet processed enough data for any to be available for pruning */
    case object NoDataAvailableForPruning extends PruningError {
      lazy val message: String = "There is no mediator data available for pruning"
    }

    /** Dynamic domain parameters available for ts were not found */
    case class MissingDomainParametersForValidPruningTsComputation(ts: CantonTimestamp)
        extends PruningError {
      override def message: String =
        show"Dynamic domain parameters to compute earliest available pruning timestamp not found for ts [$ts]"
    }

    /** The mediator can prune some data but data for the requested timestamp cannot yet be removed */
    case class CannotPruneAtTimestamp(
        requestedTimestamp: CantonTimestamp,
        earliestPruningTimestamp: CantonTimestamp,
    ) extends PruningError {
      override def message: String =
        show"Requested pruning timestamp [$requestedTimestamp] is later than the earliest available pruning timestamp [$earliestPruningTimestamp]"
    }
  }

  sealed trait PruningSafetyCheck extends Product with Serializable
  case object Safe extends PruningSafetyCheck
  case class SafeUntil(ts: CantonTimestamp) extends PruningSafetyCheck

  private[mediator] def checkPruningStatus(
      domainParameters: DomainParameters.WithValidity[DynamicDomainParameters],
      cleanTs: CantonTimestamp,
  ): PruningSafetyCheck = {
    lazy val timeout = domainParameters.parameter.participantResponseTimeout
    lazy val cappedSafePruningTs =
      CantonTimestamp.max(cleanTs - timeout, domainParameters.validFrom)

    if (cleanTs <= domainParameters.validFrom) // If these parameters apply only to the future
      Safe
    else {
      domainParameters.validUntil match {
        case None => SafeUntil(cappedSafePruningTs)
        case Some(validUntil) if cleanTs <= validUntil => SafeUntil(cappedSafePruningTs)
        case Some(validUntil) =>
          if (validUntil + timeout <= cleanTs) Safe else SafeUntil(cappedSafePruningTs)
      }
    }
  }

  /** Returns the latest safe pruning ts which is <= cleanTs */
  private[mediator] def latestSafePruningTsBefore(
      allDomainParametersChanges: NonEmptySeq[
        DomainParameters.WithValidity[DynamicDomainParameters]
      ],
      cleanTs: CantonTimestamp,
  ): Option[CantonTimestamp] = allDomainParametersChanges
    .map(checkPruningStatus(_, cleanTs))
    .collect { case SafeUntil(ts) => ts }
    .minOption
}
