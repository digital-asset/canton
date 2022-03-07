// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.EitherT
import cats.instances.future._
import cats.syntax.bifunctor._
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.config.{LocalNodeParameters, ProcessingTimeout}
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.Mediator.PruningError
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.lifecycle.{Lifecycle, StartAndCloseable, SyncCloseable}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, LoggingAlarmStreamer}
import com.digitalasset.canton.sequencing._
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.handlers.{DiscardIgnoredEvents, EnvelopeOpener}
import com.digitalasset.canton.store.{SequencedEventStore, SequencerCounterTrackerStore}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, DomainTimeTrackerConfig}
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil._
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

/** The Mediator that acts as transaction coordinator. */
class Mediator(
    val domain: DomainId,
    val mediatorId: MediatorId,
    @VisibleForTesting
    val sequencerClient: SequencerClient,
    val topologyClient: DomainTopologyClientWithInit,
    syncCrypto: DomainSyncCryptoClient,
    topologyTransactionProcessor: TopologyTransactionProcessor,
    timeTrackerConfig: DomainTimeTrackerConfig,
    state: MediatorState,
    sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    sequencedEventStore: SequencedEventStore,
    parameters: LocalNodeParameters,
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
      metrics.sequencerClient.delay.metric,
    )

  val timeTracker = DomainTimeTracker(timeTrackerConfig, clock, sequencerClient, loggerFactory)

  private val processor = new ConfirmationResponseProcessor(
    domain,
    mediatorId,
    syncCrypto,
    sequencerClient,
    timeTracker,
    state,
    new LoggingAlarmStreamer(logger),
    loggerFactory,
  )

  private val eventsProcessor = MediatorEventsProcessor(
    state,
    syncCrypto,
    topologyTransactionProcessor.createHandler(domain),
    processor,
    readyCheck,
    loggerFactory,
  )

  val stateInspection: MediatorStateInspection = new MediatorStateInspection(state)

  override protected def startAsync(): Future[Unit] = {

    sequencerClient.subscribeTracking(
      sequencerCounterTrackerStore,
      DiscardIgnoredEvents(
        EnvelopeOpener[OrdinaryEnvelopeBox](syncCrypto.crypto.pureCrypto)(handle)
      ),
      timeTracker,
    )
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

      allDomainParametersChanges <- EitherT.right(
        topologyClient.listDynamicDomainParametersChanges()
      )

      latestSafePruningTsO = Mediator.latestSafePruningTsBefore(
        allDomainParametersChanges,
        cleanTimestamp,
      )

      _ <- EitherT.fromEither {
        latestSafePruningTsO
          .toRight(PruningError.MissingDataForValidPruningTsComputation(timestamp))
          .flatMap { latestSafePruningTs =>
            Either.cond[PruningError, Unit](
              timestamp <= latestSafePruningTs,
              (),
              PruningError.CannotPruneAtTimestamp(timestamp, latestSafePruningTs),
            )
          }
      }

      _ = logger.debug(show"Pruning finalized responses up to [$timestamp]")
      _ <- EitherT.right(state.prune(timestamp))
      _ = logger.debug(show"Pruning sequenced event up to [$timestamp]")
      _ <- EitherT.right(sequencedEventStore.prune(timestamp).merge)
    } yield ()

  private def handle(tracedEvents: Traced[Seq[OrdinaryProtocolEvent]]): HandlerResult = {
    tracedEvents.withTraceContext { implicit traceContext => events =>
      // update the delay logger using the latest event we've been handed
      events.lastOption.foreach(e => delayLogger.checkForDelay(e))

      logger.trace(s"Processing ${events.size} events for the mediator")
      eventsProcessor.handle(events)
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

object Mediator {
  sealed trait PruningError
  object PruningError {

    /** The mediator has not yet processed enough data for any to be available for pruning */
    case object NoDataAvailableForPruning extends PruningError

    /** Dynamic domain parameters available for ts were not found */
    case class MissingDataForValidPruningTsComputation(ts: CantonTimestamp) extends PruningError

    /** The mediator can prune some data but data for the requested timestamp cannot yet be removed */
    case class CannotPruneAtTimestamp(
        requestedTimestamp: CantonTimestamp,
        earliestPruningTimestamp: CantonTimestamp,
    ) extends PruningError
  }

  sealed trait PruningSafetyCheck extends Product with Serializable
  case object Safe extends PruningSafetyCheck
  case class SafeUntil(ts: CantonTimestamp) extends PruningSafetyCheck

  private[mediator] def checkPruningStatus(
      domainParameters: DynamicDomainParameters.WithValidity,
      cleanTs: CantonTimestamp,
  ): PruningSafetyCheck = {
    lazy val timeout = domainParameters.parameters.participantResponseTimeout
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
      allDomainParametersChanges: Seq[DynamicDomainParameters.WithValidity],
      cleanTs: CantonTimestamp,
  ): Option[CantonTimestamp] = allDomainParametersChanges
    .map(checkPruningStatus(_, cleanTs))
    .collect { case SafeUntil(ts) => ts }
    .minOption
}
