// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.data.EitherT
import cats.implicits.toFoldableOps
import cats.instances.future.*
import cats.syntax.bifunctor.*
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, SynchronizerSuccessor}
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.protocol.messages.{
  ProtocolMessage,
  RootHashMessage,
  SerializedRootHashMessagePayload,
}
import com.digitalasset.canton.protocol.{DynamicSynchronizerParametersWithValidity, RequestId}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.RichSequencerClient
import com.digitalasset.canton.sequencing.handlers.DiscardIgnoredEvents
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, OpenEnvelope, SequencedEvent}
import com.digitalasset.canton.store.CursorPrehead.SequencerCounterCursorPrehead
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.{SequencedEventStore, SequencerCounterTrackerStore}
import com.digitalasset.canton.synchronizer.LsuSequencingTestMessageHandler
import com.digitalasset.canton.synchronizer.mediator.Mediator.PruningError
import com.digitalasset.canton.synchronizer.mediator.store.MediatorState
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.time.{Clock, SynchronizerTimeTracker, TimeAwaiter}
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.{
  MediatorId,
  PhysicalSynchronizerId,
  SynchronizerOutboxHandle,
  SynchronizerTopologyManager,
  TopologyManagerStatus,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, FutureUnlessShutdownUtil, FutureUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.concurrent.ExecutionContext

/** Responsible for events processing. Reads mediator confirmation requests and confirmation
  * responses from a sequencer and produces ConfirmationResultMessages. For scaling /
  * high-availability, several instances need to be created.
  *
  * ==Crash Recovery==
  *
  * The mediator is crash-fault tolerant: if it crashes before finalizing a request, crash recovery
  * replays that request from the sequenced event store. This is achieved by chaining a
  * [[com.digitalasset.canton.lifecycle.PromiseUnlessShutdown]] (`finalizedPromise`) into the
  * confirmation request event's async processing result. The clean sequencer counter only advances
  * once this promise completes, which happens only after the finalized response is persisted to the
  * DB ([[com.digitalasset.canton.synchronizer.mediator.store.MediatorState.storeFinalized]]).
  *
  * Out-of-order finalization is handled by
  * [[com.digitalasset.canton.sequencing.handlers.CleanSequencerCounterTracker]]'s Peano queue: the
  * clean prehead only advances to the oldest unfinished event's predecessor, regardless of which
  * later events complete first.
  *
  * '''Known limitation''': There is a narrow crash window between persisting the finalized response
  * and confirming the verdict was sequenced. If the mediator crashes after `storeFinalized` but
  * before the verdict send completes, on restart the verdict may not be re-sent (response event
  * replays against an already-finalized DB entry, which no-ops). Impact: participants timeout and
  * treat the transaction as rejected.
  */
private[mediator] class Mediator(
    val mediatorId: MediatorId,
    @VisibleForTesting
    val sequencerClient: RichSequencerClient,
    val topologyClient: SynchronizerTopologyClientWithInit,
    private[canton] val syncCrypto: SynchronizerCryptoClient,
    topologyTransactionProcessor: TopologyTransactionProcessor,
    val topologyManager: SynchronizerTopologyManager,
    val topologyManagerStatus: TopologyManagerStatus,
    val synchronizerOutboxHandle: SynchronizerOutboxHandle,
    val timeTracker: SynchronizerTimeTracker,
    val state: MediatorState,
    asynchronousProcessing: Boolean,
    private[canton] val sequencerCounterTrackerStore: SequencerCounterTrackerStore,
    sequencedEventStore: SequencedEventStore,
    parameters: CantonNodeParameters,
    clock: Clock,
    val metrics: MediatorMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with FlagCloseableAsync
    with HasCloseContext {

  val lsuSuccessorAfterUpgradeTime: Mediator.LsuSuccessorAfterUpgradeTime =
    new Mediator.LsuSuccessorAfterUpgradeTime {
      override def apply(ts: CantonTimestamp)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Option[SynchronizerSuccessor]] = for {
        snapshot <- syncCrypto.awaitSnapshot(ts)
        lsuO <- snapshot.ipsSnapshot.announcedLsu()
        activeSuccessor = lsuO.collect { case (s, _) if s.upgradeTime <= ts => s }
      } yield activeSuccessor
    }

  def psid: PhysicalSynchronizerId = sequencerClient.psid
  def protocolVersion: ProtocolVersion = sequencerClient.protocolVersion

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  /** In-memory cache of the latest clean prehead timestamp. Seeded from the persisted prehead on
    * startup, then kept in sync by [[onCleanSequencerCounterHandler]]. Allows the [[TimeAwaiter]]
    * to read the current watermark without any DB lookup.
    */
  @VisibleForTesting
  private[canton] val cleanPreheadTimestamp: AtomicReference[CantonTimestamp] =
    new AtomicReference(CantonTimestamp.MinValue)

  /** Watermark for the inspection service: safe to query verdicts with request time <= this value.
    * Driven by the clean sequencer counter prehead, which advances only after every finalized
    * response for requests up to that point has been persisted to the DB (via finalizedPromise).
    */
  private val recordOrderTimeAwaiter: TimeAwaiter = new TimeAwaiter(
    getCurrentKnownTime = () => cleanPreheadTimestamp.get(),
    timeouts = parameters.processingTimeouts,
    loggerFactory = loggerFactory,
  )

  /** Return the current watermark until which verdicts are safe to be served on the API
    */
  def getCurrentWatermark: CantonTimestamp = recordOrderTimeAwaiter.getCurrentKnownTime()

  /** Wait for the watermark to reach the provided timestamp. If it's already reached, returns a
    * None, otherwise, a future that will complete when the watermark reaches the timestamp.
    */
  def awaitWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] = recordOrderTimeAwaiter.awaitKnownTimestamp(timestamp)

  private val delayLogger =
    new DelayLogger(
      clock,
      logger,
      parameters.delayLoggingThreshold,
      metrics.sequencerClient.handler.sequencingTimeMetrics,
    )

  private val verdictSender =
    VerdictSender(sequencerClient, syncCrypto, mediatorId, parameters.batchingConfig, loggerFactory)

  private val processor = new ConfirmationRequestAndResponseProcessor(
    mediatorId,
    verdictSender,
    syncCrypto,
    timeTracker,
    state,
    asynchronousProcessing = asynchronousProcessing,
    loggerFactory,
    timeouts,
    parameters.batchingConfig,
    futureSupervisor,
  )

  private val deduplicator = MediatorEventDeduplicator.create(
    state,
    verdictSender,
    syncCrypto.ips,
    protocolVersion,
    metrics,
    loggerFactory,
  )

  private val lsuTestSequencingMessageHandler =
    new LsuSequencingTestMessageHandler(metrics, syncCrypto, loggerFactory)

  private val eventsProcessor = new MediatorEventsProcessor(
    topologyTransactionProcessor.createHandler(psid),
    lsuTestSequencingMessageHandler,
    processor,
    deduplicator,
    loggerFactory,
  )

  val stateInspection: MediatorStateInspection = new MediatorStateInspection(state)

  /** Starts the mediator. NOTE: Must only be called at most once on a mediator instance. */
  private[mediator] def start()(implicit
      initializationTraceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = synchronizeWithClosing("start") {
    for {
      preheadO <- sequencerCounterTrackerStore.preheadSequencerCounter
      _ = preheadO.map(_.timestamp).foreach(cleanPreheadTimestamp.set)
      nextTs = preheadO.fold(CantonTimestamp.MinValue)(_.timestamp.immediateSuccessor)
      _ <- state.deduplicationStore.initialize(nextTs)
      _ <-
        sequencerClient.subscribeTracking(
          sequencerCounterTrackerStore,
          DiscardIgnoredEvents(loggerFactory)(handler),
          timeTracker,
          onCleanHandler = onCleanSequencerCounterHandler,
        )

    } yield ()
  }

  private def onCleanSequencerCounterHandler(
      newTracedPrehead: Traced[SequencerCounterCursorPrehead]
  ): Unit = newTracedPrehead.withTraceContext { implicit traceContext => newPrehead =>
    // Update the in memory clean pre-head
    cleanPreheadTimestamp.set(newPrehead.timestamp)
    // Advance the in-memory watermark and unblock any inspection service streams waiting on it.
    recordOrderTimeAwaiter.notifyAwaitedFutures(newPrehead.timestamp)
    FutureUtil.doNotAwait(
      synchronizeWithClosing("prune mediator deduplication store")(
        state.deduplicationStore.prune(newPrehead.timestamp)
      ).onShutdown(logger.info("Not pruning the mediator deduplication store due to shutdown")),
      "pruning the mediator deduplication store failed",
    )
  }

  /** Prune all unnecessary data from the mediator state and sequenced events store. Will validate
    * the provided timestamp is before the prehead position of the sequenced events store, meaning
    * that all events up until this point have completed processing and can be safely removed.
    */
  @nowarn("cat=deprecation")
  def prune(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, PruningError, Unit] =
    for {
      preHeadCounterO <- EitherT
        .right(sequencerCounterTrackerStore.preheadSequencerCounter)
      preHeadTsO = preHeadCounterO.map(_.timestamp)
      cleanTimestamp <- EitherT
        .fromOption(preHeadTsO, PruningError.NoDataAvailableForPruning)
        .leftWiden[PruningError]
        .mapK(FutureUnlessShutdown.outcomeK)

      _ <- EitherT
        .cond[FutureUnlessShutdown](
          timestamp <= cleanTimestamp,
          (),
          PruningError.CannotPruneAtTimestamp(timestamp, cleanTimestamp),
        )

      synchronizerParametersChanges <- EitherT
        .right(
          topologyClient
            .awaitSnapshot(timestamp)
            .flatMap(snapshot => snapshot.listDynamicSynchronizerParametersChanges())
        )

      _ <- NonEmpty.from(synchronizerParametersChanges) match {
        case Some(synchronizerParametersChangesNes) =>
          prune(
            pruneAt = timestamp,
            cleanTimestamp = cleanTimestamp,
            synchronizerParametersChanges = synchronizerParametersChangesNes,
          )

        case None =>
          logger.info(
            s"No synchronizer parameters found for pruning at $timestamp. This is likely due to $timestamp being before synchronizer bootstrapping. Will not prune."
          )
          EitherT.pure[FutureUnlessShutdown, PruningError](())
      }

    } yield ()

  private def prune(
      pruneAt: CantonTimestamp,
      cleanTimestamp: CantonTimestamp,
      synchronizerParametersChanges: NonEmpty[Seq[DynamicSynchronizerParametersWithValidity]],
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, PruningError, Unit] = {
    val latestSafePruningTs = Mediator.latestSafePruningTsBefore(
      synchronizerParametersChanges,
      cleanTimestamp,
    )

    for {
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        pruneAt <= latestSafePruningTs,
        PruningError.CannotPruneAtTimestamp(pruneAt, latestSafePruningTs),
      )

      _ = logger.debug(show"Pruning finalized responses up to [$pruneAt]")
      _ <- EitherT.right(state.prune(pruneAt))
      _ = logger.debug(show"Pruning sequenced event up to [$pruneAt]")
      _ <- EitherT.right(sequencedEventStore.prune(pruneAt))

      // After pruning successfully, update the "max-event-age" metric
      // looking up the oldest event (in case prunedAt precedes any events and nothing was pruned).
      oldestEventTimestampO <- EitherT.right(
        stateInspection.findPruningTimestamp(NonNegativeInt.zero)
      )
      _ = MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, oldestEventTimestampO)

    } yield ()
  }

  private def handler: UnthrottledApplicationHandler[OrdinaryEnvelopeBox, ClosedEnvelope] =
    new UnthrottledApplicationHandler[OrdinaryEnvelopeBox, ClosedEnvelope] {

      override def name: String = s"mediator-$mediatorId"

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          synchronizerTimeTracker: SynchronizerTimeTracker,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        topologyTransactionProcessor.subscriptionStartsAt(start, synchronizerTimeTracker)

      private def sendMalformedRejection(
          rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
          timestamp: CantonTimestamp,
          verdict: MediatorVerdict.MediatorReject,
      )(implicit tc: TraceContext): FutureUnlessShutdown[Unit] = {
        val requestId = RequestId(timestamp)

        for {
          snapshot <- syncCrypto.awaitSnapshot(timestamp)
          synchronizerParameters <- snapshot.ipsSnapshot
            .findDynamicSynchronizerParameters()
            .flatMap(_.toFutureUS(new RuntimeException(_)))

          decisionTime <- synchronizerParameters.decisionTimeForF(timestamp)
          _ <- verdictSender.sendReject(
            requestId,
            None,
            rootHashMessages,
            verdict.toVerdict(protocolVersion),
            decisionTime,
          )
        } yield ()
      }

      override def apply(
          tracedEvents: Traced[Seq[BoxedEnvelope[OrdinarySequencedEvent, ClosedEnvelope]]]
      ): HandlerResult =
        tracedEvents.withTraceContext { implicit traceContext => events =>
          // update the delay logger using the latest event we've been handed
          events.lastOption.foreach(e => delayLogger.checkForDelay(e))

          val tracedOpenEventsWithRejectionsF = events.map { closedSignedEvent =>
            val closedEvent = closedSignedEvent.signedEvent.content

            val (openEvent, openingErrors) = SequencedEvent.openEnvelopes(closedEvent)(
              protocolVersion,
              syncCrypto.crypto.pureCrypto,
            )

            val rejectionsF =
              MonadUtil.parTraverseWithLimit_(parameters.batchingConfig.parallelism)(
                openingErrors
              ) { error =>
                val cause =
                  s"Received an envelope at ${closedEvent.timestamp} that cannot be opened. Discarding envelope... Reason: $error"
                val alarm = MediatorError.MalformedMessage.Reject(cause)
                alarm.report()

                val rootHashMessages = openEvent.envelopes.mapFilter(
                  ProtocolMessage.select[RootHashMessage[SerializedRootHashMessagePayload]]
                )

                if (rootHashMessages.nonEmpty) {
                  // In this case, we assume it is a Mediator Confirmation Request message
                  sendMalformedRejection(
                    rootHashMessages,
                    closedEvent.timestamp,
                    MediatorVerdict.MediatorReject(alarm),
                  )
                } else FutureUnlessShutdown.unit
              }

            (
              WithCounter(
                closedSignedEvent.counter,
                Traced(openEvent)(closedSignedEvent.traceContext),
              ),
              rejectionsF,
            )
          }

          val (tracedOpenEvents, rejectionsF) = tracedOpenEventsWithRejectionsF.unzip
          logger.debug(s"Processing ${tracedOpenEvents.size} events for the mediator")

          val result = FutureUnlessShutdownUtil.logOnFailureUnlessShutdown(
            eventsProcessor.handle(tracedOpenEvents),
            "Failed to handle Mediator events",
            closeContext = Some(closeContext),
          )

          rejectionsF.sequence_.flatMap { case () => result }
        }
    }

  override def closeAsync() =
    Seq(
      SyncCloseable(
        "mediator",
        LifeCycle.close(
          topologyManager,
          topologyTransactionProcessor,
          syncCrypto,
          timeTracker,
          processor,
          sequencerClient,
          topologyClient,
          sequencerCounterTrackerStore,
          recordOrderTimeAwaiter,
          state,
        )(logger),
      )
    )
}

private[mediator] object Mediator {

  /** LsuSuccessorAfterUpgradeTime gives us the successor to the current physical synchronizer id,
    * iff the provided timestamp is past the upgrade time. Otherwise it returns None.
    */
  trait LsuSuccessorAfterUpgradeTime {
    def apply(at: CantonTimestamp)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Option[SynchronizerSuccessor]]
  }

  sealed trait PruningError {
    def message: String
  }
  object PruningError {

    /** The mediator has not yet processed enough data for any to be available for pruning */
    case object NoDataAvailableForPruning extends PruningError {
      lazy val message: String = "There is no mediator data available for pruning"
    }

    /** The mediator can prune some data but data for the requested timestamp cannot yet be removed
      */
    final case class CannotPruneAtTimestamp(
        requestedTimestamp: CantonTimestamp,
        earliestPruningTimestamp: CantonTimestamp,
    ) extends PruningError {
      override def message: String =
        show"Requested pruning timestamp [$requestedTimestamp] is later than the earliest available pruning timestamp [$earliestPruningTimestamp]"
    }
  }

  /** Returns the latest safe pruning timestamp on behalf of requests governed by the provided
    * synchronizer parameters and relative to the clean timestamp and the timeout determined by the
    * "confirmationResponseTimeout" synchronizer parameter.
    *
    * If no requests can be pending anymore (or "yet" in the case of future parameters), return the
    * most "permissive" safe pruning timestamp consisting of the clean timestamp.
    */
  private[mediator] def latestSafePruningTsForSynchronizerParameters(
      synchronizerParameters: DynamicSynchronizerParametersWithValidity,
      cleanTs: CantonTimestamp,
  ): CantonTimestamp = {
    lazy val timeout = synchronizerParameters.parameters.confirmationResponseTimeout
    lazy val cappedSafePruningTs = synchronizerParameters.validFrom.max(cleanTs - timeout)

    if (cleanTs <= synchronizerParameters.validFrom) // If these parameters apply only to the future
      cleanTs
    else {
      synchronizerParameters.validUntil match {
        case None => cappedSafePruningTs
        case Some(validUntil) =>
          // cleanTs falls within the validity period of the synchronizer parameters
          if (cleanTs <= validUntil) cappedSafePruningTs
          // requests governed by the synchronizer parameters have all been processed completely
          else if (validUntil + timeout <= cleanTs) cleanTs
          // some pending requests governed by the synchronizer parameters could still time out
          else cappedSafePruningTs
      }
    }
  }

  /** Returns the latest safe pruning ts which is <= cleanTs */
  private[mediator] def latestSafePruningTsBefore(
      allSynchronizerParametersChanges: NonEmpty[Seq[DynamicSynchronizerParametersWithValidity]],
      cleanTs: CantonTimestamp,
  ): CantonTimestamp = allSynchronizerParametersChanges
    .map(latestSafePruningTsForSynchronizerParameters(_, cleanTs))
    .min1
}
