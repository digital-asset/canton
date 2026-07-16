// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.SequencerAggregatorXImpl.EventAndOrdinal
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerFailure,
  EventAggregationError,
  EventValidationError,
}
import com.digitalasset.canton.sequencing.client.pool.SubscriptionHandlerTrait.SubscriptionLivenessStatus
import com.digitalasset.canton.sequencing.client.{
  DelaySequencedEvent,
  SequencedEventValidator,
  SequencerClientSubscriptionError,
}
import com.digitalasset.canton.sequencing.{
  DelayLogger,
  MaybeCompressedSerializedEvent,
  SequencerAggregator,
  SequencerClientRecorder,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.{SequencerAlias, time}

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

trait SubscriptionHandlerTrait {
  def handleEvent(
      serializedEvent: MaybeCompressedSerializedEvent
  ): FutureUnlessShutdown[Either[SequencerClientSubscriptionError, Unit]]

  def getLivenessStatus(latest: EventAndOrdinal): Option[SubscriptionLivenessStatus]
}

object SubscriptionHandlerTrait {
  final case class SubscriptionLivenessStatus(timestampDelta: Duration, ordinalDelta: Int)
}

class SubscriptionHandler private[sequencing] (
    clock: Clock,
    metrics: SequencerClientMetrics,
    applicationHandlerFailure: SingleUseCell[ApplicationHandlerFailure],
    recorderO: Option[SequencerClientRecorder],
    sequencerAggregator: SequencerAggregator,
    eventValidator: SequencedEventValidator,
    processingDelay: DelaySequencedEvent,
    initialPriorEventO: Option[EventAndOrdinal],
    sequencerAlias: SequencerAlias,
    sequencerId: SequencerId,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging
    with SubscriptionHandlerTrait {

  // Keep track of the last event that we processed. In the event the SequencerClient is recreated, we'll restart
  // from the last successfully processed event counter, and we'll validate it is still the last event we processed
  // and that we're not seeing a sequencer fork.
  private val priorEvent = new AtomicReference[Option[EventAndOrdinal]](initialPriorEventO)

  private val delayLogger = new DelayLogger(
    clock,
    logger,
    // Only feed the metric, but do not log warnings
    time.NonNegativeFiniteDuration.MaxValue,
    metrics.handler.connectionMetrics(sequencerAlias),
  )

  override def handleEvent(
      serializedEvent: MaybeCompressedSerializedEvent
  ): FutureUnlessShutdown[Either[SequencerClientSubscriptionError, Unit]] = {
    implicit val traceContext: TraceContext = serializedEvent.traceContext
    // Process the event only if no failure has been detected
    val resultET = applicationHandlerFailure.get.fold {
      recorderO.foreach(_.recordEvent(serializedEvent))

      // to ensure that we haven't forked since we last connected, we actually subscribe from the event we last
      // successfully processed and do another round of validations on it to ensure it's the same event we really
      // did last process. However if successful, there's no need to give it to the application handler or to store
      // it as we're really sure we've already processed it.
      // we'll also see the last event replayed whenever a new subscription starts.
      val isReplayOfPriorEvent =
        priorEvent.get().map(_.event.timestamp).contains(serializedEvent.timestamp)

      if (isReplayOfPriorEvent) {
        // just validate
        logger.debug(
          s"Do not handle event with timestamp ${serializedEvent.timestamp}, as it is replayed and has already been handled."
        )
        eventValidator
          .validateOnReconnect(priorEvent.get().map(_.event), serializedEvent, sequencerId)
          .leftMap[SequencerClientSubscriptionError](EventValidationError.apply)
          .map(_ => ())
      } else {
        logger.debug(
          s"Validating sequenced event coming from $sequencerId (alias = $sequencerAlias) with timestamp ${serializedEvent.timestamp}"
        )
        for {
          validatedEvent <- eventValidator
            .validate(priorEvent.get().map(_.event), serializedEvent, sequencerId)
            .leftMap[SequencerClientSubscriptionError](EventValidationError.apply)
          _ = logger.debug("Event validation completed successfully")
          _ <- EitherT.right(
            synchronizeWithClosingF("processing-delay")(processingDelay.delay(validatedEvent))
          )
          _ = logger.debug(s"Processing delay $processingDelay completed successfully")
          _ = priorEvent.updateAndGet {
            case None => Some(EventAndOrdinal.first(validatedEvent))
            case Some(current) => Some(current.next(validatedEvent))
          }
          _ = delayLogger.checkForDelay_(validatedEvent)

          _ <- EitherT(
            sequencerAggregator
              .combineAndMergeEvent(
                sequencerId,
                validatedEvent,
              )
          )
            .leftMap[SequencerClientSubscriptionError](EventAggregationError.apply)
          _ = logger.debug("Event combined and merged successfully by the sequencer aggregator")
        } yield ()
      }
    }(err => EitherT.leftT(err))

    resultET.value
  }

  def getLivenessStatus(latest: EventAndOrdinal): Option[SubscriptionLivenessStatus] =
    priorEvent.get.map { prior =>
      val timestampDelta = latest.event.timestamp - prior.event.timestamp
      // This is safe even in case of Int overrun: e.g. (Int.MaxValue + 7) - (Int.MaxValue - 3) = 10
      val ordinalDelta = latest.ordinal.value - prior.ordinal.value

      SubscriptionLivenessStatus(timestampDelta, ordinalDelta)
    }
}

trait SubscriptionHandlerFactory {
  def create(
      eventValidator: SequencedEventValidator,
      initialPriorEventO: Option[EventAndOrdinal],
      sequencerAlias: SequencerAlias,
      sequencerId: SequencerId,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SubscriptionHandler
}

class SubscriptionHandlerFactoryImpl(
    clock: Clock,
    metrics: SequencerClientMetrics,
    applicationHandlerFailure: SingleUseCell[ApplicationHandlerFailure],
    recorderO: Option[SequencerClientRecorder],
    sequencerAggregator: SequencerAggregator,
    processingDelay: DelaySequencedEvent,
    timeouts: ProcessingTimeout,
) extends SubscriptionHandlerFactory {

  override def create(
      eventValidator: SequencedEventValidator,
      initialPriorEventO: Option[EventAndOrdinal],
      sequencerAlias: SequencerAlias,
      sequencerId: SequencerId,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SubscriptionHandler = new SubscriptionHandler(
    clock,
    metrics,
    applicationHandlerFailure,
    recorderO,
    sequencerAggregator,
    eventValidator,
    processingDelay,
    initialPriorEventO,
    sequencerAlias,
    sequencerId,
    timeouts,
    loggerFactory,
  )
}
