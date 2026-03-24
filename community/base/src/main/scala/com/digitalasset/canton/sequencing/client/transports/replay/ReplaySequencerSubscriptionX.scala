// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports.replay

import cats.syntax.either.*
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.HealthComponent
import com.digitalasset.canton.health.HealthComponent.AlwaysHealthyComponent
import com.digitalasset.canton.lifecycle.HasUnlessClosing
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.client.{
  SequencedEventValidatorFactory,
  SequencerClientSubscriptionError,
}
import com.digitalasset.canton.sequencing.{
  ProcessingSerializedEvent,
  SequencedEventHandler,
  SequencerClientRecorder,
  SequencerConnectionX,
  SequencerSubscriptionX,
  SequencerSubscriptionXFactory,
  SubscriptionHandlerXFactory,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, MonadUtil}
import com.google.common.annotations.VisibleForTesting

import java.nio.file.Path
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import scala.concurrent.ExecutionContext

/** A sequencer subscription implementation that replays events previously stored and sends then to
  * the handler.
  *
  * This subscription does not actually subscribe to the sequencer associated to the underlying
  * connection.
  *
  * @param connection
  *   the underlying connection to the sequencer
  * @param replayPath
  *   the file path from which events are read
  * @param handler
  *   handler to process the events read from the provided file path
  */
private[sequencing] class ReplaySequencerSubscriptionX[HandlerError] private[sequencing] (
    override val connection: SequencerConnectionX,
    replayPath: Path,
    handler: SequencedEventHandler[HandlerError],
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerSubscriptionX[HandlerError] {
  import ReplaySequencerSubscriptionX.ReplayStatistics

  override def start()(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info("Loading messages for replaying...")
    val messages = ErrorUtil.withThrowableLogging {
      SequencerClientRecorder.loadEvents(replayPath, logger)
    }
    logger.info(s"Start feeding ${messages.size} messages to the subscription...")
    val startTime = CantonTimestamp.now()
    val startNanos = System.nanoTime()

    val replayF = MonadUtil
      .sequentialTraverse(messages) { event =>
        logger.debug(
          s"Replaying event with sequencing timestamp ${event.timestamp}"
        )(event.traceContext)
        for {
          unitOrErr <- handler(event)
        } yield unitOrErr match {
          case Left(err) =>
            logger.error(s"The sequencer handler returned an error: $err")
          case Right(()) =>
        }
      }
      .map { _ =>
        val stopNanos = System.nanoTime()
        val duration = java.time.Duration.ofNanos(stopNanos - startNanos)
        logger.info(
          show"Finished feeding ${messages.size} messages within $duration to the subscription."
        )
        ReplaySequencerSubscriptionX.replayStatistics.add(
          ReplayStatistics(replayPath, messages.size, startTime, duration)
        )
      }

    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      replayF,
      "An exception has occurred while replaying messages.",
    )

    Either.unit
  }

  override private[sequencing] val health: HealthComponent =
    new AlwaysHealthyComponent(s"replay-subscription-${connection.name}", logger)
}

object ReplaySequencerSubscriptionX {
  @VisibleForTesting
  lazy val replayStatistics: BlockingQueue[ReplayStatistics] = new LinkedBlockingQueue()

  final case class ReplayStatistics(
      inputPath: Path,
      numberOfEvents: Int,
      startTime: CantonTimestamp,
      duration: java.time.Duration,
  )
}

/** A sequencer subscription implementation that does nothing.
  *
  * @param connection
  *   the underlying connection to the sequencer
  */
private[sequencing] class NullSequencerSubscriptionX[HandlerError] private[sequencing] (
    override val connection: SequencerConnectionX,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionX[HandlerError] {
  override def start()(implicit traceContext: TraceContext): Either[String, Unit] = Either.unit

  override private[sequencing] val health: HealthComponent =
    new AlwaysHealthyComponent(s"null-subscription-${connection.name}", logger)
}

private[sequencing] class ReplaySequencerSubscriptionXFactory(
    eventValidatorFactory: SequencedEventValidatorFactory,
    replayPath: Path,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionXFactory {
  override def create(
      connection: SequencerConnectionX,
      member: Member,
      preSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionHandlerFactory: SubscriptionHandlerXFactory,
      parent: HasUnlessClosing,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): SequencerSubscriptionX[SequencerClientSubscriptionError] = {
    val loggerWithConnection = loggerFactory.append("connection", connection.name)

    val eventValidator = eventValidatorFactory.create(loggerWithConnection)

    val sequencerAlias = SequencerAlias.tryCreate(connection.name)
    val sequencerId = connection.attributes.sequencerId

    val subscriptionHandler = subscriptionHandlerFactory.create(
      eventValidator,
      preSubscriptionEventO,
      sequencerAlias,
      sequencerId,
      loggerWithConnection,
    )

    new ReplaySequencerSubscriptionX(
      connection = connection,
      replayPath = replayPath,
      handler = subscriptionHandler.handleEvent,
      timeouts = timeouts,
      loggerFactory = loggerWithConnection,
    )
  }
}

private[sequencing] class NullSequencerSubscriptionXFactory(
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionXFactory {
  override def create(
      connection: SequencerConnectionX,
      member: Member,
      preSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionHandlerFactory: SubscriptionHandlerXFactory,
      parent: HasUnlessClosing,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): SequencerSubscriptionX[SequencerClientSubscriptionError] =
    new NullSequencerSubscriptionX(
      connection = connection,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )
}
