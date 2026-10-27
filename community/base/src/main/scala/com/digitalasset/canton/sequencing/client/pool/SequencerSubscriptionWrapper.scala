// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState, HealthComponent}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasRunOnClosing, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.SequencerAggregatorXImpl.EventAndOrdinal
import com.digitalasset.canton.sequencing.client.pool.SequencerSubscriptionWrapper.ClosingReasonHandler
import com.digitalasset.canton.sequencing.client.pool.SubscriptionHandler.SubscriptionLivenessStatus
import com.digitalasset.canton.sequencing.client.{
  SequencedEventValidatorFactory,
  SequencerClientSubscriptionError,
  SequencerSubscription,
  SubscriptionCloseReason,
}
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, LoggerUtil, SingleUseCell}

import scala.concurrent.ExecutionContext
import scala.util.Try

/** A wrapper around a subscription to a sequencer. */
trait SequencerSubscriptionWrapper extends FlagCloseable with NamedLogging {

  def connection: SequencerConnection

  def start()(implicit traceContext: TraceContext): Either[String, Unit]

  def setClosingReasonHandler(closingReasonHandler: ClosingReasonHandler)(implicit
      traceContext: TraceContext
  ): Unit

  /** Check the liveness of the subscription.
    *
    * @param eventAndOrdinal
    *   The latest event and ordinal emitted by the sequencer aggregator
    * @param maxTimestampDelta
    *   The maximum time difference allowed between the latest event received by the subscription
    *   and `eventAndOrdinal`
    * @param maxOrdinalDelta
    *   The maximum ordinal difference allowed between the latest received by the subscription and
    *   `eventAndOrdinal`
    */
  def checkLiveness(
      eventAndOrdinal: EventAndOrdinal,
      maxTimestampDelta: NonNegativeFiniteDuration,
      maxOrdinalDelta: NonNegativeInt,
  )(implicit traceContext: TraceContext): Unit

  private[sequencing] def health: HealthComponent
}

object SequencerSubscriptionWrapper {
  trait ClosingReasonHandler {
    def handleClosingReason(
        closingReason: Try[SubscriptionCloseReason[SequencerClientSubscriptionError]]
    )(implicit traceContext: TraceContext): Unit
  }
}

/** Regular subscription to a sequencer.
  *
  * @param connection
  *   the underlying connection to the sequencer
  * @param member
  *   the member for whom we request a subscription
  * @param startingTimestampO
  *   timestamp at which to start the subscription; if undefined, we subscribe from the beginning
  * @param handler
  *   handler to process the events received from the subscription
  */
class SequencerSubscriptionWrapperImpl private[sequencing] (
    override val connection: SequencerConnection,
    member: Member,
    startingTimestampO: Option[CantonTimestamp],
    handler: SubscriptionHandler,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerSubscriptionWrapper {

  override private[sequencing] val health: AtomicHealthComponent = new AtomicHealthComponent() {
    override def name: String = s"subscription-${connection.name}"

    override protected def initialHealthState: ComponentHealthState =
      ComponentHealthState.Failed()

    override protected def associatedHasRunOnClosing: HasRunOnClosing =
      SequencerSubscriptionWrapperImpl.this

    override protected def logger: TracedLogger = SequencerSubscriptionWrapperImpl.this.logger
  }

  private val subscriptionRef =
    new SingleUseCell[SequencerSubscription[SequencerClientSubscriptionError]]()

  override def start()(implicit traceContext: TraceContext): Either[String, Unit] = {
    val tsString = startingTimestampO.fold("the beginning")(ts => s"timestamp $ts")
    logger.info(s"Starting subscription at $tsString")

    val protocolVersion = connection.attributes.staticParameters.protocolVersion
    val request = SubscriptionRequest(member, startingTimestampO, protocolVersion)

    connection
      .subscribe(request, handler.handleEvent, timeouts.network.duration)
      .map { newSubscription =>
        subscriptionRef
          .putIfAbsent(newSubscription)
          .foreach(_ => ErrorUtil.invalidState("Subscription already exists"))

        health.resolveUnhealthy()
      }
  }

  override def setClosingReasonHandler(
      closingReasonHandler: ClosingReasonHandler
  )(implicit traceContext: TraceContext): Unit = {
    val closeReasonF =
      subscriptionRef.getOrElse(ErrorUtil.invalidState("no subscription")).closeReason
    closeReasonF.onComplete(closingReasonHandler.handleClosingReason(_))
  }

  override def onClosed(): Unit =
    LifeCycle.close(LifeCycle.toCloseableOption(subscriptionRef.get), handler)(logger)

  private def completeSubscription(
      reason: SubscriptionCloseReason[SequencerClientSubscriptionError]
  )(implicit traceContext: TraceContext): Unit =
    subscriptionRef.getOrElse(ErrorUtil.invalidState("No subscription")).complete(reason)

  override def toString: String = s"Subscription over ${connection.name}"

  override def checkLiveness(
      eventAndOrdinal: EventAndOrdinal,
      maxTimestampDelta: NonNegativeFiniteDuration,
      maxOrdinalDelta: NonNegativeInt,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    val statusO = handler.getLivenessStatus(eventAndOrdinal)

    val limitsAreExceeded = statusO match {
      case Some(SubscriptionLivenessStatus(timestampDelta, ordinalDelta)) =>
        logger.debug(
          s"Checking liveness: " +
            s"timestampDelta = ${LoggerUtil.roundDurationForHumans(timestampDelta)} ; ordinalDelta = $ordinalDelta ; " +
            s"configuration = (${LoggerUtil.roundDurationForHumans(maxTimestampDelta.duration)}, $maxOrdinalDelta)}"
        )

        (timestampDelta.compareTo(maxTimestampDelta.duration) > 0)
        && (ordinalDelta > maxOrdinalDelta.unwrap)

      case None =>
        logger.debug(
          s"Checking liveness: no event yet ; " +
            s"configuration = (${LoggerUtil.roundDurationForHumans(maxTimestampDelta.duration)}, $maxOrdinalDelta)}"
        )

        // We have not received any event yet -- use the aggregated event's ordinal only, implying our ordinal is 0
        eventAndOrdinal.ordinal.unwrap > maxOrdinalDelta.unwrap
    }

    if (limitsAreExceeded) {
      val message = "Subscription inactivity exceeded configured limits: " +
        s"$statusO vs. (${LoggerUtil.roundDurationForHumans(maxTimestampDelta.duration)}, $maxOrdinalDelta)"
      logger.warn(s"$message -- closing subscription")
      completeSubscription(SubscriptionCloseReason.SubscriptionIsSilent)
    }
  }
}

trait SequencerSubscriptionWrapperFactory {
  def create(
      connection: SequencerConnection,
      member: Member,
      preSubscriptionEventO: Option[EventAndOrdinal],
      subscriptionHandlerFactory: SubscriptionHandlerFactory,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): SequencerSubscriptionWrapper
}

class SequencerSubscriptionWrapperFactoryImpl(
    eventValidatorFactory: SequencedEventValidatorFactory,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionWrapperFactory {

  override def create(
      connection: SequencerConnection,
      member: Member,
      preSubscriptionEventO: Option[EventAndOrdinal],
      subscriptionHandlerFactory: SubscriptionHandlerFactory,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): SequencerSubscriptionWrapper = {
    val loggerWithConnection = loggerFactory.append("connection", connection.name)
    val startingTimestampO = preSubscriptionEventO.map(_.event.timestamp)

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

    new SequencerSubscriptionWrapperImpl(
      connection = connection,
      member = member,
      startingTimestampO = startingTimestampO,
      handler = subscriptionHandler,
      timeouts = timeouts,
      loggerFactory = loggerWithConnection,
    )
  }
}
