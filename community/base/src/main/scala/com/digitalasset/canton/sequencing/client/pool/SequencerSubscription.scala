// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{AtomicHealthComponent, ComponentHealthState, HealthComponent}
import com.digitalasset.canton.lifecycle.{HasRunOnClosing, HasUnlessClosing}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerException,
  ApplicationHandlerPassive,
  ApplicationHandlerShutdown,
}
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.HandlerException
import com.digitalasset.canton.sequencing.client.{
  Fatal,
  InternallyCompletedSequencerSubscription,
  SequencedEventValidatorFactory,
  SequencerClientSubscriptionError,
  SubscriptionCloseReason,
}
import com.digitalasset.canton.sequencing.handlers.HasReceivedEvent
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.sequencing.{ProcessingSerializedEvent, SequencedEventHandler}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import org.apache.pekko.stream.AbruptStageTerminationException

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** A subscription to a sequencer. */
trait SequencerSubscription[HandlerError]
    extends InternallyCompletedSequencerSubscription[HandlerError]
    with NamedLogging {

  def connection: SequencerConnection

  def start()(implicit traceContext: TraceContext): Either[String, Unit]

  private[sequencing] def health: HealthComponent
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
  * @param parent
  *   component whose closing indicates the subscriptions will be closed soon; used to shortcut
  *   errors and avoid warning logs when shutting down
  */
class SequencerSubscriptionImpl[HandlerError] private[sequencing] (
    override val connection: SequencerConnection,
    member: Member,
    startingTimestampO: Option[CantonTimestamp],
    handler: SequencedEventHandler[HandlerError],
    parent: HasUnlessClosing,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerSubscription[HandlerError] {
  private val retryPolicy = connection.subscriptionRetryPolicy

  override private[sequencing] val health: AtomicHealthComponent = new AtomicHealthComponent() {
    override def name: String = s"subscription-${connection.name}"

    override protected def initialHealthState: ComponentHealthState =
      ComponentHealthState.Failed()

    override protected def associatedHasRunOnClosing: HasRunOnClosing =
      SequencerSubscriptionImpl.this

    override protected def logger: TracedLogger = SequencerSubscriptionImpl.this.logger
  }

  override def start()(implicit traceContext: TraceContext): Either[String, Unit] = {
    val startingTimestampStringO = startingTimestampO
      .map(timestamp => s"the timestamp $timestamp")
      .getOrElse("the beginning")
    logger.info(s"Starting subscription at $startingTimestampStringO")

    val protocolVersion = connection.attributes.staticParameters.protocolVersion
    val request = SubscriptionRequest(member, startingTimestampO, protocolVersion)

    val (hasReceivedEvent, wrappedHandler) = HasReceivedEvent(handler)

    connection
      .subscribe(request, wrappedHandler, timeouts.network.duration)
      .map { newSubscription =>
        newSubscription.closeReason.onComplete {
          case Success(SubscriptionCloseReason.TransportChange) =>
            ErrorUtil
              .invalidState(s"Close reason 'TransportChange' cannot happen on a pool connection")

          case reason @ Success(SubscriptionCloseReason.TokenExpiration) =>
            giveUp(reason)

          case Success(_: SubscriptionCloseReason.SubscriptionError) if parent.isClosing =>
            giveUp(Success(SubscriptionCloseReason.Shutdown))

          case error @ Success(subscriptionError: SubscriptionCloseReason.SubscriptionError) =>
            val canRetry =
              retryPolicy.retryOnError(
                subscriptionError,
                receivedItems = hasReceivedEvent.hasReceivedEvent,
              )
            if (canRetry) {
              logger.debug(s"Closing sequencer subscription due to error: $subscriptionError.")
              restartConnection(connection, subscriptionError.toString)
            } else {
              // Permanently close the connection to this sequencer
              giveUp(error)
            }

          case Failure(_: AbruptStageTerminationException) if parent.isClosing =>
            giveUp(Success(SubscriptionCloseReason.Shutdown))

          case Failure(exn) =>
            val canRetry = retryPolicy.retryOnException(exn, logger)

            if (canRetry) {
              logger.warn(s"Closing sequencer subscription due to exception: $exn.", exn)
              restartConnection(connection, exn.toString)
            } else {
              // Permanently close the connection to this sequencer
              giveUp(Failure(exn))
            }

          case unrecoverableReason =>
            // Permanently close the connection to this sequencer
            giveUp(unrecoverableReason)
        }

        health.resolveUnhealthy()
      }
  }

  private def restartConnection(connection: SequencerConnection, reason: String)(implicit
      traceContext: TraceContext
  ): Unit =
    // Stop the connection non-fatally and let the subscription pool start a new subscription.
    connection.fail(reason)
  // TODO(i28761): Warn after some delay or number of failures
  // LostSequencerSubscription.Warn(connection.attributes.sequencerId).discard

  // stop the current subscription, do not retry, and propagate the reason upstream
  private def giveUp(
      reason: Try[SubscriptionCloseReason[HandlerError]]
  )(implicit traceContext: TraceContext): Unit = {
    // We need to complete the promise first, otherwise the `fatal()` will result in the close reason being
    // completed with 'Closed'.
    closeReasonPromise.tryComplete(reason).discard

    reason match {
      case Success(SubscriptionCloseReason.Closed) =>
        logger.trace("Closing sequencer subscription")
      // Normal closing of this subscription can be triggered either by:
      // - closing of the subscription pool, which closed all the subscriptions; in this case, the connection pool
      //   will also be closed and will take care of the connections
      // - failure of a connection, and the subscription pool closed the associated subscription; in this case, the
      //   connection will already be closed if need be
      // We therefore don't need to explicitly close the connection.

      case Success(SubscriptionCloseReason.Shutdown) =>
        logger.info("Closing sequencer subscription due to an ongoing shutdown")
      // If we reach here, it is due to a concurrent closing of the subscription (see above) and a subscription
      // error. Again, we don't need to explicitly close the connection.

      case Success(SubscriptionCloseReason.HandlerError(_: ApplicationHandlerShutdown.type)) =>
        logger.info("Closing sequencer subscription due to handler shutdown")
      // If we reach here, it is due to a concurrent closing of the subscription (see above) and a subscription
      // error. Again, we don't need to explicitly close the connection.

      case Success(SubscriptionCloseReason.TokenExpiration) =>
        logger.debug("Sequencer subscription was closed by the server due to a token expiration")

      case Success(SubscriptionCloseReason.HandlerError(exception: ApplicationHandlerException)) =>
        logger.error(
          s"Permanently closing sequencer subscription due to handler exception (this indicates a bug): $exception"
        )
        connection.fatal(exception.toString)

      case Success(SubscriptionCloseReason.HandlerError(ApplicationHandlerPassive(reason))) =>
        logger.info(
          s"Permanently closing sequencer subscription because instance became passive: $reason"
        )
        connection.fatal("Instance became passive")

      case Success(Fatal(reason)) if parent.isClosing =>
        logger.info(
          s"Permanently closing sequencer subscription after an error due to an ongoing shutdown: $reason"
        )
        connection.fatal("Error during shutdown")

      case Success(ex: HandlerException) =>
        logger.error(s"Permanently closing sequencer subscription due to handler exception: $ex")
        connection.fatal(ex.toString)

      case Success(error) =>
        logger.warn(s"Permanently closing sequencer subscription due to error: $error")
        connection.fatal(error.toString)

      case Failure(exception) =>
        logger.error(s"Permanently closing sequencer subscription due to exception", exception)
        connection.fatal(exception.toString)
    }
  }

  override def toString: String = s"Subscription over ${connection.name}"
}

trait SequencerSubscriptionFactory {
  def create(
      connection: SequencerConnection,
      member: Member,
      preSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionHandlerFactory: SubscriptionHandlerFactory,
      parent: HasUnlessClosing,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): SequencerSubscription[SequencerClientSubscriptionError]
}

class SequencerSubscriptionFactoryImpl(
    eventValidatorFactory: SequencedEventValidatorFactory,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionFactory {

  override def create(
      connection: SequencerConnection,
      member: Member,
      preSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionHandlerFactory: SubscriptionHandlerFactory,
      parent: HasUnlessClosing,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): SequencerSubscription[SequencerClientSubscriptionError] = {
    val loggerWithConnection = loggerFactory.append("connection", connection.name)
    val startingTimestampO = preSubscriptionEventO.map(_.timestamp)

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

    new SequencerSubscriptionImpl(
      connection = connection,
      member = member,
      startingTimestampO = startingTimestampO,
      handler = subscriptionHandler.handleEvent,
      parent = parent,
      timeouts = timeouts,
      loggerFactory = loggerWithConnection,
    )
  }
}
