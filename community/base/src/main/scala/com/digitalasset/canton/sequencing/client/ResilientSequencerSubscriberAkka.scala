// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import akka.Done
import akka.stream.scaladsl.Source
import akka.stream.{AbruptStageTerminationException, KillSwitch, Materializer}
import cats.syntax.either.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransportAkka
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.AkkaUtil.RetrySourcePolicy
import com.digitalasset.canton.util.{AkkaUtil, LoggerUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/** Attempts to create a resilient [[SequencerSubscriptionAkka]] for the [[SequencerClient]] by
  * creating underlying subscriptions using the [[SequencerSubscriptionFactoryAkka]]
  * and then recreating them if they fail with a reason that is deemed retryable.
  * If a subscription is closed or fails with a reason that is not retryable the failure will be passed downstream
  * from this subscription.
  * We determine whether an error is retryable by calling the [[SubscriptionErrorRetryPolicy]]
  * of the supplied [[SequencerSubscriptionFactoryAkka]].
  * We also will delay recreating subscriptions by an interval determined by the
  * [[com.digitalasset.canton.sequencing.client.SubscriptionRetryDelayRule]].
  * The recreated subscription starts at the last event received,
  * or at the starting counter that was given initially if no event was received at all.
  *
  * The emitted events stutter whenever the subscription is recreated
  */
class ResilientSequencerSubscriberAkka[E](
    domainId: DomainId,
    retryDelayRule: SubscriptionRetryDelayRule,
    subscriptionFactory: SequencerSubscriptionFactoryAkka[E],
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer)
    extends FlagCloseable
    with NamedLogging {
  import ResilientSequencerSubscriberAkka.*

  /** Start running the resilient sequencer subscription from the given counter */
  def subscribeFrom(startingCounter: SequencerCounter)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionAkka[E] = {

    logger.debug(s"Starting resilient sequencer subscription from counter $startingCounter")
    val initial = RestartSourceConfig(startingCounter, retryDelayRule.initialDelay)(traceContext)
    val source = AkkaUtil
      .restartSource("resilient-sequencer-subscription", initial, mkSource, policy)
      // Filter out retried errors
      .filter {
        case Left(triaged) => !triaged.retryable
        case Right(_) => true
      }
      .map(_.leftMap(_.error))
    SequencerSubscriptionAkka(source)
  }

  private val policy: RetrySourcePolicy[
    RestartSourceConfig,
    Either[TriagedError[E], OrdinarySerializedEvent],
  ] = new RetrySourcePolicy[RestartSourceConfig, Either[TriagedError[E], OrdinarySerializedEvent]] {
    override def shouldRetry(
        lastState: RestartSourceConfig,
        lastEmittedElement: Option[Either[TriagedError[E], OrdinarySerializedEvent]],
        lastFailure: Option[Throwable],
    ): Option[(FiniteDuration, RestartSourceConfig)] = {
      implicit val traceContext: TraceContext = lastState.traceContext
      val retryPolicy = subscriptionFactory.retryPolicy
      val hasReceivedEvent = lastEmittedElement.exists {
        case Left(err) => err.hasReceivedElements
        case Right(_) => true
      }
      val canRetry = lastFailure match {
        case None =>
          lastEmittedElement match {
            case Some(Right(_)) => false
            case Some(Left(err)) =>
              val canRetry = err.retryable
              if (!canRetry)
                logger.warn(s"Closing resilient sequencer subscription due to error: ${err.error}")
              canRetry
            case None =>
              logger.info("The sequencer subscription has been terminated by the server.")
              false
          }
        case Some(ex: AbruptStageTerminationException) =>
          logger.debug("Giving up on resilient sequencer subscription due to shutdown", ex)
          false
        case Some(ex) =>
          val canRetry = retryPolicy.retryOnException(ex)
          if (canRetry) {
            logger.warn(
              s"The sequencer subscription encountered an exception and will be restarted",
              ex,
            )
            true
          } else {
            logger.error(
              "Closing resilient sequencer subscription due to exception",
              ex,
            )
            false
          }
      }
      Option.when(canRetry) {
        val newDelay = retryDelayRule.nextDelay(lastState.delay, hasReceivedEvent)
        val logMessage =
          s"Waiting ${LoggerUtil.roundDurationForHumans(newDelay)} before reconnecting"
        if (newDelay < retryDelayRule.warnDelayDuration) {
          logger.debug(logMessage)
        } else {
          // TODO(#13789) Log only on INFO if we're unhealthy
          //  Unlike the old ResilientSequencerSubscription, this will use the same trace context for logging
          LostSequencerSubscription.Warn(domainId, _logOnCreation = true).discard
        }

        val nextCounter = lastEmittedElement.fold(lastState.startingCounter)(
          _.fold(_.lastSequencerCounter, _.counter)
        )
        lastState.delay -> lastState.copy(startingCounter = nextCounter, delay = newDelay)
      }
    }
  }

  private def mkSource(
      config: RestartSourceConfig
  ): Source[Either[TriagedError[E], OrdinarySerializedEvent], (KillSwitch, Future[Done])] = {
    implicit val traceContext: TraceContext = config.traceContext
    val nextCounter = config.startingCounter
    logger.debug(s"Starting new sequencer subscription from $nextCounter")
    subscriptionFactory
      .create(nextCounter)
      .source
      .statefulMap(() => (false, nextCounter))(triageError, _ => None)
  }

  private def triageError(
      state: (Boolean, SequencerCounter),
      element: Either[E, OrdinarySerializedEvent],
  )(implicit
      traceContext: TraceContext
  ): ((Boolean, SequencerCounter), Either[TriagedError[E], OrdinarySerializedEvent]) = {
    val (hasPreviouslyReceivedEvents, lastSequencerCounter) = state
    val hasReceivedEvents = hasPreviouslyReceivedEvents || element.isRight
    val triaged = element.leftMap { err =>
      val canRetry = subscriptionFactory.retryPolicy.retryOnError(err, hasReceivedEvents)
      TriagedError(canRetry, hasReceivedEvents, lastSequencerCounter, err)
    }
    val currentSequencerCounter = element.fold(_ => lastSequencerCounter, _.counter)
    val newState = (hasReceivedEvents, currentSequencerCounter)
    (newState, triaged)
  }
}

object ResilientSequencerSubscriberAkka {

  /** @param startingCounter The counter to start the next subscription from
    * @param delay If the next subscription fails with a retryable error,
    *              how long should we wait before starting a new subscription?
    */
  private[ResilientSequencerSubscriberAkka] final case class RestartSourceConfig(
      startingCounter: SequencerCounter,
      delay: FiniteDuration,
  )(val traceContext: TraceContext)
      extends PrettyPrinting {
    override def pretty: Pretty[RestartSourceConfig.this.type] = prettyOfClass(
      param("starting counter", _.startingCounter)
    )

    def copy(
        startingCounter: SequencerCounter = this.startingCounter,
        delay: FiniteDuration = this.delay,
    ): RestartSourceConfig = RestartSourceConfig(startingCounter, delay)(traceContext)
  }

  private final case class TriagedError[+E](
      retryable: Boolean,
      hasReceivedElements: Boolean,
      lastSequencerCounter: SequencerCounter,
      error: E,
  )
}

trait SequencerSubscriptionFactoryAkka[HandlerError] {
  def create(
      startingCounter: SequencerCounter
  )(implicit
      loggingContext: NamedLoggingContext
  ): SequencerSubscriptionAkka[HandlerError]

  def retryPolicy: SubscriptionErrorRetryPolicyAkka[HandlerError]
}

object SequencerSubscriptionFactoryAkka {

  /** Creates a [[SequencerSubscriptionFactoryAkka]] for a [[ResilientSequencerSubscriberAkka]]
    * that uses an underlying gRPC transport.
    * Changes to the underlying gRPC transport are not supported by the [[ResilientSequencerSubscriberAkka]];
    * these can be done via the sequencer aggregator.
    */
  def fromTransport(
      transport: SequencerClientTransportAkka,
      requiresAuthentication: Boolean,
      member: Member,
      protocolVersion: ProtocolVersion,
  ): SequencerSubscriptionFactoryAkka[transport.SubscriptionError] =
    new SequencerSubscriptionFactoryAkka[transport.SubscriptionError] {
      override def create(startingCounter: SequencerCounter)(implicit
          loggingContext: NamedLoggingContext
      ): SequencerSubscriptionAkka[transport.SubscriptionError] = {
        implicit val traceContext: TraceContext = loggingContext.traceContext
        val request = SubscriptionRequest(member, startingCounter, protocolVersion)
        if (requiresAuthentication) transport.subscribe(request)
        else transport.subscribeUnauthenticated(request)
      }

      override def retryPolicy: SubscriptionErrorRetryPolicyAkka[transport.SubscriptionError] =
        transport.subscriptionRetryPolicyAkka
    }

}