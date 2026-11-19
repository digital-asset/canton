// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.pool

import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{HealthListener, HealthQuasiComponent}
import com.digitalasset.canton.lifecycle.LifeCycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerConnectionPoolMetrics
import com.digitalasset.canton.sequencing.ProcessingSerializedEvent
import com.digitalasset.canton.sequencing.SequencerAggregator.SequencerAggregatorError
import com.digitalasset.canton.sequencing.SequencerAggregatorXImpl.EventAndOrdinal
import com.digitalasset.canton.sequencing.client.SequencerClient.CloseReason.UnrecoverableError
import com.digitalasset.canton.sequencing.client.SequencerClientSubscriptionError.{
  ApplicationHandlerException,
  ApplicationHandlerShutdown,
  EventAggregationError,
  EventValidationError,
}
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError.{
  TrustAssumptionViolated,
  UnreachableTrustThreshold,
}
import com.digitalasset.canton.sequencing.client.pool.InternalSequencerConnection.SequencerConnectionState
import com.digitalasset.canton.sequencing.client.pool.SequencerPenalties.Penalty
import com.digitalasset.canton.sequencing.client.pool.SequencerSubscriptionPool.{
  SequencerSubscriptionPoolConfig,
  SequencerSubscriptionPoolHealth,
}
import com.digitalasset.canton.sequencing.client.pool.SequencerSubscriptionPoolImpl.{
  ConfigWithThreshold,
  SubscriptionStartProvider,
}
import com.digitalasset.canton.sequencing.client.pool.SequencerSubscriptionWrapper.ClosingReasonHandler
import com.digitalasset.canton.sequencing.client.{
  SequencerClient,
  SequencerClientSubscriptionError,
  SubscriptionCloseReason,
}
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, WallClock}
import com.digitalasset.canton.topology.{Member, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, LoggerUtil, Mutex}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

final class SequencerSubscriptionPoolImpl private[sequencing] (
    private val initialConfig: SequencerSubscriptionPoolConfig,
    subscriptionWrapperFactory: SequencerSubscriptionWrapperFactory,
    subscriptionHandlerFactory: SubscriptionHandlerFactory,
    pool: SequencerConnectionPool,
    member: Member,
    private val initialSubscriptionEventO: Option[ProcessingSerializedEvent],
    subscriptionStartProvider: SubscriptionStartProvider,
    useNewAggregator: Boolean,
    metrics: SequencerConnectionPoolMetrics,
    metricsContext: MetricsContext,
    protected override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SequencerSubscriptionPool {

  /** Use a wall clock for scheduling restart delays, so that the pool can make progress even in
    * tests that use static time without explicitly advancing the time
    */
  private val wallClock = new WallClock(timeouts, loggerFactory)

  /** Reference to the currently active configuration */
  private val configRef = new AtomicReference[SequencerSubscriptionPoolConfig](initialConfig)

  /** Flag indicating whether the connection pool has been started */
  private val startedRef = new AtomicBoolean(false)

  /** Tracks the current subscriptions */
  private val trackedSubscriptions = mutable.Set[SubscriptionManager]()

  /** Used to synchronize access to the mutable structure [[trackedSubscriptions]] */
  private val lock = new Mutex()

  /** Holds the token for the current [[adjustConnectionsIfNeeded]] request */
  private val currentRequest = new AtomicLong(0)

  /** Sequencers whose subscriptions failed since the last event was successfully aggregated. */
  private val faultySequencersRef = new AtomicReference[Set[SequencerId]](Set.empty)

  override def config: SequencerSubscriptionPoolConfig = configRef.get

  /** Use this instead of [[config]] to obtain a snapshot of all the current configuration
    * parameters at once.
    */
  private def currentConfigWithThreshold: ConfigWithThreshold =
    ConfigWithThreshold(config, pool.config.trustThreshold)

  private implicit val mc: MetricsContext = metricsContext

  metrics.subscriptionThreshold.updateValue(
    currentConfigWithThreshold.activeThreshold.value
  )

  override def updateConfig(
      newConfig: SequencerSubscriptionPoolConfig
  )(implicit traceContext: TraceContext): Unit = {
    configRef.set(newConfig)
    logger.info(s"Configuration updated to: $newConfig")

    metrics.subscriptionThreshold.updateValue(config.livenessMargin.value)

    // We might need new connections
    adjustConnectionsIfNeeded()
  }

  override val health: SequencerSubscriptionPoolHealth = new SequencerSubscriptionPoolHealth(
    name = "sequencer-subscription-pool",
    associatedHasRunOnClosing = this,
    logger = logger,
  )

  override def getSubscriptionsHealthStatus: Seq[HealthQuasiComponent] =
    subscriptions.view.map(_.health).toSeq

  /** Examine the current number of subscriptions in comparison to the configured trust threshold
    * with liveness margin. If we are under, we request additional connections, and if we can't
    * obtain enough, we reschedule the check later after
    * [[SequencerSubscriptionPoolConfig.subscriptionRequestDelay]]. If we are over, we drop some
    * subscriptions.
    */
  private def adjustConnectionsIfNeeded()(implicit traceContext: TraceContext): Unit = {
    // Overflow is harmless, it will just roll over to negative
    val myToken = currentRequest.incrementAndGet()

    def adjustInternal(): Unit =
      lock.exclusive {
        if (!isClosing && currentRequest.get == myToken) {
          val activeThreshold = currentConfigWithThreshold.activeThreshold

          val current = trackedSubscriptions.toSet
          logger.debug(
            s"[token = $myToken] ${current.size} current subscriptions, active threshold = $activeThreshold"
          )

          val nbToRequest = activeThreshold.unwrap - current.size

          PositiveInt.create(nbToRequest) match {
            case Right(nbToRequestAsPosInt) =>
              logger.debug(s"Requesting $nbToRequest additional connection(s)")

              val currentSeqIds = current.map(_.connection.attributes.sequencerId)
              val newConnections =
                pool.getConnections(
                  "subscription-pool",
                  nbToRequestAsPosInt,
                  excluded = currentSeqIds,
                  // We don't have a topology snapshot at hand to restrict sequencers.
                  // It is not a problem, because even if we subscribe to an offboarded sequencer, either:
                  // - the received events will be discarded
                  // - if the token has expired, obtaining a new one will fail the signature check.
                  acceptableO = None,
                )
              logger.debug(
                s"Received ${newConnections.size} new connection(s): ${newConnections.map(_.name)}"
              )

              val newSubscriptions = newConnections.flatMap { connection =>
                val sequencerId = connection.attributes.sequencerId
                val subscriptionWrapper = createSubscriptionWrapper(connection)
                for {
                  _ <- subscriptionWrapper
                    .start()
                    .leftMap(error =>
                      logger.warn(s"Failed to start subscription for $sequencerId: $error")
                    )
                    .toOption
                } yield {
                  logger.debug(s"Successfully started subscription for $sequencerId")
                  metrics.updateSubscriptionHealth(
                    mc.withExtraLabels("connection" -> connection.config.name),
                    1,
                  )
                  new SubscriptionManager(subscriptionWrapper)
                }
              }

              trackedSubscriptions ++= newSubscriptions

              metrics.activeSubscriptions.updateValue(
                trackedSubscriptions.size
              )

              updateHealth()

              // Note that the following calls to `register` may trigger a reentrant call here: indeed, `register`
              // will register on the connection health, which will immediately trigger a `poke`. If the connection
              // has meanwhile gone bad (state != `Validated`), we will remove the connection from the subscription
              // pool (as this runs in the same thread and the lock is reentrant) and call back here to adjust.
              // Similarly, a subscription that closes immediately could call back here to adjust.
              // This should however not interfere with our processing, and obtain a new connection.
              newSubscriptions.foreach(_.register())

              if (newConnections.sizeIs < nbToRequest) scheduleNext()

            case Left(_) if nbToRequest < 0 =>
              val toRemove = trackedSubscriptions.take(-nbToRequest)
              logger.info(
                s"Dropping ${toRemove.size} extra subscription(s): ${toRemove.map(_.subscriptionWrapper.connection.name).mkString(", ")}"
              )
              removeSubscriptionsFromPool(toRemove.toSeq*)

            case _ => // nbToRequest == 0
              logger.debug("No additional connection needed")
          }

        } else logger.debug(s"Cancelling request with token = $myToken")
      }

    def scheduleNext()(implicit traceContext: TraceContext): Unit =
      if (!isClosing) {
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          {
            val delay = config.subscriptionRequestDelay
            logger.debug(
              s"Scheduling new check in ${LoggerUtil.roundDurationForHumans(delay.duration)}"
            )
            wallClock.scheduleAfterCancelledOnShutdown(
              _ => adjustInternal(),
              s"${getClass.getName}: schedule next adjustment",
              delay.duration,
            )
          },
          "adjustConnectionsIfNeeded",
        )
      }.discard

    adjustInternal()
  }

  private def createSubscriptionWrapper(connection: SequencerConnection)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionWrapper = {
    val preSubscriptionEventO = subscriptionStartProvider.getLatestProcessedEventO.orElse(
      // The aggregator has not yet seen any event, the first one it will see and propagate will be 1.
      // We subscribe to the previous event, so we set its ordinal to 0.
      initialSubscriptionEventO.map(EventAndOrdinal.zero)
    )

    subscriptionWrapperFactory.create(
      connection,
      member,
      preSubscriptionEventO,
      subscriptionHandlerFactory,
    )
  }

  private val closeReasonPromise = Promise[SequencerClient.CloseReason]()

  override def completion: Future[SequencerClient.CloseReason] = closeReasonPromise.future

  // TODO(i28969): Clean up shutdown and notification mechanism
  // When completing the subscription pool with a reason, we generally also update the health of the subscription pool.
  // This is because other places in the code react to one or the other: for example, the sequencer client will watch
  // the promise and close with the associated reason, which will propagate up to the synchronizer connection manager,
  // whereas the liveness health of mediator nodes has the sequencer client's health as a critical dependency.
  // If we are completing due to a lost subscription, it can trigger another fatal condition happening concurrently (e.g.
  // threshold no longer reachable). Only the first reason will complete the promise, but the health change will be
  // arbitrary.
  // Furthermore, since the health of a component can freely change between any state, an arbitrary health change
  // can mean that a Fatal health which should trigger the liveness to go to NOT_SERVING can be missed if the component
  // closes before liveness sees it (as closing overrides the health to Failed).
  // The following attempts to maintain coherency by limiting the changes to the first completion call, and updating the
  // health before completing so that it has time to propagate.
  // This whole shutdown and notification mechanism, partly inherited from the old transports, should definitely be
  // revisited and cleaned up.
  private val completed = new AtomicBoolean(false)
  private def completeWithReason(
      reason: Try[SequencerClient.CloseReason],
      updateHealth: (SequencerSubscriptionPoolHealth, String) => Unit = (_, _) => (),
  )(implicit traceContext: TraceContext): Unit =
    if (!completed.getAndSet(true)) {
      updateHealth(
        health,
        reason match {
          case Success(error) => error.toString
          case Failure(exc) => exc.getMessage
        },
      )
      logger.debug(s"Completing sequencer subscription pool with reason: $reason")
      closeReasonPromise.tryComplete(reason).discard
    }

  private class SubscriptionManager(val subscriptionWrapper: SequencerSubscriptionWrapper)
      extends NamedLogging
      with AutoCloseable
      with ClosingReasonHandler {
    val connection: SequencerConnection = subscriptionWrapper.connection

    protected override val loggerFactory: NamedLoggerFactory =
      SequencerSubscriptionPoolImpl.this.loggerFactory
        .append("connection", s"${connection.name}")

    private val connectionListener = new HealthListener {
      override def name: String = s"SequencerSubscriptionPool-${connection.name}"

      override def poke()(implicit traceContext: TraceContext): Unit = {
        val state = connection.health.getState

        state match {
          case SequencerConnectionState.Validated =>

          case SequencerConnectionState.Initial | SequencerConnectionState.Started |
              SequencerConnectionState.Starting | SequencerConnectionState.Stopping(_) |
              SequencerConnectionState.Stopped(_) | SequencerConnectionState.Fatal(_) =>
            removeSubscriptionsFromPool(SubscriptionManager.this)
        }
      }
    }

    def register()(implicit traceContext: TraceContext): Unit = {
      connection.health.registerOnHealthChange(connectionListener).discard[Boolean]
      subscriptionWrapper.setClosingReasonHandler(this)
    }

    override def close(): Unit = {
      // If the connection comes back, the listener will be a different instance,
      // so we need to unregister to prevent the listeners to accumulate
      connection.health.unregisterOnHealthChange(connectionListener).discard[Boolean]
      LifeCycle.close(subscriptionWrapper)(logger)
    }

    override def toString: String = s"SubscriptionManager for $subscriptionWrapper"

    private def sequencerId: SequencerId = connection.attributes.sequencerId

    /** Record that this sequencer caused a failure (see [[faultySequencersRef]]), and penalize it.
      * Report if notable conditions are identified, and update the health of the pool accordingly.
      */
    private def handleFaultySequencer(
        failure: String,
        penalty: Penalty,
        penaltyReason: String,
        throwableO: Option[Throwable] = None,
    )(implicit traceContext: TraceContext): Unit = {
      val message = s"Closing sequencer subscription due to $failure"
      throwableO.fold(logger.warn(message))(logger.warn(message, _))

      connection.increasePenalty(penalty, reason = penaltyReason)

      val trustThreshold = currentConfigWithThreshold.trustThreshold

      val previous = faultySequencersRef.getAndUpdate(_ + sequencerId)
      val current =
        previous + sequencerId // Could be == previous if `sequencerId` was already in the set

      val trustAssumptionIsViolated =
        // We want to report only if we reached the threshold now
        previous.sizeIs < trustThreshold.value && current.sizeIs >= trustThreshold.value

      val thresholdIsUnreachableO =
        // We want to report only if the number of faulty sequencers has increased
        if (!previous.contains(sequencerId))
          pool
            .isThresholdStillReachable(trustThreshold, ignored = current)
            .left
            .toOption
        else None

      if (trustAssumptionIsViolated)
        TrustAssumptionViolated.Failure(faultySequencerIds = current).report()

      thresholdIsUnreachableO.foreach { error =>
        // Instantiating the error will log it
        UnreachableTrustThreshold
          .Failure(
            faultySequencerIds = current,
            trustThreshold,
            error.nbRemaining,
          )
          .discard
        lock.exclusive(updateHealth())
      }
    }

    override def handleClosingReason(
        closingReason: Try[SubscriptionCloseReason[SequencerClientSubscriptionError]]
    )(implicit traceContext: TraceContext): Unit = {
      def complete(reason: Try[SequencerClient.CloseReason]): Unit = {
        completeWithReason(reason, updateHealth = _.fatalOccurred(_))
        LifeCycle.close(SequencerSubscriptionPoolImpl.this)(logger)
      }

      // Notes:
      // * Removing the subscription from the pool closes it while keeping the connection available in the connection pool.
      //   The subscription pool will request new connections if needed, and may receive that connection again.
      // * Failing the connection ensures that the connection is removed from the connection pool until it is revalidated,
      //   which prevents the subscription pool from attempting a new subscription on a connection that is likely to fail again.
      // * Completing the subscription pool currently leads to the closing of the sequencer client, the subscription
      //   pool, and therefore of all the subscription and connections.
      //   Fatally closing the connection in this situation is done solely for the purpose of having an appropriate
      //   error message associated to the closing of the connection.

      closingReason match {
        // The pool already removed this subscription and closed it on purpose (e.g. because its connection is no
        // longer validated, or because it is an extra subscription). How it closed is irrelevant, and must not
        // affect its connection: for example, the close may have timed out.
        case _ if !lock.exclusive(trackedSubscriptions.contains(this)) =>
          logger.debug(s"Sequencer subscription removed from the pool was closed: $closingReason")

        case Success(SubscriptionCloseReason.Closed) =>
          logger.debug("Sequencer subscription was closed")
          removeSubscriptionsFromPool(this)

        case Success(SubscriptionCloseReason.Shutdown) =>
          logger.info("Sequencer subscription was shutdown")
          removeSubscriptionsFromPool(this)

        case _ if SequencerSubscriptionPoolImpl.this.isClosing =>
          logger.info("Closing sequencer subscription due to an ongoing shutdown")
          removeSubscriptionsFromPool(this)

        case Success(subscriptionError: SubscriptionCloseReason.SubscriptionError) =>
          val canRetry =
            connection.subscriptionRetryPolicy.retryOnError(subscriptionError)
          if (canRetry) {
            logger.debug(s"Closing sequencer subscription due to error: $subscriptionError.")
          } else {
            logger.warn(s"Closing sequencer subscription due to error: $subscriptionError")
            connection.increasePenalty(
              Penalty.SubscriptionError,
              reason = s"subscription error: $subscriptionError",
            )
          }
          connection.fail(closingReason.toString)

        case Success(_: SubscriptionCloseReason.PermissionDeniedError) =>
          logger.warn("Closing sequencer subscription due to permission denied")
          connection.increasePenalty(
            Penalty.SubscriptionPermissionDenied,
            reason = "permission denied",
          )
          connection.fail(closingReason.toString)

        case Success(SubscriptionCloseReason.TokenExpiration) =>
          logger.debug(
            "Sequencer subscription was closed by the server due to a token expiration"
          )
          removeSubscriptionsFromPool(this)

        case Success(SubscriptionCloseReason.SubscriptionIsSilent) =>
          logger.info("Closing sequencer subscription due to inactivity")
          connection.increasePenalty(Penalty.SubscriptionInactive, reason = "inactive subscription")
          removeSubscriptionsFromPool(this)

        case Success(SubscriptionCloseReason.HandlerError(ApplicationHandlerShutdown)) =>
          logger.info(
            "Closing sequencer subscription due to the application handler shutting down"
          )
          complete(Success(SequencerClient.CloseReason.ClientShutdown))

        case Success(
              SubscriptionCloseReason.HandlerError(exception: ApplicationHandlerException)
            ) =>
          logger.error(
            s"Permanently closing sequencer subscription due to application handler exception (this indicates a bug): $exception"
          )
          connection.fatal(closingReason.toString)
          complete(
            Success(
              SequencerClient.CloseReason.UnrecoverableError(
                s"application handler returned error: $exception"
              )
            )
          )

        case Success(SubscriptionCloseReason.HandlerError(EventAggregationError(error))) =>
          error match {
            case _: SequencerAggregatorError.BogusEvent if useNewAggregator =>
              logger.warn(s"Closing sequencer subscription due to aggregation error: $error")
              connection.increasePenalty(
                Penalty.SubscriptionAggregationError,
                reason = "bogus event on subscription",
              )
              connection.fail(closingReason.toString)

            case _: SequencerAggregatorError.BogusEvent =>
              ErrorUtil.invalidState(
                s"BogusEvent error received from the aggregator, but the new aggregator is not enabled: $error"
              )

            case _: SequencerAggregatorError.ThresholdUnreachable if useNewAggregator =>
              // All involved sequencer subscriptions will receive this error.
              // We close them all and give them a penalty, hoping that the new subscriptions we obtain
              // will resolve the conflict.
              //
              // A more sophisticated approach would be to react differently depending on whether the
              // liveness margin was set according to the recommendations of `trust threshold - 1` or not.
              // If e.g. `trust threshold = 3` and `liveness margin = 1`, it is possible that we receive 2 messages from
              // malicious sequencers, followed by 2 by honest sequencers. We cannot reach a threshold, but the trust
              // assumption of at most `trust threshold - 1` Byzantine nodes was not violated.
              // If the trust assumption however was violated, we can report an error and recommend a restart,
              // similarly to how we handle faulty sequencers.
              // TODO(i37780): Implement this new approach
              logger.warn(
                s"Closing sequencer subscription due to aggregation error: $error"
              )
              connection.increasePenalty(
                Penalty.SubscriptionAggregationError,
                reason = "threshold unreachable on subscription",
              )
              connection.fail(closingReason.toString)

            case _: SequencerAggregatorError.ThresholdUnreachable =>
              ErrorUtil.invalidState(
                s"ThresholdUnreachable error received from the aggregator, but the new aggregator is not enabled: $error"
              )

            case _: SequencerAggregatorError.NotTheSameContentHash if !useNewAggregator =>
              logger.error(
                s"Permanently closing sequencer subscription due to aggregation error: $error"
              )
              connection.fatal(closingReason.toString)
              complete(
                Success(
                  SequencerClient.CloseReason.UnrecoverableError(s"event aggregation error: $error")
                )
              )

            case _: SequencerAggregatorError.NotTheSameContentHash =>
              ErrorUtil.invalidState(
                s"NotTheSameContentHash error received from the aggregator, but the new aggregator is enabled: $error"
              )
          }

        case Success(SubscriptionCloseReason.HandlerError(error: EventValidationError)) =>
          handleFaultySequencer(
            s"event validation error: $error",
            Penalty.SubscriptionAggregationError,
            penaltyReason = "event validation error on subscription",
          )
          connection.fail(closingReason.toString)

        case Success(SubscriptionCloseReason.HandlerException(throwable)) =>
          handleFaultySequencer(
            s"handler exception: $throwable",
            Penalty.SubscriptionHandlerException,
            penaltyReason = "handler exception on subscription",
          )
          connection.fail(closingReason.toString)

        case Failure(throwable) =>
          val canRetry =
            connection.subscriptionRetryPolicy.retryOnException(throwable, logger)
          if (canRetry) {
            // This currently cannot happen because the used retry policy always returns `false`.
            // Kept here in case the retry policy changes in the future.
            logger.warn(s"Closing sequencer subscription due to exception", throwable)
            connection.increasePenalty(
              Penalty.SubscriptionRetriableFailure,
              reason = "retryable failure on subscription",
            )
          } else
            handleFaultySequencer(
              "exception",
              Penalty.SubscriptionNonRetriableFailure,
              penaltyReason = "non-retryable failure on subscription",
              throwableO = Some(throwable),
            )
          connection.fail(closingReason.toString)
      }
    }
  }

  /** NOTE: This method MUST be called with the lock held! */
  private def updateHealth()(implicit traceContext: TraceContext): Unit = {
    val currentConfig = currentConfigWithThreshold

    // If the trust threshold cannot be reached without the faulty sequencers, this takes precedence
    // in how we update the health
    val faultySequencers = faultySequencersRef.get

    val faultySequencersFailureMessageO =
      if (faultySequencers.isEmpty) None
      else
        pool
          .isThresholdStillReachable(currentConfig.trustThreshold, ignored = faultySequencers)
          .left
          .toOption
          .map(error =>
            s"The trust threshold of ${currentConfig.trustThreshold} cannot currently be reached:" +
              s" ${error.nbRemaining} remaining sequencer(s), ${faultySequencers.size} faulty."
          )

    trackedSubscriptions.size match {
      case nb if nb >= currentConfig.activeThreshold.unwrap =>
        faultySequencersFailureMessageO.fold(health.resolveUnhealthy())(health.failureOccurred(_))

      case nb if nb >= currentConfig.trustThreshold.unwrap =>
        faultySequencersFailureMessageO.fold(
          health.degradationOccurred(
            s"below liveness margin: $nb subscription(s) available, trust threshold = ${currentConfig.trustThreshold}," +
              s" liveness margin = ${currentConfig.livenessMargin}"
          )
        )(health.failureOccurred(_))

      case _ if isClosing => health.failureOccurred("closing")

      case nb =>
        pool
          .isThresholdStillReachable(currentConfig.trustThreshold, ignored = Set.empty)
          .fold(
            error =>
              completeWithReason(
                Success(UnrecoverableError(error.toString)),
                updateHealth = _.fatalOccurred(_),
              ),
            _ =>
              faultySequencersFailureMessageO.fold(
                health.failureOccurred(
                  s"only $nb subscription(s) available, trust threshold = ${currentConfig.trustThreshold}"
                )
              )(health.failureOccurred(_)),
          )
    }
  }

  override def start()(implicit traceContext: TraceContext): Unit =
    if (startedRef.getAndSet(true)) {
      logger.debug("Subscription pool already started -- ignoring")
    } else {
      adjustConnectionsIfNeeded()
    }

  override def onClosed(): Unit = {
    val instances = lock.exclusive(trackedSubscriptions.toSeq)
    LifeCycle.close(instances)(logger)
    completeWithReason(Success(SequencerClient.CloseReason.ClientShutdown))(TraceContext.empty)
    super.onClosed()
  }

  override def subscriptions: Set[SequencerSubscriptionWrapper] =
    lock.exclusive(trackedSubscriptions.toSet).map(_.subscriptionWrapper)

  override def checkLiveness(
      eventAndCounter: EventAndOrdinal
  )(implicit traceContext: TraceContext): Unit = {
    // This is called by the aggregator after a successful aggregation.
    // We have made progress and can therefore clear the set of faulty sequencers ("all is forgiven").
    //
    // NOTE: `checkLiveness` and `handleClosingReason` can be called concurrently. We can therefore have a race
    // between `checkLiveness` and `handleFaultySequencer`:
    //
    // - `handleFaultySequencer` runs first and the sequencer failure is lost because the set is subsequently cleared.
    //   The sequencer has nonetheless been penalized, so we will prioritize other sequencers for subscriptions,
    //   but if they also fail we will come back to this one at some point. If it fails again, it will be added
    //   again to the set, and emit warnings or change the health if need be.
    //
    // - `checkLiveness` runs first and the sequencer is marked as faulty for the next round. We will "unfairly" steer
    //   away from this sequencer due to the penalty, and if enough other subscriptions fail we may emit warnings
    //   or change the health without having tried this sequencer for this round. Since we keep retrying,
    //   we will eventually retry this sequencer though, and it will have a chance to provide a valid event,
    //   possibly recovering the health.
    //
    // Both cases will eventually behave as intended, albeit with a possible delay, a spurious warning or
    // a short change in health status. Given that those don't affect the processing, they are harmless.
    val faultySequencers = faultySequencersRef.getAndSet(Set.empty)
    if (faultySequencers.nonEmpty) {
      logger.info(
        s"Successfully processed an event after failures caused by ${faultySequencers.size} sequencer(s)" +
          " -- clearing the set of faulty sequencers"
      )
      // The health may have changed
      lock.exclusive(updateHealth())
    }

    val conf = config
    if (!conf.silentSubscriptionDetectionIsDisabled)
      lock
        .exclusive(trackedSubscriptions.toSet)
        .foreach(
          _.subscriptionWrapper
            .checkLiveness(eventAndCounter, conf.maxTimestampDelta, conf.maxOrdinalDelta)
        )
  }

  private def removeSubscriptionsFromPool(managers: SubscriptionManager*)(implicit
      traceContext: TraceContext
  ): Unit =
    lock.exclusive {
      managers.foreach { manager =>
        if (trackedSubscriptions.remove(manager)) {
          logger.debug(s"Removed ${manager.connection.name} from the subscription pool")
          manager.close()
          metrics.updateSubscriptionHealth(
            mc.withExtraLabels("connection" -> manager.connection.config.name),
            0,
          )
        }
      }

      metrics.activeSubscriptions.updateValue(trackedSubscriptions.size)

      updateHealth()

      // Immediately request new connections if needed
      adjustConnectionsIfNeeded()
    }
}

object SequencerSubscriptionPoolImpl {
  private final case class ConfigWithThreshold(
      private val poolConfig: SequencerSubscriptionPoolConfig,
      trustThreshold: PositiveInt,
  ) {
    val livenessMargin: NonNegativeInt = poolConfig.livenessMargin
    val subscriptionRequestDelay: NonNegativeFiniteDuration = poolConfig.subscriptionRequestDelay
    lazy val activeThreshold: PositiveInt = trustThreshold + livenessMargin
  }

  /** Trait for an object that can provide the starting event for a subscription
    */
  trait SubscriptionStartProvider {

    /** Return the latest event that was successfully aggregated and deposited in the inbox/
      */
    def getLatestProcessedEventO: Option[EventAndOrdinal]
  }
}

class SequencerSubscriptionPoolFactoryImpl(
    subscriptionWrapperFactory: SequencerSubscriptionWrapperFactory,
    subscriptionHandlerFactory: SubscriptionHandlerFactory,
    metrics: SequencerConnectionPoolMetrics,
    metricsContext: MetricsContext,
    timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends SequencerSubscriptionPoolFactory
    with NamedLogging {
  import SequencerSubscriptionPool.SequencerSubscriptionPoolConfig

  def create(
      initialConfig: SequencerSubscriptionPoolConfig,
      connectionPool: SequencerConnectionPool,
      member: Member,
      initialSubscriptionEventO: Option[ProcessingSerializedEvent],
      subscriptionStartProvider: SubscriptionStartProvider,
      useNewAggregator: Boolean,
  )(implicit ec: ExecutionContext): SequencerSubscriptionPool =
    new SequencerSubscriptionPoolImpl(
      initialConfig,
      subscriptionWrapperFactory,
      subscriptionHandlerFactory,
      connectionPool,
      member,
      initialSubscriptionEventO,
      subscriptionStartProvider,
      useNewAggregator,
      metrics,
      metricsContext,
      timeouts,
      loggerFactory,
    )
}
