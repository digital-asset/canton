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
import com.digitalasset.canton.sequencing.client.pool.InternalSequencerConnection.SequencerConnectionState
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
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUnlessShutdownUtil, LoggerUtil, Mutex}

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
                  metrics
                    .subscriptionHealth(mc.withExtraLabels("connection" -> connection.config.name))
                    .updateValue(1)
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

    override def handleClosingReason(
        closingReason: Try[SubscriptionCloseReason[SequencerClientSubscriptionError]]
    )(implicit traceContext: TraceContext): Unit = {
      def complete(reason: Try[SequencerClient.CloseReason]): Unit = {
        completeWithReason(reason, updateHealth = _.fatalOccurred(_))
        LifeCycle.close(SequencerSubscriptionPoolImpl.this)(logger)
      }

      // Notes:
      // * Removing the subscription from the pool ensures that it will be closed and the subscription pool will
      //   request new connections if needed.
      // * Completing the subscription pool currently leads to the closing of the sequencer client, the subscription
      //   pool, and therefore of all the subscription and connections.
      //   Fatally closing the connection in this situation is done solely for the purpose of having an appropriate
      //   error message associated to the closing of the connection.

      closingReason match {
        case Success(SubscriptionCloseReason.Closed) =>
          logger.debug("Sequencer subscription was closed")
          removeSubscriptionsFromPool(this)

        case Success(SubscriptionCloseReason.Shutdown) =>
          logger.info("Sequencer subscription was shutdown")
          removeSubscriptionsFromPool(this)

        case _ if SequencerSubscriptionPoolImpl.this.isClosing =>
          logger.info("Closing sequencer subscription due to an ongoing shutdown")
          removeSubscriptionsFromPool(this)

        case error @ Success(subscriptionError: SubscriptionCloseReason.SubscriptionError) =>
          val canRetry =
            connection.subscriptionRetryPolicy.retryOnError(subscriptionError)
          if (canRetry) {
            logger.debug(s"Closing sequencer subscription due to error: $subscriptionError.")
            connection.fail(closingReason.toString)
          } else {
            logger.warn(s"Permanently closing sequencer subscription due to error: $error")
            connection.fatal(closingReason.toString)
          }

        case Success(_: SubscriptionCloseReason.PermissionDeniedError) =>
          logger.warn("Permanently closing sequencer subscription due to permission denied")
          connection.fatal(closingReason.toString)

        case Success(SubscriptionCloseReason.TokenExpiration) =>
          logger.debug(
            "Sequencer subscription was closed by the server due to a token expiration"
          )
          removeSubscriptionsFromPool(this)

        case Success(SubscriptionCloseReason.SubscriptionIsSilent) =>
          logger.info("Closing sequencer subscription due to inactivity")
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
            case _: SequencerAggregatorError.BogusEvent =>
              // Only happens with the new aggregator
              logger.warn(s"Closing sequencer subscription due to aggregation error: $error")
              connection.fail(closingReason.toString)

            case _: SequencerAggregatorError.ThresholdUnreachable =>
              // Only happens with the new aggregator

              // Simple approach: we close the connection to the synchronizer.
              //
              // A more sophisticated approach would be to check whether the liveness margin was set according to the
              // recommendations of `trust threshold - 1`.
              // If e.g. `trust threshold = 3` and `liveness margin = 1`, it is possible that we receive 2 messages from
              // malicious sequencers, followed by 2 by honest sequencers. We cannot reach a threshold, but the trust
              // assumption of at most `trust threshold - 1` Byzantine nodes was not violated. In this case we could just
              // close all subscriptions and obtain new ones, with the hope that the situation resolves.
              // TODO(i33532): Implement this new approach.
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
              // Only happens with the old aggregator
              logger.error(
                s"Permanently closing sequencer subscription due to aggregation error: $error"
              )
              connection.fatal(closingReason.toString)
              complete(
                Success(
                  SequencerClient.CloseReason.UnrecoverableError(s"event aggregation error: $error")
                )
              )
          }

        case Success(SubscriptionCloseReason.HandlerError(error: EventValidationError)) =>
          logger.warn(
            s"Permanently closing sequencer subscription due to event validation error: $error"
          )
          connection.fatal(closingReason.toString)
          complete(
            Success(
              SequencerClient.CloseReason.UnrecoverableError(s"event validation error: $error")
            )
          )

        case Success(SubscriptionCloseReason.HandlerException(throwable)) =>
          logger.error(
            s"Permanently closing sequencer subscription due to handler exception: $throwable"
          )
          connection.fatal(closingReason.toString)

        case Failure(throwable) =>
          val canRetry =
            connection.subscriptionRetryPolicy.retryOnException(throwable, logger)
          if (canRetry) {
            logger.warn(
              s"Closing sequencer subscription due to exception: $throwable.",
              throwable,
            )
            connection.fail(closingReason.toString)
          } else {
            logger.error(
              s"Permanently closing sequencer subscription due to exception",
              throwable,
            )
            connection.fatal(closingReason.toString)
          }
      }
    }
  }

  private def updateHealth()(implicit traceContext: TraceContext): Unit = {
    val currentConfig = currentConfigWithThreshold

    trackedSubscriptions.size match {
      case nb if nb >= currentConfig.activeThreshold.unwrap => health.resolveUnhealthy()

      case nb if nb >= currentConfig.trustThreshold.unwrap =>
        health.degradationOccurred(
          s"below liveness margin: $nb subscription(s) available, trust threshold = ${currentConfig.trustThreshold}," +
            s" liveness margin = ${currentConfig.livenessMargin}"
        )

      case _ if isClosing => health.failureOccurred("closing")

      case nb =>
        pool
          .isThresholdStillReachable(currentConfig.trustThreshold, Set.empty)
          .fold(
            error =>
              completeWithReason(
                Success(UnrecoverableError(error.toString)),
                updateHealth = _.fatalOccurred(_),
              ),
            _ =>
              health.failureOccurred(
                s"only $nb subscription(s) available, trust threshold = ${currentConfig.trustThreshold}"
              ),
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
          metrics
            .subscriptionHealth(mc.withExtraLabels("connection" -> manager.connection.config.name))
            .updateValue(0)
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
  )(implicit ec: ExecutionContext): SequencerSubscriptionPool =
    new SequencerSubscriptionPoolImpl(
      initialConfig,
      subscriptionWrapperFactory,
      subscriptionHandlerFactory,
      connectionPool,
      member,
      initialSubscriptionEventO,
      subscriptionStartProvider,
      metrics,
      metricsContext,
      timeouts,
      loggerFactory,
    )
}
