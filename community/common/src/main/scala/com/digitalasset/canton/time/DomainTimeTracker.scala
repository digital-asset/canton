// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.syntax.option._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.OrdinaryApplicationHandler
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.time.DomainTimeTracker._
import com.digitalasset.canton.time.admin.v0
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.Thereafter.syntax._
import com.digitalasset.canton.util._
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicReference
import scala.Ordering.Implicits._
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

/** Configuration for the domain time tracker.
  * @param observationLatency Even if the host and domain clocks are perfectly synchronized there will always be some latency
  *                           for an event to be delivered (storage, transmission, processing).
  *                           If the current host time exceeds the next expected timestamp by this observation latency
  *                           then we will request a time proof (unless we have received a recent event within the
  *                           patience duration described below).
  * @param patienceDuration We will only request a time proof if this given duration has elapsed since we last received
  *                         an event (measured using the host clock). This prevents requesting timestamps when we
  *                         are observing events from the domain (particularly if the local node is catching up on
  *                         old activity).
  * @param minObservationDuration We will try to ensure that we receive a time at least once during this duration (measured
  *                               using the host clock). This is practically useful if there is no other activity on
  *                               the domain as the sequencer client will then have an event to acknowledge allowing
  *                               sequenced events to be pruned before this point. We may in the future use this to monitor
  *                               clock skews between the host and domain.
  * @param timeRequest configuration for how we ask for a time proof.
  */
case class DomainTimeTrackerConfig(
    observationLatency: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(250),
    patienceDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(500),
    minObservationDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(24),
    timeRequest: TimeProofRequestConfig = TimeProofRequestConfig(),
) extends HasProtoV0[v0.DomainTimeTrackerConfig] {
  override def toProtoV0: v0.DomainTimeTrackerConfig = v0.DomainTimeTrackerConfig(
    observationLatency.toProtoPrimitive.some,
    patienceDuration.toProtoPrimitive.some,
    minObservationDuration.toProtoPrimitive.some,
    timeRequest.toProtoV0.some,
  )
}

object DomainTimeTrackerConfig {
  def fromProto(
      configP: v0.DomainTimeTrackerConfig
  ): ParsingResult[DomainTimeTrackerConfig] =
    for {
      observationLatency <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("observationLatency"),
        "observationLatency",
        configP.observationLatency,
      )
      patienceDuration <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("patienceDuration"),
        "patienceDuration",
        configP.patienceDuration,
      )
      minObservationDuration <- ProtoConverter.parseRequired(
        NonNegativeFiniteDuration.fromProtoPrimitive("minObservationDuration"),
        "minObservationDuration",
        configP.minObservationDuration,
      )
      timeProofRequest <- ProtoConverter.parseRequired(
        TimeProofRequestConfig.fromProto,
        "timeProofRequest",
        configP.timeProofRequest,
      )
    } yield DomainTimeTrackerConfig(
      observationLatency,
      patienceDuration,
      minObservationDuration,
      timeProofRequest,
    )
}

/** Provides a variety of methods for tracking time on the domain.
  *  - fetchTime and fetchTimeProof allows for proactively asking for a recent time or time proof.
  *  - requestTick asks the tracker to ensure that an event is witnessed for the given time or greater (useful for timeouts).
  *  - awaitTick will return a future to wait for the given time being reached on the target domain.
  *
  * We currently assume that the domain and our host are roughly synchronized
  * and typically won't expect to see a time on a domain until we have passed that point on our local clock.
  * We then wait for `observationLatency` past the timestamp we are expecting to elapse on our local clock
  * as transmission of an event with that timestamp will still take some time to arrive at our host.
  * This avoids frequently asking for times before we've reached the timestamps we're looking for locally.
  *
  * We also take into account a `patienceDuration` that will cause us to defer asking for a time if we
  * have recently seen events for the domain. This is particularly useful if we are significantly behind and
  * reading many old events from the domain.
  *
  * If no activity is happening on the domain we will try to ensure that we have observed an event at least once
  * during the `minObservationDuration`.
  */
class DomainTimeTracker(
    config: DomainTimeTrackerConfig,
    clock: Clock,
    timeRequestSubmitter: TimeProofRequestSubmitter,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasFlushFuture {

  // timestamps that we are waiting to observe held in an ascending order queue
  // modifications to pendingTicks must be made while holding the `lock`
  private val pendingTicks: PriorityBlockingQueue[AwaitingTick] =
    new PriorityBlockingQueue[AwaitingTick](
      PriorityBlockingQueueUtil.DefaultInitialCapacity,
      AwaitingTick.ordering,
    )

  /** Ensures that changes to [[timestampRef]] and [[pendingTicks]] happen atomically */
  private val lock: AnyRef = new Object

  private def withLock[A](fn: => A): A = {
    blocking {
      lock.synchronized { fn }
    }
  }

  // the maximum timestamp we can support waiting for without causing an overflow
  private val maxPendingTick = CantonTimestamp.MaxValue.minus(config.observationLatency.unwrap)

  private val timestampRef: AtomicReference[LatestAndNext[CantonTimestamp]] =
    new AtomicReference[LatestAndNext[CantonTimestamp]](LatestAndNext.empty)

  private val timeProofRef: AtomicReference[LatestAndNext[TimeProof]] =
    new AtomicReference[LatestAndNext[TimeProof]](LatestAndNext.empty)

  // kick off the scheduling to ensure we see timestamps at least occasionally
  ensureMinObservationDuration()

  /** Fetch the latest timestamp we have observed from the domain.
    * Note this isn't restored on startup so will be empty until the first event after starting is seen.
    */
  def latestTime: Option[CantonTimestamp] = timestampRef.get().latest.map(_.value)

  /** Fetches a recent domain timestamp.
    * If the latest received event has been received within the given `freshnessBound` (measured on the participant clock) this domain timestamp
    * will be immediately returned.
    * If a sufficiently fresh timestamp is unavailable then a request for a time proof will be made, however
    * the returned future will be resolved by the first event after this call (which may not necessarily be
    * the response to our time proof request).
    *
    * @return The future completes with the domain's timestamp of the event.
    *         So if the participant's local clock is ahead of the domain clock,
    *         the timestamp may be earlier than now minus the freshness bound.
    */
  def fetchTime(freshnessBound: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero)(implicit
      traceContext: TraceContext
  ): Future[CantonTimestamp] =
    fetch(freshnessBound, timestampRef, requiresTimeProof = false)

  /** Similar to `fetchTime` but will only return time proof. */
  def fetchTimeProof(freshnessBound: NonNegativeFiniteDuration = NonNegativeFiniteDuration.Zero)(
      implicit traceContext: TraceContext
  ): Future[TimeProof] =
    fetch(freshnessBound, timeProofRef, requiresTimeProof = true)

  /** Register that we want to observe a domain time.
    * The tracker will attempt to make sure that we observe a sequenced event with this timestamp or greater.
    *
    * The maximum timestamp that we support waiting for is [[data.CantonTimestamp.MaxValue]] minus the configured
    * observation latency. If a greater value is provided a warning will be logged but no error will be
    * thrown or returned.
    */
  def requestTick(ts: CantonTimestamp)(implicit traceContext: TraceContext): Unit =
    requestTicks(Seq(ts))

  /** Register that we want to observe domain times.
    * The tracker will attempt to make sure that we observe a sequenced event with the given timestamps or greater.
    *
    * The maximum timestamp that we support waiting for is [[data.CantonTimestamp.MaxValue]] minus the configured
    * observation latency. If a greater value is provided a warning will be logged but no error will be
    * thrown or returned.
    */
  def requestTicks(timestamps: Seq[CantonTimestamp])(implicit traceContext: TraceContext): Unit = {
    val (toRequest, tooLarge) = timestamps.partition(_ < maxPendingTick)
    if (tooLarge.nonEmpty) {
      val first = tooLarge.minOption.getOrElse(
        ErrorUtil.internalError(new RuntimeException("A non-empty Seq must have a minimum"))
      )
      val last = tooLarge.maxOption.getOrElse(
        ErrorUtil.internalError(new RuntimeException("A non-empty Seq must have a maximum"))
      )
      logger.warn(
        s"Ignoring request for ${tooLarge.size} ticks from $first to $last as they are too large"
      )
    }
    if (toRequest.nonEmpty) {
      withLock {
        toRequest.foreach { tick =>
          pendingTicks.put(new AwaitingTick(tick))
        }
      }
      maybeScheduleUpdate()
    }
  }

  /** Waits for an event with a timestamp greater or equal to `ts` to be observed from the domain.
    * If we have already witnessed an event with a timestamp equal or exceeding the given `ts` then `None`
    * will be returned.
    */
  def awaitTick(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): Option[Future[CantonTimestamp]] = {
    val latest = timestampRef.get().latest
    if (latest.exists(_.value >= ts)) {
      logger.debug(s"No await time for ${ts} as we are already at $latest")
      None
    } else {
      logger.debug(s"Await time for ${ts} as we are at ${latest.map(_.value)} ")
      // wait for this timestamp to be observed
      val promise = Promise[CantonTimestamp]()
      withLock {
        pendingTicks.put(new AwaitingTick(ts, promise.some))
      }
      maybeScheduleUpdate()
      promise.future.some
    }
  }

  /** Create a [[sequencing.OrdinaryApplicationHandler]] for updating this time tracker */
  def wrapHandler[Env](
      handler: OrdinaryApplicationHandler[Env]
  ): OrdinaryApplicationHandler[Env] = { tracedEvents =>
    tracedEvents.withTraceContext { implicit batchTraceContext => events =>
      update(events)

      // call the wrapped handler
      handler(tracedEvents)
    }
  }

  @VisibleForTesting
  private[time] def update(events: Seq[OrdinarySequencedEvent[_]])(implicit
      batchTraceContext: TraceContext
  ): Unit = {
    withLock {
      def updateOne(event: OrdinarySequencedEvent[_]): Unit = {
        val oldTimestamp =
          timestampRef.getAndSet(LatestAndNext(received(event.timestamp).some, None))
        oldTimestamp.next.foreach(_.trySuccess(event.timestamp))

        TimeProof.fromEventO(event).foreach { proof =>
          val oldTimeProof = timeProofRef.getAndSet(LatestAndNext(received(proof).some, None))
          oldTimeProof.next.foreach(_.trySuccess(proof))
          timeRequestSubmitter.handleTimeProof(proof)
        }
      }

      @SuppressWarnings(Array("org.wartremover.warts.While"))
      def removeTicks(ts: CantonTimestamp): Unit = {
        // remove pending ticks up to and including this timestamp
        while (Option(pendingTicks.peek()).exists(_.ts <= ts)) {
          val removed = pendingTicks.poll()
          // complete any futures waiting for them
          removed.complete()
        }
      }

      // currently all actions from events are synchronous and do not return errors so this simple processing is safe.
      // for timestamps we could just take the latest event in batch, however as we're also looking for time proofs
      // we supply every event sequentially.
      // this could likely be optimised to just process the latest time proof and timestamp from the batch if required.
      events.foreach(updateOne)
      events.lastOption.foreach(event => removeTicks(event.timestamp))
    }
    maybeScheduleUpdate()
  }

  private def fetch[A](
      freshnessBound: NonNegativeFiniteDuration,
      latestAndNextRef: AtomicReference[LatestAndNext[A]],
      requiresTimeProof: Boolean,
  )(implicit traceContext: TraceContext): Future[A] = {
    val now = clock.now
    // TODO(error handling): This could underflow and throw an exception if we specify a very large freshness bound duration like 10000 years.
    val receivedWithin = now.minus(freshnessBound.unwrap)

    val (future, needUpdate) = withLock {
      val newState = latestAndNextRef.updateAndGet { latestAndNext =>
        latestAndNext.latest match {
          case Some(Received(_value, receivedAt)) if receivedAt >= receivedWithin =>
            latestAndNext
          case _latest => latestAndNext.withNextSet
        }
      }
      newState.latest match {
        case Some(Received(value, receivedAt)) if receivedAt >= receivedWithin =>
          Future.successful(value) -> false
        case _ =>
          val promise = newState.next.getOrElse(
            ErrorUtil.internalError(
              new IllegalStateException("Should have set to a promise in prior block")
            )
          )

          // if we're looking for a time proof then just request one; no need to call `maybeScheduleUpdate()`
          // as the TimeProofRequestSubmitter itself retries if it doesn't get one soon enough.
          // otherwise if looking for a timestamp we don't care what domain time we're looking for (just the next),
          // so just register a pending tick for the earliest point.
          // we use MinValue rather than Epoch so it will still be considered far before "now" when initially started
          // using the simclock.
          if (requiresTimeProof) timeRequestSubmitter.fetchTimeProof()
          else pendingTicks.put(new AwaitingTick(CantonTimestamp.MinValue))
          promise.future -> !requiresTimeProof
      }
    }
    if (needUpdate) maybeScheduleUpdate()
    future
  }

  /** When we expect to observe the earliest timestamp in local time. */
  @VisibleForTesting
  private[time] def earliestExpectedObservationTime(): Option[CantonTimestamp] =
    Option(withLock(pendingTicks.peek())).map(_.ts.add(config.observationLatency.unwrap))

  /** Local time of when we'd like to see the next event produced.
    * If we are waiting to observe a timestamp, this value will be the greater of:
    *  - the local time of when we'd like to see the earliest tick
    *  - the time we last received an event offset plus the configured patience duration
    *
    * This allows doing nothing for a long period if the timestamp we're looking at is far in the future.
    * However if the domain is far behind but regularly producing events we will wait until we haven't
    * witnessed an event for the patience duration.
    */
  private def nextScheduledCheck(): Option[CantonTimestamp] = {
    // if we're not waiting for an event, then we don't need to see one
    earliestExpectedObservationTime().map { expectedEvent =>
      timestampRef
        .get()
        .latest
        .fold(expectedEvent)(
          _.receivedAt.add(config.patienceDuration.unwrap).max(expectedEvent)
        )
    }
  }

  /** we're unable to cancel an update once scheduled, so if we decide to schedule an earlier update than an update already
    * scheduled we update this to the earlier value and then check this value when the scheduled task is run
    */
  private val nextScheduledUpdate: AtomicReference[Option[CantonTimestamp]] =
    new AtomicReference[Option[CantonTimestamp]](None)

  /** After [[pendingTicks]] or [[timestampRef]] have been updated, call this to determine whether a scheduled update is required.
    * It will be scheduled if there isn't an existing or earlier update pending.
    */
  private def maybeScheduleUpdate()(implicit traceContext: TraceContext): Unit = {

    def updateNow(): Unit = {
      // Fine to repeatedly call without guards as the submitter will make no more than one request in-flight at once
      // The next call to update will complete the promise in `timestampRef.get().next`.
      timeRequestSubmitter.fetchTimeProof()
    }

    nextScheduledCheck() foreach { updateBy =>
      // if we've already surpassed when we wanted to see a time, just ask for one
      // means that we're waiting on a timestamp and we're not receiving regular updates
      val now = clock.now

      if (updateBy <= now) updateNow()
      else {
        def updateCondition(current: Option[CantonTimestamp]): Boolean =
          !current.exists(ts => ts > now && ts <= updateBy)
        val current = nextScheduledUpdate.getAndUpdate { current =>
          if (updateCondition(current)) updateBy.some else current
        }
        if (updateCondition(current)) {
          // schedule next update
          val nextF = clock.scheduleAt(_ => maybeScheduleUpdate(), updateBy).unwrap
          addToFlush(s"scheduled update at $updateBy")(nextF)
          FutureUtil.doNotAwait(nextF, s"scheduled update at $updateBy failed")
        }
      }
    }
  }

  private def received[A](value: A) = Received(value, receivedAt = clock.now)

  @VisibleForTesting
  protected[time] def flush(): Future[Unit] = doFlush()

  override def onClosed(): Unit = timeRequestSubmitter.close()

  /** In the absence of any real activity on the domain we will infrequently request a time.
    * Short of being aware of a relatively recent domain time, it will allow features like sequencer pruning
    * to keep a relatively recent acknowledgment point for the member even if they're not doing anything.
    */
  private def ensureMinObservationDuration(): Unit = withNewTraceContext { implicit traceContext =>
    val minObservationDuration = config.minObservationDuration.duration
    def performUpdate(expectedUpdateBy: CantonTimestamp): Unit =
      performUnlessClosing {
        val lastObserved = timestampRef.get().latest.map(_.receivedAt)

        // did we see an event within the observation window
        if (lastObserved.exists(_ >= expectedUpdateBy.minus(minObservationDuration))) {
          // we did
          scheduleNextUpdate()
        } else {
          // we didn't so ask for a time
          logger.debug(
            s"The minimum observation duration $minObservationDuration has elapsed since last observing the domain time (${lastObserved.map(_.toString).getOrElse("never")}) so will request a proof of time"
          )
          FutureUtil.doNotAwait(
            // fetchTime shouldn't fail (if anything it will never complete due to infinite retries or closing)
            // but ensure schedule is called regardless
            fetchTime().thereafter(_ => scheduleNextUpdate()),
            "Failed to fetch a time to ensure the minimum observation duration",
          )
        }
      }.onShutdown(())

    def scheduleNextUpdate(): Unit =
      performUnlessClosing {
        val latestTimestamp = timestampRef.get().latest.fold(clock.now)(_.receivedAt)
        val expectUpdateBy = latestTimestamp.add(minObservationDuration).immediateSuccessor

        val _ = clock.scheduleAt(performUpdate, expectUpdateBy)
      }.onShutdown(())

    scheduleNextUpdate()
  }

}

object DomainTimeTracker {

  private class AwaitingTick(
      val ts: CantonTimestamp,
      promiseO: Option[Promise[CantonTimestamp]] = None,
  ) {
    def complete(): Unit = promiseO.foreach(_.trySuccess(ts))
  }
  private object AwaitingTick {
    implicit val ordering: Ordering[AwaitingTick] = Ordering.by(_.ts)
  }

  /** Keep track of a value, and when we received said value, measured on the participant's clock */
  case class Received[+A](value: A, receivedAt: CantonTimestamp)

  /** Keep track of the latest value received and a promise to complete when the next one arrives
    * It is not a case class so that equality is object identity (equality on promises is anyway object identity).
    */
  class LatestAndNext[A](val latest: Option[Received[A]], val next: Option[Promise[A]]) {
    def withNextSet: LatestAndNext[A] =
      next.fold(LatestAndNext(latest, Promise[A]().some))(_ => this)
  }
  object LatestAndNext {
    def apply[A](latest: Option[Received[A]], next: Option[Promise[A]]): LatestAndNext[A] =
      new LatestAndNext(latest, next)
    def empty[A]: LatestAndNext[A] = LatestAndNext(None, None)
  }

  def apply(
      config: DomainTimeTrackerConfig,
      clock: Clock,
      sequencerClient: SequencerClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): DomainTimeTracker =
    new DomainTimeTracker(
      config,
      clock,
      TimeProofRequestSubmitter(config.timeRequest, clock, sequencerClient, loggerFactory),
      sequencerClient.timeouts,
      loggerFactory,
    )
}
