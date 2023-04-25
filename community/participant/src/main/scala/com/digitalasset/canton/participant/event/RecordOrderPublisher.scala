// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import cats.Eval
import cats.syntax.foldable.*
import cats.syntax.option.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{
  CantonTimestamp,
  PeanoQueue,
  TaskScheduler,
  TaskSchedulerMetrics,
}
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher.PendingPublish
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  SequencedSubmission,
}
import com.digitalasset.canton.participant.store.EventLogId.DomainEventLogId
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.{
  InFlightBySequencingInfo,
  InFlightReference,
}
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.store.{
  EventLogId,
  MultiDomainEventLog,
  SingleDimensionEventLog,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

/** Publishes upstream events and active contract set changes in the order of their record time.
  *
  * The protocol processors emit events and active contract set changes in the result message order,
  * which differs from the record time (= sequencing time) order.
  * The [[RecordOrderPublisher]] pushes the events to [[store.SingleDimensionEventLog]] and the active contract set changes
  * (including empty changes on time proofs) to the appropriate listener, which is normally [[pruning.AcsCommitmentProcessor]].
  * The events are published only after the [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]]
  * has observed the timestamp.
  *
  * All sequencer counters above `initSc` must eventually be signalled to the [[RecordOrderPublisher]] using [[tick]].
  * An event is published only when all sequencer counters between `initSc` and its associated sequencer counter
  * have been signalled.
  *
  * @param initSc The initial sequencer counter from which on events should be published
  * @param initTimestamp The initial timestamp from which on events should be published
  * @param eventLog The event log to publish the events to
  * @param metrics The task scheduler metrics
  * @param executionContextForPublishing Execution context for publishing the events
  */
class RecordOrderPublisher(
    domainId: DomainId,
    initSc: SequencerCounter,
    initTimestamp: CantonTimestamp,
    eventLog: SingleDimensionEventLog[DomainEventLogId],
    multiDomainEventLog: Eval[MultiDomainEventLog],
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    metrics: TaskSchedulerMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
)(implicit val executionContextForPublishing: ExecutionContext, elc: ErrorLoggingContext)
    extends NamedLogging
    with FlagCloseableAsync {

  // Synchronization to block publication of new events until preceding events recovered by crash recovery
  // have been published
  private val recovered: PromiseUnlessShutdown[Unit] =
    new PromiseUnlessShutdown[Unit]("recovered", futureSupervisor)

  private[this] val taskScheduler: TaskScheduler[PublicationTask] =
    new TaskScheduler(
      initSc,
      initTimestamp,
      PublicationTask.orderingSameTimestamp,
      metrics,
      timeouts,
      loggerFactory.appendUnnamedKey("task scheduler owner", "RecordOrderPublisher"),
      futureSupervisor,
    )

  private val acsChangeListener = new AtomicReference[Option[AcsChangeListener]](None)

  /** Schedules the given `event` to be published on the `eventLog`, and schedules the causal "tick" defined by `clock`.
    *
    * @param requestSequencerCounter The sequencer counter associated with the message that corresponds to the request
    * @param eventO The timestamped event to be published
    */
  def schedulePublication(
      requestSequencerCounter: SequencerCounter,
      requestCounter: RequestCounter,
      requestTimestamp: CantonTimestamp,
      eventO: Option[TimestampedEvent],
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (eventO.isEmpty) Future.unit
    else {
      logger.debug(
        s"Schedule publication for request counter $requestCounter: event = ${eventO.isDefined}"
      )

      for {
        _ <- eventO.traverse_(eventLog.insert(_))
      } yield {
        val inFlightReference =
          InFlightBySequencingInfo(
            domainId,
            SequencedSubmission(requestSequencerCounter, requestTimestamp),
          )
        val task =
          EventPublicationTask(requestSequencerCounter, requestTimestamp, requestCounter)(
            eventO,
            Some(inFlightReference),
          )
        taskScheduler.scheduleTask(task)
      }
    }

  def scheduleRecoveries(
      toRecover: Seq[PendingPublish]
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Schedule recovery for ${toRecover.length} pending events.")

    val recoverF = MonadUtil.sequentialTraverse_[FutureUnlessShutdown, PendingPublish](toRecover) {
      pendingPublish =>
        logger.info(s"Recover pending causality update $pendingPublish")

        val eventO = pendingPublish match {
          case RecordOrderPublisher.PendingTransferPublish(rc, ts, eventLogId) => None
          case RecordOrderPublisher.PendingEventPublish(event, ts, eventLogId) => Some(event)
        }

        val inFlightRef = eventO.flatMap(tse =>
          tse.requestSequencerCounter.map(sc =>
            InFlightBySequencingInfo(
              eventLog.id.domainId,
              sequenced = SequencedSubmission(sc, tse.timestamp),
            )
          )
        )

        publishEvent(eventO, pendingPublish.rc, inFlightRef)
    }

    recovered.completeWith(recoverF)
    ()
  }

  def scheduleAcsChangePublication(
      recordSequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      requestCounter: RequestCounter,
      acsChange: AcsChange,
  ): Unit = TraceContext.withNewTraceContext { implicit traceContext =>
    taskScheduler.scheduleTask(
      AcsChangePublicationTask(recordSequencerCounter, timestamp, requestCounter.some)(acsChange)
    )
  }

  /** Schedules an empty acs change publication task to be published to the `acsChangeListener`.
    */
  def scheduleEmptyAcsChangePublication(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  ): Unit = TraceContext.withNewTraceContext { implicit traceContext =>
    if (sequencerCounter >= initSc) {
      taskScheduler.scheduleTask(
        AcsChangePublicationTask(sequencerCounter, timestamp, requestCounterO = None)(
          AcsChange.empty
        )
      )
    }
  }

  /** Signals the progression of time to the record order publisher
    * Does not notify the ACS commitment processor.
    *
    * @see TaskScheduler.addTick for the behaviour.
    */
  def tick(sequencerCounter: SequencerCounter, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit =
    if (timestamp > initTimestamp) {
      logger.debug(
        s"Observing time $timestamp for sequencer counter $sequencerCounter for publishing"
      )
      // Schedule a time observation task that delays the event publication
      // until the InFlightSubmissionTracker has synchronized with submission registration.
      taskScheduler.scheduleTask(TimeObservationTask(sequencerCounter, timestamp))
      taskScheduler.addTick(sequencerCounter, timestamp)
    }

  /** The returned future completes after all events that are currently scheduled for publishing have been published. */
  @VisibleForTesting
  def flush(): Future[Unit] = {
    recovered.future.flatMap { _bool =>
      taskScheduler.flush()
    }
  }

  /** Used to inspect the state of the sequencerCounterQueue, for testing purposes. */
  @VisibleForTesting
  def readSequencerCounterQueue(
      sequencerCounter: SequencerCounter
  ): PeanoQueue.AssociatedValue[CantonTimestamp] =
    taskScheduler.readSequencerCounterQueue(sequencerCounter)

  private sealed trait PublicationTask extends TaskScheduler.TimedTask

  private object PublicationTask {
    def orderingSameTimestamp: Ordering[PublicationTask] = Ordering.by(rankSameTimestamp)

    private def rankSameTimestamp(x: PublicationTask): Option[(Option[RequestCounter], Int)] =
      x match {
        case _: TimeObservationTask =>
          // TimeObservationTask comes first so that we synchronize with the InFlightSubmissionTracker before publishing an event.
          // All TimeObservationTasks with the same timestamp are considered equal.
          None
        case task: EventPublicationTask =>
          (
            task.requestCounter.some,
            0, // EventPublicationTask comes before AcsChangePublicationTask if they have the same tie breaker. This is an arbitrary decision.
          ).some
        case task: AcsChangePublicationTask => (task.requestCounterO, 1).some
      }
  }

  /** Task to synchronize with the [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]].
    * Must run before the event is published to the event log to make sure that a submission to be registered cannot
    * concurrently be deleted.
    */
  private case class TimeObservationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext)
      extends PublicationTask {
    override def perform(): FutureUnlessShutdown[Unit] = {
      performUnlessClosingUSF("observe-timestamp-task") {
        /*
         * Observe timestamp will only return an UnknownDomain error if the domain ID is not in the connectedDomainMap
         * in the CantonSyncService. When disconnecting from the domain we first remove the domain from the map and only then
         * close the sync domain, so there's a window at the beginning of shutdown where we could get an UnknownDomain error here.
         * To prevent further tasks to be executed we return a 'FutureUnlessShutdown.abortedDueToShutdown' which will
         * cause the execution queue in the task scheduler to abort all subsequent queued tasks instead of running them.
         * Note that in this context 'shutdown' doesn't necessarily mean complete node shutdown, closing of the SyncDomain is enough,
         * which can happen when we disconnect from the domain (without necessarily shutting down the node entirely)
         */
        inFlightSubmissionTracker
          .observeTimestamp(domainId, timestamp)
          .mapK(FutureUnlessShutdown.outcomeK)
          .valueOrF { case InFlightSubmissionTracker.UnknownDomain(_domainId) =>
            logger.info(
              s"Skip the synchronization with the in-flight submission tracker for sequencer counter $sequencerCounter at $timestamp due to shutdown."
            )
            FutureUnlessShutdown.abortedDueToShutdown
          }
      }
    }

    override def pretty: Pretty[this.type] = prettyOfClass(
      param("timestamp", _.timestamp),
      param("sequencer counter", _.sequencerCounter),
    )
  }

  /** Task to register the causality update `update` and publish the event `event` if defined.
    * Some causality updates are not currently associated with an event, for example transfer-ins
    * require a causality update but are not always associated with an event. This is why the event
    * is optional.
    */
  private[RecordOrderPublisher] case class EventPublicationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
      requestCounter: RequestCounter,
  )(
      val eventO: Option[TimestampedEvent],
      val inFlightReference: Option[InFlightReference],
  )(implicit val traceContext: TraceContext)
      extends PublicationTask {

    def recordTime: RecordTime = RecordTime(timestamp, tieBreaker = requestCounter.unwrap)
    def tieBreaker: Long = requestCounter.unwrap

    override def perform(): FutureUnlessShutdown[Unit] = {
      for {
        _recovered <- recovered.futureUS
        _unit <- publishEvent(eventO, requestCounter, inFlightReference)
      } yield ()
    }

    override def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp),
        param("sequencerCounter", _.sequencerCounter),
        param("event", _.eventO),
      )
  }

  private def publishEvent(
      eventO: Option[TimestampedEvent],
      requestCounter: RequestCounter,
      inFlightRef: Option[InFlightReference],
  )(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingUSF("publish-event") {
      logger.debug(s"Publish event with request counter $requestCounter")
      for {
        _published <- eventO.fold(FutureUnlessShutdown.unit) { event =>
          val data = PublicationData(eventLog.id, event, inFlightRef)
          FutureUnlessShutdown.outcomeF(multiDomainEventLog.value.publish(data))
        }
      } yield ()
    }

  // tieBreaker is used to order tasks with the same timestamp
  private case class AcsChangePublicationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
      requestCounterO: Option[RequestCounter],
  )(val acsChange: AcsChange)(implicit val traceContext: TraceContext)
      extends PublicationTask {

    def recordTime: RecordTime =
      RecordTime(timestamp, requestCounterO.map(_.unwrap).getOrElse(RecordTime.lowestTiebreaker))

    override def perform(): FutureUnlessShutdown[Unit] = {
      FutureUnlessShutdown.pure(acsChangeListener.get.foreach(_.publish(recordTime, acsChange)))
    }

    override def pretty: Pretty[this.type] =
      prettyOfClass(param("timestamp", _.timestamp), param("sequencerCounter", _.sequencerCounter))
  }

  def setAcsChangeListener(listener: AcsChangeListener): Unit = {
    val _ = acsChangeListener.getAndUpdate {
      case None => Some(listener)
      case Some(_acsChangeListenerAlreadySet) =>
        throw new IllegalArgumentException("ACS change listener already set")
    }
  }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    Seq(
      SyncCloseable("taskScheduler", taskScheduler.close()),
      SyncCloseable("recovered-promise", recovered.shutdown()),
    )
  }
}
object RecordOrderPublisher {
  sealed trait PendingPublish {
    def rc: RequestCounter // TODO(#10497) should that be a localOffset ?
    val ts: CantonTimestamp
    val createsEvent: Boolean
    val eventLogId: EventLogId
  }

  final case class PendingTransferPublish(
      override val rc: RequestCounter,
      ts: CantonTimestamp,
      eventLogId: EventLogId,
  ) extends PendingPublish {
    override val createsEvent: Boolean = false
  }

  final case class PendingEventPublish(
      event: TimestampedEvent,
      ts: CantonTimestamp,
      eventLogId: EventLogId,
  ) extends PendingPublish {
    override def rc = RequestCounter(event.localOffset)
    override val createsEvent: Boolean = true
  }

}
