// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import cats.syntax.foldable._
import cats.syntax.option._
import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{
  CantonTimestamp,
  PeanoQueue,
  TaskScheduler,
  TaskSchedulerMetrics,
}
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, FlagCloseableAsync, SyncCloseable}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.event.RecordOrderPublisher.PendingPublish
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker.EventClock
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  SequencedSubmission,
}
import com.digitalasset.canton.participant.protocol.{CausalityUpdate, SingleDomainCausalTracker}
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
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.topology.DomainId
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise}

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
    multiDomainEventLog: MultiDomainEventLog,
    singleDomainCausalTracker: SingleDomainCausalTracker,
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    metrics: TaskSchedulerMetrics,
    override protected val timeouts: ProcessingTimeout,
    useCausalityTracking: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContextForPublishing: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  // Synchronization to block publication of new events until preceding events recovered by crash recovery
  // have been published
  private val recovered: Promise[Unit] = Promise()

  private[this] val taskScheduler: TaskScheduler[PublicationTask] =
    new TaskScheduler(
      initSc,
      initTimestamp,
      PublicationTask.orderingSameTimestamp,
      metrics,
      timeouts,
      loggerFactory.appendUnnamedKey("task scheduler owner", "RecordOrderPublisher"),
    )(executionContextForPublishing)

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
      updateO: Option[CausalityUpdate],
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (eventO.isEmpty && updateO.isEmpty) Future.unit
    else {
      logger.debug(
        s"Schedule publication for request counter $requestCounter: event = ${eventO.isDefined}, causality update = ${updateO.isDefined}"
      )
      for {
        _unit <- eventO.fold(
          // There is no [[TimestampedEvent]], so this publication represents a transfer
          updateO.traverse_[Future, Unit](u => eventLog.storeTransferUpdate(u))
        )(event => eventLog.insert(event, updateO))
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
            updateO,
          )
        taskScheduler.scheduleTask(task)
      }
    }

  def scheduleRecoveries(
      toRecover: Seq[PendingPublish]
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Schedule recovery for ${toRecover.length} pending events.")

    val recoverF = MonadUtil.sequentialTraverse_(toRecover) { pendingPublish =>
      logger.info(s"Recover pending causality update $pendingPublish")

      val eventO = pendingPublish match {
        case RecordOrderPublisher.PendingTransferPublish(rc, updateS, ts, eventLogId) => None
        case RecordOrderPublisher.PendingEventPublish(update, event, ts, eventLogId) => Some(event)
      }

      val inFlightRef = eventO.flatMap(tse =>
        tse.requestSequencerCounter.map(sc =>
          InFlightBySequencingInfo(
            eventLog.id.domainId,
            sequenced = SequencedSubmission(sc, tse.timestamp),
          )
        )
      )

      publishEvent(pendingPublish.update, eventO, pendingPublish.rc, inFlightRef)
    }

    recovered.completeWith(recoverF)
    ()
  }

  def scheduleAcsChangePublication(
      recordSequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
      requestCounterO: Option[RequestCounter],
      acsChange: AcsChange,
  )(implicit traceContext: TraceContext): Unit = {
    taskScheduler.scheduleTask(
      AcsChangePublicationTask(recordSequencerCounter, timestamp, requestCounterO)(acsChange)
    )
  }

  /** Schedules an empty acs change publication task to be published to the `acsChangeListener`.
    */
  def scheduleEmptyAcsChangePublication(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Unit =
    if (sequencerCounter >= initSc) {
      taskScheduler.scheduleTask(
        AcsChangePublicationTask(sequencerCounter, timestamp, requestCounterO = None)(
          AcsChange.empty
        )
      )
    }

  /** Signals the progression of time to the record order publisher
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
    override def perform(): Future[Unit] =
      inFlightSubmissionTracker.observeTimestamp(domainId, timestamp).valueOr {
        case InFlightSubmissionTracker.UnknownDomain(_domainId) =>
          // TODO(#6175) Prevent that later tasks get executed if the domain ID is not found,
          //  which should only happen during shutdown
          logger.info(
            s"Skip the synchronization with the in-flight submission tracker for sequencer counter $sequencerCounter at $timestamp due to shutdown."
          )
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
      updateO: Option[CausalityUpdate],
  )(implicit val traceContext: TraceContext)
      extends PublicationTask {

    def recordTime: RecordTime = RecordTime(timestamp, tieBreaker = requestCounter)
    def tieBreaker: Long = requestCounter

    override def perform(): Future[Unit] = {
      for {
        _recovered <- recovered.future
        _unit <- publishEvent(updateO, eventO, requestCounter, inFlightReference)
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
      updateO: Option[CausalityUpdate],
      eventO: Option[TimestampedEvent],
      requestCounter: RequestCounter,
      inFlightRef: Option[InFlightReference],
  )(implicit tc: TraceContext): Future[Unit] = {
    logger.debug(s"Publish event with request counter $requestCounter")
    for {
      clockO <- waitPublishableO(updateO)
      _published <- eventO.fold(Future.unit) { event =>
        val data = PublicationData(eventLog.id, event, inFlightRef)
        multiDomainEventLog.publish(data)
      }
    } yield {
      published(clockO, requestCounter)
    }
  }

  private def published(clockO: Option[EventClock], requestCounter: RequestCounter)(implicit
      tc: TraceContext
  ): Unit = {
    val () = clockO.foreach { c =>
      if (useCausalityTracking)
        singleDomainCausalTracker.globalCausalOrderer.registerPublished(c)
      else ()
    }
    logger.debug(s"Participant completed publishing for event with request counter $requestCounter")
  }

  private def waitPublishableO(
      updateO: Option[CausalityUpdate]
  )(implicit tc: TraceContext): Future[Option[EventClock]] = {
    val updateAndUseTracking = updateO.flatMap { update =>
      if (useCausalityTracking) Some(update)
      else {
        logger.debug(s"Causality tracking is not enabled so not awaiting causal dependencies.")
        None
      }
    }
    updateAndUseTracking.traverse(up => waitPublishable(up))
  }

  private def waitPublishable(
      update: CausalityUpdate
  )(implicit tc: TraceContext): Future[EventClock] = {
    for {
      clockEvent <- singleDomainCausalTracker.registerCausalityUpdate(update)
      clock = clockEvent.clock
      _ = logger.debug(
        s"Causal update $update given clock $clock with dependencies ${clock.waitOn}"
      )
      _unit <- {
        //Block until the event is causally publishable
        singleDomainCausalTracker.globalCausalOrderer.waitPublishable(clock)
      }
    } yield clock
  }

  // tieBreaker is used to order tasks with the same timestamp
  private case class AcsChangePublicationTask(
      override val sequencerCounter: SequencerCounter,
      override val timestamp: CantonTimestamp,
      requestCounterO: Option[RequestCounter],
  )(val acsChange: AcsChange)(implicit val traceContext: TraceContext)
      extends PublicationTask {

    def recordTime: RecordTime =
      RecordTime(timestamp, requestCounterO.getOrElse(RecordTime.lowestTiebreaker))

    override def perform(): Future[Unit] = {
      Future.successful(acsChangeListener.get.foreach(_.publish(recordTime, acsChange)))
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
      SyncCloseable("taskScheduler", taskScheduler.close())
    )
  }
}
object RecordOrderPublisher {
  sealed trait PendingPublish {
    def rc: RequestCounter
    val update: Option[CausalityUpdate]
    val ts: CantonTimestamp
    val createsEvent: Boolean
    val eventLogId: EventLogId
  }

  case class PendingTransferPublish(
      override val rc: RequestCounter,
      updateS: CausalityUpdate,
      ts: CantonTimestamp,
      eventLogId: EventLogId,
  ) extends PendingPublish {
    override val update: Option[CausalityUpdate] = Some(updateS)
    override val createsEvent: Boolean = false
  }

  case class PendingEventPublish(
      update: Option[CausalityUpdate],
      event: TimestampedEvent,
      ts: CantonTimestamp,
      eventLogId: EventLogId,
  ) extends PendingPublish {
    override def rc = event.localOffset
    override val createsEvent: Boolean = true
  }

}
