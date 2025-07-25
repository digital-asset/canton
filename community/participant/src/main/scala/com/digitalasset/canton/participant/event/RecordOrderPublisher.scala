// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.event

import cats.implicits.catsSyntaxOptionId
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{
  CantonTimestamp,
  LogicalUpgradeTime,
  SynchronizerSuccessor,
  TaskScheduler,
  TaskSchedulerMetrics,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.{
  EmptyAcsPublicationRequired,
  LogicalSynchronizerUpgradeTimeReached,
}
import com.digitalasset.canton.ledger.participant.state.{
  FloatingUpdate,
  SequencedUpdate,
  SynchronizerUpdate,
  Update,
}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Helper trait for Online Party Replication event publishing. Refer to methods in the
  * [[RecordOrderPublisher]] for documentation.
  */
sealed trait PublishesOnlinePartyReplicationEvents {
  def schedulePublishAddContracts(buildEventAtRecordTime: CantonTimestamp => Update)(implicit
      traceContext: TraceContext
  ): UnlessShutdown[Unit]

  def publishBufferedEvents()(implicit traceContext: TraceContext): UnlessShutdown[Unit]
}

/** Publishes upstream events and active contract set changes in the order of their record time.
  *
  * The protocol processors produce events and active contract set changes in the result message
  * order in phase 7. The [[RecordOrderPublisher]] pushes the events to indexer and the active
  * contract set changes in record time order (which equals sequencing time order). (including empty
  * changes on time proofs) to the appropriate listener, which is normally
  * [[pruning.AcsCommitmentProcessor]]. The events are published only after the
  * [[com.digitalasset.canton.participant.protocol.submission.InFlightSubmissionTracker]] has
  * observed the timestamp.
  *
  * All sequencer counters above and including `initSc` must eventually be signalled to the
  * [[RecordOrderPublisher]] using [[tick]]. An event is published only when all sequencer counters
  * between `initSc` and its associated sequencer counter have been signalled.
  *
  * @param initSc
  *   The initial sequencer counter from which on events should be published
  * @param initTimestamp
  *   The initial timestamp from which on events should be published
  * @param metrics
  *   The task scheduler metrics
  * @param executionContextForPublishing
  *   Execution context for publishing the events
  */
class RecordOrderPublisher private (
    psid: PhysicalSynchronizerId,
    initSc: SequencerCounter,
    val initTimestamp: CantonTimestamp,
    ledgerApiIndexer: LedgerApiIndexer,
    metrics: TaskSchedulerMetrics,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
    clock: Clock,
)(implicit val executionContextForPublishing: ExecutionContext)
    extends PublishesOnlinePartyReplicationEvents
    with NamedLogging
    with FlagCloseableAsync
    with HasCloseContext
    with PromiseUnlessShutdownFactory {

  private[this] val taskScheduler: TaskScheduler[PublicationTask] =
    TaskScheduler[PublicationTask](
      initSc,
      initTimestamp,
      PublicationTask.orderingSameTimestamp,
      metrics,
      exitOnFatalFailures = exitOnFatalFailures,
      timeouts,
      loggerFactory.appendUnnamedKey("task scheduler owner", "RecordOrderPublisher"),
      futureSupervisor,
      clock,
    )

  private val ledgerApiIndexerBuffer =
    new AtomicReference[Option[EventBuffer]](None)

  private val synchronizerSuccessor: AtomicReference[Option[SynchronizerSuccessor]] =
    new AtomicReference(None)

  private def onlyForTestingRecordAcceptedTransactions(event: SequencedUpdate): Unit =
    for {
      store <- ledgerApiIndexer.onlyForTestingTransactionInMemoryStore
      transactionAccepted <- event match {
        case txAccepted: Update.TransactionAccepted => Some(txAccepted)
        case _ => None
      }
    } {
      store.put(
        updateId = transactionAccepted.updateId,
        lfVersionedTransaction = transactionAccepted.transaction,
      )
    }

  /** Schedules the given `eventO` to be published on the `eventLog`, and schedules the causal
    * "tick" defined by `clock`. Tick must be called exactly once for all sequencer counters higher
    * than initTimestamp.
    *
    * @param event
    *   The update event to be published.
    * @param sequencerCounter
    *   The SequencerCounter of the sequenced event which resulted in this event
    * @param rcO
    *   The optional request counter for logging as RCs are more human-readable than timestamps.
    */
  def tick(event: SequencedUpdate, sequencerCounter: SequencerCounter, rcO: Option[RequestCounter])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    synchronizeWithClosingF(functionFullName) {
      if (event.recordTime > initTimestamp) {
        rcO
          .foreach(requestCounter =>
            logger.debug(s"Schedule publication for request counter $requestCounter")
          )
        onlyForTestingRecordAcceptedTransactions(event)
        taskScheduler.scheduleTask(EventPublicationTask(event, sequencerCounter))
        logger.debug(
          s"Observing time ${event.recordTime} for sequencer counter $sequencerCounter for publishing (with event:$event, requestCounterO:$rcO)"
        )
        taskScheduler.addTick(sequencerCounter, event.recordTime)
        // this adds backpressure from indexer queue to protocol processing:
        //   indexer pekko source queue back-pressures via offer Future,
        //   this propagates via in RecoveringQueue,
        //   which propagates here in the taskScheduler's SimpleExecutionQueue,
        //   which bubble up exactly here: waiting for all the possible event enqueueing to happen after the tick.
        taskScheduler.flush()
      } else {
        logger.debug(
          s"Skipping tick at sequencerCounter:$sequencerCounter timestamp:${event.recordTime} (publication of event $event)"
        )
        Future.unit
      }
    }

  /** Schedule a floating event, if the current synchronizer time is earlier than timestamp.
    * @param timestamp
    *   The desired timestamp of the publication: if cannot be met, the function will return a Left.
    * @param eventFactory
    *   A function returning an optional Update to be published. This function will be executed as
    *   the scheduled task is executing. (if scheduling is possible) This function will be called
    *   with timestamp.
    * @param onScheduled
    *   A function creating a FutureUnlessShutdown[T]. This function will be only executed, if the
    *   scheduling is possible. This function will be executed before the
    *   scheduleFloatingEventPublication returns. (if scheduling is possible) Execution of the
    *   floating event publication will wait for the onScheduled operation to finish.
    * @param traceContext
    *   Should be the TraceContext of the event
    * @return
    *   A Left with the current synchronizer time, if scheduling is not possible as the synchronizer
    *   time is bigger. A Right with the result of the onScheduled FutureUnlessShutdown[T].
    */
  def scheduleFloatingEventPublication[T](
      timestamp: CantonTimestamp,
      eventFactory: CantonTimestamp => Option[FloatingUpdate],
      onScheduled: () => FutureUnlessShutdown[T], // perform will wait for this to complete
  )(implicit
      traceContext: TraceContext
  ): UnlessShutdown[Either[CantonTimestamp, FutureUnlessShutdown[T]]] =
    synchronizeWithClosingSync(functionFullName) {
      taskScheduler
        .scheduleTaskIfLater(
          desiredTimestamp = timestamp,
          taskFactory = _ => {
            val resultFUS = onScheduled()
            FloatingEventPublicationTask(
              waitFor = resultFUS,
              timestamp = timestamp,
            )(() => eventFactory(timestamp))
          },
        )
        .map(_.waitFor)
    }

  /** Schedule a floating event, if the current synchronizer time is earlier than timestamp.
    * @param timestamp
    *   The desired timestamp of the publication: if cannot be met, the function will return a Left.
    * @param eventFactory
    *   A function returning an optional Update to be published. This function will be executed as
    *   the scheduled task is executing. (if scheduling is possible) This function will be called
    *   with timestamp.
    * @param traceContext
    *   Should be the TraceContext of the event
    * @return
    *   A Left with the current synchronizer time, if scheduling is not possible as the synchronizer
    *   time is bigger. A Right with unit if possible.
    */
  def scheduleFloatingEventPublication(
      timestamp: CantonTimestamp,
      eventFactory: CantonTimestamp => Option[FloatingUpdate],
  )(implicit traceContext: TraceContext): UnlessShutdown[Either[CantonTimestamp, Unit]] =
    scheduleFloatingEventPublication(
      timestamp = timestamp,
      eventFactory = eventFactory,
      onScheduled = () => FutureUnlessShutdown.unit,
    ).map(_.map(_ => ()))

  /** Schedule a floating event immediately: with the synchronizer time of the last published event.
    * @param eventFactory
    *   A function returning an optional Update to be published. This function will be executed as
    *   the scheduled task is executing. This function will be called with the realized synchronizer
    *   timestamp.
    * @param traceContext
    *   Should be the TraceContext of the event
    * @return
    *   The timestamp used for the publication.
    */
  def scheduleFloatingEventPublicationImmediately(
      eventFactory: CantonTimestamp => Option[FloatingUpdate]
  )(implicit traceContext: TraceContext): UnlessShutdown[CantonTimestamp] =
    synchronizeWithClosingSync(functionFullName) {
      taskScheduler
        .scheduleTaskImmediately(
          taskFactory = immediateTimestamp =>
            eventFactory(immediateTimestamp) match {
              case Some(event) =>
                publishOrBuffer(
                  event,
                  s"floating event immediately with timestamp $immediateTimestamp",
                )
              case None =>
                logger.debug(
                  s"Skip publish-immediately floating event with timestamp $immediateTimestamp: nothing to publish"
                )
                FutureUnlessShutdown.unit
            },
          taskTraceContext = traceContext,
        )
    }

  /** Schedules an empty acs change publication task to be published to the `acsChangeListener`.
    */
  def scheduleEmptyAcsChangePublication(
      sequencerCounter: SequencerCounter,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): UnlessShutdown[Unit] =
    synchronizeWithClosingSync(functionFullName) {
      if (sequencerCounter >= initSc) {
        scheduleFloatingEventPublication(
          timestamp = timestamp,
          eventFactory = EmptyAcsPublicationRequired(psid.logical, _).some,
        ).foreach(
          _.toOption.getOrElse(
            ErrorUtil.invalidState(
              "Trying to schedule empty ACS change publication too late: the specified timestamp is already ticked."
            )
          )
        )
      }
    }

  /** Schedules the beginning of buffering of Ledger API Indexer event publishing for Online Party
    * Replication.
    *
    * Meant to be scheduled when the PartyToParticipant topology transaction adds a new participant
    * to an existing party.
    */
  def scheduleEventBuffering(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): UnlessShutdown[Either[CantonTimestamp, Unit]] =
    synchronizeWithClosingSync(functionFullName) {
      taskScheduler
        .scheduleTaskIfLater(
          desiredTimestamp = timestamp,
          taskFactory = _ => {
            FloatingBufferEventsPublicationTask(timestamp = timestamp)
          },
        )
        .map(_ => ())
    }

  /** Schedules publishing of an Online Party Replication ACS batch as soon as possible.
    *
    * Meant to be called only between enclosing [[scheduleEventBuffering]] and
    * [[publishBufferedEvents]] calls.
    */
  def schedulePublishAddContracts(
      buildEventAtRecordTime: CantonTimestamp => Update
  )(implicit traceContext: TraceContext): UnlessShutdown[Unit] =
    scheduleBufferingEventTaskImmediately { timestamp =>
      logger.debug(s"Publish add contracts at $timestamp")
      ledgerApiIndexerBuffer.get() match {
        case None =>
          throw new IllegalStateException(
            "Buffering of LedgerApiIndexer events should be started before adding contracts"
          )
        case Some(buffer) =>
          val event = buffer.markEventsWithRecordTime(buildEventAtRecordTime)

          logger.debug(s"Publishing contract add $event")
          publishLedgerApiIndexerEvent(event)
      }
    }

  /** Schedules flushing of events that were buffered during Online Party Replication as soon as
    * possible.
    *
    * Meant to be called once Online Party Replication has succeeded.
    */
  def publishBufferedEvents()(implicit traceContext: TraceContext): UnlessShutdown[Unit] =
    scheduleBufferingEventTaskImmediately { timestamp =>
      ledgerApiIndexerBuffer.getAndSet(None) match {
        case None => FutureUnlessShutdown.unit
        case Some(buffer) =>
          val bufferedEvents = buffer.extractAndClearBufferedEvents()
          logger.info(
            s"Flushing ${bufferedEvents.size} buffered events to Ledger API Indexer at $timestamp"
          )
          MonadUtil
            .sequentialTraverse_(bufferedEvents) { event =>
              logger.debug(s"Flushing event $event")
              publishLedgerApiIndexerEvent(event)
            }
      }
    }

  // TODO(#26580) More validation and setting should be done in case of cancelled upgrade (and when attempting the next one)
  def setSuccessor(successor: SynchronizerSuccessor): Unit =
    synchronizerSuccessor.set(Some(successor))

  private def scheduleBufferingEventTaskImmediately(
      perform: CantonTimestamp => FutureUnlessShutdown[Unit]
  )(implicit traceContext: TraceContext): UnlessShutdown[Unit] =
    synchronizeWithClosingSync(functionFullName)(
      taskScheduler
        .scheduleTaskImmediately(taskFactory = perform, taskTraceContext = traceContext)
        .discard
    )

  private sealed trait PublicationTask extends TaskScheduler.TimedTask

  private sealed trait SequencedPublicationTask
      extends PublicationTask
      with TaskScheduler.TimedTaskWithSequencerCounter

  private object PublicationTask {
    def orderingSameTimestamp: Ordering[PublicationTask] = Ordering.by(rankSameTimestamp)

    // For each tick we have exactly one EventPublicationTask, which always should precede all the other scheduled
    // floating events.
    private def rankSameTimestamp(x: PublicationTask): (Int, Option[SequencerCounter]) =
      x match {
        case task: EventPublicationTask => 0 -> Some(task.sequencerCounter)
        case _: FloatingEventPublicationTask[_] => 1 -> None
        case _: FloatingBufferEventsPublicationTask =>
          2 -> None // Follows the corresponding topology task scheduled via FloatingEventPublicationTask
      }
  }

  /** Task to publish the event `event` if defined. */
  private[RecordOrderPublisher] case class EventPublicationTask(
      event: SequencedUpdate,
      override val sequencerCounter: SequencerCounter,
  )(implicit val traceContext: TraceContext)
      extends SequencedPublicationTask {

    override val timestamp: CantonTimestamp = event.recordTime

    override def perform(): FutureUnlessShutdown[Unit] =
      publishOrBuffer(event, s"event with synchronizer index ${event.synchronizerIndex}")

    override protected def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp),
        param("sequencerCounter", _.sequencerCounter),
        param("event", _.event),
      )

    override def close(): Unit = ()
  }

  /** Task to publish floating event. */
  private[RecordOrderPublisher] case class FloatingEventPublicationTask[T](
      waitFor: FutureUnlessShutdown[T], // ability to hold back publication execution
      override val timestamp: CantonTimestamp,
  )(
      eventO: () => Option[FloatingUpdate]
  )(implicit val traceContext: TraceContext)
      extends PublicationTask {

    override def perform(): FutureUnlessShutdown[Unit] =
      waitFor.transformWith {
        case Success(Outcome(_)) =>
          eventO() match {
            case Some(event) =>
              publishOrBuffer(event, s"floating event with timestamp $timestamp")
            case None =>
              logger.debug(
                s"Skip publishing floating event with timestamp $timestamp: nothing to publish"
              )
              FutureUnlessShutdown.unit
          }

        case Success(AbortedDueToShutdown) =>
          logger.debug(
            s"Skip publishing floating event with timestamp $timestamp due to shutting down"
          )
          FutureUnlessShutdown.unit

        case Failure(err) =>
          logger.debug(
            s"Skip publishing floating event with timestamp $timestamp due to failed pre-requisite operation",
            err,
          )
          FutureUnlessShutdown.unit
      }

    override protected def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp)
      )

    override def close(): Unit = ()
  }

  /** Task to begin buffering Ledger API Indexer events for Online Party Replication. */
  private[RecordOrderPublisher] case class FloatingBufferEventsPublicationTask(
      override val timestamp: CantonTimestamp
  )(implicit val traceContext: TraceContext)
      extends PublicationTask {

    override def perform(): FutureUnlessShutdown[Unit] = {
      logger.info(s"Begin buffering LedgerApiIndexer events at $timestamp")
      val eventBufferEnabled = ledgerApiIndexerBuffer
        .compareAndSet(
          None,
          Some(new EventBuffer(timestamp, loggerFactory)),
        )
      // TODO(#23097): Error until we add support for multiple concurrent OPRs on TP/synchronizer.
      ErrorUtil.requireState(eventBufferEnabled, "Event buffering already started")
      FutureUnlessShutdown.unit
    }

    override protected def pretty: Pretty[this.type] =
      prettyOfClass(
        param("timestamp", _.timestamp)
      )

    override def close(): Unit = ()
  }

  private def publishOrBuffer(event: Update, log: String)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    ledgerApiIndexerBuffer
      .get() match {
      case None =>
        logger.debug(s"Publish $log")
        publishLedgerApiIndexerEvent(event)
      case Some(buffer) =>
        logger.debug(s"Buffer $log")
        buffer.bufferEvent(event)
        FutureUnlessShutdown.unit
    }

  private def publishLedgerApiIndexerEvent(
      event: Update
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val successorO = synchronizerSuccessor.get()

    successorO match {
      // If the event's record time is at or after the upgrade time,
      // replace the event with a notification that the upgrade time has been reached
      case Some(successor)
          if !LogicalUpgradeTime.canProcessKnowingSuccessor(successorO, event.recordTime) =>
        event match {
          case synchronizerUpdate: SynchronizerUpdate =>
            val upgradeTimeReached = LogicalSynchronizerUpgradeTimeReached(
              synchronizerUpdate.synchronizerId,
              successor.upgradeTime,
            )
            logger.debug(
              s"Not publishing event whose record time ${event.recordTime} is greater than upgrade time ${successor.upgradeTime} $event but publishing $upgradeTimeReached instead"
            )

            ledgerApiIndexer.enqueue(upgradeTimeReached).map(_ => ())

          case other =>
            logger.debug(
              s"Not publishing event whose record time ${other.recordTime} is greater than upgrade time ${successor.upgradeTime}: $other"
            )

            FutureUnlessShutdown.unit
        }

      case _ => ledgerApiIndexer.enqueue(event).map(_ => ())
    }
  }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    Seq(
      AsyncCloseable(
        "taskScheduler-flush",
        taskScheduler.flush(),
        timeouts.shutdownProcessing,
      ),
      SyncCloseable("taskScheduler", taskScheduler.close()),
    )
  }
}

object RecordOrderPublisher {

  /** The apply methods ensures that the successor gets initialized.
    */
  def apply(
      psid: PhysicalSynchronizerId,
      synchronizerSuccessor: Option[SynchronizerSuccessor],
      initSc: SequencerCounter,
      initTimestamp: CantonTimestamp,
      ledgerApiIndexer: LedgerApiIndexer,
      metrics: TaskSchedulerMetrics,
      exitOnFatalFailures: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
  )(implicit executionContextForPublishing: ExecutionContext): RecordOrderPublisher = {
    val rop = new RecordOrderPublisher(
      psid,
      initSc,
      initTimestamp,
      ledgerApiIndexer,
      metrics,
      exitOnFatalFailures,
      timeouts,
      loggerFactory,
      futureSupervisor,
      clock,
    )

    synchronizerSuccessor.foreach(rop.setSuccessor)

    rop
  }
}
