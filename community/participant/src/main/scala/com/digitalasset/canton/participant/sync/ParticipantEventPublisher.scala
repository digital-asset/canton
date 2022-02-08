// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.alternative._
import cats.syntax.option._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, FlagCloseableAsync}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.{LedgerSyncEvent, LocalOffset}
import com.digitalasset.canton.participant.ledger.api.CantonLedgerApiServerWrapper
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.sync.TimestampedEvent.{
  EventId,
  TimelyRejectionEventId,
  TransactionEventId,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil, SimpleExecutionQueue}
import com.digitalasset.canton.{LedgerConfiguration, LedgerSubmissionId}
import com.google.common.annotations.VisibleForTesting
import java.time.Duration

import scala.concurrent.{ExecutionContext, Future}

/** Helper to publish participant events in a thread-safe way. For "regular" SingleDimensionEventLogs representing
  * domains thread safety is taken care of by the [[com.digitalasset.canton.participant.event.RecordOrderPublisher]].
  *
  * ParticipantEventPublisher also encapsulates the participant clock generating unique participant recordTime.
  *
  * @param participantId        participant id
  * @param participantEventLog  participant-local event log
  * @param multiDomainEventLog  multi domain event log for registering participant event log
  * @param participantClock     clock for the current time to stamp published events with
  * @param maxDeduplicationTime maximum deduplication time window duration to request ledger api server to enforce
  * @param loggerFactory        named logger factory
  */
class ParticipantEventPublisher(
    participantId: ParticipantId,
    private val participantEventLog: ParticipantEventLog,
    multiDomainEventLog: MultiDomainEventLog,
    participantClock: Clock,
    maxDeduplicationTime: Duration,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  import com.digitalasset.canton.util.ShowUtil._

  private val executionQueue = new SimpleExecutionQueue()

  private def publishInternal(
      event: LedgerSyncEvent
  )(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      localOffset <- participantEventLog.nextLocalOffset()
      timestampedEvent = TimestampedEvent(
        event,
        localOffset,
        None,
        EventId.fromLedgerSyncEvent(event),
      )
      _ <- participantEventLog.insert(timestampedEvent, None)
      publicationData = PublicationData(
        participantEventLog.id,
        timestampedEvent,
        inFlightReference = None, // No in-flight tracking for events published via this method
      )
      _ <- multiDomainEventLog.publish(publicationData)
    } yield ()
  }

  def publish(event: LedgerSyncEvent)(implicit traceContext: TraceContext): Future[Unit] = {
    ErrorUtil.requireArgument(
      event.recordTime == ParticipantEventPublisher.now.toLf,
      show"RecordTime not initialized with 'now' literal. Participant event: $event",
    )
    executionQueue.execute(
      publishInternal(LedgerEvent.setTimestamp(event, participantClock.uniqueTime().toLf)),
      s"publish event ${event.description} with record time ${event.recordTime}",
    )
  }

  /** Publishes each given event with its IDs in the participant event log unless the ID is already present.
    *
    * @return A [[scala.Left$]] for the events whose [[com.digitalasset.canton.participant.sync.TimestampedEvent.EventId]]
    *         is already present. The other events are published.
    */
  def publishWithIds(
      events: Seq[Traced[(EventId, LedgerSyncEvent)]]
  )(implicit traceContext: TraceContext): EitherT[Future, Seq[TimestampedEvent], Unit] = EitherT {
    def go: Future[Either[List[TimestampedEvent], Unit]] =
      for {
        insertResult <- allocateOffsetsAndInsert(events)
        (clashes, insertedEvents) = events
          .lazyZip(insertResult)
          .map { case (Traced((eventId, event)), result) =>
            result.map(localOffset => TimestampedEvent(event, localOffset, None, eventId.some))
          }
          .toList
          .separate
        // Make sure to call publish in order so that the MultiDomainEventLog does not complain about decreasing local offsets
        _ <- MonadUtil.sequentialTraverse_(insertedEvents) { event =>
          val inFlightReference = event.eventId.flatMap {
            case timelyReject: TimelyRejectionEventId =>
              Some(timelyReject.asInFlightReference)
            case _: TransactionEventId => None
          }
          multiDomainEventLog.publish(
            PublicationData(participantEventLog.id, event, inFlightReference)
          )
        }
      } yield Either.cond(clashes.isEmpty, (), clashes)

    executionQueue.execute(go, s"insert events with IDs ${events.map(_.value._1)}")
  }

  @VisibleForTesting
  private[sync] def allocateOffsetsAndInsert(
      events: Seq[Traced[(EventId, LedgerSyncEvent)]]
  )(implicit traceContext: TraceContext): Future[Seq[Either[TimestampedEvent, LocalOffset]]] = {
    val eventCount = events.size
    for {
      newOffsets <- participantEventLog.nextLocalOffsets(eventCount)
      offsetAndEvent = newOffsets.lazyZip(events)
      timestampedEvents = offsetAndEvent.map((localOffset, tracedEvent) =>
        tracedEvent.withTraceContext(implicit traceContext => { case (eventId, event) =>
          TimestampedEvent(event, localOffset, None, eventId.some)
        })
      )
      insertionResult <- participantEventLog.insertsUnlessEventIdClash(
        timestampedEvents.map(e => TimestampedEventAndCausalChange(e, None))
      )
    } yield newOffsets.lazyZip(insertionResult).map { (localOffset, result) =>
      result.map { case () => localOffset }
    }
  }

  def publishTimeModelConfigNeededUpstreamOnlyIfFirst(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    executionQueue.execute(
      for {
        maybeFirstOffset <- multiDomainEventLog.locateOffset(1).value
        _ <-
          if (maybeFirstOffset.isEmpty) {
            val event = LedgerSyncEvent.ConfigurationChanged(
              recordTime = ParticipantEventPublisher.now.toLf,
              submissionId = LedgerSubmissionId.assertFromString("TimeModel config"),
              participantId = participantId.toLf,
              newConfiguration = LedgerConfiguration(
                generation = 1L,
                timeModel = CantonLedgerApiServerWrapper.maximumToleranceTimeModel,
                maxDeduplicationTime = maxDeduplicationTime,
              ),
            )
            publishInternal(event)
          } else Future.unit
      } yield (),
      "publish first TimeModel configuration",
    )
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty._
    Seq(
      executionQueue.asCloseable("participant-event-publisher-queue", timeouts.shutdownShort.unwrap)
    )
  }
}

object ParticipantEventPublisher {
  // Used to indicate that timestamp is supposed to be overwritten with participant clock time. Only acceptable
  // recordTime for participant events to publish.
  val now: CantonTimestamp = CantonTimestamp.Epoch
}
