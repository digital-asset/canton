// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import cats.data.OptionT
import cats.syntax.option._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.daml.ledger.participant.state.v2.ChangeId
import com.daml.ledger.participant.state.v2.Update.{CommandRejected, TransactionAccepted}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher.PendingPublish
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.EventLogId.{
  DomainEventLogId,
  ParticipantEventLogId,
}
import com.digitalasset.canton.participant.store.InFlightSubmissionStore.InFlightReference
import com.digitalasset.canton.participant.store.MultiDomainEventLog.OnPublish
import com.digitalasset.canton.participant.store.db.DbMultiDomainEventLog
import com.digitalasset.canton.participant.store.memory.InMemoryMultiDomainEventLog
import com.digitalasset.canton.participant.sync.TimestampedEvent.EventId
import com.digitalasset.canton.participant.sync.{
  SyncDomainPersistentStateLookup,
  TimestampedEvent,
  TimestampedEventAndCausalChange,
}
import com.digitalasset.canton.participant.{GlobalOffset, LedgerSyncEvent, LocalOffset}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.{DomainId, LedgerSubmissionId, LedgerTransactionId}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** The multi domain event log merges the events from several [[SingleDimensionEventLog]]s to a single event stream.
  *
  * The underlying [[SingleDimensionEventLog]] either refer to a domain ("domain event log") or
  * to the underlying participant ("participant event log").
  *
  * Ordering guarantees:
  * 1. Events belonging to the same SingleDimensionEventLog have the same relative order in the MultiDomainEventLog
  * 2. Events are globally ordered such that any two (unpruned) events appear in the same relative order in different
  *    subscriptions and lookups.
  */
trait MultiDomainEventLog extends AutoCloseable { this: NamedLogging =>

  import MultiDomainEventLog.PublicationData

  def indexedStringStore: IndexedStringStore

  protected implicit def executionContext: ExecutionContext

  /** Appends a new event to the event log.
    *
    * The new event must already have been published in the [[SingleDimensionEventLog]] with id
    * [[MultiDomainEventLog.PublicationData.eventLogId]] at offset [[MultiDomainEventLog.PublicationData.localOffset]].
    *
    * The method is idempotent, i.e., it does nothing, if it is called twice with the same
    * [[com.digitalasset.canton.participant.store.EventLogId]] and
    * [[com.digitalasset.canton.participant.LocalOffset]].
    *
    * The returned future completes already when the event has been successfully inserted into the internal
    * (in-memory) inbox. Events will be persisted asynchronously. Actual publication has happened
    * before a subsequent [[flush]] call's future completes.
    *
    * The caller must await completion of the returned future before calling the method again.
    * Otherwise, the method may fail with an exception or return a failed future.
    * (This restriction arises, because some implementations will offer the event to an Akka source queue, and the
    * number of concurrent offers for such queues is bounded.)
    *
    * The event log will stall, i.e., log an error and refuse to publish further events in the following cases:
    * <ul>
    *   <li>If an event cannot be persisted, even after retrying.</li>
    *   <li>If events are published out of order, i.e., `publish(id, o2).flatMap(_ => publish(id, o1))` with `o1 < o2`.
    *       Exception: The event log will not stall in case of republication of previously published events, i.e.,
    *       `publish(id, o1).flatMap(_ => publish(id, o2).flatMap(_ => publish(id, o1)))` will not stall the event log.
    *   </li>
    * </ul>
    */
  def publish(data: PublicationData): Future[Unit]

  /** Finds unpublished events in the single dimension event log.
    * More precisely, finds all events from `id` with:
    * <ul>
    * <li>local offset strictly greater than the local offset of the last published event from `id`</li>
    * <li>local offset smaller than or equal to `upToInclusiveO` (if defined).</li>
    * </ul>
    */
  def fetchUnpublished(id: EventLogId, upToInclusiveO: Option[LocalOffset])(implicit
      traceContext: TraceContext
  ): Future[Seq[PendingPublish]]

  /** Removes all events with offset up to `upToInclusive`. */
  def prune(upToInclusive: GlobalOffset)(implicit traceContext: TraceContext): Future[Unit]

  /** Yields an akka source with all stored events, optionally starting from a given offset.
    * @throws java.lang.IllegalArgumentException if `beginWith` is lower than [[MultiDomainEventLog.ledgerFirstOffset]].
    */
  def subscribe(beginWith: Option[GlobalOffset])(implicit
      traceContext: TraceContext
  ): Source[(GlobalOffset, Traced[LedgerSyncEvent]), NotUsed]

  /** Yields all events with offset up to `upToInclusive`. */
  def lookupEventRange(upToInclusive: Option[GlobalOffset], limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[(GlobalOffset, TimestampedEventAndCausalChange)]]

  /** Yields the global offset, event and publication time for all the published events with the given IDs.
    * Unpublished events are ignored.
    */
  def lookupByEventIds(eventIds: Seq[EventId])(implicit
      traceContext: TraceContext
  ): Future[Map[EventId, (GlobalOffset, TimestampedEventAndCausalChange, CantonTimestamp)]]

  /** Find the domain of a committed transaction. */
  def lookupTransactionDomain(transactionId: LedgerTransactionId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, DomainId]

  /** Yields the greatest local offsets for the underlying [[SingleDimensionEventLog]] with global offset less than
    * or equal to `upToInclusive`.
    *
    * @return `(domainLastOffsets, participantLastOffset)`, where `domainLastOffsets` maps the domains in `domainIds`
    *         to the greatest offset of the corresponding domain event log (if it exists) and
    *         `participantLastOffset` is the greatest participant offset.
    */
  def lastDomainOffsetsBeforeOrAtGlobalOffset(
      upToInclusive: GlobalOffset,
      domainIds: List[DomainId],
      participantEventLogId: ParticipantEventLogId,
  )(implicit
      traceContext: TraceContext
  ): Future[(Map[DomainId, LocalOffset], Option[LocalOffset])] = {
    for {
      domainLogIds <- domainIds.traverse(IndexedDomain.indexed(indexedStringStore))
      domainOffsets <- domainLogIds.traverseFilter { domainId =>
        lastLocalOffsetBeforeOrAt(
          DomainEventLogId(domainId),
          upToInclusive,
          CantonTimestamp.MaxValue,
        )
          .map(_.map(domainId.item -> _))
      }
      participantOffset <- lastLocalOffsetBeforeOrAt(
        participantEventLogId,
        upToInclusive,
        CantonTimestamp.MaxValue,
      )
    } yield (domainOffsets.toMap, participantOffset)
  }

  /** Returns the greatest local offset of the [[SingleDimensionEventLog]] given by `eventLogId`, if any,
    * such that the following holds:
    * <ol>
    *   <li>The assigned global offset is below or at `upToInclusive`.</li>
    *   <li>The record time of the event is below or at `timestampInclusive`</li>
    * </ol>
    */
  def lastLocalOffsetBeforeOrAt(
      eventLogId: EventLogId,
      upToInclusive: GlobalOffset,
      timestampInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]]

  /** Yields the `deltaFromBeginning`-lowest global offset (if it exists).
    * I.e., `locateOffset(0)` yields the smallest offset, `localOffset(1)` the second smallest offset, and so on.
    */
  def locateOffset(deltaFromBeginning: Long)(implicit
      traceContext: TraceContext
  ): OptionT[Future, GlobalOffset]

  /** Returns the data associated with the given offset, if any */
  def lookupOffset(globalOffset: GlobalOffset)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (EventLogId, LocalOffset, CantonTimestamp)]

  /** Returns the [[com.digitalasset.canton.participant.GlobalOffset]] under which the given local offset of
    * the given event log was published, if any, along with its publication time
    */
  def globalOffsetFor(eventLogId: EventLogId, localOffset: LocalOffset)(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, CantonTimestamp)]]

  /** Yields the largest global offset whose publication time is before or at `upToInclusive`, if any.
    * The publication time is measured on the participant's local clock.
    */
  def getOffsetByTimeUpTo(upToInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): OptionT[Future, GlobalOffset]

  /** Yields the smallest global offset whose publication time is at or after `fromInclusive`, if any.
    * The publication time is measured on the participant's local clock.
    */
  def getOffsetByTimeAtOrAfter(fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (GlobalOffset, EventLogId, LocalOffset)]

  /** Returns the last linearized [[com.digitalasset.canton.participant.LocalOffset]] for the [[SingleDimensionEventLog]] `id`, if any
    */
  def lastLocalOffset(id: EventLogId)(implicit
      traceContext: TraceContext
  ): Future[Option[LocalOffset]]

  /** Yields the highest global offset up to the given bound, if any. */
  def lastGlobalOffset(upToInclusive: GlobalOffset = Long.MaxValue)(implicit
      traceContext: TraceContext
  ): OptionT[Future, GlobalOffset]

  protected val onPublish: AtomicReference[OnPublish] =
    new AtomicReference[OnPublish](OnPublish.DoNothing)

  /** Sets the listener to be called whenever events are published to the multi-domain event log. */
  def setOnPublish(newOnPublish: OnPublish): Unit = onPublish.set(newOnPublish)

  protected def notifyOnPublish(
      published: Seq[OnPublish.Publication]
  )(implicit traceContext: TraceContext): Unit =
    if (published.nonEmpty) {
      val _ = Try(onPublish.get().notify(published)).recover { case ex =>
        // If the observer throws, then we just log and carry on.
        logger.error(
          show"Notifying the MultiDomainEventLog listener failed for offsets ${published.map(_.globalOffset)}",
          ex,
        )
      }
    }

  /** Returns a lower bound on the latest publication time of a published event.
    * All events published later will receive the same or higher publication time.
    * Increases monotonically, even across restarts.
    */
  def publicationTimeLowerBound: CantonTimestamp

  /** Returns a future that completes after all publications have happened whose [[publish]] future has completed
    * before the call to [[flush]].
    */
  def flush(): Future[Unit]
}

object MultiDomainEventLog {

  val ledgerFirstOffset = 1L

  def apply(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      participantEventLog: ParticipantEventLog,
      storage: Storage,
      clock: Clock,
      metrics: ParticipantMetrics,
      indexedStringStore: IndexedStringStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      mat: Materializer,
      traceContext: TraceContext,
  ): Future[MultiDomainEventLog] =
    storage match {
      case _: MemoryStorage =>
        val mdel =
          InMemoryMultiDomainEventLog(
            syncDomainPersistentStates,
            participantEventLog,
            clock,
            timeouts,
            indexedStringStore,
            metrics,
            loggerFactory,
          )
        Future.successful(mdel)
      case dbStorage: DbStorage =>
        DbMultiDomainEventLog(
          dbStorage,
          clock,
          metrics,
          timeouts,
          indexedStringStore,
          loggerFactory,
        )
    }

  case class PublicationData(
      eventLogId: EventLogId,
      event: TimestampedEvent,
      inFlightReference: Option[InFlightReference],
  ) extends HasTraceContext {
    override def traceContext: TraceContext = event.traceContext
    def localOffset: LocalOffset = event.localOffset
    def numberOfNodes: Int = event.eventSize
  }

  /** Listener for publications of the multi-domain event log.
    *
    * Unlike [[MultiDomainEventLog.subscribe]], notification can be lost when the participant crashes.
    * Conversely, [[OnPublish.Publication]] contains more information than just the
    * [[com.digitalasset.canton.participant.LedgerSyncEvent]].
    */
  trait OnPublish {

    /** This method is called after the [[MultiDomainEventLog]] has assigned
      * new [[com.digitalasset.canton.participant.GlobalOffset]]s to a batch of
      * [[com.digitalasset.canton.participant.LedgerSyncEvent]]s.
      *
      * If this method throws a [[scala.util.control.NonFatal]] exception, the [[MultiDomainEventLog]]
      * logs and discards the exception. That is, throwing an exception does not interrupt processing or cause a shutdown.
      */
    def notify(published: Seq[OnPublish.Publication])(implicit
        batchTraceContext: TraceContext
    ): Unit
  }

  object OnPublish {
    case class Publication(
        globalOffset: GlobalOffset,
        publicationTime: CantonTimestamp,
        inFlightReference: Option[InFlightReference],
        deduplicationInfo: Option[DeduplicationInfo],
    ) extends PrettyPrinting {

      override def pretty: Pretty[Publication] = prettyOfClass(
        param("global offset", _.globalOffset),
        paramIfDefined("in-flight reference", _.inFlightReference),
        paramIfDefined("deduplication info", _.deduplicationInfo),
      )
    }

    case object DoNothing extends OnPublish {
      override def notify(published: Seq[OnPublish.Publication])(implicit
          batchTraceContext: TraceContext
      ): Unit = ()
    }
  }

  case class DeduplicationInfo(
      changeId: ChangeId,
      submissionId: Option[LedgerSubmissionId],
      acceptance: Boolean,
      eventTraceContext: TraceContext,
  ) extends PrettyPrinting {
    override def pretty: Pretty[DeduplicationInfo] = prettyOfClass(
      param("change id", _.changeId),
      paramIfDefined("submission id", _.submissionId),
      param("acceptance", _.acceptance),
      param("event trace context", _.eventTraceContext),
    )
  }

  object DeduplicationInfo {

    /** If the event generates a command completion event,
      * returns the [[DeduplicationInfo]] for it.
      */
    def fromEvent(
        event: LedgerSyncEvent,
        eventTraceContext: TraceContext,
    ): Option[DeduplicationInfo] =
      event match {
        case accepted: TransactionAccepted =>
          // The indexer outputs a completion event iff the completion info is set.
          accepted.optCompletionInfo.map { completionInfo =>
            val changeId = completionInfo.changeId
            DeduplicationInfo(
              changeId,
              completionInfo.submissionId,
              acceptance = true,
              eventTraceContext,
            )
          }
        case CommandRejected(recordTime, completionInfo, reason) =>
          val changeId = completionInfo.changeId
          DeduplicationInfo(
            changeId,
            completionInfo.submissionId,
            acceptance = false,
            eventTraceContext,
          ).some
        case _ => None
      }

    def fromTimestampedEvent(event: TimestampedEvent): Option[DeduplicationInfo] =
      fromEvent(event.event, event.traceContext)
  }
}
