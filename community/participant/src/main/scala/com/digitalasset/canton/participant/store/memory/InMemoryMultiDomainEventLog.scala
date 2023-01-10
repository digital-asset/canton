// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import akka.NotUsed
import akka.stream.scaladsl.Source
import cats.data.OptionT
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.digitalasset.canton.LedgerTransactionId
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{AsyncCloseable, FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.event.RecordOrderPublisher.{
  PendingEventPublish,
  PendingPublish,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.store.EventLogId.{
  DomainEventLogId,
  ParticipantEventLogId,
}
import com.digitalasset.canton.participant.store.MultiDomainEventLog.*
import com.digitalasset.canton.participant.store.{
  EventLogId,
  MultiDomainEventLog,
  ParticipantEventLog,
  SingleDimensionEventLog,
}
import com.digitalasset.canton.participant.sync.TimestampedEvent.{EventId, TransactionEventId}
import com.digitalasset.canton.participant.sync.{
  LedgerSyncEvent,
  SyncDomainPersistentStateLookup,
  TimestampedEvent,
  TimestampedEventAndCausalChange,
}
import com.digitalasset.canton.participant.{GlobalOffset, LocalOffset}
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil, SimpleExecutionQueue}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.concurrent.{ExecutionContext, Future}

class InMemoryMultiDomainEventLog(
    lookupEvent: NamedLoggingContext => (
        EventLogId,
        LocalOffset,
    ) => Future[TimestampedEventAndCausalChange],
    lookupOffsetsBetween: NamedLoggingContext => EventLogId => (
        LocalOffset,
        LocalOffset,
    ) => Future[Seq[LocalOffset]],
    byEventId: NamedLoggingContext => EventId => OptionT[Future, (EventLogId, LocalOffset)],
    participantEventLogId: ParticipantEventLogId,
    clock: Clock,
    metrics: ParticipantMetrics,
    override val indexedStringStore: IndexedStringStore,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(override protected implicit val executionContext: ExecutionContext)
    extends MultiDomainEventLog
    with FlagCloseable
    with NamedLogging {

  private case class Entries(
      firstOffset: GlobalOffset,
      lastOffset: GlobalOffset,
      lastLocalOffsets: Map[EventLogId, LocalOffset],
      references: Set[(EventLogId, LocalOffset)],
      referencesByOffset: SortedMap[GlobalOffset, (EventLogId, LocalOffset, CantonTimestamp)],
      publicationTimeUpperBound: CantonTimestamp,
  )

  private val entriesRef: AtomicReference[Entries] =
    new AtomicReference(
      Entries(
        ledgerFirstOffset,
        ledgerFirstOffset - 1,
        Map.empty,
        Set.empty,
        TreeMap.empty,
        CantonTimestamp.MinValue,
      )
    )

  private val dispatcher: Dispatcher[GlobalOffset] = Dispatcher[GlobalOffset](
    loggerFactory.name,
    ledgerFirstOffset - 1, // start index is exclusive
    ledgerFirstOffset - 1, // end index is inclusive
  )

  private val executionQueue = new SimpleExecutionQueue()

  override def publish(data: PublicationData): Future[Unit] = {
    implicit val traceContext: TraceContext = data.traceContext
    val PublicationData(id, event, inFlightReference) = data
    val localOffset = event.localOffset

    // Overkill to use an executionQueue here, but it facilitates testing, because it
    // makes it behave similar to the DB version.
    FutureUtil.doNotAwait(
      executionQueue.executeUnlessFailed(
        {
          // Since we're in an execution queue, we don't need the compare-and-set operations
          val Entries(
            firstOffset,
            lastOffset,
            lastLocalOffsets,
            references,
            referencesByOffset,
            publicationTimeUpperBound,
          ) = entriesRef.get()

          if (references.contains((id, localOffset))) {
            logger.info(
              show"Skipping publication of event reference from event log $id with local offset $localOffset, as it has already been published."
            )
          } else if (lastLocalOffsets.get(id).forall(_ < localOffset)) {
            val nextOffset = lastOffset + 1
            val now = clock.monotonicTime()
            val publicationTime = Ordering[CantonTimestamp].max(now, publicationTimeUpperBound)
            if (now < publicationTime) {
              logger.info(
                s"Local participant clock at $now is before a previous publication time $publicationTime. Has the clock been reset, e.g., during participant failover?"
              )
            }
            logger.debug(
              show"Published event from event log $id with local offset $localOffset at global offset $nextOffset with publication time $publicationTime."
            )
            val newEntries = Entries(
              firstOffset,
              nextOffset,
              lastLocalOffsets + (id -> localOffset),
              references + ((id, localOffset)),
              referencesByOffset + (nextOffset -> ((id, localOffset, publicationTime))),
              publicationTime,
            )
            entriesRef.set(newEntries)

            dispatcher.signalNewHead(newEntries.lastOffset) // new end index is inclusive
            val deduplicationInfo = DeduplicationInfo.fromTimestampedEvent(event)
            val publication = OnPublish.Publication(
              newEntries.lastOffset,
              publicationTime,
              inFlightReference,
              deduplicationInfo,
            )
            notifyOnPublish(Seq(publication))

            metrics.updatesPublished.mark(event.eventSize.toLong)(MetricsContext.Empty)
          } else {
            ErrorUtil.internalError(
              new IllegalArgumentException(
                show"Unable to publish event at id $id and localOffset $localOffset, as that would reorder events."
              )
            )
          }
          Future.unit
        },
        s"publish event $id",
      ),
      "An exception occurred while publishing an event. Stop publishing events.",
    )
    Future.unit
  }

  override def fetchUnpublished(id: EventLogId, upToInclusiveO: Option[LocalOffset])(implicit
      traceContext: TraceContext
  ): Future[Seq[PendingPublish]] = {
    val fromExclusive = entriesRef.get().lastLocalOffsets.getOrElse(id, Long.MinValue)
    val upToInclusive = upToInclusiveO.getOrElse(Long.MaxValue)
    for {
      unpublishedOffsets <- lookupOffsetsBetween(namedLoggingContext)(id)(
        fromExclusive + 1,
        upToInclusive,
      )
      unpublishedEvents <- unpublishedOffsets.parTraverse(offset =>
        lookupEvent(namedLoggingContext)(id, offset)
      )
    } yield unpublishedEvents.map { tseAndUpdate =>
      PendingEventPublish(
        tseAndUpdate.causalityUpdate,
        tseAndUpdate.tse,
        tseAndUpdate.tse.timestamp,
        id,
      )
    }
  }

  override def prune(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] = {
    entriesRef
      .updateAndGet {
        case Entries(
              firstOffset,
              nextOffset,
              lastLocalOffsets,
              references,
              referencesByOffset,
              publicationTimeUpperBound,
            ) =>
          val pruned = referencesByOffset.rangeTo(upToInclusive)
          val newReferences = references -- pruned.values.map {
            case (eventLogId, localOffset, _processingTime) => eventLogId -> localOffset
          }
          val newReferencesByOffset = referencesByOffset -- pruned.keys
          Entries(
            firstOffset max (upToInclusive + 1),
            nextOffset max (upToInclusive + 1),
            lastLocalOffsets,
            newReferences,
            newReferencesByOffset,
            publicationTimeUpperBound,
          )
      }
      .discard[Entries]
    Future.unit
  }

  override def subscribe(
      beginWith: Option[GlobalOffset]
  )(implicit tc: TraceContext): Source[(GlobalOffset, Traced[LedgerSyncEvent]), NotUsed] = {
    logger.debug(show"Subscribing at ${beginWith.showValueOrNone}...")

    dispatcher.startingAt(
      beginWith.getOrElse(entriesRef.get.firstOffset) - 1, // start index is exclusive
      RangeSource { (fromExcl, toIncl) =>
        Source(entriesRef.get.referencesByOffset.range(fromExcl + 1, toIncl + 1))
          .mapAsync(1) { // Parallelism 1 is ok, as the lookup operation are quite fast with in memory stores.
            case (globalOffset, (id, localOffset, _processingTime)) =>
              for {
                eventAndCausalChange <- lookupEvent(namedLoggingContext)(id, localOffset)
              } yield globalOffset -> Traced(eventAndCausalChange.tse.event)(
                eventAndCausalChange.tse.traceContext
              )
          }
      },
    )
  }

  override def subscribeForDomainUpdates(
      startExclusive: GlobalOffset,
      endInclusive: GlobalOffset,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): Source[(GlobalOffset, Traced[LedgerSyncEvent]), NotUsed] = {
    logger.debug(show"Subscribing for domain $domainId at $startExclusive...")

    def eventBelongsToDomain(
        eventLogId: EventLogId,
        timestampedEvent: TimestampedEventAndCausalChange,
    ): Boolean =
      eventLogId match {
        case EventLogId.DomainEventLogId(indexedDomain) =>
          // Covers all events in the single domain event logs
          indexedDomain.domainId == domainId

        case `participantEventLogId` =>
          // Covers CommandRejected events in the participant event log
          timestampedEvent.tse.eventId.exists(_.associatedDomain.exists(_ == domainId))

        case _ =>
          false
      }

    dispatcher.startingAt(
      startExclusive = startExclusive,
      subSource = RangeSource { (fromExcl, toIncl) =>
        Source(entriesRef.get.referencesByOffset.range(fromExcl + 1, toIncl + 1))
          .mapAsync(1) { // Parallelism 1 is ok, as the lookup operation are quite fast with in memory stores.
            case (globalOffset, (id, localOffset, _processingTime)) =>
              for {
                eventAndCausalChange <- lookupEvent(namedLoggingContext)(id, localOffset)
              } yield {
                if (eventBelongsToDomain(id, eventAndCausalChange))
                  List(
                    globalOffset -> Traced(eventAndCausalChange.tse.event)(
                      eventAndCausalChange.tse.traceContext
                    )
                  )
                else Nil
              }
          }
          .flatMapConcat(Source(_))
      },
      endInclusive = Some(endInclusive),
    )
  }

  override def lookupEventRange(upToInclusive: Option[GlobalOffset], limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[(GlobalOffset, TimestampedEventAndCausalChange)]] = {
    val referencesInRange =
      entriesRef.get().referencesByOffset.rangeTo(upToInclusive.getOrElse(Long.MaxValue))
    val limitedReferencesInRange = limit match {
      case Some(n) => referencesInRange.take(n)
      case None => referencesInRange
    }
    limitedReferencesInRange.toList.parTraverse {
      case (globalOffset, (id, localOffset, _processingTime)) =>
        lookupEvent(namedLoggingContext)(id, localOffset).map(globalOffset -> _)
    }
  }

  override def lookupByEventIds(
      eventIds: Seq[TimestampedEvent.EventId]
  )(implicit traceContext: TraceContext): Future[
    Map[TimestampedEvent.EventId, (GlobalOffset, TimestampedEventAndCausalChange, CantonTimestamp)]
  ] = {
    eventIds
      .parTraverseFilter { eventId =>
        byEventId(namedLoggingContext)(eventId).flatMap { case (eventLogId, localOffset) =>
          OptionT(globalOffsetFor(eventLogId, localOffset)).semiflatMap {
            case (globalOffset, publicationTime) =>
              lookupEvent(namedLoggingContext)(eventLogId, localOffset).map { event =>
                (eventId, (globalOffset, event, publicationTime))
              }
          }
        }.value
      }
      .map(_.toMap)
  }

  override def lookupTransactionDomain(
      transactionId: LedgerTransactionId
  )(implicit traceContext: TraceContext): OptionT[Future, DomainId] =
    byEventId(namedLoggingContext)(TransactionEventId(transactionId)).subflatMap {
      case (DomainEventLogId(id), _localOffset) => Some(id.item)
      case (ParticipantEventLogId(_), _localOffset) => None
    }

  override def lastLocalOffsetBeforeOrAt(
      eventLogId: EventLogId,
      upToInclusive: GlobalOffset,
      timestampInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]] = {
    val referencesUpTo = entriesRef.get().referencesByOffset.rangeTo(upToInclusive).values
    val reversedLocalOffsets =
      referencesUpTo
        .collect { case (id, localOffset, _processingTime) if id == eventLogId => localOffset }
        .toList
        .reverse
    reversedLocalOffsets.collectFirstSomeM { localOffset =>
      lookupEvent(namedLoggingContext)(eventLogId, localOffset).map(eventAndCausalChange =>
        if (eventAndCausalChange.tse.timestamp <= timestampInclusive) Some(localOffset) else None
      )
    }
  }

  override def locateOffset(
      deltaFromBeginning: GlobalOffset
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] =
    OptionT.fromOption(
      entriesRef.get().referencesByOffset.drop(deltaFromBeginning.toInt).headOption.map {
        case (offset, _) => offset
      }
    )

  override def lookupOffset(globalOffset: GlobalOffset)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (EventLogId, LocalOffset, CantonTimestamp)] =
    OptionT.fromOption(entriesRef.get().referencesByOffset.get(globalOffset))

  override def globalOffsetFor(eventLogId: EventLogId, localOffset: LocalOffset)(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, CantonTimestamp)]] = Future.successful {
    entriesRef.get().referencesByOffset.collectFirst {
      case (globalOffset, (id, offset, publicationTime))
          if id == eventLogId && offset == localOffset =>
        globalOffset -> publicationTime
    }
  }

  override def getOffsetByTimeUpTo(
      upToInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] = {
    val entries = entriesRef.get()
    // As timestamps are increasing with global offsets, we could do a binary search here,
    // but it's not yet worth the effort.
    val lastO = entries.referencesByOffset
      .to(Iterable)
      .takeWhile { case (_offset, (_eventLogId, _localOffset, publicationTime)) =>
        publicationTime <= upToInclusive
      }
      .lastOption
    val offsetO = lastO.map { case (offset, data) => offset }
    OptionT(Future.successful(offsetO))
  }

  override def getOffsetByTimeAtOrAfter(fromInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (GlobalOffset, EventLogId, LocalOffset)] = {
    val entries = entriesRef.get()
    // As timestamps are increasing with global offsets, we could do a binary search here,
    // but it's not yet worth the effort.
    val offsetO = entries.referencesByOffset.to(Iterable).collectFirst {
      case (offset, (eventLogId, localOffset, publicationTime))
          if publicationTime >= fromInclusive =>
        (offset, eventLogId, localOffset)
    }
    OptionT(Future.successful(offsetO))
  }

  override def lastLocalOffset(
      id: EventLogId
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]] =
    Future.successful {
      entriesRef.get().lastLocalOffsets.get(id)
    }

  override def lastGlobalOffset(
      uptoInclusive: GlobalOffset = Long.MaxValue
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] = OptionT.fromOption(
    entriesRef.get().referencesByOffset.rangeTo(uptoInclusive).lastOption.map { case (offset, _) =>
      offset
    }
  )

  override def publicationTimeLowerBound: CantonTimestamp =
    entriesRef.get().referencesByOffset.lastOption.fold(CantonTimestamp.MinValue) {
      case (_offset, (_eventLogId, _localOffset, publicationTime)) => publicationTime
    }

  override def onClosed(): Unit = {
    import TraceContext.Implicits.Empty.*
    Lifecycle.close(
      executionQueue
        .asCloseable("InMemoryMultiDomainEventLog.executionQueue", timeouts.shutdownShort.duration),
      AsyncCloseable(
        s"${this.getClass}: dispatcher",
        dispatcher.shutdown(),
        timeouts.shutdownShort.duration,
      ),
    )(logger)
  }

  override def flush(): Future[Unit] = executionQueue.flush()
}

object InMemoryMultiDomainEventLog extends HasLoggerName {
  def apply(
      syncDomainPersistentStates: SyncDomainPersistentStateLookup,
      participantEventLog: ParticipantEventLog,
      clock: Clock,
      timeouts: ProcessingTimeout,
      indexedStringStore: IndexedStringStore,
      metrics: ParticipantMetrics,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): InMemoryMultiDomainEventLog = {

    def allEventLogs: Map[EventLogId, SingleDimensionEventLog[EventLogId]] =
      syncDomainPersistentStates.getAll.map { case (_, state) =>
        val eventLog = state.eventLog
        (eventLog.id: EventLogId) -> eventLog
      } + (participantEventLog.id -> participantEventLog)

    new InMemoryMultiDomainEventLog(
      lookupEvent(allEventLogs),
      lookupOffsetsBetween(allEventLogs),
      byEventId(allEventLogs),
      participantEventLog.id,
      clock,
      metrics,
      indexedStringStore,
      timeouts,
      loggerFactory,
    )
  }

  private def lookupEvent(allEventLogs: => Map[EventLogId, SingleDimensionEventLog[EventLogId]])(
      namedLoggingContext: NamedLoggingContext
  )(id: EventLogId, localOffset: LocalOffset): Future[TimestampedEventAndCausalChange] = {
    implicit val loggingContext: NamedLoggingContext = namedLoggingContext
    implicit val tc: TraceContext = loggingContext.traceContext
    implicit val ec: ExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)
    allEventLogs(id)
      .eventAt(localOffset)
      .getOrElse(
        ErrorUtil.internalError(
          new IllegalStateException(
            show"Unable to lookup event at offset $localOffset in event log $id."
          )
        )
      )
  }

  private def lookupOffsetsBetween(
      allEventLogs: => Map[EventLogId, SingleDimensionEventLog[EventLogId]]
  )(namedLoggingContext: NamedLoggingContext)(
      id: EventLogId
  )(fromInclusive: LocalOffset, upToInclusive: LocalOffset): Future[Seq[LocalOffset]] = {
    implicit val loggingContext: NamedLoggingContext = namedLoggingContext
    implicit val tc: TraceContext = loggingContext.traceContext
    implicit val ec: ExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)
    for {
      events <- allEventLogs(id).lookupEventRange(
        Some(fromInclusive),
        Some(upToInclusive),
        None,
        None,
        None,
      )
    } yield events.rangeFrom(fromInclusive).rangeTo(upToInclusive).keySet.toSeq
  }

  private def byEventId(allEventLogs: => Map[EventLogId, SingleDimensionEventLog[EventLogId]])(
      namedLoggingContext: NamedLoggingContext
  )(eventId: EventId): OptionT[Future, (EventLogId, LocalOffset)] = {
    implicit val loggingContext: NamedLoggingContext = namedLoggingContext
    implicit val tc: TraceContext = loggingContext.traceContext
    implicit val ec: ExecutionContext = DirectExecutionContext(loggingContext.tracedLogger)

    OptionT(for {
      result <- allEventLogs.toList.parTraverseFilter { case (eventLogId, eventLog) =>
        eventLog.eventById(eventId).map(event => eventLogId -> event.tse.localOffset).value
      }
    } yield result.headOption)
  }
}
