// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import cats.data.{NonEmptyList, OptionT}
import cats.syntax.foldable._
import cats.syntax.functorFilter._
import cats.syntax.option._
import cats.syntax.traverseFilter._
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  CloseContext,
  FlagCloseableAsync,
  HasCloseContext,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.event.RecordOrderPublisher.{
  PendingEventPublish,
  PendingPublish,
  PendingTransferPublish,
}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.protocol.CausalityUpdate
import com.digitalasset.canton.participant.store.MultiDomainEventLog.{
  DeduplicationInfo,
  OnPublish,
  PublicationData,
}
import com.digitalasset.canton.participant.store.db.DbMultiDomainEventLog._
import com.digitalasset.canton.participant.store.{EventLogId, MultiDomainEventLog}
import com.digitalasset.canton.participant.sync.TimestampedEvent.TransactionEventId
import com.digitalasset.canton.participant.sync.{TimestampedEvent, TimestampedEventAndCausalChange}
import com.digitalasset.canton.participant.{
  GlobalOffset,
  LedgerSyncEvent,
  LocalOffset,
  RequestCounter,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.Implicits.{
  getResultLfPartyId => _,
  getResultPackageId => _,
}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util._
import com.digitalasset.canton.LedgerTransactionId
import com.digitalasset.canton.topology.DomainId
import io.functionmeta.functionFullName
import slick.jdbc.GetResult

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

/** Must be created by factory methods on DbSingleDimensionEventLog for optionality on how to perform the required
  * async initialization of current head.
  *
  * @param publicationTimeBoundInclusive The highest publication time of all previously published events
  *                                      (or [[com.digitalasset.canton.data.CantonTimestamp.MinValue]] if no events were published).
  * @param inboxSize capacity of source queue that stores event before they are persisted.
  * @param maxConcurrentPublications maximum number of concurrent calls to `publish`.
  *                                  If event publication back-pressures for some reason (e.g. db is unavailable),
  *                                  `publish` will throw an exception,
  *                                  if the number of concurrent calls exceeds this number.
  *                                  A high number comes with higher memory usage, as Akka allocates a buffer of that size internally.
  * @param maxBatchSize maximum number of events that will be persisted in a single database transaction.
  * @param batchTimeout after this timeout, the collect events so far will be persisted, even if `maxBatchSize` has
  *                     not been attained.
  *                     A small number comes with higher CPU usage, as Akka schedules periodic task at that delay.
  */
class DbMultiDomainEventLog private[db] (
    initialLastGlobalOffset: Option[GlobalOffset],
    publicationTimeBoundInclusive: CantonTimestamp,
    lastLocalOffsets: TrieMap[Int, LocalOffset],
    inboxSize: Int = 4000,
    maxConcurrentPublications: Int = 100,
    maxBatchSize: Int,
    batchTimeout: FiniteDuration = 10.millis,
    storage: DbStorage,
    clock: Clock,
    metrics: ParticipantMetrics,
    val indexedStringStore: IndexedStringStore,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(
    override protected implicit val executionContext: ExecutionContext,
    implicit val mat: Materializer,
) extends MultiDomainEventLog
    with FlagCloseableAsync
    with HasCloseContext
    with NamedLogging
    with HasFlushFuture {

  import TimestampedEvent.getResultTimestampedEvent
  import storage.api._
  import storage.converters._
  import ParticipantStorageImplicits._

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("multi-domain-event-log")

  private val dispatcher: Dispatcher[GlobalOffset] =
    Dispatcher(
      loggerFactory.name,
      MultiDomainEventLog.ledgerFirstOffset - 1, // start index is exclusive
      initialLastGlobalOffset.getOrElse(
        MultiDomainEventLog.ledgerFirstOffset - 1
      ), // end index is inclusive
    )

  /** Lower bound on the last offset that we have signalled to the dispatcher. */
  private val lastPublishedOffset: AtomicLong = new AtomicLong(
    initialLastGlobalOffset.getOrElse(MultiDomainEventLog.ledgerFirstOffset - 1)
  )

  /** Non-strict upper bound on the publication time of all previous events.
    * Increases monotonically.
    */
  private val publicationTimeUpperBoundRef: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](publicationTimeBoundInclusive)

  /** Non-strict lower bound on the latest publication time of a published event.
    * Increases monotonically.
    */
  private val publicationTimeLowerBoundRef: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](publicationTimeBoundInclusive)

  /** The [[scala.concurrent.Promise]] is completed after the event has been published. */
  private val source = Source.queue[(PublicationData, Promise[Unit])](
    bufferSize = inboxSize,
    overflowStrategy = OverflowStrategy.backpressure,
    maxConcurrentOffers = maxConcurrentPublications,
  )

  private val publicationFlow = source
    .viaMat(KillSwitches.single)(Keep.both)
    .groupedWithin(maxBatchSize, batchTimeout)
    .mapAsync(1)(doPublish)
    .toMat(Sink.ignore)(Keep.both)

  private val ((eventsQueue, killSwitch), done) =
    AkkaUtil.runSupervised(
      logger.error("An exception occurred while publishing an event. Stop publishing events.", _)(
        TraceContext.empty
      ),
      publicationFlow,
    )

  override def publish(data: PublicationData): Future[Unit] = {
    implicit val traceContext: TraceContext = data.traceContext
    val promise = Promise[Unit]()
    for {
      result <- eventsQueue.offer(data -> promise)
    } yield {
      result match {
        case QueueOfferResult.Enqueued => // nothing to do
          addToFlushAndLogError(s"Publish offset ${data.localOffset} from ${data.eventLogId}")(
            promise.future
          )
        case _: QueueCompletionResult =>
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Failed to publish event, as the queue is completed. $result"
            )
          )
        case QueueOfferResult.Dropped =>
          // This should never happen due to overflowStrategy backpressure.
          ErrorUtil.internalError(
            new IllegalStateException(
              s"Failed to publish event. The event has been unexpectedly dropped."
            )
          )
      }
    }
  }

  /** Allocates the global offsets for the batch of events and notifies the dispatcher on [[onPublish]].
    * Must run sequentially.
    */
  private def doPublish(events: Seq[(PublicationData, Promise[Unit])]): Future[Unit] = {
    val eventsToPublish = events.map(_._1)
    implicit val batchTraceContext: TraceContext = TraceContext.ofBatch(eventsToPublish)(logger)

    def publications(
        cap: GlobalOffset,
        lastEvents: Seq[(GlobalOffset, EventLogId, LocalOffset, CantonTimestamp)],
    ): Seq[OnPublish.Publication] = {
      /* `insert` assigns global offsets in ascending order, but some events may be skipped because they are already there.
       * So we go through the events in reverse order and try to match them against the found global offsets,
       * stopping at the global offset that was known previously.
       */
      val cappedLastEvents = lastEvents.iterator.takeWhile {
        case (globalOffset, _eventLogId, _localOffset, _publicationTime) => globalOffset > cap
      }
      IterableUtil
        .subzipBy(cappedLastEvents, eventsToPublish.reverseIterator) {
          case (
                (globalOffset, allocatedEventLogId, allocatedLocalOffset, publicationTime),
                PublicationData(eventLogId, event, inFlightReference),
              ) =>
            if (allocatedEventLogId == eventLogId && allocatedLocalOffset == event.localOffset) {
              val deduplicationInfo = DeduplicationInfo.fromTimestampedEvent(event)
              OnPublish
                .Publication(globalOffset, publicationTime, inFlightReference, deduplicationInfo)
                .some
            } else None
        }
        .reverse
    }

    def nextPublicationTime(): CantonTimestamp = {
      val now = clock.monotonicTime()
      val next = publicationTimeUpperBoundRef.updateAndGet(oldPublicationTime =>
        Ordering[CantonTimestamp].max(now, oldPublicationTime)
      )
      if (now < next) {
        logger.info(
          s"Local participant clock at $now is before a previous publication time $next. Has the clock been reset, e.g., during participant failover?"
        )
      }
      next
    }

    def advancePublicationTimeLowerBound(newBound: CantonTimestamp): Unit = {
      val oldBound = publicationTimeLowerBoundRef.getAndUpdate(oldBound =>
        Ordering[CantonTimestamp].max(oldBound, newBound)
      )
      if (oldBound < newBound)
        logger.trace(s"Advanced publication time lower bound to $newBound")
      else
        logger.trace(
          s"Publication time lower bound remains at $oldBound as new bound $newBound is not higher"
        )
    }

    for {
      _ <- enforceInOrderPublication(eventsToPublish)
      publicationTime = nextPublicationTime()
      _ <- insert(eventsToPublish, publicationTime)
      // Find the global offsets assigned to the inserted events
      foundEvents <- lastEvents(events.size)
    } yield {
      val newGlobalOffsetO = foundEvents.headOption.map(_._1)
      newGlobalOffsetO match {
        case Some(newGlobalOffset) =>
          logger.debug(show"Signalling global offset $newGlobalOffset.")
          dispatcher.signalNewHead(newGlobalOffset)

          val previousGlobalOffset = lastPublishedOffset.getAndSet(newGlobalOffset)
          val published = publications(previousGlobalOffset, foundEvents)
          // Advance the publication time lower bound to `publicationTime`
          // only if it was actually stored for at least one event.
          if (published.nonEmpty) {
            advancePublicationTimeLowerBound(publicationTime)
          }
          notifyOnPublish(published)

          events.foreach { case (data @ PublicationData(id, event, _inFlightReference), promise) =>
            promise.success(())
            logger.debug(
              show"Published event from event log $id with local offset ${event.localOffset} with publication time $publicationTime."
            )(data.traceContext)
            metrics.updatesPublished.metric.mark(event.eventSize.toLong)
          }

        case None =>
          ErrorUtil.internalError(
            new IllegalStateException(
              "Failed to publish events to linearized_event_log. The table appears to be empty."
            )
          )
      }
    }
  }

  private def enforceInOrderPublication(events: Seq[PublicationData]): Future[Unit] = {
    val eventsBeforeOrAtLastLocalOffset = events.mapFilter {
      case data @ PublicationData(id, event, _inFlightReference) =>
        implicit val traceContext: TraceContext = data.traceContext
        val localOffset = event.localOffset
        lastLocalOffsets.get(id.index) match {
          case Some(lastLocalOffset) if localOffset <= lastLocalOffset =>
            logger.info(
              show"The event at id $id and local offset $localOffset can't be published, because the last published offset is already $lastLocalOffset."
            )
            Some(Traced((id, localOffset)))
          case _ =>
            lastLocalOffsets.put(id.index, localOffset)
            None
        }
    }

    eventsBeforeOrAtLastLocalOffset.traverse_(_.withTraceContext(implicit traceContext => {
      case (id, localOffset) =>
        for {
          existingGlobalOffsetO <- globalOffsetFor(id, localOffset)
        } yield existingGlobalOffsetO match {
          case Some(_) =>
            logger.info(
              show"The event at id $id and local offset $localOffset already exists in linearized_event_log. Nothing to do."
            )
          case None =>
            ErrorUtil.internalError(
              new IllegalArgumentException(
                show"Unable to publish event at id $id and localOffset $localOffset, as that would reorder events."
              )
            )
        }
    }))
  }

  private def insert(events: Seq[PublicationData], publicationTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    processingTime.metric.event {
      val syncCommit = storage.profile match {
        case _: Profile.Postgres => sqlu"set local synchronous_commit=on"
        // Don't do anything for H2/Oracle. According to our docs it is up to the user to enforce synchronous replication.
        // Any changes here are on a best-effort basis, but we won't guarantee they will be sufficient.
        case _: Profile.H2 => sqlu" "
        // Oracle requires an updating statement here and will not accept an empty string so using a noop dummy update
        case _: Profile.Oracle => sqlu"UPDATE linearized_event_log SET log_id = 1 WHERE 1 = 0"
      }

      val insertStatement = storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          """insert /*+ IGNORE_ROW_ON_DUPKEY_INDEX ( linearized_event_log ( local_offset, log_id ) ) */
            |into linearized_event_log(log_id, local_offset, publication_time)
            |values (?, ?, ?)""".stripMargin
        case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
          """insert into linearized_event_log (log_id, local_offset, publication_time)
            |values (?, ?, ?)
            |on conflict do nothing""".stripMargin
      }
      val bulkInsert = DbStorage.bulkOperation_(
        insertStatement,
        events,
        storage.profile,
      ) { pp => tracedEvent =>
        val PublicationData(id, event, _inFlightReference) = tracedEvent
        pp >> id.index
        pp >> event.localOffset
        pp >> publicationTime
      }
      val query = syncCommit.andThen(bulkInsert).transactionally
      storage.queryAndUpdate(query, functionFullName)
    }

  override def fetchUnpublished(id: EventLogId, upToInclusiveO: Option[LocalOffset])(implicit
      traceContext: TraceContext
  ): Future[List[PendingPublish]] = {
    implicit val getResultEC: GetResult[CausalityUpdate] =
      CausalityUpdate.hasVersionedWrapperGetResult

    val fromExclusive = lastLocalOffsets.getOrElse(id.index, Long.MinValue)
    val upToInclusive = upToInclusiveO.getOrElse(Long.MaxValue)
    logger.info(s"Fetch unpublished from $id up to ${upToInclusiveO}")

    processingTime.metric.event {
      for {
        unpublishedLocalOffsets <- DbSingleDimensionEventLog.lookupEventRange(
          storage = storage,
          eventLogId = id,
          fromInclusive = (fromExclusive + 1L).some,
          toInclusive = upToInclusive.some,
          fromTimestampInclusive = None,
          toTimestampInclusive = None,
          limit = None,
        )

        unpublishedTransfers <- storage.query(
          sql"""select request_counter, request_timestamp, causality_update from transfer_causality_updates where log_id = ${id.index} and request_counter > $fromExclusive and request_counter <= $upToInclusive order by request_counter"""
            .as[(RequestCounter, CantonTimestamp, CausalityUpdate)],
          functionFullName,
        )
      } yield {
        val publishes: List[PendingPublish] = unpublishedLocalOffsets.toList.map {

          case (rc, tseAndUpdate) =>
            PendingEventPublish(
              tseAndUpdate.causalityUpdate,
              tseAndUpdate.tse,
              tseAndUpdate.tse.timestamp,
              id,
            )
        }
        val transferPublishes: List[PendingPublish] = unpublishedTransfers.toList.map {
          case (rc, timestamp, clock) =>
            PendingTransferPublish(rc, clock, timestamp, id)
        }
        (publishes ++ transferPublishes).sortBy(pending => pending.rc)
      }
    }
  }

  override def prune(
      upToInclusive: GlobalOffset
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      storage.update_(
        sqlu"delete from linearized_event_log where global_offset <= $upToInclusive",
        functionFullName,
      )
    }

  override def subscribe(beginWith: Option[GlobalOffset])(implicit
      traceContext: TraceContext
  ): Source[(GlobalOffset, Traced[LedgerSyncEvent]), NotUsed] = {
    dispatcher.startingAt(
      beginWith.getOrElse(MultiDomainEventLog.ledgerFirstOffset) - 1, // start index is exclusive
      RangeSource { (fromExcl, toIncl) =>
        Source(RangeUtil.partitionIndexRange(fromExcl, toIncl, maxBatchSize.toLong))
          .mapAsync(1) { case (batchFromExcl, batchToIncl) =>
            storage.query(
              sql"""select /*+ INDEX (linearized_event_log pk_linearized_event_log, event_log pk_event_log) */ global_offset, content, trace_context
                  from linearized_event_log lel join event_log el on lel.log_id = el.log_id and lel.local_offset = el.local_offset
                  where global_offset > $batchFromExcl and global_offset <= $batchToIncl
                  order by global_offset asc"""
                .as[(GlobalOffset, Traced[LedgerSyncEvent])],
              functionFullName,
            )
          }
          .mapConcat(identity)
      },
    )
  }

  override def lookupEventRange(upToInclusive: Option[GlobalOffset], limit: Option[Int])(implicit
      traceContext: TraceContext
  ): Future[Seq[(GlobalOffset, TimestampedEventAndCausalChange)]] = {

    implicit val getResultECO: GetResult[Option[CausalityUpdate]] =
      CausalityUpdate.hasVersionedWrapperGetResultO
    import TimestampedEventAndCausalChange.getResultTimestampedEventAndCausalChange

    processingTime.metric.event {
      storage
        .query(
          sql"""select global_offset, el.local_offset, request_sequencer_counter, el.event_id, content, trace_context, causality_update
                from linearized_event_log lel join event_log el on lel.log_id = el.log_id and lel.local_offset = el.local_offset
                where global_offset <= ${upToInclusive.getOrElse(Long.MaxValue)}
                order by global_offset asc #${storage.limit(limit.getOrElse(Int.MaxValue))}"""
            .as[(GlobalOffset, TimestampedEventAndCausalChange)],
          functionFullName,
        )
    }
  }

  override def lookupByEventIds(
      eventIds: Seq[TimestampedEvent.EventId]
  )(implicit traceContext: TraceContext): Future[
    Map[TimestampedEvent.EventId, (GlobalOffset, TimestampedEventAndCausalChange, CantonTimestamp)]
  ] = {
    NonEmptyList.fromList(eventIds.toList) match {
      case None => Future.successful(Map.empty)
      case Some(nonEmptyEventIds) =>
        val inClauses =
          DbStorage.toInClauses_(
            "el.event_id",
            nonEmptyEventIds,
            PositiveNumeric.tryCreate(maxBatchSize),
          )
        val queries = inClauses.map { inClause =>
          import DbStorage.Implicits.BuilderChain._
          (sql"""
            select global_offset,
              el.local_offset, request_sequencer_counter, el.event_id, content, trace_context, causality_update,
              publication_time
            from linearized_event_log lel join event_log el on lel.log_id = el.log_id and lel.local_offset = el.local_offset
            where 
            """ ++ inClause).as[(GlobalOffset, TimestampedEventAndCausalChange, CantonTimestamp)]
        }
        storage.sequentialQueryAndCombine(queries, functionFullName).map { events =>
          events.map {
            case data @ (
                  globalOffset,
                  TimestampedEventAndCausalChange(event, _causalChange),
                  _publicationTime,
                ) =>
              val eventId = event.eventId.getOrElse(
                ErrorUtil.internalError(
                  new DbDeserializationException(
                    s"Event $event at global offset $globalOffset does not have an event ID."
                  )
                )
              )
              eventId -> data
          }.toMap
        }
    }
  }

  override def lookupTransactionDomain(transactionId: LedgerTransactionId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, DomainId] = processingTime.metric.optionTEvent {
    storage
      .querySingle(
        sql"""select log_id from event_log where event_id = ${TransactionEventId(transactionId)}"""
          .as[Int]
          .headOption,
        functionFullName,
      )
      .flatMap(idx => IndexedDomain.fromDbIndexOT("event_log", indexedStringStore)(idx))
      .map(_.domainId)
  }

  override def lastLocalOffsetBeforeOrAt(
      eventLogId: EventLogId,
      upToInclusive: GlobalOffset,
      timestampInclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]] =
    processingTime.metric.event {
      // Note for idempotent retries, we don't require that the global offset has an actual ledger entry reference
      val query =
        sql"""select lel.local_offset
              from linearized_event_log lel join event_log el on lel.log_id = el.log_id and lel.local_offset = el.local_offset
              where lel.log_id = ${eventLogId.index} and global_offset <= $upToInclusive and el.ts <= $timestampInclusive
              order by global_offset desc #${storage.limit(1)}"""
          .as[LocalOffset]
          .headOption
      storage.query(query, functionFullName)
    }

  override def locateOffset(
      deltaFromBeginning: GlobalOffset
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] =
    processingTime.metric.optionTEvent {
      // The following query performs a table scan which can in theory become a problem if deltaFromBeginning is large.
      // We cannot simply perform a lookup as big/serial columns can have gaps.
      // However as we are planning to prune in batches, deltaFromBeginning will be limited by the batch size and be
      // reasonable.
      storage.querySingle(
        sql"select global_offset from linearized_event_log order by global_offset #${storage.limit(1, deltaFromBeginning)}"
          .as[GlobalOffset]
          .headOption,
        functionFullName,
      )
    }

  override def lookupOffset(globalOffset: GlobalOffset)(implicit
      traceContext: TraceContext
  ): OptionT[Future, (EventLogId, LocalOffset, CantonTimestamp)] =
    processingTime.metric.optionTEvent {
      storage
        .querySingle(
          sql"select log_id, local_offset, publication_time from linearized_event_log where global_offset = $globalOffset"
            .as[(Int, LocalOffset, CantonTimestamp)]
            .headOption,
          functionFullName,
        )
        .flatMap { case (logIndex, offset, ts) =>
          EventLogId.fromDbLogIdOT("linearized_event_log", indexedStringStore)(logIndex).map { x =>
            (x, offset, ts)
          }
        }
    }

  override def globalOffsetFor(eventLogId: EventLogId, localOffset: LocalOffset)(implicit
      traceContext: TraceContext
  ): Future[Option[(GlobalOffset, CantonTimestamp)]] =
    processingTime.metric.event {
      storage.query(
        sql"""
      select lel.global_offset, lel.publication_time
      from linearized_event_log lel
      where lel.log_id = ${eventLogId.index} and lel.local_offset = $localOffset
      #${storage.limit(1)}
      """.as[(GlobalOffset, CantonTimestamp)].headOption,
        functionFullName,
      )
    }

  override def getOffsetByTimeUpTo(
      upToInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): OptionT[Future, GlobalOffset] =
    processingTime.metric.optionTEvent {
      // The publication time increases with the global offset,
      // so we order first by the publication time so that the same index `idx_linearized_event_log_publication_time`
      // can be used for the where clause and the ordering
      val query =
        sql"""
          select global_offset
          from linearized_event_log
          where publication_time <= $upToInclusive
          order by publication_time desc, global_offset desc
          #${storage.limit(1)}
          """.as[GlobalOffset].headOption
      storage.querySingle(query, functionFullName)
    }

  override def getOffsetByTimeAtOrAfter(
      fromInclusive: CantonTimestamp
  )(implicit traceContext: TraceContext): OptionT[Future, (GlobalOffset, EventLogId, LocalOffset)] =
    processingTime.metric.optionTEvent {
      // The publication time increases with the global offset,
      // so we order first by the publication time so that the same index `idx_linearized_event_log_publication_time`
      // can be used for the where clause and the ordering
      val query =
        sql"""
          select global_offset, log_id, local_offset
          from linearized_event_log
          where publication_time >= $fromInclusive
          order by publication_time asc, global_offset asc 
          #${storage.limit(1)}
          """.as[(GlobalOffset, Int, LocalOffset)].headOption
      storage.querySingle(query, functionFullName).flatMap { case (offset, logIndex, localOffset) =>
        EventLogId.fromDbLogIdOT("linearized_event_log", indexedStringStore)(logIndex).map { x =>
          (offset, x, localOffset)
        }
      }
    }

  override def lastLocalOffset(
      id: EventLogId
  )(implicit traceContext: TraceContext): Future[Option[LocalOffset]] =
    processingTime.metric.event {
      storage
        .query(
          sql"""select local_offset from linearized_event_log where log_id = ${id.index} order by local_offset desc #${storage
            .limit(1)}"""
            .as[LocalOffset]
            .headOption,
          functionFullName,
        )
    }

  override def lastGlobalOffset(upToInclusive: GlobalOffset = Long.MaxValue)(implicit
      traceContext: TraceContext
  ): OptionT[Future, GlobalOffset] =
    OptionT(lastOffsetAndPublicationTime(storage, upToInclusive)).map(_._1)

  /** Returns the `count` many last events in descending global offset order. */
  private def lastEvents(count: Int)(implicit
      traceContext: TraceContext
  ): Future[Seq[(GlobalOffset, EventLogId, LocalOffset, CantonTimestamp)]] = {
    val query = storage.profile match {
      case Profile.Oracle(jdbc) =>
        sql"select * from ((select global_offset, log_id, local_offset, publication_time from linearized_event_log order by global_offset desc)) where rownum < ${count + 1}"
          .as[(GlobalOffset, Int, LocalOffset, CantonTimestamp)]
      case _ =>
        sql"select global_offset, log_id, local_offset, publication_time from linearized_event_log order by global_offset desc #${storage
          .limit(count)}"
          .as[(GlobalOffset, Int, LocalOffset, CantonTimestamp)]
    }
    storage.query(query, functionFullName).flatMap { vec =>
      vec.traverseFilter { case (offset, logId, localOffset, ts) =>
        EventLogId
          .fromDbLogIdOT("lineralized event log", indexedStringStore)(logId)
          .map { evLogId =>
            (offset, evLogId, localOffset, ts)
          }
          .value
      }
    }
  }

  override def publicationTimeLowerBound: CantonTimestamp = publicationTimeLowerBoundRef.get()

  override def flush(): Future[Unit] = doFlush()

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty._
    List[AsyncOrSyncCloseable](
      SyncCloseable("eventsQueue.complete", eventsQueue.complete()),
      SyncCloseable("killSwitch.shutdown", killSwitch.shutdown()),
      AsyncCloseable(
        "eventsQueue.completion",
        eventsQueue.watchCompletion(),
        timeouts.shutdownShort.unwrap,
      ),
      AsyncCloseable(
        "done",
        done.map(_ => ()).recover {
          // The Akka stream supervisor has already logged an exception as an error and stopped the stream
          case NonFatal(e) =>
            logger.debug(s"Ignored exception in Akka stream done future during shutdown", e)
        },
        timeouts.shutdownShort.unwrap,
      ),
    )
  }
}

object DbMultiDomainEventLog {

  def apply(
      storage: DbStorage,
      clock: Clock,
      metrics: ParticipantMetrics,
      timeouts: ProcessingTimeout,
      indexedStringStore: IndexedStringStore,
      loggerFactory: NamedLoggerFactory,
      maxBatchSize: Int = 1000,
  )(implicit
      ec: ExecutionContext,
      mat: Materializer,
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[DbMultiDomainEventLog] =
    for {
      headAndPublicationTime <- lastOffsetAndPublicationTime(storage)
      localHeads <- lastLocalOffsets(storage)
    } yield {
      // We never purge the multi-domain event log completely, so if there ever was a publication time recorded,
      // we will find an upper bound.
      val initialPublicationTime = headAndPublicationTime.fold(CantonTimestamp.MinValue)(_._2)
      new DbMultiDomainEventLog(
        headAndPublicationTime.map(_._1),
        initialPublicationTime,
        localHeads,
        maxBatchSize = maxBatchSize,
        storage = storage,
        clock = clock,
        metrics = metrics,
        indexedStringStore = indexedStringStore,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )
    }

  private[db] def lastOffsetAndPublicationTime(
      storage: DbStorage,
      upToInclusive: GlobalOffset = Long.MaxValue,
  )(implicit
      traceContext: TraceContext,
      closeContext: CloseContext,
  ): Future[Option[(GlobalOffset, CantonTimestamp)]] = {
    import storage.api._

    val query =
      sql"""select global_offset, publication_time from linearized_event_log where global_offset <= $upToInclusive
            order by global_offset desc #${storage.limit(1)}"""
        .as[(GlobalOffset, CantonTimestamp)]
        .headOption
    storage.query(query, functionFullName)
  }

  private[db] def lastLocalOffsets(storage: DbStorage)(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
      closeContext: CloseContext,
  ): Future[TrieMap[Int, LocalOffset]] = {
    import storage.api._

    storage.query(
      {
        for {
          rows <-
            sql"""select log_id, max(local_offset) from linearized_event_log group by log_id"""
              .as[(Int, LocalOffset)]
        } yield {
          val result = new TrieMap[Int, LocalOffset]()
          result ++= rows
          result
        }
      },
      functionFullName,
    )
  }
}
