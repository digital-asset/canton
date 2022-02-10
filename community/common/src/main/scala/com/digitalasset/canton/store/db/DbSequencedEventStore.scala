// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.functor._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.String3
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging._
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.protocol.version.VersionedSignedContent
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, PossiblyIgnoredSerializedEvent}
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent, SignedContent}
import com.digitalasset.canton.store._
import com.digitalasset.canton.store.db.DbSequencedEventStore._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, Thereafter}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.serialization.ProtoConverter
import io.functionmeta.functionFullName
import slick.jdbc.{GetResult, SetParameter}

import java.util.concurrent.Semaphore
import scala.concurrent.{ExecutionContext, Future, blocking}

class DbSequencedEventStore(
    override protected[this] val storage: DbStorage,
    client: SequencerClientDiscriminator,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SequencedEventStore
    with FlagCloseable
    with NamedLogging
    with DbPrunableByTime[SequencerClientDiscriminator, Nothing] {

  override protected[this] val partitionKey: SequencerClientDiscriminator = client

  override protected[this] def partitionColumn: String = "client"
  override protected[this] def pruning_status_table: String = "sequenced_event_store_pruning"

  /** Semaphore to prevent concurrent writes to the db.
    * Concurrent calls can be problematic because they may introduce gaps in the stored sequencer counters.
    * The methods [[ignoreEvents]] and [[unignoreEvents]] are not meant to be executed concurrently.
    */
  private val semaphore: Semaphore = new Semaphore(1)

  private def withLock[F[_], Content[_], A](body: => F[A], caller: String)(implicit
      thereafter: Thereafter[F, Content],
      traceContext: TraceContext,
  ): F[A] = {
    import Thereafter.syntax._
    // Avoid unnecessary call to blocking, if a permit is available right away.
    if (!semaphore.tryAcquire()) {
      // This should only occur when the caller is ignoring events, so ok to log with info level.
      logger.info(s"Delaying call to $caller, because another write is in progress.")
      blocking(semaphore.acquireUninterruptibly())
    }
    body.thereafter(_ => semaphore.release())
  }

  override protected[this] implicit def setParameterDiscriminator
      : SetParameter[SequencerClientDiscriminator] =
    SequencerClientDiscriminator.setClientDiscriminatorParameter

  import com.digitalasset.canton.store.SequencedEventStore._
  import storage.api._
  import storage.converters._

  override protected val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("sequenced-event-store")

  implicit val getResultPossiblyIgnoredSequencedEvent: GetResult[PossiblyIgnoredSerializedEvent] =
    GetResult { r =>
      val typ = r.<<[SequencedEventDbType]
      val sequencerCounter = r.<<[SequencerCounter]
      val timestamp = r.<<[CantonTimestamp]
      val eventBytes = r.<<[Array[Byte]]
      val traceContext: TraceContext = r.<<[TraceContext]
      val ignore = r.<<[Boolean]

      typ match {
        case SequencedEventDbType.IgnoredEvent =>
          IgnoredSequencedEvent(timestamp, sequencerCounter, None)(traceContext)
        case _ =>
          val signedEvent = ProtoConverter
            .protoParserArray(VersionedSignedContent.parseFrom)(eventBytes)
            .flatMap(
              SignedContent.fromProtoVersioned(
                SequencedEvent.fromByteString(ClosedEnvelope.fromProtoV0)
              )
            )
            .valueOr(err =>
              throw new DbDeserializationException(s"Failed to deserialize sequenced event: $err")
            )
          if (ignore) {
            IgnoredSequencedEvent(timestamp, sequencerCounter, Some(signedEvent))(traceContext)
          } else {
            OrdinarySequencedEvent(signedEvent)(traceContext)
          }
      }
    }

  override def store(
      events: Seq[OrdinarySerializedEvent]
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (events.isEmpty) Future.unit
    else
      processingTime.metric.event {
        withLock(
          storage.queryAndUpdate(bulkInsertQuery(events), functionFullName).void,
          functionFullName,
        )
      }

  private def bulkInsertQuery(
      events: Seq[PossiblyIgnoredSerializedEvent]
  )(implicit loggingContext: ErrorLoggingContext): DBIOAction[Unit, NoStream, Effect.All] = {
    val insertSql = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        """
           insert /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( sequenced_events ( ts, client ) ) */
           into sequenced_events (client, ts, sequenced_event, type, sequencer_counter, trace_context, ignore)
           values (?, ?, ?, ?, ?, ?, ?)
      	""".stripMargin

      case _ =>
        "insert into sequenced_events (client, ts, sequenced_event, type, sequencer_counter, trace_context, ignore) " +
          "values (?, ?, ?, ?, ?, ?, ?) " +
          "on conflict do nothing"
    }

    DbStorage.bulkOperation_(insertSql, events, storage.profile) { pp => event =>
      pp >> partitionKey
      pp >> event.timestamp
      pp >> event.underlyingEventBytes(ProtocolVersion.default)
      pp >> event.dbType
      pp >> event.counter
      pp >> event.traceContext
      pp >> event.isIgnored
    }
  }

  override def find(criterion: SequencedEventStore.SearchCriterion)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencedEventNotFoundError, PossiblyIgnoredSerializedEvent] =
    processingTime.metric.eitherTEvent {
      val query = criterion match {
        case ByTimestamp(timestamp) =>
          // The implementation assumes that we timestamps on sequenced events increases monotonically with the sequencer counter
          // It therefore is fine to take the first event that we find.
          sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore from sequenced_events
                where client = $partitionKey and ts = $timestamp"""
        case LatestUpto(inclusive) =>
          sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore from sequenced_events
                where client = $partitionKey and ts <= $inclusive
                order by ts desc #${storage.limit(1)}"""
      }
      storage
        .querySingle(query.as[PossiblyIgnoredSerializedEvent].headOption, functionFullName)
        .toRight(SequencedEventNotFoundError(criterion))
    }

  override def findRange(criterion: SequencedEventStore.RangeCriterion, limit: Option[Int])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencedEventRangeOverlapsWithPruning, Seq[PossiblyIgnoredSerializedEvent]] =
    EitherT(processingTime.metric.event {
      criterion match {
        case ByTimestampRange(lowerInclusive, upperInclusive) =>
          for {
            events <- storage.query(
              sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore from sequenced_events
                    where client = $partitionKey and $lowerInclusive <= ts  and ts <= $upperInclusive
                    order by ts #${limit.fold("")(storage.limit(_))}"""
                .as[PossiblyIgnoredSerializedEvent],
              functionFullName,
            )
            // check for pruning after we've read the events so that we certainly catch the case
            // if pruning is started while we're reading (as we're not using snapshot isolation here)
            pruningO <- pruningStatus.merge
          } yield pruningO match {
            case Some(pruningStatus) if pruningStatus.timestamp >= lowerInclusive =>
              Left(SequencedEventRangeOverlapsWithPruning(criterion, pruningStatus, events))
            case _ =>
              Right(events)
          }
      }
    })

  override def sequencedEvents(
      limit: Option[Int] = None
  )(implicit traceContext: TraceContext): Future[Seq[PossiblyIgnoredSerializedEvent]] =
    processingTime.metric.event {
      storage.query(
        sql"""select type, sequencer_counter, ts, sequenced_event, trace_context, ignore from sequenced_events
              where client = $partitionKey
              order by ts #${limit.fold("")(storage.limit(_))}"""
          .as[PossiblyIgnoredSerializedEvent],
        functionFullName,
      )
    }

  override protected[canton] def doPrune(
      beforeAndIncluding: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, Nothing, Unit] =
    processingTime.metric.eitherTEvent[Nothing, Unit] {
      val query =
        sqlu"delete from sequenced_events where client = $partitionKey and ts <= $beforeAndIncluding"

      EitherT.right[Nothing](
        storage
          .update(query, functionFullName)
          .map { nrPruned =>
            logger.info(
              s"Pruned at least $nrPruned entries from the sequenced event store of client $partitionKey older or equal to $beforeAndIncluding"
            )
          }
      )
    }

  override def ignoreEvents(from: SequencerCounter, to: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    withLock(
      {
        for {
          _ <- appendEmptyIgnoredEvents(from, to)
          _ <- EitherT.right(setIgnoreStatus(from, to, ignore = true))
        } yield ()
      },
      functionFullName,
    )

  private def appendEmptyIgnoredEvents(from: SequencerCounter, to: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    processingTime.metric.eitherTEvent {
      for {
        lastSequencerCounterAndTimestampO <- EitherT.right(
          storage.query(
            sql"""select sequencer_counter, ts from sequenced_events where client = $partitionKey
               order by sequencer_counter desc #${storage.limit(1)}"""
              .as[(SequencerCounter, CantonTimestamp)]
              .headOption,
            functionFullName,
          )
        )

        (firstSc, firstTs) = lastSequencerCounterAndTimestampO match {
          case Some((lastSc, lastTs)) => (lastSc + 1, lastTs.immediateSuccessor)
          case None =>
            // Starting with MinValue.immediateSuccessor, because elsewhere we assume that MinValue is a strict lower bound on event timestamps.
            (from, CantonTimestamp.MinValue.immediateSuccessor)
        }

        _ <- EitherTUtil.condUnitET[Future](
          from <= firstSc || from > to,
          ChangeWouldResultInGap(firstSc, from - 1),
        )

        events = ((firstSc max from) to to).map { sc =>
          val ts = firstTs.addMicros(sc - firstSc)
          IgnoredSequencedEvent(ts, sc, None)(traceContext)
        }

        _ <- EitherT.right(storage.queryAndUpdate(bulkInsertQuery(events), functionFullName))
      } yield ()
    }

  private def setIgnoreStatus(from: SequencerCounter, to: SequencerCounter, ignore: Boolean)(
      implicit traceContext: TraceContext
  ): Future[Unit] = processingTime.metric.event {
    storage.update_(
      sqlu"update sequenced_events set ignore = $ignore where client = $partitionKey and $from <= sequencer_counter and sequencer_counter <= $to",
      functionFullName,
    )
  }

  override def unignoreEvents(from: SequencerCounter, to: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    withLock(
      {
        for {
          _ <- deleteEmptyIgnoredEvents(from, to)
          _ <- EitherT.right(setIgnoreStatus(from, to, ignore = false))
        } yield ()
      },
      functionFullName,
    )

  private def deleteEmptyIgnoredEvents(from: SequencerCounter, to: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ChangeWouldResultInGap, Unit] =
    processingTime.metric.eitherTEvent {
      for {
        lastNonEmptyEventSequencerCounter <- EitherT.right(
          storage.query(
            sql"""select sequencer_counter from sequenced_events
              where client = $partitionKey and type != ${SequencedEventDbType.IgnoredEvent}
              order by sequencer_counter desc #${storage.limit(1)}"""
              .as[SequencerCounter]
              .headOption,
            functionFullName,
          )
        )

        fromEffective = lastNonEmptyEventSequencerCounter.fold(from)(c => (c + 1) max from)

        lastSequencerCounter <- EitherT.right(
          storage.query(
            sql"""select sequencer_counter from sequenced_events
              where client = $partitionKey
              order by sequencer_counter desc #${storage.limit(1)}"""
              .as[SequencerCounter]
              .headOption,
            functionFullName,
          )
        )

        _ <- EitherTUtil.condUnitET[Future](
          lastSequencerCounter.forall(_ <= to) || fromEffective > to,
          ChangeWouldResultInGap(fromEffective, to),
        )

        _ <- EitherT.right(
          storage.update(
            sqlu"""delete from sequenced_events
               where client = $partitionKey and type = ${SequencedEventDbType.IgnoredEvent}
                 and $fromEffective <= sequencer_counter and sequencer_counter <= $to""",
            functionFullName,
          )
        )
      } yield ()
    }

  private[canton] override def delete(
      from: SequencerCounter
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      storage.update_(
        sqlu"delete from sequenced_events where client = $partitionKey and sequencer_counter >= $from",
        functionFullName,
      )
    }
}

object DbSequencedEventStore {
  sealed trait SequencedEventDbType {
    val name: String3
  }

  object SequencedEventDbType {

    case object Deliver extends SequencedEventDbType {
      override val name: String3 = String3.tryCreate("del")
    }

    case object DeliverError extends SequencedEventDbType {
      override val name: String3 = String3.tryCreate("err")
    }

    case object IgnoredEvent extends SequencedEventDbType {
      override val name: String3 = String3.tryCreate("ign")
    }

    implicit val setParameterSequencedEventType: SetParameter[SequencedEventDbType] = (v, pp) =>
      pp.setString(v.name.str)

    implicit val getResultSequencedEventType: GetResult[SequencedEventDbType] = GetResult(r =>
      r.nextString() match {
        case Deliver.name.str => Deliver
        case DeliverError.name.str => DeliverError
        case IgnoredEvent.name.str => IgnoredEvent
        case unknown =>
          throw new DbDeserializationException(s"Unknown sequenced event type [$unknown]")
      }
    )
  }
}
