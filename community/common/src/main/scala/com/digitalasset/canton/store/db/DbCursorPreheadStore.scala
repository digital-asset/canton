// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.{DbStorage, TransactionalStoreUpdate}
import com.digitalasset.canton.store.{CursorPrehead, CursorPreheadStore}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import io.functionmeta.functionFullName
import slick.jdbc.GetResult

import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

/** DB storage for a cursor prehead for a domain
  *
  * @param cursorTable The table name to store the cursor prehead.
  *                    The table must define the following columns:
  *                    <ul>
  *                      <li>client varchar not null primary key</li>
  *                      <li>prehead_counter bigint not null</li>
  *                      <li>ts bigint not null</li>
  *                    </ul>
  * @param processingTime The metric to be used for DB queries
  */
// TODO(#459) Switch to a different superclass or tagging for `Counter`
class DbCursorPreheadStore[Counter <: Long: GetResult](
    client: SequencerClientDiscriminator,
    private val storage: DbStorage,
    cursorTable: String,
    processingTime: GaugeM[TimedLoadGauge, Double],
    override protected val loggerFactory: NamedLoggerFactory,
)(override private[store] implicit val ec: ExecutionContext)
    extends CursorPreheadStore[Counter]
    with NamedLogging {
  import storage.api._

  @nowarn("msg=match may not be exhaustive")
  override def prehead(implicit
      traceContext: TraceContext
  ): Future[Option[CursorPrehead[Counter]]] =
    processingTime.metric.event {
      val preheadQuery =
        sql"""select prehead_counter, ts from #$cursorTable where client = $client order by prehead_counter desc #${storage
          .limit(2)}"""
          .as[(Counter, CantonTimestamp)]
      storage.query(preheadQuery, functionFullName).map {
        case Seq() => None
        case (preheadCounter, preheadTimestamp) +: rest =>
          if (rest.nonEmpty)
            logger.warn(
              s"Found several preheads for $client in $cursorTable instead of at most one; using $preheadCounter as prehead"
            )
          Some(CursorPrehead(preheadCounter, preheadTimestamp))
      }
    }

  @VisibleForTesting
  override private[canton] def overridePreheadUnsafe(
      newPrehead: Option[CursorPrehead[Counter]]
  )(implicit traceContext: TraceContext): Future[Unit] = processingTime.metric.event {
    logger.info(s"Override prehead counter in $cursorTable to $newPrehead")
    newPrehead match {
      case None => delete()
      case Some(CursorPrehead(counter, timestamp)) =>
        val query = storage.profile match {
          case _: DbStorage.Profile.H2 =>
            sqlu"merge into #$cursorTable (client, prehead_counter, ts) values ($client, $counter, $timestamp)"
          case _: DbStorage.Profile.Postgres =>
            sqlu"""insert into #$cursorTable (client, prehead_counter, ts) values ($client, $counter, $timestamp)
                     on conflict (client) do update set prehead_counter = $counter, ts = $timestamp"""
          case _: DbStorage.Profile.Oracle =>
            sqlu"""merge into #$cursorTable ct
                   using (
                    select 
                      $client client,
                      $counter counter,
                      $timestamp ts
                    from dual
                   ) val
                   on (val.client = ct.client)
                   when matched then
                    update set ct.prehead_counter = val.counter, ct.ts = val.ts
                   when not matched then
                    insert (client, prehead_counter, ts) values (val.client, val.counter, val.ts)"""
        }
        storage.update_(query, functionFullName)
    }
  }

  override def advancePreheadToTransactionalStoreUpdate(
      newPrehead: CursorPrehead[Counter]
  )(implicit traceContext: TraceContext): TransactionalStoreUpdate = {
    logger.debug(s"Advancing prehead in $cursorTable to $newPrehead")
    val CursorPrehead(counter, timestamp) = newPrehead
    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""
          merge into #$cursorTable as cursor_table
          using dual
          on cursor_table.client = $client
            when matched and cursor_table.prehead_counter < $counter
              then update set cursor_table.prehead_counter = $counter, cursor_table.ts = $timestamp
            when not matched then insert (client, prehead_counter, ts) values ($client, $counter, $timestamp)
          """
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
          insert into #$cursorTable as cursor_table (client, prehead_counter, ts)
          values ($client, $counter, $timestamp)
          on conflict (client) do
            update set prehead_counter = $counter, ts = $timestamp
            where cursor_table.prehead_counter < $counter
          """
      case _: DbStorage.Profile.Oracle =>
        sqlu"""
          merge into #$cursorTable cursor_table
          using (
            select
              $client client
            from dual
           ) val
            on (cursor_table.client = val.client)
            when matched then
              update set cursor_table.prehead_counter = $counter, cursor_table.ts = $timestamp
              where cursor_table.prehead_counter < $counter
            when not matched then
              insert (client, prehead_counter, ts) values (val.client, $counter, $timestamp)
          """
    }
    new TransactionalStoreUpdate.DbTransactionalStoreUpdate(
      query,
      storage,
      Some(processingTime.metric),
    )
  }

  override def rewindPreheadTo(
      newPreheadO: Option[CursorPrehead[Counter]]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.info(s"Rewinding prehead to $newPreheadO")
    newPreheadO match {
      case None => delete()
      case Some(CursorPrehead(counter, timestamp)) =>
        val query =
          sqlu"""
            update #$cursorTable 
            set prehead_counter = $counter, ts = $timestamp
            where client = $client and prehead_counter > $counter"""
        storage.update_(query, "rewind prehead")
    }
  }

  private[this] def delete()(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      storage.update_(sqlu"""delete from #$cursorTable where client = $client""", functionFullName)
    }
}
