// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.{IndexedDomain, IndexedString, PrunableByTime}
import com.digitalasset.canton.tracing.TraceContext
import io.functionmeta.functionFullName
import slick.jdbc.SetParameter

import scala.concurrent.{ExecutionContext, Future}

/** Mixin for an db store that stores the latest point in time when
  * pruning has started or finished.
  *
  * The pruning method of the store must use [[advancePruningTimestamp]] to signal the start end completion
  * of each pruning.
  */
trait DbPrunableByTime[PartitionKey, E] extends PrunableByTime[E] {
  this: DbStore =>

  protected[this] implicit def setParameterDiscriminator: SetParameter[PartitionKey]

  /** The table name to store the pruning timestamp in.
    * The table must define the following fields:
    * <ul>
    *   <li>[[partitionColumn]] primary key</li>
    *   <li>`phase` stores the [[com.digitalasset.canton.pruning.PruningPhase]]</li>
    *   <li>`ts` stores the [[com.digitalasset.canton.data.CantonTimestamp]]<li>
    * </ul>
    */
  protected[this] def pruning_status_table: String

  protected[this] def partitionColumn: String

  protected[this] def partitionKey: PartitionKey

  protected[this] implicit val ec: ExecutionContext

  import storage.api._

  protected val processingTime: GaugeM[TimedLoadGauge, Double]

  override def pruningStatus(implicit
      traceContext: TraceContext
  ): EitherT[Future, E, Option[PruningStatus]] =
    EitherT.right[E](processingTime.metric.event {
      val query = sql"""
        select phase, ts from #$pruning_status_table
        where #$partitionColumn = $partitionKey
        """.as[PruningStatus].headOption
      storage.query(query, functionFullName)
    })

  protected[canton] def advancePruningTimestamp(phase: PruningPhase, timestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): EitherT[Future, E, Unit] = processingTime.metric.eitherTEvent {
    val query = storage.profile match {
      case _: DbStorage.Profile.H2 =>
        sqlu"""
          merge into #$pruning_status_table as pruning_status
          using dual
          on pruning_status.#$partitionColumn = $partitionKey
            when matched and (pruning_status.ts < $timestamp or (pruning_status.ts = $timestamp and $phase = ${PruningPhase.Completed}))
              then update set pruning_status.phase = $phase, pruning_status.ts = $timestamp
            when not matched then insert (#$partitionColumn, phase, ts) values ($partitionKey, $phase, $timestamp)
          """
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
          insert into #$pruning_status_table as pruning_status (#$partitionColumn, phase, ts)
          values ($partitionKey, CAST($phase as pruning_phase), $timestamp)
          on conflict (#$partitionColumn) do
            update set phase = CAST($phase as pruning_phase), ts = $timestamp
            where pruning_status.ts < $timestamp or (pruning_status.ts = $timestamp and CAST($phase as pruning_phase) = CAST(${PruningPhase.Completed} as pruning_phase))
          """
      case _: DbStorage.Profile.Oracle =>
        sqlu"""
          merge into #$pruning_status_table pruning_status
          using (
            select 
              $partitionKey partitionKey, 
              $phase phase, 
              $timestamp timestamp
              from
                dual
          ) val 
          on (pruning_status.#$partitionColumn = val.partitionKey)
            when matched then
                update set pruning_status.phase = val.phase, pruning_status.ts = val.timestamp
                where (pruning_status.ts < val.timestamp or (pruning_status.ts = val.timestamp and val.phase = ${PruningPhase.Completed}))
            when not matched then 
              insert (#$partitionColumn, phase, ts) values (val.partitionKey, val.phase, val.timestamp)
          """
    }

    for {
      rowCount <- EitherT.right(storage.update(query, "pruning status upsert"))
      _ <-
        if (logger.underlying.isDebugEnabled && rowCount != 1 && phase == PruningPhase.Started) {
          pruningStatus.map {
            case Some(previous) if previous.timestamp > timestamp =>
              logger.debug(
                s"Pruning at $timestamp started after another later pruning at ${previous.timestamp}."
              )
            case _ =>
          }
        } else EitherT.pure[Future, E](())
    } yield ()
  }
}

/** Specialized [[DbPrunableByTime]] that uses the [[com.digitalasset.canton.topology.DomainId]] as discriminator */
trait DbPrunableByTimeDomain[E] extends DbPrunableByTime[IndexedDomain, E] {
  this: DbStore =>

  protected[this] def domainId: IndexedDomain

  override protected[this] def partitionKey: IndexedDomain = domainId

  override protected[this] val partitionColumn = "domain_id"

  override protected[this] implicit val setParameterDiscriminator: SetParameter[IndexedDomain] =
    IndexedString.setParameterIndexedString

}
