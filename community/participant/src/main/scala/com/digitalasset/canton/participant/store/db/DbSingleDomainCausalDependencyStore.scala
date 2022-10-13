// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.metrics.MetricHandle.Gauge
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker.DomainPerPartyCausalState
import com.digitalasset.canton.participant.store.SingleDomainCausalDependencyStore
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{DiscardOps, LfPartyId, RequestCounter}
import io.functionmeta.functionFullName

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future, Promise}

class DbSingleDomainCausalDependencyStore(
    val domainId: DomainId,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SingleDomainCausalDependencyStore
    with DbStore {

  override val state: DomainPerPartyCausalState = new TrieMap()

  private val initializedP: Promise[Unit] = Promise()
  override protected val initialized: Future[Unit] = initializedP.future

  def initialize(lastIncludedO: Option[RequestCounter])(implicit tc: TraceContext): Future[Unit] = {
    import DbStorage.Implicits.*
    import storage.api.*

    def initialize(lastIncluded: RequestCounter): Future[Unit] = {
      val query = sql"""select party_id, domain_id, max(domain_ts)
         from per_party_causal_dependencies
         where owning_domain_id = $domainId and request_counter <= $lastIncluded
         group by (owning_domain_id, request_counter, party_id, domain_id)
         """.as[(LfPartyId, DomainId, CantonTimestamp)]

      for {
        tuples <- storage.query(query, functionFullName)
      } yield {
        tuples.foreach { case ((party, domain, timestamp)) =>
          val domainDependencies: Map[DomainId, CantonTimestamp] =
            state.getOrElseUpdate(party, immutable.Map.empty)
          val nextDependencies = domainDependencies.updated(domain, timestamp)
          state.update(party, nextDependencies)
        }
        logger.info(s"${this.getClass} initialized from $lastIncluded")
      }
    }

    lastIncludedO.fold(Future.unit)(initialize).map { case () =>
      initializedP.trySuccess(()).discard
    }
  }

  private val processingTime: Gauge[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("causal-dependency-store")

  override protected def persistentInsert(
      rc: RequestCounter,
      ts: CantonTimestamp,
      clocks: Map[LfPartyId, Map[DomainId, CantonTimestamp]],
      transferOutId: Option[TransferId],
  )(implicit tc: TraceContext): Future[Unit] = {

    processingTime.metric.event {
      DbSingleDomainCausalDependencyStore.persistentInsert(storage)(
        Some(rc),
        ts,
        clocks,
        transferOutId,
        domainId,
        storage.profile,
      )
    }
  }

}

object DbSingleDomainCausalDependencyStore {
  def persistentInsert(storage: DbStorage)(
      rc: Option[RequestCounter],
      ts: CantonTimestamp,
      clocks: Map[LfPartyId, Map[DomainId, CantonTimestamp]],
      transferOutId: Option[TransferId],
      domainId: DomainId,
      profile: Profile,
  )(implicit elc: ErrorLoggingContext, closeContext: CloseContext): Future[Unit] = {
    val insertStatement =
      """
        |insert into per_party_causal_dependencies (owning_domain_id, constraint_ts, request_counter, party_id, transfer_origin_domain_if_present, domain_id, domain_ts)
        |values (?, ?, ?, ?, ?, ?, ?)""".stripMargin

    val toInsert = clocks.toSeq.flatMap { case (party, map) =>
      map.toSeq.map(domainTs => party -> domainTs)
    }
    val bulkInsert = DbStorage.bulkOperation_(insertStatement, toInsert, profile) { pp => pair =>
      import DbStorage.Implicits.*
      val (party, (otherDomain, ts)) = pair
      pp >> domainId
      pp >> ts
      pp >> rc
      pp >> party
      pp >> transferOutId.map(_.sourceDomain)
      pp >> otherDomain
      pp >> ts
    }

    storage.queryAndUpdate(bulkInsert, functionFullName)(elc.traceContext, closeContext)
  }
}
