// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.MultiDomainCausalityStore
import com.digitalasset.canton.protocol.TransferId
import com.digitalasset.canton.protocol.messages.VectorClock
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import io.functionmeta.functionFullName

import scala.concurrent.{ExecutionContext, Future}

class DbMultiDomainCausalityStore private (
    override protected val storage: DbStorage,
    indexedStringStore: IndexedStringStore,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends MultiDomainCausalityStore
    with DbStore {

  override protected def persistCausalityMessageState(
      id: TransferId,
      vectorClocks: List[VectorClock],
  )(implicit tc: TraceContext): Future[Unit] = {

    val requestCounter =
      None // We aren't connected to the source domain, so we don't have a request counter
    val clocks = vectorClocks.map { vc =>
      vc.partyId -> vc.clock
    }.toMap
    DbSingleDomainCausalDependencyStore.persistentInsert(storage)(
      requestCounter,
      id.requestTimestamp,
      clocks,
      Some(id),
      id.sourceDomain,
      storage.profile,
    )
  }

  override def loadTransferOutStateFromPersistentStore(
      transferId: TransferId,
      parties: Set[LfPartyId],
  )(implicit tc: TraceContext): Future[Option[Map[LfPartyId, VectorClock]]] = {
    import DbStorage.Implicits.BuilderChain.*
    import DbStorage.Implicits.*
    import storage.api.*

    val partiesSql: DbStorage.SQLActionBuilderChain = parties.toList
      .map { partyId: LfPartyId =>
        sql"$partyId"
      }
      .intercalate(sql", ")

    // Get the highest timestamp that each stakeholder informee has observed from the transfer's source domain at the
    // time of the transfer-out
    val constraintsSql = {
      (sql"""select party_id, domain_id, max(domain_ts)
                 from per_party_causal_dependencies
                 where owning_domain_id = ${transferId.sourceDomain}
                 and party_id in (""" ++ partiesSql ++ sql""")
                 and constraint_ts <= ${transferId.requestTimestamp}
                 group by (party_id, domain_id)
                 """).as[(LfPartyId, DomainId, CantonTimestamp)]
    }

    for {
      vector <- storage.query(constraintsSql, functionFullName)
    } yield {

      if (vector.nonEmpty) {
        val perPartyDependencies: Map[LfPartyId, Map[DomainId, CantonTimestamp]] =
          vector.foldLeft(Map.empty: Map[LfPartyId, Map[DomainId, CantonTimestamp]]) {
            case (acc, (party, domain, timestamp)) =>
              val partyState: Map[DomainId, CantonTimestamp] = acc.getOrElse(party, Map.empty)
              acc.updated(party, partyState.updated(domain, timestamp))
          }

        val clocks = perPartyDependencies.map { case (id, domains) =>
          id -> VectorClock(transferId.sourceDomain, transferId.requestTimestamp, id, domains)
        }

        val allPartiesSeenTxOut = clocks.values.forall { clk =>
          clk.clock.get(transferId.sourceDomain).contains(transferId.requestTimestamp)
        }
        if (allPartiesSeenTxOut) Some(clocks) else None

      } else { None }
    }
  }

  /** Initialise the in-memory tracking of the highest seen timestamp on each domain */
  def initialise(implicit tc: TraceContext): Future[Unit] = {
    for { firstMap <- highestSeenPerDomain } yield {
      firstMap.foreach { case (k, v) => highestSeen.put(k, v).discard[Option[CantonTimestamp]] }
    }
  }

  private def highestSeenPerDomain(implicit
      tc: TraceContext
  ): Future[Map[DomainId, CantonTimestamp]] = {
    import com.digitalasset.canton.data.CantonTimestamp.getResultTimestamp
    import storage.api.*

    val query = sql"""select lel.log_id, max(el.ts)
                from linearized_event_log lel join event_log el on lel.log_id = el.log_id and lel.local_offset = el.local_offset
                group by lel.log_id
                """
      .as[(Int, CantonTimestamp)]

    for {
      pairs <- storage.query(query, functionFullName)
      mappedPairs <- pairs.parTraverseFilter {
        case (index, ts) if index > 0 =>
          IndexedDomain
            .fromDbIndexOT("linearized_event_log", indexedStringStore)(index)
            .map(x => (x.domainId, ts))
            .value
        case _ => Future.successful(None)
      }
    } yield {
      mappedPairs.toMap
    }
  }

}
object DbMultiDomainCausalityStore {
  def apply(
      storage: DbStorage,
      indexedStringStore: IndexedStringStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, tc: TraceContext): Future[MultiDomainCausalityStore] = {

    val lookup =
      new DbMultiDomainCausalityStore(storage, indexedStringStore, timeouts, loggerFactory)
    for { _unit <- lookup.initialise } yield lookup

  }
}
