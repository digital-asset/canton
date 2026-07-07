// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.ContractRef
import com.digitalasset.canton.platform.store.backend.common.{
  ContractStorageBackendTemplate,
  QueryStrategy,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.SynchronizerId

import java.sql.Connection

class PostgresContractStorageBackend(
    stringInterning: StringInterning,
    ledgerEndCache: LedgerEndCache,
) extends ContractStorageBackendTemplate(PostgresQueryStrategy, stringInterning, ledgerEndCache) {

  private def toIntArrayLiteral(values: Iterable[Int]): String =
    values.mkString("ARRAY[", ", ", "]::integer[]")

  private def toLongArrayLiteral(values: Iterable[Long]): String =
    values.mkString("ARRAY[", ", ", "]::bigint[]")

  override def lastActivations(synchronizerContracts: Iterable[(SynchronizerId, Long)])(
      connection: Connection
  ): Map[(SynchronizerId, Long), Long] =
    if (synchronizerContracts.isEmpty) Map.empty
    else
      ledgerEndCache()
        .map { ledgerEnd =>
          val inputWithIndex = synchronizerContracts.zipWithIndex
          val indexArrayLiteral = toIntArrayLiteral(inputWithIndex.view.map(_._2))
          val synchronizerIdArrayLiteral = toIntArrayLiteral(
            inputWithIndex.view.map(_._1._1).map(stringInterning.synchronizerId.internalize)
          )
          val internalContractIdArrayLiteral = toLongArrayLiteral(inputWithIndex.view.map(_._1._2))
          // Resorting here to non-prepared statement as the combination of prepared statement and unnest and cross lateral join produced very inefficient query plans with PostgreSQL.
          // For Future reference:
          //   * Wrong query plan involved traversing the event_sequential_id index backwards in a index scan and eliminating candidates with filters on table itself (the good plan is the descending index only scan with index condition over the contract ID)
          //   * Query plans without prepared statement results in an efficient plan in tests
          //   * Only the prepared statement via JDBC resulted in inefficient plans (creating prepared statements for example via psql tool with PREPARE was not exhibiting the same problem)
          val results = QueryStrategy
            .plainJdbcQuery(s"""
          SELECT input.index as result_index, activate_evs.event_sequential_id as result_event_sequential_id
          FROM UNNEST($indexArrayLiteral, $synchronizerIdArrayLiteral, $internalContractIdArrayLiteral) AS input(index, synchronizer_id, internal_contract_id)
          CROSS JOIN LATERAL (
            SELECT *
            FROM lapi_events_activate_contract activate_evs
            WHERE activate_evs.internal_contract_id = input.internal_contract_id
            AND activate_evs.event_sequential_id <= ${ledgerEnd.lastEventSeqId}
            AND EXISTS ( -- subquery for triggering (event_sequential_id) INCLUDE (synchronizer_id) index usage
              SELECT 1
              FROM lapi_events_activate_contract as activate_evs2
              WHERE
                activate_evs2.event_sequential_id = activate_evs.event_sequential_id AND
                activate_evs2.synchronizer_id = input.synchronizer_id
            )
            ORDER BY activate_evs.event_sequential_id DESC
            LIMIT 1
          ) activate_evs""")(resultSet =>
              (
                resultSet.getInt("result_index"),
                resultSet.getLong("result_event_sequential_id"),
              )
            )(connection)
            .toMap
          inputWithIndex.iterator.flatMap { case (synCon, index) =>
            results.get(index).map(synCon -> _)
          }.toMap
        }
        .getOrElse(Map.empty)

  override final def supportsBatchKeyStateLookups: Boolean = true

  override def contractKeysPlain(
      keyPageQueries: Seq[ContractStorageBackend.KeyLookupPageQuery],
      validAtEventSeqId: Long,
  )(connection: Connection): Seq[ContractStorageBackend.KeyLookupPageResult] =
    if (keyPageQueries.isEmpty) Seq.empty
    else {
      val queriesWithIndex = keyPageQueries.zipWithIndex

      def toStringArrayLiteral(values: Iterable[String]): String =
        values.map(v => s"'$v'").mkString("ARRAY[", ", ", "]::text[]")

      val indexArrayLiteral = toIntArrayLiteral(queriesWithIndex.view.map(_._2))
      val keyHashArrayLiteral = toStringArrayLiteral(
        queriesWithIndex.view.map(_._1.key.hash.bytes.toHexString)
      )
      val eventSeqIdUpperBounds = queriesWithIndex.view.map { case (q, _) =>
        q.nextPageToken.map(_ - 1).getOrElse(validAtEventSeqId)
      }
      val upperBoundArrayLiteral = toLongArrayLiteral(eventSeqIdUpperBounds)
      val limitArrayLiteral = toIntArrayLiteral(queriesWithIndex.view.map(_._1.limit))

      val results: Vector[(Int, Long, Long)] = QueryStrategy.plainJdbcQuery(
        s"""
        SELECT input.idx as result_index,
               activate.event_sequential_id as result_event_seq_id,
               activate.internal_contract_id as result_internal_contract_id
        FROM UNNEST($indexArrayLiteral, $keyHashArrayLiteral, $upperBoundArrayLiteral, $limitArrayLiteral)
             AS input(idx, key_hash, upper_bound, lim)
        CROSS JOIN LATERAL (
          SELECT event_sequential_id, internal_contract_id
          FROM lapi_events_activate_contract
          WHERE
            create_key_hash = input.key_hash
            AND event_sequential_id <= input.upper_bound
            AND NOT EXISTS (
              SELECT 1
              FROM lapi_events_deactivate_contract
              WHERE
                deactivated_event_sequential_id = lapi_events_activate_contract.event_sequential_id
                AND lapi_events_deactivate_contract.event_sequential_id <= $validAtEventSeqId
            )
          ORDER BY event_sequential_id DESC
          LIMIT input.lim + 1
        ) activate
        ORDER BY input.idx, activate.event_sequential_id DESC"""
      )(resultSet =>
        (
          resultSet.getInt("result_index"),
          resultSet.getLong("result_event_seq_id"),
          resultSet.getLong("result_internal_contract_id"),
        )
      )(connection)

      val groupedResults: Map[Int, Vector[ContractRef]] =
        results
          .groupBy(_._1)
          .view
          .mapValues(_.map(t => ContractRef(internalContractId = t._3, eventSequentialId = t._2)))
          .toMap

      queriesWithIndex.map { case (query, index) =>
        val contractRefs = groupedResults.getOrElse(index, Vector.empty)
        ContractStorageBackend.KeyLookupPageResult(
          contractRefs = contractRefs.take(query.limit),
          nextPageToken = Option
            .when(contractRefs.sizeIs == query.limit + 1)(
              contractRefs.lastOption.map(_.eventSequentialId + 1)
            )
            .flatten,
        )
      }
    }
}
