// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{bool, long}
import anorm.~
import com.digitalasset.canton.platform.Key
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.platform.store.backend.{ContractStorageBackend, PersistentEventType}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.topology.SynchronizerId

import java.sql.Connection

class ContractStorageBackendTemplate(
    queryStrategy: QueryStrategy,
    stringInterning: StringInterning,
    ledgerEndCache: LedgerEndCache,
) extends ContractStorageBackend {

  /** Batch lookup of key states
    *
    * If the backend does not support batch lookups, the implementation will fall back to sequential
    * lookups
    */
  override def keyStates(keys: Seq[Key], validAtEventSeqId: Long)(
      connection: Connection
  ): Map[Key, Long] =
    keys.iterator
      .flatMap(key =>
        keyState(key, validAtEventSeqId)(connection)
          .map(key -> _)
      )
      .toMap

  /** Sequential lookup of key states */
  override def keyState(key: Key, validAtEventSeqId: Long)(
      connection: Connection
  ): Option[Long] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.HashToStatement
    SQL"""
         WITH last_contract_key_create AS (
                SELECT lapi_events_activate_contract.*
                  FROM lapi_events_activate_contract
                 WHERE create_key_hash = ${key.hash}
                   AND event_sequential_id <= $validAtEventSeqId
                 ORDER BY event_sequential_id DESC
                 ${QueryStrategy.limitClause(Some(1))}
              )
         SELECT internal_contract_id
           FROM last_contract_key_create
         WHERE NOT EXISTS
                (SELECT 1
                   FROM lapi_events_deactivate_contract
                  WHERE
                    internal_contract_id = last_contract_key_create.internal_contract_id
                    AND event_sequential_id <= $validAtEventSeqId
                    AND event_type = ${PersistentEventType.ConsumingExercise.asInt}
                )"""
      .as(long("internal_contract_id").singleOpt)(connection)
  }

  override def activeContracts(internalContractIds: Seq[Long], beforeEventSeqId: Long)(
      connection: Connection
  ): Map[Long, Boolean] =
    if (internalContractIds.isEmpty) Map.empty
    else {
      SQL"""
       SELECT
         internal_contract_id,
         NOT EXISTS (
           SELECT 1
           FROM lapi_events_deactivate_contract
           WHERE
             internal_contract_id = lapi_events_activate_contract.internal_contract_id
             AND event_sequential_id <= $beforeEventSeqId
             AND event_type = ${PersistentEventType.ConsumingExercise.asInt}
           LIMIT 1
         ) active
       FROM lapi_events_activate_contract
       WHERE
         internal_contract_id ${queryStrategy.anyOf(internalContractIds)}
         AND event_sequential_id <= $beforeEventSeqId"""
        .asVectorOf(long("internal_contract_id") ~ bool("active"))(connection)
        .view
        .map { case internalContractId ~ active =>
          internalContractId -> active
        }
        .toMap
    }

  override def lastActivations(synchronizerContracts: Iterable[(SynchronizerId, Long)])(
      connection: Connection
  ): Map[(SynchronizerId, Long), Long] =
    ledgerEndCache()
      .map { ledgerEnd =>
        synchronizerContracts.iterator.flatMap { case (synchronizerId, internalContractId) =>
          val internedSynchronizerId = stringInterning.synchronizerId.internalize(synchronizerId)
          SQL"""
          SELECT event_sequential_id
          FROM lapi_events_activate_contract as activate
          WHERE
            internal_contract_id = $internalContractId AND
            event_sequential_id <= ${ledgerEnd.lastEventSeqId} AND
            EXISTS ( -- subquery for triggering (event_sequential_id) INCLUDE (synchronizer_id) index usage
              SELECT 1
              FROM lapi_events_activate_contract as activate2
              WHERE
                activate2.event_sequential_id = activate.event_sequential_id AND
                activate2.synchronizer_id = $internedSynchronizerId
            )
          ORDER BY event_sequential_id DESC
          LIMIT 1"""
            .as(long("event_sequential_id").singleOpt)(connection)
            .map((synchronizerId, internalContractId) -> _)
        }.toMap
      }
      .getOrElse(Map.empty)

  override def supportsBatchKeyStateLookups: Boolean = false

  override def nonUniqueContractKey(
      keyPageQuery: ContractStorageBackend.KeysPageQuery
  )(connection: Connection): ContractStorageBackend.KeysPageResult = {
    import com.digitalasset.canton.platform.store.backend.Conversions.HashToStatement
    val eventSeqIdUpperBoundInclusive =
      keyPageQuery.nextPageToken
        .map(_ - 1) // making the exclusive token the inclusive bound
        .getOrElse(keyPageQuery.validAtEventSeqId)
    val (eventSeqIds, internalContractIds) = SQL"""
      SELECT event_sequential_id, internal_contract_id
      FROM lapi_events_activate_contract
      WHERE
        create_key_hash = ${keyPageQuery.key.hash}
        AND event_sequential_id <= $eventSeqIdUpperBoundInclusive
        AND NOT EXISTS (
          SELECT 1
          FROM lapi_events_deactivate_contract
          WHERE
            deactivated_event_sequential_id = lapi_events_activate_contract.event_sequential_id
            AND lapi_events_deactivate_contract.event_sequential_id <= ${keyPageQuery.validAtEventSeqId}
        )
      ORDER BY event_sequential_id DESC
      ${QueryStrategy.limitClause(Some(keyPageQuery.limit + 1))}"""
      .asVectorOf(
        long("event_sequential_id") ~ long("internal_contract_id") map {
          case eventSeqId ~ internalContractId => (eventSeqId -> internalContractId)
        }
      )(connection)
      .unzip
    ContractStorageBackend.KeysPageResult(
      internalContractIds =
        // we asked for limit plus 1
        internalContractIds.take(keyPageQuery.limit),
      nextPageToken = Option
        // we asked for one more, so there is only make sense to continue if there is limit+1 results
        // and then the exclusive token should be one above the identified one
        // note: subsequent query still can return empty in case validAtEventSeqId increased and this
        // causes all potential activations in the next page to be inactivated.
        .when(eventSeqIds.sizeIs == keyPageQuery.limit + 1)(
          eventSeqIds.lastOption.map(_ + 1)
        )
        .flatten,
    )
  }
}
