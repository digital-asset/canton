// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.ContractReassignment
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.party.GeneratesUniqueUpdateIds
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.ContractActivationChangeBatch
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfContractId, UpdateId}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

class DbPartyReplicationIndexingStore(
    override protected val storage: DbStorage,
    indexedSynchronizer: IndexedSynchronizer,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends DbStore
    with PartyReplicationIndexingStore {
  import storage.api.*
  import storage.converters.*

  override def addImportedContractActivations(
      partyId: PartyId,
      toc: TimeOfChange,
      contractActivationsToIndex: NonEmpty[Seq[ContractReassignment]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val isIndexed = false
    val insertQuery =
      s"""insert into par_party_replication_indexing(synchronizer_idx, party_id, ts, repair_counter, contract_id, change, is_indexed, reassignment_counter)
         values (?, ?, ?, ?, ?, CAST(? as change_type), $isIndexed, ?)
         on conflict do nothing"""
    val insertAll =
      DbStorage.bulkOperation_(insertQuery, contractActivationsToIndex, storage.profile) {
        pp => element =>
          val ContractReassignment(contractInstance, _, _, reassignmentCounter) = element
          pp >> indexedSynchronizer
          pp >> partyId
          pp >> toc
          pp >> contractInstance.contractId
          pp >> ChangeType.Activation
          pp >> reassignmentCounter
      }
    storage.queryAndUpdate(insertAll, functionFullName)
  }

  override def consumeNextActivationChangesBatch(
      partyId: PartyId,
      indexingBatchCounter: NonNegativeLong,
      maxBatchSize: PositiveInt,
  )(
      generatesUniqueUpdateIds: GeneratesUniqueUpdateIds
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[
    Option[ContractActivationChangeBatch]
  ] = for {
    nextBatch <- storage
      .query(
        sql"""
        select ts, repair_counter, contract_id, change, reassignment_counter
        from par_party_replication_indexing
        where synchronizer_idx = $indexedSynchronizer and party_id = $partyId and indexing_batch_counter is null and update_id is null and is_indexed = false
        order by synchronizer_idx, party_id, ts, repair_counter, contract_id
        #${storage.limit(maxBatchSize.unwrap)}
        """.as[
          (
              TimeOfChange,
              LfContractId,
              ChangeType,
              ReassignmentCounter,
          )
        ],
        functionFullName,
      )
    updateId = generatesUniqueUpdateIds.uniqueUpdateId(indexingBatchCounter, nextBatch)
    update =
      """
         update par_party_replication_indexing
         set indexing_batch_counter = ?, update_id = ?
         where synchronizer_idx = ? and party_id = ? and ts = ? and repair_counter = ? and contract_id = ?
        """
    bulkUpdate = DbStorage.bulkOperation(update, nextBatch, storage.profile) { pp => element =>
      val (toc, contractId, _change, _reassignmentCounter) = element
      pp >> Some(indexingBatchCounter)
      pp >> Some(updateId)
      pp >> indexedSynchronizer
      pp >> partyId
      pp >> toc
      pp >> contractId
    }
    _ <- storage.queryAndUpdate(bulkUpdate, "update batch counter and update id for indexing")
  } yield (
    NonEmpty
      .from(nextBatch.map { case (_toc, contractId, change, reassignmentCounterO) =>
        contractId -> (change, reassignmentCounterO)
      }.toMap)
      .map(ContractActivationChangeBatch(updateId, _))
  )

  override def markContractActivationChangesAsIndexed(updateId: UpdateId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val update =
      sqlu"update par_party_replication_indexing set is_indexed = true where update_id = $updateId"
    storage.update_(update, s"update mark batch indexed")
  }

  override def purgeContractActivationChanges(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val delete = sqlu"delete from par_party_replication_indexing where party_id = $partyId"
    storage.update_(delete, s"delete party replication activation changes")
  }

  protected[store] override def listContractActivationChanges(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[
    (
        TimeOfChange,
        LfContractId,
        ChangeType,
        Option[(NonNegativeLong, UpdateId)],
        Boolean,
        ReassignmentCounter,
    )
  ]] = storage
    .query(
      sql"""
        select ts, repair_counter, contract_id, change, indexing_batch_counter, update_id, is_indexed, reassignment_counter
        from par_party_replication_indexing
        where synchronizer_idx = $indexedSynchronizer and party_id = $partyId
        order by synchronizer_idx, party_id, ts, repair_counter, contract_id
        """.as[
        (
            TimeOfChange,
            LfContractId,
            ChangeType,
            Option[NonNegativeLong],
            Option[UpdateId],
            Boolean,
            ReassignmentCounter,
        )
      ],
      functionFullName,
    )
    .map(_.map { case (toc, cid, change, ctrO, updateIdO, isIndexed, reassignmentCounter) =>
      (ctrO, updateIdO) match {
        case (None, None) => (toc, cid, change, None, isIndexed, reassignmentCounter)
        case (Some(ctr), Some(updateId)) =>
          (toc, cid, change, Some((ctr, updateId)), isIndexed, reassignmentCounter)
        case _ =>
          throw new IllegalStateException(
            s"Unexpected db-inconsistency on $cid at $toc and $change: Either both or neither should be set $ctrO, $updateIdO"
          )
      }
    })
}
