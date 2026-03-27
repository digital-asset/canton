// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.ContractReassignment
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.admin.party.GeneratesUniqueUpdateIds
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.{
  ActivationChange,
  ContractActivationChangeBatch,
}
import com.digitalasset.canton.participant.store.memory.InMemoryPartyReplicationIndexingStore.{
  KeyType,
  KeyValueType,
  ValueType,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfContractId, UpdateId}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class InMemoryPartyReplicationIndexingStore(
)(implicit val executionContext: ExecutionContext)
    extends PartyReplicationIndexingStore {

  private val store = TrieMap.empty[KeyType, ValueType]
  private val mutex = Mutex()

  private val notIndexed = false
  private val notIndexing: Option[(NonNegativeLong, UpdateId)] = None

  override def addImportedContractActivations(
      partyId: PartyId,
      toc: TimeOfChange,
      contractActivationsToIndex: NonEmpty[Seq[ContractReassignment]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = mutex.exclusive {
    contractActivationsToIndex.foreach { case ContractReassignment(contract, _, _, ctr) =>
      store
        .putIfAbsent(
          (partyId, toc, contract.contractId),
          (ChangeType.Activation, ctr, notIndexing, notIndexed),
        )
        .discard
    }
    FutureUnlessShutdown.unit
  }

  override def consumeNextActivationChangesBatch(
      partyId: PartyId,
      indexingBatchCounter: NonNegativeLong,
      maxBatchSize: PositiveInt,
  )(generatesUniqueUpdateIds: GeneratesUniqueUpdateIds)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ContractActivationChangeBatch]] = for {
    batch <- collectActivationChanges {
      case (
            (_partyId, toc, contractId),
            (changeType, reassignmentCounter, None, false),
          ) =>
        (toc, contractId, changeType, None, false, reassignmentCounter)
    }.map(_.take(maxBatchSize.unwrap).map { case (toc, contractId, change, _, _, ctr) =>
      (toc, contractId, change, ctr)
    })
    updateId = generatesUniqueUpdateIds.uniqueUpdateId(indexingBatchCounter, batch)
    _ = batch.foreach { case (toc, cid, change, ctr) =>
      store.update(
        (partyId, toc, cid),
        (change, ctr, Some((indexingBatchCounter, updateId)), false),
      )
    }
  } yield NonEmpty
    .from(batch.map { case (_, cid, change, reassignmentCounter) =>
      cid -> (change, reassignmentCounter)
    }.toMap)
    .map(ContractActivationChangeBatch(updateId, _))

  override def markContractActivationChangesAsIndexed(updateId: UpdateId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = processSynchronized {
    case (key, (change, ctr, Some((batchCtr, `updateId`)), false)) =>
      (key, (change, ctr, Some((batchCtr, updateId)), true))
  } { case (key, value) => store.replace(key, value).discard }

  override def purgeContractActivationChanges(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    processSynchronized { case (key @ (`partyId`, _, _), _) => key }(store.remove(_).discard)

  private def processSynchronized[T](
      pf: PartialFunction[KeyValueType, T]
  )(update: T => Unit): FutureUnlessShutdown[Unit] = mutex.exclusive {
    store.iterator.collect(pf).foreach(update)
    FutureUnlessShutdown.unit
  }

  override protected[store] def listContractActivationChanges(
      partyId: PartyId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Seq[ActivationChange]] =
    collectActivationChanges {
      case (
            (`partyId`, toc, contractId),
            (changeType, reassignmentCounter, indexingInfo, isIndexed),
          ) =>
        (toc, contractId, changeType, indexingInfo, isIndexed, reassignmentCounter)
    }

  private def collectActivationChanges(
      pf: PartialFunction[KeyValueType, ActivationChange]
  ): FutureUnlessShutdown[Seq[ActivationChange]] = FutureUnlessShutdown.pure(
    mutex.exclusive(
      store.iterator
        .collect(pf)
        .toSeq
        .sorted(
          Ordering.by[ActivationChange, (TimeOfChange, String)] { case (toc, cid, _, _, _, _) =>
            (toc, cid.coid)
          }
        )
    )
  )
}

object InMemoryPartyReplicationIndexingStore {
  private type KeyType = (PartyId, TimeOfChange, LfContractId)
  private type ValueType = (
      ChangeType,
      ReassignmentCounter,
      Option[(NonNegativeLong, UpdateId)], // indexing_batch_counter, update_id
      Boolean, // is_indexed
  )
  private type KeyValueType = (KeyType, ValueType)
}
