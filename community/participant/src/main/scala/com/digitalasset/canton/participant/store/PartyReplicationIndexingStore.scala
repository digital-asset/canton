// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.ContractReassignment
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.party.GeneratesUniqueUpdateIds
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.ContractActivationChangeBatch
import com.digitalasset.canton.participant.store.db.DbPartyReplicationIndexingStore
import com.digitalasset.canton.participant.store.memory.InMemoryPartyReplicationIndexingStore
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfContractId, UpdateId}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** The party replication indexing store tracks changes to the ActiveContractStore between the point
  * in time contract are imported on a target participant, while they are scheduled to be passed to
  * the indexer, and until they are confirmed indexed by the indexer.
  *
  * The following components modify the party replication indexing store:
  *   1. Online party replication
  *      [[com.digitalasset.canton.participant.protocol.party.TargetParticipantAcsPersistence]]
  *      inserts imported active contract activations for subsequent indexing.
  *
  *   1. Canton protocol transaction processing append active contract changes on behalf of
  *      onboarding parties and concurrent Daml transactions such that corresponding changes can be
  *      passed to the indexer in a causality preserving order within the synchronizer in which the
  *      party is replicated.
  *
  *   1. The [[com.digitalasset.canton.participant.admin.party.PartyReplicationIndexingWorkflow]]
  *      updates active contract changes as they are passed to the indexer.
  *
  *   1. The indexer updates active contract changes after they have been indexed.
  *
  *   1. The [[com.digitalasset.canton.participant.admin.party.PartyReplicationIndexingWorkflow]]
  *      deletes active contract changes after party replication has completed.
  */
trait PartyReplicationIndexingStore {

  /** Add imported active contract activations for subsequent indexing.
    *
    * @param partyId
    *   party being replicated for which to add the contract activations
    * @param toc
    *   time of change at which the contract has been activated in the canton protocol, used to
    *   order contract activation changes in a causality order allowing to index activations before
    *   deactivations and to batch indexing into separate
    *   [[com.digitalasset.canton.ledger.participant.state.Update.OnPRReassignmentAccepted]] indexer
    *   events
    * @param contractActivationsToIndex
    *   contract activations
    */
  def addImportedContractActivations(
      partyId: PartyId,
      toc: TimeOfChange,
      contractActivationsToIndex: NonEmpty[Seq[ContractReassignment]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Retrieve the next batch of contract activation changes for indexing by contract ids.
    *
    * This method MUST NOT be called multiple times concurrently as method implementations are not
    * idempotent.
    *
    * Note:
    *   - The size of the returned contract activation changes might be less than the maximum batch
    *     size.
    *   - If the returned size is less than the maximum batch size, this does not imply that
    *     indexing is done as more concurrent transactions can produce additional activation changes
    *     until party replication is complete according to PartyToParticipant topology.
    *
    * @param partyId
    *   party being replicated for which the contract activation changes are to be retrieved
    * @param indexingBatchCounter
    *   unique indexing batch counter to associate with the returned contract activation changes
    * @param maxBatchSize
    *   maximum batch size
    * @return
    *   contract activation changes as a map of contract id to details such as change type and
    *   reassignment counter as well as a unique update id associated with the returned contract
    *   activation changes
    */
  def consumeNextActivationChangesBatch(
      partyId: PartyId,
      indexingBatchCounter: NonNegativeLong,
      maxBatchSize: PositiveInt,
  )(
      generatesUniqueUpdateIds: GeneratesUniqueUpdateIds
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ContractActivationChangeBatch]]

  /** Indexer confirms an
    * [[com.digitalasset.canton.ledger.participant.state.Update.OnPRReassignmentAccepted]] has been
    * indexed
    *
    * @param updateId
    *   id of the update that has been indexed
    */
  def markContractActivationChangesAsIndexed(
      updateId: UpdateId
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Purge activation changes after they are no longer needed particularly after party replication
    * has completed.
    *
    * @param partyId
    *   party corresponding to party replication
    */
  def purgeContractActivationChanges(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  // Helper for unit testing
  protected[store] def listContractActivationChanges(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[
    (
        TimeOfChange,
        LfContractId,
        ActiveContractStore.ChangeType,
        Option[(NonNegativeLong, UpdateId)],
        Boolean,
        ReassignmentCounter,
    )
  ]]
}

object PartyReplicationIndexingStore {
  type ActivationChange = (
      TimeOfChange,
      LfContractId,
      ChangeType,
      Option[(NonNegativeLong, UpdateId)],
      Boolean,
      ReassignmentCounter,
  )

  def apply(
      storage: Storage,
      indexedSynchronizer: IndexedSynchronizer,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): PartyReplicationIndexingStore =
    storage match {
      case _: MemoryStorage => new InMemoryPartyReplicationIndexingStore
      case jdbc: DbStorage =>
        new DbPartyReplicationIndexingStore(jdbc, indexedSynchronizer, timeouts, loggerFactory)
    }

  /** Holds batch of contract activation changes along with a unique updateId.
    *
    * @param updateId
    *   unique update id associated with the contract activation changes
    * @param activationChanges
    *   contract activation changes as a map of contract id to details such as change type and
    *   reassignment counter
    */
  final case class ContractActivationChangeBatch(
      updateId: UpdateId,
      activationChanges: NonEmpty[
        Map[LfContractId, (ActiveContractStore.ChangeType, ReassignmentCounter)]
      ],
  )
}
