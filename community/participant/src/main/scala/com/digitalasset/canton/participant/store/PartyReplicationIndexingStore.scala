// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.{
  ActivationChange,
  ActivationChangeBatchEntry,
  ContractActivationChangeBatch,
  Watermark,
}
import com.digitalasset.canton.participant.store.db.DbPartyReplicationIndexingStore
import com.digitalasset.canton.participant.store.memory.InMemoryPartyReplicationIndexingStore
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{ReassignmentCounter, checked}
import slick.jdbc.{GetResult, SetParameter}

import scala.annotation.tailrec
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
  *   1. Canton protocol transaction processing appends active contract changes on behalf of
  *      onboarding parties and concurrent Daml transactions such that corresponding changes can be
  *      passed to the indexer in a causality preserving order within the synchronizer in which the
  *      party is replicated.
  *
  *   1. The [[com.digitalasset.canton.participant.admin.party.PartyReplicationIndexingWorkflow]]
  *      updates active contract changes as they are passed to the indexer.
  *
  *   1. The indexer updates the active contract changes watermark after indexing.
  *
  *   1. The [[com.digitalasset.canton.participant.admin.party.PartyReplicationIndexingWorkflow]]
  *      deletes active contract changes after party replication has completed.
  *
  *   1. Synchronizer crash recovery deletes contract activation changes that need to be replayed to
  *      the indexer upon synchronizer reconnect.
  */
trait PartyReplicationIndexingStore {
  this: NamedLogging =>

  /** Add imported active contract activations for subsequent indexing.
    *
    * @param watermark
    *   watermark with the timestamp at which the contracts have been activated in the canton
    *   protocol and with a deterministically ordered ACS contract counter. The timestamp is used to
    *   order contract activation changes in a causality order allowing to index activations before
    *   deactivations and to batch indexing into separate
    *   [[com.digitalasset.canton.ledger.participant.state.Update.OnPRReassignmentAccepted]] indexer
    *   events
    * @param contractActivationsToIndex
    *   contract activations
    */
  def addImportedContractActivations(
      watermark: Watermark,
      contractActivationsToIndex: NonEmpty[Seq[ContractReassignment]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Add contract activations and deactivations on behalf of concurrent transactions for subsequent
    * indexing.
    *
    * Ignores the call when indexer pausing is enabled until this OnPR mode is removed.
    *
    * @param timestamp
    *   the request-id sequencing timestamp of the request that activated and deactivated contracts
    * @param changes
    *   ordered activations changes each with contract id, whether the contract is activated or
    *   deactivated, and reassignment counter
    */
  def addContractActivationChanges(
      timestamp: CantonTimestamp,
      changes: Seq[(LfContractId, (ActiveContractStore.ChangeType, ReassignmentCounter))],
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
    * @param maxBatchSize
    *   maximum batch size
    * @return
    *   ordered contract activation changes as well as a unique batch watermark associated with the
    *   returned contract activation changes
    */
  def consumeNextActivationChangesBatch(maxBatchSize: PositiveInt)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[ContractActivationChangeBatch]]

  /** Indexer confirms an
    * [[com.digitalasset.canton.ledger.participant.state.Update.OnPRReassignmentAccepted]] has been
    * indexed
    *
    * @param watermark
    *   watermark of the update that has been indexed
    */
  def markContractActivationChangesAsIndexed(
      watermark: Watermark
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

  /** Purge activation changes after they are no longer needed particularly after party replication
    * has completed.
    */
  def purgeContractActivationChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  /** Synchronizer crash recovery hook to remove partially written activation changes
    *
    * @param deleteStartingAtInclusive
    *   timestamp to delete changes from inclusively, i.e. delete entries with ts equal or larger
    *   than the specified timestamp
    */
  def deleteSince(deleteStartingAtInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  // Helper for unit testing
  protected[store] def listContractActivationChanges()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Seq[ActivationChange]]

  /** Helper to trim a batch to work around constraints wrt what types of changes can be indexed in
    * one batch:
    *
    *   1. The same contract id cannot be activated and deactivated in the same batch.
    *   1. TODO(#32464): Make reassignment counter optional for archives and index archives as
    *      TransactionAccepted events separate from OnPRReassignmentAccepted events.
    */
  protected def trimActivationChangesBatch(
      changesNE: NonEmpty[Seq[ActivationChangeBatchEntry]]
  )(implicit traceContext: TraceContext): NonEmpty[Seq[ActivationChangeBatchEntry]] = {

    @tailrec
    def go(
        changes: Seq[ActivationChangeBatchEntry],
        acc: Seq[ActivationChangeBatchEntry],
        seenCids: Set[LfContractId],
    ): Seq[ActivationChangeBatchEntry] = changes match {
      case Nil => acc.reverse
      case ActivationChangeBatchEntry(_, cid, _, _) +: _ if seenCids.contains(cid) => acc.reverse
      case (head @ ActivationChangeBatchEntry(_, cid, _, _)) +: rest =>
        go(rest, head +: acc, seenCids + cid)
      case _ =>
        checked(ErrorUtil.invalidState("should not get here as Nil and head +: tail are covering"))
    }

    NonEmpty
      .from(go(changesNE, Nil, Set.empty))
      .getOrElse(checked(ErrorUtil.invalidState("can never trim batch to empty")))
  }

  protected def pauseIndexingDuringOnPR: Boolean
}

object PartyReplicationIndexingStore {
  final case class ActivationChange(
      watermark: Watermark,
      contractId: LfContractId,
      change: ChangeType,
      isIndexing: Boolean,
      isIndexed: Boolean,
      reassignmentCounter: ReassignmentCounter,
  )

  /** Subset of activation change fields needed for indexing augmented with an id change ordinal
    * used for indexing progress watermarks.
    */
  final case class ActivationChangeBatchEntry(
      watermark: Watermark,
      contractId: LfContractId,
      change: ChangeType,
      reassignmentCounter: ReassignmentCounter,
  )
  object ActivationChangeBatchEntry {
    implicit val orderingActivationChangeBatchEntry: Ordering[ActivationChangeBatchEntry] =
      Ordering.by[ActivationChangeBatchEntry, Watermark](_.watermark)
  }

  final case class Watermark(timestamp: CantonTimestamp, counter: NonNegativeLong)

  object Watermark {
    implicit val orderingWatermark: Ordering[Watermark] =
      Ordering.by[Watermark, (CantonTimestamp, NonNegativeLong)](watermark =>
        (watermark.timestamp, watermark.counter)
      )

    implicit val getResultWatermark: GetResult[Watermark] = GetResult { r =>
      val ts = r.<<[CantonTimestamp]
      val counter = r.<<[NonNegativeLong]
      Watermark(ts, counter)
    }

    implicit val setParameterWatermark: SetParameter[Watermark] = (watermark, pp) => {
      pp >> watermark.timestamp
      pp >> watermark.counter
    }

    lazy val MinValue = Watermark(CantonTimestamp.MinValue, NonNegativeLong.zero)
  }

  def apply(
      storage: Storage,
      indexedSynchronizer: IndexedSynchronizer,
      pauseIndexingDuringOnPR: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): PartyReplicationIndexingStore =
    storage match {
      case _: MemoryStorage =>
        new InMemoryPartyReplicationIndexingStore(pauseIndexingDuringOnPR, loggerFactory)
      case jdbc: DbStorage =>
        new DbPartyReplicationIndexingStore(
          jdbc,
          indexedSynchronizer,
          pauseIndexingDuringOnPR,
          timeouts,
          loggerFactory,
        )
    }

  /** Holds batch of contract activation changes along with a unique updateId.
    *
    * @param activationChanges
    *   ordered contract activation changes with contract id, change type, and reassignment counter
    * @param onprBatchWatermark
    *   watermark holds an opaque, but unique ordinal useful to the caller to come up with a unique
    *   indexer UpdateId
    */
  final case class ContractActivationChangeBatch(
      activationChanges: NonEmpty[
        Seq[(LfContractId, (ActiveContractStore.ChangeType, ReassignmentCounter))]
      ],
      onprBatchWatermark: Watermark,
  )
}
