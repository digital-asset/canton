// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.implicits.toTraverseOps
import cats.{Eval, Monad}
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{CryptoPureApi, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.ledger.participant.state.{
  IndexingWatermark,
  Reassignment,
  ReassignmentInfo,
  SynchronizerUpdate,
  Update,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationIndexingWorkflow.{
  ContractActivation,
  ContractActivationChange,
  ContractDeactivation,
  indexingBatchSize,
}
import com.digitalasset.canton.participant.event.{AcsChangeSupport, RecordOrderPublisher}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.Watermark
import com.digitalasset.canton.participant.store.{
  ContractStore,
  PartyReplicationIndexingStore,
  PersistedContractInstance,
}
import com.digitalasset.canton.protocol.{ReassignmentId, UpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{
  ErrorUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  ReassignmentTag,
}
import com.digitalasset.canton.{LfPartyId, checked}
import com.digitalasset.nonempty.NonEmpty
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

/** Target participant ACS indexing functionality shared between the OnPR sequencer channel target
  * processor and the file-based ACS importer.
  *
  * @param pauseSynchronizerIndexingDuringPartyReplication
  *   whether to pause indexing during party replication (deprecated mode)
  */
class PartyReplicationIndexingWorkflow(
    contractStore: Eval[ContractStore],
    pauseSynchronizerIndexingDuringPartyReplication: Boolean,
    batchingConfig: BatchingConfig,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  /** Pass the next batch of contract activation changes available in the indexing store to the
    * indexer. Unpause indexing if previously paused.
    *
    * @param partyId
    *   The ID of the party being replicated and whose events are being indexed.
    * @param synchronizerId
    *   The ID of the synchronizer where the party is being replicated.
    * @param indexingProgress
    *   The indexing progress at the beginning of this call.
    * @return
    *   The indexing progress at the end of this call.
    */
  def indexNextContractActivationChangeBatch(
      partyId: LfPartyId,
      synchronizerId: SynchronizerId,
      indexingProgress: PartyReplicationStatus.AcsIndexingProgress,
      indexingStore: PartyReplicationIndexingStore,
      recordOrderPublisher: RecordOrderPublisher,
      pureCrypto: CryptoPureApi,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[PartyReplicationStatus.AcsIndexingProgress] = {
    val onprBatchCounter = indexingProgress.nextIndexingCounter

    logger.debug(s"Indexing party $partyId batch $onprBatchCounter")

    for {
      nextBatchO <- consumeNextActivationChangesBatch(indexingStore, indexingBatchSize)
      numContractsIndexedO <- nextBatchO.traverse {
        case (contractActivationChanges, indexingWatermark) =>
          indexContractActivationChangeBatch(
            contractActivationChanges,
            indexingWatermark,
            partyId,
            synchronizerId,
            recordOrderPublisher,
            indexingStore,
            pureCrypto,
          )
      }

      updatedProgress = numContractsIndexedO.fold(
        indexingProgress.copy(
          // If we managed to drain all changes to be indexed, remember the change count
          // at which we last drained.
          indexingAlmostDoneWatermarkO = Some(indexingProgress.indexedContractActivationChangeCount)
        )
      )(numChangesIndexed =>
        indexingProgress.copy(
          indexedContractActivationChangeCount =
            indexingProgress.indexedContractActivationChangeCount +
              checked(NonNegativeLong.tryCreate(numChangesIndexed.unwrap.toLong)),
          nextIndexingCounter = onprBatchCounter.increment.toNonNegative,
        )
      )

      // If indexing was paused, unpause indexing, when we first drain the changes to index.
      _ <-
        if (numContractsIndexedO.isEmpty && pauseSynchronizerIndexingDuringPartyReplication)
          FutureUnlessShutdown.lift(recordOrderPublisher.publishBufferedEvents())
        else FutureUnlessShutdown.unit

    } yield updatedProgress
  }

  /** Helper to turn the output of the indexing store into a format suitable for OnPR indexing.
    * Shared between regular "catch-up" indexing and "flush" indexing to finish once the onboarding
    * flag is cleared.
    */
  private def consumeNextActivationChangesBatch(
      indexingStore: PartyReplicationIndexingStore,
      batchSize: PositiveInt,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[(NonEmpty[Seq[ContractActivationChange]], IndexingWatermark)]] =
    for {
      nextBatchO <- indexingStore.consumeNextActivationChangesBatch(batchSize)
      nextActivationChangesBatchO <- nextBatchO.traverse(nextBatch =>
        MonadUtil
          .parTraverseWithLimit(batchingConfig.parallelism)(
            nextBatch.activationChanges.forgetNE
          ) { case (contractId, (change, reassignmentCounter)) =>
            // TODO(#34683): Once the indexing store has the contract instance and internal id,
            //  remove this lookup that may fail due to indexer-initiated contract store pruning.
            contractStore.value
              .lookupPersisted(contractId)
              .map[ContractActivationChange] { contractO =>
                val persistedContract = contractO.getOrElse(
                  // We cannot gracefully handle a missing contract here. Silently dropping it
                  // deadlocks the downstream pipeline, which requires this data to advance the watermark.
                  // Failing fast is required until the split-store architecture has been eliminated by #34683.
                  ErrorUtil.invalidState(
                    s"Contract $contractId not in contract store (possible pruning race)"
                  )
                )
                val contractInst = persistedContract.asContractInstance
                val packageId = contractInst.templateId.packageId

                val reassignment = ContractReassignment(
                  contractInst,
                  // TODO(#26468): Use validation packages
                  ReassignmentTag.Source(packageId),
                  ReassignmentTag.Target(packageId),
                  reassignmentCounter,
                )

                change match {
                  case ChangeType.Activation =>
                    ContractActivation(reassignment, persistedContract.internalContractId)
                  case ChangeType.Deactivation => ContractDeactivation(reassignment)
                }
              }
          }
          .map { contracts =>
            // Invariant check: parTraverseWithLimit is a structure-preserving functor mapping.
            // Because nextBatch.activationChanges is statically NonEmpty, `contracts` must be NonEmpty.
            val contractsNE =
              NonEmpty
                .from(contracts)
                .getOrElse(
                  ErrorUtil.invalidState(
                    "Invariant violation: parTraverseWithLimit broke collection size preservation"
                  )
                )
            (
              contractsNE,
              nextBatch.onprBatchWatermark.toIndexingWatermark(nextBatch.acsCommitmentTiebreaker),
            )
          }
      )
    } yield nextActivationChangesBatchO

  /** Helper to index a contract batch looking up the contracts to index in the contract store and
    * passing the indexer update event to the record order publisher.
    */
  private def indexContractActivationChangeBatch(
      contractActivationChangesNE: NonEmpty[Seq[ContractActivationChange]],
      watermarkToIndexUpTo: IndexingWatermark,
      partyId: LfPartyId,
      synchronizerId: SynchronizerId,
      recordOrderPublisher: RecordOrderPublisher,
      indexingStore: PartyReplicationIndexingStore,
      pureCrypto: CryptoPureApi,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[PositiveInt] =
    FutureUnlessShutdown
      .lift(
        recordOrderPublisher.schedulePublishAddContracts(
          indexerEventFromActivationChanges(
            watermarkToIndexUpTo,
            contractActivationChangesNE,
            partyId,
            synchronizerId,
            indexingStore,
            pureCrypto,
          )
        )
      )
      .map(_ => // Returning positive int as activationChangesNE is non-empty
        checked(PositiveInt.tryCreate(contractActivationChangesNE.size))
      )

  def flushContractActivationChangesToIndexer(
      partyIds: NonEmpty[Set[LfPartyId]],
      synchronizerId: SynchronizerId,
      publishAt: EffectiveTime,
      indexingStore: PartyReplicationIndexingStore,
      pureCrypto: CryptoPureApi,
  )(publishUpdate: SynchronizerUpdate => FutureUnlessShutdown[Unit])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    MonadUtil
      .sequentialTraverse_(partyIds) { partyId =>
        // Retry until indexing store drained
        Monad[FutureUnlessShutdown].tailRecM[Unit, Unit](()) { _ =>
          consumeNextActivationChangesBatch(indexingStore, indexingBatchSize).flatMap {
            case Some((contractActivationChanges, watermark)) =>
              val indexerEvent = indexerEventFromActivationChanges(
                watermark,
                contractActivationChanges,
                partyId,
                synchronizerId,
                indexingStore,
                pureCrypto,
              )(publishAt.value)
              publishUpdate(indexerEvent).map {
                if (logger.underlying.isDebugEnabled())
                  logger.debug(s"Offered OnPR update batch to indexer $contractActivationChanges")
                Left(_)
              }
            case None =>
              FutureUnlessShutdown.pure(Right(())) // Drained, terminate loop
          }
        }
      }
      .flatMap(_ =>
        // Purge contract activations after there is no more batch, i.e. the flush is done.
        // TODO(#30121): With TP-crash recovery, we can only purge once the indexer has
        //  confirmed that the indexer has consumed all activation changes in case
        //  if a crash.
        indexingStore.purgeContractActivationChanges()
      )

  /** Determines the indexer event corresponding to the contract activation changes
    * @param watermark
    *   The watermark used to track indexing progress.
    * @param activationChanges
    *   The contract activation changes (activations and deactivations) to publish.
    * @param timestamp
    *   The record time to publish the event at.
    * @return
    *   The event to publish.
    */
  private def indexerEventFromActivationChanges(
      watermark: IndexingWatermark,
      activationChanges: NonEmpty[Seq[ContractActivationChange]],
      partyId: LfPartyId,
      synchronizerId: SynchronizerId,
      indexingStore: PartyReplicationIndexingStore,
      pureCrypto: CryptoPureApi,
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Update.OnPRReassignmentAccepted = {
    // Add the watermark fields to the hash to arrive at unique per-OnPR updateIds.
    val updateId = UpdateId(
      activationChanges
        .foldLeft {
          pureCrypto
            .build(HashPurpose.OnlinePartyReplicationId)
            .addString(partyId)
            .addLong(watermark.timestamp.toMicros)
            .addLong(watermark.counter.unwrap)
        } { case (builder, change) =>
          builder
            .addLong(change.contract.counter.v)
            .addString(change.contract.contract.inst.contractId.coid)
        }
        .finish()
    )

    val contractIdCounters = activationChanges.map(_.contract match {
      // TODO(#26468): Use validation packages
      case ContractReassignment(contract, _, _, reassignmentCounter) =>
        (contract.contractId, reassignmentCounter)
    })

    val artificialReassignmentInfo = ReassignmentInfo(
      sourceSynchronizer = ReassignmentTag.Source(synchronizerId),
      targetSynchronizer = ReassignmentTag.Target(synchronizerId),
      submitter = None,
      reassignmentId = ReassignmentId(
        ReassignmentTag.Source(synchronizerId),
        ReassignmentTag.Target(synchronizerId),
        timestamp, // artificial unassign has same timestamp as the assign
        contractIdCounters,
      ),
      isReassigningParticipant = false,
    )
    val commitSet = NonEmpty
      .from(activationChanges.collect[ContractReassignment] {
        case ContractActivation(reassignment, _) => reassignment
      })
      .fold(CommitSet.empty)(reassignmentsNE =>
        CommitSet.createForAssignment(
          artificialReassignmentInfo.reassignmentId,
          reassignmentsNE,
          artificialReassignmentInfo.sourceSynchronizer,
          completeReassignmentInStore = false,
        )
      )
      .copy(unassignments =
        activationChanges
          .collect { case ContractDeactivation(reassignment) => reassignment }
          .map(reassignment =>
            reassignment.contract.contractId -> CommitSet.UnassignmentCommit(
              targetSynchronizerId = ReassignmentTag.Target(synchronizerId),
              stakeholders = reassignment.contract.metadata.stakeholders,
              reassignmentCounter = reassignment.counter,
            )
          )
          .toMap
      )
    val acsChangeFactory = AcsChangeSupport.fromCommitSet(commitSet)
    Update
      .OnPRReassignmentAccepted(
        workflowId = None,
        updateId = updateId,
        reassignmentInfo = artificialReassignmentInfo,
        reassignment = Reassignment.Batch(
          activationChanges.zipWithIndex.map[Reassignment] {
            // TODO(#26468): Use validation packages
            case (
                  ContractActivation(
                    ContractReassignment(contract, _, _, reassignmentCounter),
                    internalContractId,
                  ),
                  idx,
                ) =>
              Reassignment.Assign(
                reassignmentCounter = reassignmentCounter.v,
                nodeId = idx,
                persistedContractInstance = PersistedContractInstance(
                  internalContractId = internalContractId,
                  inst = contract.inst,
                ),
              )
            case (
                  ContractDeactivation(ContractReassignment(contract, _, _, reassignmentCounter)),
                  idx,
                ) =>
              Reassignment.Unassign(
                contractId = contract.contractId,
                templateId = contract.templateId,
                packageName = contract.inst.packageName,
                stakeholders = contract.metadata.stakeholders,
                assignmentExclusivity = None,
                reassignmentCounter = reassignmentCounter.v,
                nodeId = idx,
                keyOpt = contract.contractKeyWithMaintainers,
              )
          }
        ),
        recordTime = timestamp,
        watermark = watermark,
        synchronizerId = synchronizerId,
        acsChangeFactory = acsChangeFactory,
      )
      .tap(update =>
        // TODO(#30121): Move indexer confirmation to indexer post-processing
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          indexingStore.markContractActivationChangesAsIndexed(Watermark.fromIndexing(watermark)),
          s"Failed to mark update ${update.updateId} indexed",
          level = Level.WARN,
        )
      )
  }
}

object PartyReplicationIndexingWorkflow {
  private sealed trait ContractActivationChange {
    def contract: ContractReassignment
  }
  private final case class ContractActivation(
      contract: ContractReassignment,
      internalContractId: Long,
  ) extends ContractActivationChange
  private final case class ContractDeactivation(contract: ContractReassignment)
      extends ContractActivationChange

  private lazy val indexingBatchSize = PositiveInt.tryCreate(200)
}
