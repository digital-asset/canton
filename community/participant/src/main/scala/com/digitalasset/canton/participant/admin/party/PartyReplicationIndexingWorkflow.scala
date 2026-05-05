// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.Eval
import cats.data.EitherT
import cats.implicits.toTraverseOps
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.crypto.{CryptoPureApi, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationIndexingWorkflow.{
  ContractToIndex,
  indexingBatchSize,
}
import com.digitalasset.canton.participant.event.{AcsChangeSupport, RecordOrderPublisher}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.store.ActiveContractStore.ChangeType
import com.digitalasset.canton.participant.store.PartyReplicationIndexingStore.ContractActivationChangeBatch
import com.digitalasset.canton.participant.store.{
  ContractStore,
  PartyReplicationIndexingStore,
  PersistedContractInstance,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId, UpdateId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{
  EitherTUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  ReassignmentTag,
}
import com.digitalasset.canton.{ReassignmentCounter, RepairCounter, checked}
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

// Not sealed for testing
trait GeneratesUniqueUpdateIds {
  def uniqueUpdateId(
      onprBatchCounter: NonNegativeLong,
      batch: Seq[(TimeOfChange, LfContractId, ChangeType, ReassignmentCounter)],
  ): UpdateId
}

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
    * @param params
    *   party replication parameters, e.g. with partyId and synchronizerId
    * @param indexingProgress
    *   indexing progress at the beginning of this call
    * @param connectedSynchronizer
    *   connected synchronizer that the party replication target participant needs for access to the
    *   indexing store, record order publisher, and pure crypto
    * @return
    *   indexing progress at the end of this call
    */
  def indexNextContractActivationChangeBatch(
      params: PartyReplicationStatus.ReplicationParams,
      indexingProgress: PartyReplicationStatus.AcsIndexingProgress,
      indexingStore: PartyReplicationIndexingStore,
      recordOrderPublisher: RecordOrderPublisher,
      pureCrypto: CryptoPureApi,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PartyReplicationStatus.AcsIndexingProgress] = {
    val onprBatchCounter = indexingProgress.nextIndexingCounter

    logger.debug(
      s"Indexing request ${params.requestId} party ${params.partyId} batch $onprBatchCounter"
    )

    val generatesUniqueUpdateIds = new GeneratesUniqueUpdateIds {
      override def uniqueUpdateId(
          onprBatchCounter: NonNegativeLong,
          batch: Seq[(TimeOfChange, LfContractId, ChangeType, ReassignmentCounter)],
      ): UpdateId = {
        // Add the onpr batch counter to the hash to arrive at unique per-OPR updateIds.
        val hash = batch
          .foldLeft {
            pureCrypto
              .build(HashPurpose.OnlinePartyReplicationId)
              .addString(params.partyId.toProtoPrimitive)
              .addLong(onprBatchCounter.unwrap)
          } {
            // TODO(#26468): Use validation packages
            case (builder, (_toc, contractId, _change, reassignmentCounter)) =>
              builder
                .addLong(reassignmentCounter.v)
                .addString(contractId.coid)
          }
          .finish()
        UpdateId(hash)
      }
    }

    for {
      nextBatchO <- EitherT.right[String](
        indexingStore
          .consumeNextActivationChangesBatch(
            params.partyId,
            onprBatchCounter,
            indexingBatchSize,
          )(generatesUniqueUpdateIds)
      )
      numContractsIndexedO <- nextBatchO.traverse {
        case ContractActivationChangeBatch(updateId, contractActivationChanges) =>
          indexContractActivationChangeBatch(
            contractActivationChanges,
            onprBatchCounter,
            updateId,
            params,
            recordOrderPublisher,
            indexingStore,
          )
      }

      updatedProgress = numContractsIndexedO.fold(
        indexingProgress.copy(
          // If we managed to drain all changes to be indexed, remember the change count
          // at which we last drained.
          indexingAlmostDoneWatermarkO = Some(indexingProgress.indexedContractActivationChangeCount)
        )
      )(numContractsIndexed =>
        indexingProgress.copy(
          indexedContractActivationChangeCount =
            indexingProgress.indexedContractActivationChangeCount +
              checked(NonNegativeLong.tryCreate(numContractsIndexed.unwrap.toLong)),
          nextIndexingCounter = onprBatchCounter.increment.toNonNegative,
        )
      )

      // If indexing was paused, unpause indexing, when we first drain the changes to index.
      _ <- EitherTUtil.ifThenET(
        numContractsIndexedO.isEmpty && pauseSynchronizerIndexingDuringPartyReplication
      )(
        EitherT.right[String](
          FutureUnlessShutdown.lift(recordOrderPublisher.publishBufferedEvents())
        )
      )
    } yield updatedProgress
  }

  /** Helper to index a contract batch looking up the contracts to index in the contract store and
    * passing the indexer update event to the record order publisher.
    */
  private def indexContractActivationChangeBatch(
      contractActivationChanges: NonEmpty[Map[LfContractId, (ChangeType, ReassignmentCounter)]],
      onprIndexingCounter: NonNegativeLong,
      updateId: UpdateId,
      params: PartyReplicationStatus.ReplicationParams,
      recordOrderPublisher: RecordOrderPublisher,
      indexingStore: PartyReplicationIndexingStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PositiveInt] =
    for {
      activeContracts <- MonadUtil.parTraverseWithLimit(batchingConfig.parallelism)(
        contractActivationChanges.forgetNE.toSeq
      ) { case (contractId, (_, reassignmentCounter)) =>
        EitherT(
          contractStore.value
            .lookupPersisted(contractId)
            .map(
              _.toRight(s"Unable to look up contract $contractId in contract store")
                .map[ContractToIndex](persistedContract =>
                  (
                    ContractReassignment(
                      persistedContract.asContractInstance,
                      // TODO(#26468): Use validation packages
                      ReassignmentTag
                        .Source(persistedContract.asContractInstance.templateId.packageId),
                      ReassignmentTag
                        .Target(persistedContract.asContractInstance.templateId.packageId),
                      reassignmentCounter,
                    ),
                    persistedContract.internalContractId,
                  )
                )
            )
        )
      }
      activeContractsNE <- EitherT.fromEither[FutureUnlessShutdown](
        NonEmpty
          .from(activeContracts)
          .toRight(
            s"OnPR indexing batch $onprIndexingCounter with update $updateId not expected to be empty"
          )
      )
      _ <- EitherT.right[String](
        FutureUnlessShutdown.lift(
          recordOrderPublisher.schedulePublishAddContracts(
            indexerEventFromActiveContracts(
              onprIndexingCounter,
              updateId,
              activeContractsNE,
              params,
              indexingStore,
            )
          )
        )
      )
    } yield {
      // Returning positive int as activeContractsNE is non-empty
      checked(PositiveInt.tryCreate(activeContractsNE.size))
    }

  /** Determines the indexer event corresponding to the imported active contracts
    * @param onprIndexingCounter
    *   the batch counter used for a unique updateId
    * @param activeContracts
    *   the active contracts to publish
    * @param timestamp
    *   the record time to publish the event at
    * @return
    *   the event to publish
    */
  private def indexerEventFromActiveContracts(
      onprIndexingCounter: NonNegativeLong,
      updateId: UpdateId,
      activeContracts: NonEmpty[Seq[ContractToIndex]],
      params: PartyReplicationStatus.ReplicationParams,
      indexingStore: PartyReplicationIndexingStore,
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Update.OnPRReassignmentAccepted = {
    val contractIdCounters = activeContracts.map {
      // TODO(#26468): Use validation packages
      case (ContractReassignment(contract, _, _, reassignmentCounter), _) =>
        (contract.contractId, reassignmentCounter)
    }

    val artificialReassignmentInfo = ReassignmentInfo(
      sourceSynchronizer = ReassignmentTag.Source(params.synchronizerId),
      targetSynchronizer = ReassignmentTag.Target(params.synchronizerId),
      submitter = None,
      reassignmentId = ReassignmentId(
        ReassignmentTag.Source(params.synchronizerId),
        ReassignmentTag.Target(params.synchronizerId),
        timestamp, // artificial unassign has same timestamp as the assign
        contractIdCounters,
      ),
      isReassigningParticipant = false,
    )
    val commitSet = CommitSet.createForAssignment(
      artificialReassignmentInfo.reassignmentId,
      activeContracts.map { case (reassignment, _) => reassignment },
      artificialReassignmentInfo.sourceSynchronizer,
    )
    val acsChangeFactory = AcsChangeSupport.fromCommitSet(commitSet)
    Update
      .OnPRReassignmentAccepted(
        workflowId = None,
        updateId = updateId,
        reassignmentInfo = artificialReassignmentInfo,
        reassignment = Reassignment.Batch(
          activeContracts.zipWithIndex.map {
            // TODO(#26468): Use validation packages
            case (
                  (ContractReassignment(contract, _, _, reassignmentCounter), internalContractId),
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
          }
        ),
        recordTime = timestamp,
        // TODO(#30678): Replace OnPR repair counter with an indexer-generated OnPR counter
        repairCounter = RepairCounter.apply(onprIndexingCounter.unwrap),
        synchronizerId = params.synchronizerId,
        acsChangeFactory = acsChangeFactory,
      )
      .tap(update =>
        // TODO(#30121): Move indexer confirmation to indexer post-processing
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          indexingStore.markContractActivationChangesAsIndexed(
            update.updateId
          ),
          s"Failed to mark update ${update.updateId} indexed",
          level = Level.WARN,
        )
      )
  }
}

object PartyReplicationIndexingWorkflow {
  type ContractToIndex = (ContractReassignment, Long)

  private lazy val indexingBatchSize = PositiveInt.tryCreate(200)
}
