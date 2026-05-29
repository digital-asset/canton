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
  ContractActivation,
  ContractActivationChange,
  ContractDeactivation,
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

    for {
      nextBatchO <- EitherT.right[String](
        indexingStore.consumeNextActivationChangesBatch(indexingBatchSize)
      )
      numContractsIndexedO <- nextBatchO.traverse {
        case ContractActivationChangeBatch(contractActivationChanges, batchWatermark) =>
          // Add the onpr batch counter to the hash to arrive at unique per-OPR updateIds.
          val hash = contractActivationChanges
            .foldLeft {
              pureCrypto
                .build(HashPurpose.OnlinePartyReplicationId)
                .addByteString(params.requestId.unwrap)
                .addLong(batchWatermark.timestamp.toMicros)
                .addLong(batchWatermark.counter.unwrap)
            } {
              // TODO(#26468): Use validation packages
              case (builder, (contractId, (_change, reassignmentCounter))) =>
                builder
                  .addLong(reassignmentCounter.v)
                  .addString(contractId.coid)
            }
            .finish()

          val updateId = UpdateId(hash)
          indexContractActivationChangeBatch(
            contractActivationChanges,
            batchWatermark.counter,
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
      )(numChangesIndexed =>
        indexingProgress.copy(
          indexedContractActivationChangeCount =
            indexingProgress.indexedContractActivationChangeCount +
              checked(NonNegativeLong.tryCreate(numChangesIndexed.unwrap.toLong)),
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
      contractActivationChanges: NonEmpty[Seq[(LfContractId, (ChangeType, ReassignmentCounter))]],
      onprIndexingCounter: NonNegativeLong,
      updateId: UpdateId,
      params: PartyReplicationStatus.ReplicationParams,
      recordOrderPublisher: RecordOrderPublisher,
      indexingStore: PartyReplicationIndexingStore,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PositiveInt] =
    for {
      activationChanges <- MonadUtil.parTraverseWithLimit(batchingConfig.parallelism)(
        contractActivationChanges.forgetNE
      ) { case (contractId, (change, reassignmentCounter)) =>
        EitherT(
          contractStore.value
            .lookupPersisted(contractId)
            .map(
              _.toRight(s"Unable to look up contract $contractId in contract store")
                .map[ContractActivationChange] { persistedContract =>
                  val reassignment = ContractReassignment(
                    persistedContract.asContractInstance,
                    // TODO(#26468): Use validation packages
                    ReassignmentTag
                      .Source(persistedContract.asContractInstance.templateId.packageId),
                    ReassignmentTag
                      .Target(persistedContract.asContractInstance.templateId.packageId),
                    reassignmentCounter,
                  )

                  change match {
                    case ChangeType.Activation =>
                      ContractActivation(reassignment, persistedContract.internalContractId)
                    case ChangeType.Deactivation => ContractDeactivation(reassignment)
                  }
                }
            )
        )
      }
      activationChangesNE <- EitherT.fromEither[FutureUnlessShutdown](
        NonEmpty
          .from(activationChanges)
          .toRight(
            s"OnPR indexing batch $onprIndexingCounter with update $updateId not expected to be empty"
          )
      )
      _ <- EitherT.right[String](
        FutureUnlessShutdown.lift(
          recordOrderPublisher.schedulePublishAddContracts(
            indexerEventFromActivationChanges(
              onprIndexingCounter,
              updateId,
              activationChangesNE,
              params,
              indexingStore,
            )
          )
        )
      )
    } yield {
      // Returning positive int as activationChangesNE is non-empty
      checked(PositiveInt.tryCreate(activationChangesNE.size))
    }

  /** Determines the indexer event corresponding to the contract activation changes
    * @param onprIndexingCounter
    *   the batch counter used for a unique updateId
    * @param activationChanges
    *   the contract activation changes (activations and deactivations) to publish
    * @param timestamp
    *   the record time to publish the event at
    * @return
    *   the event to publish
    */
  private def indexerEventFromActivationChanges(
      onprIndexingCounter: NonNegativeLong,
      updateId: UpdateId,
      activationChanges: NonEmpty[Seq[ContractActivationChange]],
      params: PartyReplicationStatus.ReplicationParams,
      indexingStore: PartyReplicationIndexingStore,
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Update.OnPRReassignmentAccepted = {
    val contractIdCounters = activationChanges.map(_.contract match {
      // TODO(#26468): Use validation packages
      case ContractReassignment(contract, _, _, reassignmentCounter) =>
        (contract.contractId, reassignmentCounter)
    })

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
    val commitSet = NonEmpty
      .from(activationChanges.collect[ContractReassignment] {
        case ContractActivation(reassignment, _) => reassignment
      })
      .fold(CommitSet.empty)(reassignmentsNE =>
        CommitSet.createForAssignment(
          artificialReassignmentInfo.reassignmentId,
          reassignmentsNE,
          artificialReassignmentInfo.sourceSynchronizer,
        )
      )
      .copy(unassignments =
        activationChanges
          .collect { case ContractDeactivation(reassignment) => reassignment }
          .map(reassignment =>
            reassignment.contract.contractId -> CommitSet.UnassignmentCommit(
              targetSynchronizerId = ReassignmentTag.Target(params.synchronizerId),
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
            PartyReplicationIndexingStore.Watermark(timestamp, onprIndexingCounter)
          ),
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
