// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.{Eval, Monad}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.Threading
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
import com.digitalasset.canton.participant.store.{ContractStore, PartyReplicationIndexingStore}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId, UpdateId}
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{
  EitherTUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  ReassignmentTag,
}
import com.digitalasset.canton.{ReassignmentCounter, RepairCounter}
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
  * @param partyId
  *   The party that is being replicated.
  * @param psid
  *   physical synchronizer id
  * @param indexingStore
  *   indexing store with contract activation changes for subsequent Ledger API indexing.
  * @param recordOrderPublisher
  *   record order publisher for publishing indexer events
  * @param pureCrypto
  *   used to compute unique indexer event update ids
  * @param pauseSynchronizerIndexingDuringPartyReplication
  *   whether to pause indexing during party replication (deprecated mode)
  */
class PartyReplicationIndexingWorkflow(
    partyId: PartyId,
    psid: PhysicalSynchronizerId,
    indexingStore: PartyReplicationIndexingStore,
    contractStore: Eval[ContractStore],
    recordOrderPublisher: RecordOrderPublisher,
    protected val pureCrypto: CryptoPureApi,
    pauseSynchronizerIndexingDuringPartyReplication: Boolean,
    batchingConfig: BatchingConfig,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends GeneratesUniqueUpdateIds
    with NamedLogging {

  /** Pass all the imported ACS contract batches from the indexing store to the indexer. Unpause
    * indexing if previously paused.
    */
  def indexAllContractBatches()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val initialOnPRBatchCounter = NonNegativeLong.zero

    for {
      _ <- EitherT(Monad[FutureUnlessShutdown].tailRecM(initialOnPRBatchCounter) { nextCtr =>
        (for {
          nextBatchO <- EitherT.right[String](
            indexingStore
              .consumeNextActivationChangesBatch(
                partyId,
                nextCtr,
                indexingBatchSize,
              )(this)
          )

          _ <- nextBatchO.fold(EitherTUtil.unitUS[String]) {
            case ContractActivationChangeBatch(updateId, contractActivationChanges) =>
              indexContractBatch(contractActivationChanges, nextCtr, updateId)
          }
        } yield Either.cond(nextBatchO.isEmpty, (), nextCtr.increment.toNonNegative))
          .fold[Either[NonNegativeLong, Either[String, Unit]]](
            err => Right(Left(err)),
            _.map(Right[String, Unit]),
          )
      })

      _ <-
        if (pauseSynchronizerIndexingDuringPartyReplication)
          EitherT.right[String](
            FutureUnlessShutdown.lift(recordOrderPublisher.publishBufferedEvents())
          )
        else EitherTUtil.unitUS[String]

      // Delete the items from the party replication indexing store since all contract
      // activation changes have been indexed.
      _ <- EitherT.right[String](indexingStore.purgeContractActivationChanges(partyId))
    } yield ()
  }

  /** Helper to index a contract batch looking up the contracts to index in the contract store and
    * passing the indexer update event to the record order publisher.
    */
  private def indexContractBatch(
      contractActivationChanges: NonEmpty[Map[LfContractId, (ChangeType, ReassignmentCounter)]],
      onprIndexingCounter: NonNegativeLong,
      updateId: UpdateId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    // Artificially space out indexing of the ACS to limit indexing impact on concurrent
    // Daml transactions, until indexing with back-off is driven by the PartyReplicator.
    // Do so only when not pausing the indexer to avoid slowing down existing tests and
    // because when pausing the indexer, the indexer is fully dedicated to OnPR during
    // ACS import on the specific synchronizer.
    if (!pauseSynchronizerIndexingDuringPartyReplication) {
      Threading.sleep(200)
    }

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
            )
          )
        )
      )
    } yield ()
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
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Update.OnPRReassignmentAccepted = {
    val contractIdCounters = activeContracts.map {
      // TODO(#26468): Use validation packages
      case (ContractReassignment(contract, _, _, reassignmentCounter), _) =>
        (contract.contractId, reassignmentCounter)
    }

    val artificialReassignmentInfo = ReassignmentInfo(
      sourceSynchronizer = ReassignmentTag.Source(psid.logical),
      targetSynchronizer = ReassignmentTag.Target(psid.logical),
      submitter = None,
      reassignmentId = ReassignmentId(
        ReassignmentTag.Source(psid.logical),
        ReassignmentTag.Target(psid.logical),
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
                ledgerEffectiveTime = contract.inst.createdAt.time,
                createNode = contract.toLf,
                contractAuthenticationData = contract.inst.authenticationData,
                reassignmentCounter = reassignmentCounter.v,
                nodeId = idx,
                internalContractId = internalContractId,
              )
          }
        ),
        recordTime = timestamp,
        // TODO(#30678): Replace OnPR repair counter with an indexer-generated OnPR counter
        repairCounter = RepairCounter.apply(onprIndexingCounter.unwrap),
        synchronizerId = psid.logical,
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

  override def uniqueUpdateId(
      onprBatchCounter: NonNegativeLong,
      batch: Seq[(TimeOfChange, LfContractId, ChangeType, ReassignmentCounter)],
  ): UpdateId = {
    // Add the onpr batch counter to the hash to arrive at unique per-OPR updateIds.
    val hash = batch
      .foldLeft {
        pureCrypto
          .build(HashPurpose.OnlinePartyReplicationId)
          .addString(partyId.toProtoPrimitive)
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

object PartyReplicationIndexingWorkflow {
  type ContractToIndex = (ContractReassignment, Long)

  private lazy val indexingBatchSize = PositiveInt.tryCreate(20)
}
