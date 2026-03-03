// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.{CryptoPureApi, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationIndexingWorkflow.ContractToIndex
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.event.{AcsChangeSupport, RecordOrderPublisher}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.protocol.{ReassignmentId, UpdateId}
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{MonadUtil, ReassignmentTag}

/** Target participant ACS indexing functionality shared between the OnPR sequencer channel target
  * processor and the file-based ACS importer.
  *
  * @param requestId
  *   the online party replication, party add request identifier
  * @param psid
  *   physical synchronizer id
  * @param recordOrderPublisher
  *   record order publisher for publishing indexer events
  * @param pureCrypto
  *   used to compute unique indexer event update ids
  * @param pauseSynchronizerIndexingDuringPartyReplication
  *   whether to pause indexing during party replication (deprecated mode)
  */
class PartyReplicationIndexingWorkflow(
    requestId: AddPartyRequestId,
    psid: PhysicalSynchronizerId,
    recordOrderPublisher: RecordOrderPublisher,
    pureCrypto: CryptoPureApi,
    pauseSynchronizerIndexingDuringPartyReplication: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  /** Pass all the imported ACS contract batches to the indexer. Unpause indexing if previously
    * paused.
    */
  def indexAllContractBatches(
      allBatches: Seq[NonEmpty[Seq[ContractToIndex]]]
  )(implicit traceContext: TraceContext): UnlessShutdown[Unit] = for {
    _ <- MonadUtil.sequentialTraverse_(allBatches.zipWithIndex) { case (batch, index) =>
      // Artificially space out indexing of the ACS to limit indexing impact on concurrent
      // Daml transactions, until indexing with back-off is driven by the PartyReplicator.
      // Do so only when not pausing the indexer to avoid slowing down existing tests and
      // because when pausing the indexer, the indexer is fully dedicated to OnPR during
      // ACS import on the specific synchronizer.
      if (!pauseSynchronizerIndexingDuringPartyReplication) {
        Threading.sleep(200)
      }
      indexContractBatch(batch, NonNegativeLong.tryCreate(index.toLong))
    }
    _ <-
      if (pauseSynchronizerIndexingDuringPartyReplication)
        recordOrderPublisher.publishBufferedEvents()
      else UnlessShutdown.unit
  } yield ()

  /* Helper to index a contract batch if the ROP timestamp has passed beyond the
   * specified `minTsExclusive`.
   * Returns the new minimum ts to enforce the next event or the unchanged minimum ts
   * if not indexing has been performed.
   */
  private def indexContractBatch(
      contractBatch: Seq[ContractToIndex],
      onprIndexingCounter: NonNegativeLong,
  )(implicit
      traceContext: TraceContext
  ): UnlessShutdown[Unit] =
    NonEmpty.from(contractBatch) match {
      case Some(acsToIndex) =>
        recordOrderPublisher.schedulePublishAddContracts(
          indexerEventFromActiveContracts(
            onprIndexingCounter = onprIndexingCounter,
            activeContracts = acsToIndex,
          )
        )
      case None =>
        UnlessShutdown.unit
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
      activeContracts: NonEmpty[Seq[ContractToIndex]],
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Update.OnPRReassignmentAccepted = {
    val uniqueUpdateId = {
      // Add the repairCounter to the hash to arrive at unique per-OPR updateIds.
      val hash = activeContracts
        .foldLeft {
          pureCrypto
            .build(HashPurpose.OnlinePartyReplicationId)
            .addByteString(requestId.unwrap)
            .addLong(onprIndexingCounter.unwrap)
        } {
          // TODO(#26468): Use validation packages
          case (builder, (ContractReassignment(contract, _, _, reassignmentCounter), _)) =>
            builder
              .addLong(reassignmentCounter.v)
              .addString(contract.contractId.coid)
        }
        .finish()
      UpdateId(hash)
    }

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
    Update.OnPRReassignmentAccepted(
      workflowId = None,
      updateId = uniqueUpdateId,
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
  }
}

object PartyReplicationIndexingWorkflow {
  type ContractToIndex = (ContractReassignment, Long)
}
