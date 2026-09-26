// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.ContractReassignment
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.party.AcsTransferContractHandler.AcsTransferCheckpoint
import com.digitalasset.canton.participant.protocol.party.TargetParticipantAcsPersistence.PersistsContracts
import com.digitalasset.canton.participant.store.{
  ParticipantNodePersistentState,
  PartyReplicationIndexingStore,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId}
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.nonempty.NonEmpty
import com.digitalasset.nonempty.NonEmptyColl.*

import scala.concurrent.ExecutionContext

trait AcsTransferContractHandler {

  /** Handles contracts as they arrive in chunks during the party ACS transfer.
    *
    * The handler is deliberately unaware of how the caller tracks online party replication
    * progress: it consumes the checkpoint reached before the chunk and reports the checkpoint
    * reached after it. Recording the returned checkpoint is up to the caller, which alone knows how
    * to represent its own ephemeral progress.
    *
    * @param contracts
    *   the contracts to handle
    * @param checkpoint
    *   the transfer checkpoint reached before this chunk
    * @return
    *   the transfer checkpoint reached after this chunk
    */
  def handleContracts(
      contracts: NonEmpty[Seq[ActiveContract]],
      checkpoint: AcsTransferCheckpoint,
      synchronizerId: SynchronizerId,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, AcsTransferCheckpoint] = for {
    validatedContracts <- EitherT.fromEither[FutureUnlessShutdown](
      ReceivedContractValidation.validateContracts(contracts, synchronizerId)
    )
    checkpointAfter <- handleValidatedContracts(validatedContracts, checkpoint)
  } yield checkpointAfter

  def handleValidatedContracts(
      validatedActivations: NonEmpty[Seq[ContractReassignment]],
      checkpoint: AcsTransferCheckpoint,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AcsTransferCheckpoint]
}

object AcsTransferContractHandler {

  /** How far the target participant ACS transfer has progressed.
    *
    * @param processedContractCount
    *   the total number of contracts transferred thus far
    * @param nextPersistenceCounter
    *   the repair counter with which to persist the next chunk of contracts
    */
  final case class AcsTransferCheckpoint(
      processedContractCount: NonNegativeLong,
      nextPersistenceCounter: RepairCounter,
  )
}

/** Target participant ACS persistence functionality shared between the OnPR sequencer channel
  * target processor and the file-based ACS importer.
  * @param requestId
  *   the online party replication, party add request identifier
  * @param partyOnboardingAt
  *   the effective time of the onboarding PartyToParticipant topology transaction
  * @param persistsContracts
  *   interface to persist a batch of contracts to the contract store
  * @param requestTracker
  *   request tracker to update the active contract store journal along with in-memory state
  * @param indexingStore
  *   indexing store to add imported contract information to for subsequent Ledger API indexing
  */
class TargetParticipantAcsPersistence(
    requestId: AddPartyRequestId,
    psid: PhysicalSynchronizerId,
    partyOnboardingAt: EffectiveTime,
    persistsContracts: PersistsContracts,
    requestTracker: RequestTracker,
    indexingStore: PartyReplicationIndexingStore,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends AcsTransferContractHandler
    with NamedLogging {

  /** Import contracts as part of online party replication performing the following activities.
    *   - persist contracts at a determined time of change updating in-memory request-tracker state
    *     accordingly
    *   - schedule publishing of the corresponding indexer event
    *
    * @param validatedActivations
    *   the validated contracts to import
    * @param checkpoint
    *   the transfer checkpoint reached before this chunk
    * @return
    *   the transfer checkpoint reached after this chunk
    */
  override def handleValidatedContracts(
      validatedActivations: NonEmpty[Seq[ContractReassignment]],
      checkpoint: AcsTransferCheckpoint,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, AcsTransferCheckpoint] =
    for {
      _ <- persistsContracts
        .persistContracts(validatedActivations.map(_.contract))
        .leftMap(err => s"Failed to persist contracts: $err")
      repairCounter = checkpoint.nextPersistenceCounter
      toc = TimeOfChange(partyOnboardingAt.value, Some(repairCounter))
      replicatedContracts = validatedActivations.map {
        // TODO(#26468): Use validation packages
        case ContractReassignment(contract, _, _, reassignmentCounter) =>
          (
            contract.contractId,
            ReassignmentTag.Source(psid.logical),
            reassignmentCounter,
            toc,
          )
      }
      _ <- requestTracker
        .addReplicatedContracts(requestId, partyOnboardingAt.value, replicatedContracts)
        .leftMap(e => s"Failed to add contracts $replicatedContracts to ActiveContractStore: $e")

      indexingWatermark = PartyReplicationIndexingStore.Watermark(
        toc.timestamp,
        // Indexing tracks activations at the contract-level rather than per batch to allow indexing
        // in different batches from the batches used for import.
        checkpoint.processedContractCount,
      )

      _ <- EitherT.right[String](
        indexingStore.addImportedContractActivations(
          indexingWatermark,
          validatedActivations,
        )
      )
    } yield AcsTransferCheckpoint(
      checkpoint.processedContractCount + NonNegativeLong.size(validatedActivations),
      repairCounter + 1,
    )
}

object TargetParticipantAcsPersistence {

  // TODO(#22251): Make this configurable.
  private[party] val contractsToRequestEachTime = PositiveInt.tryCreate(10)

  // not sealed for testing
  trait PersistsContracts {

    /** Persist the contracts in the contract store.
      */
    def persistContracts(
        contracts: NonEmpty[Seq[ContractInstance]]
    )(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Map[LfContractId, Long]]
  }

  final class PersistsContractsImpl(
      participantNodePersistentState: Eval[ParticipantNodePersistentState]
  ) extends PersistsContracts {
    override def persistContracts(contracts: NonEmpty[Seq[ContractInstance]])(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Map[LfContractId, Long]] = EitherT.right[String](
      participantNodePersistentState.value.contractStore.storeContracts(contracts)
    )
  }
}
