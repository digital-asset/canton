// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.admin.party.{
  PartyReplicationIndexingWorkflow,
  PartyReplicationStatus,
  PartyReplicationTestInterceptor,
}
import com.digitalasset.canton.participant.config.AlphaOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.store.{
  AcsReplicationProgress,
  ParticipantNodePersistentState,
  PartyReplicationIndexingStore,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, blocking}

/** The party replication file importer imports a party's active contracts on a specific
  * synchronizer and timestamp previously exported from a source participant.
  *
  * @param partyId
  *   The party that is being replicated.
  * @param requestId
  *   The "add party" request id that this replication is associated with.
  * @param psid
  *   The physical id of the synchronizer to replicate active contracts in.
  * @param partyOnboardingAt
  *   The timestamp immediately on which the ACS snapshot is based.
  * @param replicationProgressState
  *   Interface for processor to read and update ACS replication progress.
  * @param persistsContracts
  *   Interface to persist imported contracts to the ContractStore.
  * @param requestTracker
  *   Canton protocol request tracker used to persist to the ActiveContractStore and update the
  *   in-memory request state in a consistent way.
  * @param indexingStore
  *   Store to insert imported contract activations for subsequent indexing.
  * @param testOnlyInterceptorO
  *   Test interceptor only alters behavior in integration tests.
  */
class PartyReplicationFileImporter(
    partyId: PartyId,
    requestId: AddPartyRequestId,
    protected val psid: PhysicalSynchronizerId,
    partyOnboardingAt: EffectiveTime,
    protected val replicationProgressState: AcsReplicationProgress,
    persistsContracts: TargetParticipantAcsPersistence.PersistsContracts,
    requestTracker: RequestTracker,
    acsReader: Iterator[ActiveContract],
    indexingWorkflow: PartyReplicationIndexingWorkflow,
    indexingStore: PartyReplicationIndexingStore,
    testOnlyInterceptorO: Option[PartyReplicationTestInterceptor],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TargetParticipantAcsPersistence(
      partyId,
      requestId,
      psid,
      partyOnboardingAt,
      replicationProgressState,
      persistsContracts,
      requestTracker,
      indexingStore,
    ) {

  def importEntireAcsSnapshotInOneGo()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val numContractsImported = new AtomicInteger(0)
    for {
      _ <- MonadUtil.sequentialTraverse_(
        acsReader.grouped(TargetParticipantAcsPersistence.contractsToRequestEachTime.unwrap).toSeq
      ) { contracts =>
        awaitTestInterceptor()
        logger.info(s"About to import contracts starting at ordinal ${numContractsImported.get}")
        val contractsNE =
          NonEmpty
            .from(contracts)
            .getOrElse(throw new IllegalStateException("Grouped ACS must be nonempty"))
        importContracts(contractsNE).map(totalNumContractsImported =>
          numContractsImported.set(totalNumContractsImported.unwrap)
        )
      }
      progress <- EitherT.fromEither[FutureUnlessShutdown](
        replicationProgressState
          .getAcsReplicationProgress(requestId)
          .toRight(s"Party replication $requestId is unexpectedly unknown")
      )
      _ <- replicationProgressState.updateAcsReplicationProgress(
        requestId,
        PartyReplicationStatus.EphemeralFileImporterProgress(
          progress.processedContractCount,
          progress.nextPersistenceCounter,
          fullyProcessedAcs = true,
          this,
        ),
      )
      _ <- indexingWorkflow.indexAllContractBatches()
    } yield ()
  }

  private def awaitTestInterceptor()(implicit
      traceContext: TraceContext
  ): Unit = testOnlyInterceptorO.foreach { interceptor =>
    @tailrec
    def go(): Unit =
      replicationProgressState
        .getAcsReplicationProgress(requestId)
        .map(interceptor.onTargetParticipantProgress) match {
        case None => ()
        case Some(PartyReplicationTestInterceptor.Proceed) => ()
        case Some(PartyReplicationTestInterceptor.Wait) =>
          blocking(Threading.sleep(1000))
          go()
      }

    go()
  }

  override protected def newProgress(
      updatedProcessedContractsCount: NonNegativeInt,
      usedRepairCounter: RepairCounter,
  ): PartyReplicationStatus.AcsReplicationProgress =
    PartyReplicationStatus.EphemeralFileImporterProgress(
      updatedProcessedContractsCount,
      usedRepairCounter + 1,
      fullyProcessedAcs = false,
      this,
    )
}

object PartyReplicationFileImporter {
  def apply(
      partyId: PartyId,
      requestId: AddPartyRequestId,
      partyOnboardingAt: EffectiveTime,
      replicationProgressState: AcsReplicationProgress,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      connectedSynchronizer: ConnectedSynchronizer,
      acsReader: Iterator[ActiveContract],
      config: AlphaOnlinePartyReplicationConfig,
      batchingConfig: BatchingConfig,
      testOnlyInterceptorO: Option[PartyReplicationTestInterceptor],
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext) = {
    val partyReplicationIndexingStore =
      connectedSynchronizer.synchronizerHandle.syncPersistentState.partyReplicationIndexingStoreIfOnPREnabled
        .getOrElse(throw new IllegalStateException("Expect store when OnPR enabled"))
    val requestSpecificLoggerFactory = loggerFactory
      .append("psid", connectedSynchronizer.psid.toProtoPrimitive)
      .append("partyId", partyId.toProtoPrimitive)
      .append("requestId", requestId.toHexString)
    new PartyReplicationFileImporter(
      partyId,
      requestId,
      connectedSynchronizer.psid,
      partyOnboardingAt,
      replicationProgressState,
      new TargetParticipantAcsPersistence.PersistsContractsImpl(participantNodePersistentState),
      connectedSynchronizer.ephemeral.requestTracker,
      acsReader,
      new PartyReplicationIndexingWorkflow(
        partyId,
        connectedSynchronizer.psid,
        partyReplicationIndexingStore,
        participantNodePersistentState.map(_.contractStore),
        connectedSynchronizer.ephemeral.recordOrderPublisher,
        connectedSynchronizer.synchronizerHandle.syncPersistentState.pureCryptoApi,
        config.pauseSynchronizerIndexingDuringPartyReplication,
        batchingConfig,
        requestSpecificLoggerFactory,
      ),
      partyReplicationIndexingStore,
      testOnlyInterceptorO,
      requestSpecificLoggerFactory,
    )
  }
}
