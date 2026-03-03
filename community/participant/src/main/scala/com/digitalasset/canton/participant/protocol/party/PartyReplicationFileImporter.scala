// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.concurrent.Threading
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
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, blocking}

class PartyReplicationFileImporter(
    requestId: AddPartyRequestId,
    protected val psid: PhysicalSynchronizerId,
    partyOnboardingAt: EffectiveTime,
    protected val replicationProgressState: AcsReplicationProgress,
    persistsContracts: TargetParticipantAcsPersistence.PersistsContracts,
    requestTracker: RequestTracker,
    acsReader: Iterator[ActiveContract],
    indexingWorkflow: PartyReplicationIndexingWorkflow,
    testOnlyInterceptorO: Option[PartyReplicationTestInterceptor],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TargetParticipantAcsPersistence(
      requestId,
      psid,
      partyOnboardingAt,
      replicationProgressState,
      persistsContracts,
      requestTracker,
    ) {

  def importEntireAcsSnapshotInOneGo()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val numContractsImported = new AtomicInteger(0)
    for {
      contractsToIndex <- MonadUtil.sequentialTraverse(
        acsReader.grouped(TargetParticipantAcsPersistence.contractsToRequestEachTime.unwrap).toSeq
      ) { contracts =>
        awaitTestInterceptor()
        logger.info(s"About to import contracts starting at ordinal ${numContractsImported.get}")
        val contractsNE =
          NonEmpty
            .from(contracts)
            .getOrElse(throw new IllegalStateException("Grouped ACS must be nonempty"))
        importContracts(contractsNE).map { case (totalNumContractsImported, contractsToIndex) =>
          numContractsImported.set(totalNumContractsImported.unwrap)
          contractsToIndex
        }
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
      _ <- EitherT.right[String](
        FutureUnlessShutdown.lift(indexingWorkflow.indexAllContractBatches(contractsToIndex))
      )
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
      testOnlyInterceptorO: Option[PartyReplicationTestInterceptor],
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext) = {
    val requestSpecificLoggerFactory = loggerFactory
      .append("psid", connectedSynchronizer.psid.toProtoPrimitive)
      .append("partyId", partyId.toProtoPrimitive)
      .append("requestId", requestId.toHexString)
    new PartyReplicationFileImporter(
      requestId,
      connectedSynchronizer.psid,
      partyOnboardingAt,
      replicationProgressState,
      new TargetParticipantAcsPersistence.PersistsContractsImpl(participantNodePersistentState),
      connectedSynchronizer.ephemeral.requestTracker,
      acsReader,
      new PartyReplicationIndexingWorkflow(
        requestId,
        connectedSynchronizer.psid,
        connectedSynchronizer.ephemeral.recordOrderPublisher,
        connectedSynchronizer.synchronizerHandle.syncPersistentState.pureCryptoApi,
        config.pauseSynchronizerIndexingDuringPartyReplication,
        requestSpecificLoggerFactory,
      ),
      testOnlyInterceptorO,
      requestSpecificLoggerFactory,
    )
  }
}
