// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.ledger.participant.state.SynchronizerUpdate
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.AlphaOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.nonempty.NonEmpty

import scala.concurrent.ExecutionContext

/** Exposes party replication related activities in response to authorized topology transactions
  * particularly when the PartyToParticipant onboarding flag is set or cleared.
  */
final class PartyReplicationTriggers(
    syncService: CantonSyncService,
    config: AlphaOnlinePartyReplicationConfig,
    batchingConfig: BatchingConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends FlagCloseable
    with NamedLogging {
  private[participant] val indexingWorkflow =
    new PartyReplicationIndexingWorkflow(
      syncService.participantNodePersistentState.map(_.contractStore),
      config.pauseSynchronizerIndexingDuringPartyReplication,
      batchingConfig,
      loggerFactory,
    )

  /** Flush OnPR events to the indexer when the PTP.onboarding flag is cleared to signify that a
    * previously onboarding party is now fully hosted on the participant.
    */
  private[participant] def flushContractActivationChangesToIndexer(
      partyIds: NonEmpty[Set[LfPartyId]],
      synchronizerId: SynchronizerId,
      publishAt: EffectiveTime,
  )(publishUpdate: SynchronizerUpdate => FutureUnlessShutdown[Unit])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val connectedSynchronizer =
      syncService
        .readyConnectedSynchronizerById(synchronizerId)
        .getOrElse(
          ErrorUtil.invalidState(
            s"Synchronizer $synchronizerId not connected while flushing ${partyIds.mkString(", ")} to indexer"
          )
        )

    val indexingStore =
      connectedSynchronizer.synchronizerHandle.syncPersistentState.partyReplicationIndexingStoreIfOnPREnabled
        .getOrElse(
          ErrorUtil.invalidState(
            s"Synchronizer $synchronizerId not connected while flushing ${partyIds.mkString(", ")} to indexer"
          )
        )

    val pureCrypto = connectedSynchronizer.synchronizerHandle.syncPersistentState.pureCryptoApi

    indexingWorkflow.flushContractActivationChangesToIndexer(
      partyIds,
      synchronizerId,
      publishAt,
      indexingStore,
      pureCrypto,
    )(publishUpdate)
  }
}
