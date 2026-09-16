// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import cats.syntax.contravariantSemigroupal.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsDigestStore.allCheckpointsFilter
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.LsuSource
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.store.ChunkPurgeable
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.ExecutionContext

/** Computes which stores can be purged after LSU.
  */
class PostLsuPurgeableStoresComputation(
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    syncPersistentStateManager: SyncPersistentStateManager,
    acsDigestProcessorEnabled: Boolean,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def compute()(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Seq[ChunkPurgeable]] = {
    val persistentStates = syncPersistentStateManager.getAll

    val successorPerPsid: Map[PhysicalSynchronizerId, PhysicalSynchronizerId] =
      synchronizerConnectionConfigStore
        .getAll()
        .mapFilter { config =>
          (config.predecessor, config.configuredPsid.toOption).mapN { case (predecessor, psid) =>
            predecessor.psid -> psid
          }
        }
        .toMap

    val candidates = synchronizerConnectionConfigStore
      .getAll()
      // keep only synchronizer that were LSUed
      .filter(_.status == LsuSource)
      .flatMap(_.configuredPsid.toOption)
      // and that have an active physical connection
      .filter(psid => synchronizerConnectionConfigStore.getActive(psid.logical).isRight)

    // Consider only synchronizer that have successor topology initialized
    // so that purging does not get in the way of local copy.
    val filteredCandidates = for {
      psid <- candidates
      successorPsid <- successorPerPsid.get(psid).toList
      successorPersistentState <- persistentStates.get(successorPsid).toList
      if (successorPersistentState.connectivityStatusStore.isTopologyInitialized)
      synchronizerPredecessor <- synchronizerConnectionConfigStore
        .get(successorPsid)
        .toOption
        .flatMap(_.predecessor)
        .toList
      predecessorState <- persistentStates.get(psid).toList
    } yield predecessorState -> synchronizerPredecessor

    // For each predecessor synchronizer, we check if the latest ACS digest checkpoint is after the upgrade time
    MonadUtil
      .sequentialTraverse(filteredCandidates) { case (predecessorState, synchronizerPredecessor) =>
        if (acsDigestProcessorEnabled) {
          logger.debug(
            s"Checking if ACS digest processor has caught up for predecessor synchronizer ${predecessorState.psid}"
          )
          val acsDigestCheckpoint = predecessorState.acsDigestStore.latestCheckpointUpTo(
            Offset.MaxValue,
            allCheckpointsFilter,
          )

          acsDigestCheckpoint.map { checkpointO =>
            logger.debug(
              s"ACS digest processor latest checkpoint: $acsDigestCheckpoint"
            )
            checkpointO.fold(Seq.empty[ChunkPurgeable]) { checkpoint =>
              if (checkpoint.timepoint.recordTime > synchronizerPredecessor.upgradeTime) {
                logger.debug(
                  s"ACS digest processor has progressed beyond upgrade time ${synchronizerPredecessor.upgradeTime}) for predecessor ${synchronizerPredecessor.psid}, stores are safe to be purged"
                )
                predecessorState.purgeableStores
              } else {
                logger.debug(
                  s"ACS digest processor has not yet progressed beyond upgrade time ${synchronizerPredecessor.upgradeTime}) for predecessor ${synchronizerPredecessor.psid}"
                )
                Seq.empty[ChunkPurgeable]
              }
            }
          }
        } else {
          logger.debug(
            s"Not considering ACS digest processor for store pruging as it is disabled"
          )
          FutureUnlessShutdown.pure(predecessorState.purgeableStores)
        }
      }
      .map(_.flatten)
  }
}
