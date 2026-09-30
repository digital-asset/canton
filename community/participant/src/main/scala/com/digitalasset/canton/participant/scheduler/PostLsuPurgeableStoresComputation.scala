// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import cats.syntax.contravariantSemigroupal.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.data.{Offset, SynchronizerPredecessor}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.AcsDigestStore.allCheckpointsFilter
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.LsuSource
import com.digitalasset.canton.participant.store.{AcsDigestStore, SynchronizerConnectionConfigStore}
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

    MonadUtil
      .sequentialTraverse(filteredCandidates) { case (predecessorState, synchronizerPredecessor) =>
        for {
          acsCommitmentsPastUpgradeTime <- acsCommitmentsChecks(
            predecessorState.acsDigestStore,
            synchronizerPredecessor,
          )
        } yield {
          if (
            acsCommitmentsPastUpgradeTime && isCleanSynchronizerIndexAfterUpgradeTime(
              synchronizerPredecessor
            )
          )
            predecessorState.purgeableStores
          else
            Seq.empty
        }
      }
      .map(_.flatten)
  }

  /** Returns true if the stores of the predecessor can be pruned from the point of view of crash
    * recovery/clean synchronizer index.
    *
    * Note that this currently ensures that processing of the offboarding events by the reassignment
    * store is completed before we purge the related topology.
    */
  // TODO(#23636) Consider removing this
  private def isCleanSynchronizerIndexAfterUpgradeTime(
      synchronizerPredecessor: SynchronizerPredecessor
  ): Boolean = {
    val cleanSynchronizerIndex = syncPersistentStateManager.ledgerApiStore.value
      .cleanSynchronizerIndex(synchronizerPredecessor.psid.logical)

    cleanSynchronizerIndex.map(_.recordTime).fold(false)(_ >= synchronizerPredecessor.upgradeTime)
  }

  /** Returns true if the stores of the predecessor can be pruned from the point of view of ACS
    * commitments processing.
    */
  private def acsCommitmentsChecks(
      acsDigestStore: AcsDigestStore,
      synchronizerPredecessor: SynchronizerPredecessor,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Boolean] =
    // Check if the latest ACS digest checkpoint is after the upgrade time
    if (acsDigestProcessorEnabled) {
      logger.debug(
        s"Checking if ACS digest processor has caught up for predecessor synchronizer ${synchronizerPredecessor.psid}"
      )
      val acsDigestCheckpoint = acsDigestStore.latestCheckpointUpTo(
        Offset.MaxValue,
        allCheckpointsFilter,
      )

      acsDigestCheckpoint.map { checkpointO =>
        logger.debug(
          s"ACS digest processor latest checkpoint: $acsDigestCheckpoint"
        )
        checkpointO.fold(false) { checkpoint =>
          if (checkpoint.timepoint.recordTime > synchronizerPredecessor.upgradeTime) {
            logger.debug(
              s"ACS digest processor has progressed beyond upgrade time ${synchronizerPredecessor.upgradeTime}) for predecessor ${synchronizerPredecessor.psid}, stores are safe to be purged"
            )
            true
          } else {
            logger.debug(
              s"ACS digest processor has not yet progressed beyond upgrade time ${synchronizerPredecessor.upgradeTime}) for predecessor ${synchronizerPredecessor.psid}"
            )
            false
          }
        }
      }
    } else {
      logger.debug(s"Not considering ACS digest processor for store purging as it is disabled")
      FutureUnlessShutdown.pure(true)
    }
}
