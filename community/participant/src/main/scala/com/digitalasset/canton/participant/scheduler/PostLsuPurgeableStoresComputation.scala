// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import cats.syntax.contravariantSemigroupal.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore.LsuSource
import com.digitalasset.canton.participant.sync.SyncPersistentStateManager
import com.digitalasset.canton.store.ChunkPurgeable
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext

/** Computes which stores can be purged after LSU.
  */
class PostLsuPurgeableStoresComputation(
    synchronizerConnectionConfigStore: SynchronizerConnectionConfigStore,
    syncPersistentStateManager: SyncPersistentStateManager,
) {

  def compute()(implicit
      traceContext: TraceContext
  ): Seq[ChunkPurgeable] = {
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
    for {
      psid <- candidates
      successorPsid <- successorPerPsid.get(psid).toList
      successorPersistentState <- persistentStates.get(successorPsid).toList
      if (successorPersistentState.connectivityStatusStore.isTopologyInitialized)
      predecessorState <- persistentStates.get(psid).toList
      purgableStore <- predecessorState.purgeableStores
    } yield purgableStore
  }
}
