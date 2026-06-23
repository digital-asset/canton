// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import cats.syntax.contravariantSemigroupal.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
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
)(implicit
    ec: ExecutionContext
) {

  def compute()(implicit
      traceContext: TraceContext
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
    MonadUtil
      .sequentialTraverse(candidates) { psid =>
        successorPerPsid
          .get(psid)
          .flatMap(persistentStates.get) match {
          case Some(successorPersistentState) =>
            successorPersistentState.connectivityStatusStore.isTopologyInitialized().map {
              case true =>
                persistentStates.get(psid).fold(Seq.empty[ChunkPurgeable])(_.purgeableStores)
              case false => Nil
            }

          case None => FutureUnlessShutdown.pure(Seq.empty)
        }
      }
      .map(_.flatten)
  }
}
