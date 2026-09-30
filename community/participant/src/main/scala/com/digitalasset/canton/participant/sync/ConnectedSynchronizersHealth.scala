// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.{ComponentStatus, HealthComponent, HealthQuasiComponent}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizersHealth.SynchronizerHealth
import com.digitalasset.canton.topology.PhysicalSynchronizerId

import scala.collection.concurrent.TrieMap

/** Tracks the health components of all connected synchronizers.
  *
  * This registry does not own the components it holds: they are created, owned, and closed by each
  * [[ConnectedSynchronizer]] and its subcomponents (sequencer client, ACS commitment processor,
  * connection pool).
  */
final class ConnectedSynchronizersHealth {

  private val healthBySynchronizer: TrieMap[PhysicalSynchronizerId, SynchronizerHealth] =
    TrieMap.empty

  /** Registers the health components of a newly connected synchronizer.
    *
    * An existing entry for the same psid is overwritten. Concurrent calls for the same psid cannot
    * happen because all connects and disconnects are serialized through the `connectQueue` of the
    * `SynchronizerConnectionsManager`. An entry can therefore only pre-exist after a disconnect
    * that failed before its [[remove]] call; on the subsequent reconnect, overwriting with the
    * fresh components is the desired behavior.
    */
  def set(psid: PhysicalSynchronizerId, health: SynchronizerHealth): Unit =
    healthBySynchronizer.put(psid, health).discard

  def remove(psid: PhysicalSynchronizerId): Unit =
    healthBySynchronizer.remove(psid).discard

  def get(psid: PhysicalSynchronizerId): Option[SynchronizerHealth] =
    healthBySynchronizer.get(psid)

  def clear(): Unit = healthBySynchronizer.clear()

  def snapshot: Map[PhysicalSynchronizerId, SynchronizerHealth] =
    healthBySynchronizer.readOnlySnapshot().toMap

  /** The raw components of all connected synchronizers, flattened, for the node's health service.
    */
  def allHealthComponents: Seq[HealthQuasiComponent] =
    snapshot.values.toSeq.flatMap(_.allComponents)

  /** One [[com.digitalasset.canton.health.ComponentStatus]] per component per synchronizer, with
    * the full physical synchronizer id attached as the
    * [[com.digitalasset.canton.health.ComponentStatus.SynchronizerLabelKey]] label.
    */
  def componentStatuses: Seq[ComponentStatus] =
    snapshot.toSeq.flatMap { case (psid, health) =>
      health.allComponents.map(component =>
        ComponentStatus(
          component.name,
          component.getState.toComponentHealthState,
          labels = Map(ComponentStatus.SynchronizerLabelKey -> psid.toProtoPrimitive),
        )
      )
    }

  /** Component statuses for the node's status report: the given node-level components as-is,
    * followed by the synchronizer-labeled statuses of all connected synchronizers. Raw
    * per-synchronizer components contained in `nodeComponents` are dropped in favor of their
    * labeled counterparts from [[componentStatuses]].
    */
  def componentStatusesWith(nodeComponents: Seq[HealthQuasiComponent]): Seq[ComponentStatus] = {
    val rawSynchronizerComponents = allHealthComponents.toSet
    nodeComponents
      .filterNot(rawSynchronizerComponents.contains)
      .map(_.toComponentStatus) ++ componentStatuses
  }
}

object ConnectedSynchronizersHealth {

  final case class SynchronizerHealth(
      connectedSynchronizer: HealthComponent,
      ephemeral: HealthComponent,
      sequencerClient: HealthComponent,
      sequencerConnectionPool: () => Seq[HealthQuasiComponent],
      acsCommitmentProcessor: HealthComponent,
  ) {

    def allComponents: Seq[HealthQuasiComponent] =
      Seq(
        connectedSynchronizer,
        ephemeral,
        sequencerClient,
        acsCommitmentProcessor,
      ) ++ sequencerConnectionPool()
  }
}
