// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.TopologyConfig
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.store.SyncPersistentState
import com.digitalasset.canton.topology.DefaultTestIdentities.defaultDynamicSynchronizerParameters
import com.digitalasset.canton.topology.admin.grpc.PsidLookup
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{TopologyStoreTestData, ValidatedTopologyTransaction}
import com.digitalasset.canton.topology.transaction.SynchronizerParametersState
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  PhysicalSynchronizerId,
  SynchronizerId,
  TopologyManagerError,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

final class TopologyLookupTest
    extends BaseTest
    with AnyWordSpecLike
    with Matchers
    with HasExecutionContext {
  "TopologyLookup" should {
    "allow to create offline topology client from topology store" in {
      val psid = DefaultTestIdentities.physicalSynchronizerId
      val otherPsid = psid.incrementSerial

      val persistentState = mock[SyncPersistentState]

      val topologyStoreTestData =
        new TopologyStoreTestData(testedProtocolVersion, loggerFactory, parallelExecutionContext)

      val topologyStore = new InMemoryTopologyStore[SynchronizerStore](
        SynchronizerStore(psid),
        predecessor = None,
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      )

      val ts = CantonTimestamp.now()
      val mapping = SynchronizerParametersState(psid.logical, defaultDynamicSynchronizerParameters)

      val signedTx = topologyStoreTestData.makeSignedTx(mapping)(topologyStoreTestData.p1Key)

      topologyStore
        .update(
          SequencedTime(ts),
          EffectiveTime(ts),
          removals = Map(),
          Seq(ValidatedTopologyTransaction(signedTx)),
        )
        .futureValueUS

      when(persistentState.staticSynchronizerParameters).thenAnswer(
        defaultStaticSynchronizerParameters
      )
      when(persistentState.topologyStore).thenAnswer(topologyStore)

      val topologyLookup = new TopologyLookup(
        clock = wallClock,
        topologyConfig = TopologyConfig(),
        timeouts = timeouts,
        futureSupervisor = FutureSupervisor.Noop,
        topologyManagerO = _ => None, // Simulate offline
        topologyClientO = _ => None, // Simulate offline
        psidLookup = new PsidLookup {
          override def activePsidFor(
              synchronizerId: SynchronizerId
          ): Option[PhysicalSynchronizerId] = None
        },
        syncPersistentStateO =
          (id: PhysicalSynchronizerId) => if (id == psid) Some(persistentState) else None,
        loggerFactory = loggerFactory,
      )

      val approximateSnapshot =
        topologyLookup.maybeOfflineApproximateSnapshot(psid).futureValueUS.value
      approximateSnapshot.timestamp shouldBe ts.immediateSuccessor
      approximateSnapshot
        .findDynamicSynchronizerParameters()
        .futureValueUS
        .value
        .parameters shouldBe defaultDynamicSynchronizerParameters

      topologyLookup
        .maybeOfflineApproximateSnapshot(otherPsid)
        .futureValueUS
        .left
        .value shouldBe ParticipantTopologyManagerError.IdentityManagerParentError(
        TopologyManagerError.TopologyStoreUnknown.Failure(SynchronizerStore(otherPsid))
      )
    }
  }
}
