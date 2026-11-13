// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CachingConfigs, TopologyConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.cache.TopologyStateLookup
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{NoPackageDependencies, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.{TopologyChangeOp, TopologyMapping}
import com.digitalasset.canton.topology.{DefaultTestIdentities, KeyCollection, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, CiTestRun, HasExecutionContext}
import com.digitalasset.nonempty.NonEmpty
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class TopologyConsistencyCheckTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  "The CI topology consistency setting" should {
    "preserve an existing opt-out from additional consistency checks" in {
      TopologyConfig(
        enableTopologySnapshotConsistencyChecks = CiTestRun.topologySnapshotConsistencyChecks
      ).shouldCheckTopologySnapshots(enableAdditionalConsistencyChecks = false) shouldBe false
    }

    "control reference-store reads by the real topology client" in {
      val config = TopologyConfig(
        enableTopologySnapshotConsistencyChecks = CiTestRun.topologySnapshotConsistencyChecks
      )
      val expectedChecks = CiTestRun.topologySnapshotConsistencyChecks.getOrElse(true)
      val store = spy(
        new InMemoryTopologyStore(
          TopologyStoreId.SynchronizerStore(DefaultTestIdentities.physicalSynchronizerId),
          predecessor = None,
          testedProtocolVersion,
          loggerFactory,
          timeouts,
        )
      )
      val lookup = mock[TopologyStateLookup]
      when(lookup.cacheCleanAsOf).thenReturn(Some(EffectiveTime(CantonTimestamp.MinValue)))
      when(lookup.makeCopy()).thenReturn(lookup)
      when(
        lookup.lookupForUids(
          any[EffectiveTime],
          any[Boolean],
          any[NonEmpty[Seq[UniqueIdentifier]]],
          any[Set[TopologyMapping.Code]],
          any[Option[TopologyChangeOp]],
          any[Boolean],
        )(any[TraceContext], any[ExecutionContext])
      ).thenReturn(
        FutureUnlessShutdown.pure(
          Map.empty[UniqueIdentifier, Seq[GenericStoredTopologyTransaction]]
        )
      )

      val client = WriteThroughCacheSynchronizerTopologyClient
        .create(
          new SimClock(loggerFactory = loggerFactory),
          defaultStaticSynchronizerParameters,
          store,
          lookup,
          synchronizerUpgradeTime = None,
          sequencerSnapshotTimestamp = None,
          cleanSynchronizerRecordTime = None,
          NoPackageDependencies,
          CachingConfigs(),
          enableConsistencyChecks = true,
          config,
          timeouts,
          FutureSupervisor.Noop,
          loggerFactory,
        )
        .futureValueUS

      try {
        val snapshot = client.snapshot(client.latestTopologyChangeTimestamp).futureValueUS
        snapshot.isInstanceOf[ValidatingTopologySnapshot] shouldBe expectedChecks
        snapshot
          .allKeys(DefaultTestIdentities.participant1)
          .futureValueUS shouldBe KeyCollection.empty
        val referenceReads = mockingDetails(store).getInvocations.asScala.count(
          _.getMethod.getName == "findPositiveTransactions"
        )
        referenceReads shouldBe (if (expectedChecks) 1 else 0)
        info(
          s"TOPOLOGY_CONSISTENCY_PROOF nightly=${sys.env.getOrElse("CANTON_NIGHTLY_TEST_RUN", "unset")} " +
            s"snapshot=${snapshot.getClass.getSimpleName} reference-store-lookups=$referenceReads"
        )
      } finally {
        client.close()
        store.close()
      }
    }
  }
}
