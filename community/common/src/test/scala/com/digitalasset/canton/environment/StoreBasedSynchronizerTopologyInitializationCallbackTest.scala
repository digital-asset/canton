// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.config.{BatchAggregatorConfig, BatchingConfig, TopologyConfig}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.topology.DefaultTestIdentities.physicalSynchronizerId
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{InitialTopologySnapshotValidator, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransaction.GenericStoredTopologyTransaction
import com.digitalasset.canton.topology.store.TopologyStoreId
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.atomic.AtomicInteger

class StoreBasedSynchronizerTopologyInitializationCallbackTest
    extends BaseTestWordSpec
    with FlagCloseable
    with HasCloseContext
    with HasExecutionContext {

  private implicit val actorSystem: ActorSystem = ActorSystem(getClass.getSimpleName)
  private implicit val materializer: Materializer = Materializer(actorSystem)

  "StoreBasedSynchronizerTopologyInitializationCallback" should {
    "eventually succeed after a several failed attempts" in {
      val mkTopologyTransactionSource: () => EitherT[
        FutureUnlessShutdown,
        String,
        Source[GenericStoredTopologyTransaction, NotUsed],
      ] = {
        val callCount = new AtomicInteger(0)

        () =>
          EitherT.pure[FutureUnlessShutdown, String](
            if (callCount.getAndIncrement() < 3) {
              Source.failed(new IllegalStateException("Illegal state"))
            } else {
              Source(Seq.empty)
            }
          )
      }

      val topologyStore = new InMemoryTopologyStore[TopologyStoreId](
        SynchronizerStore(physicalSynchronizerId),
        predecessor = None,
        testedProtocolVersion,
        loggerFactory,
        timeouts,
      )

      val pureCrypto = new SymbolicPureCrypto

      val validator = new InitialTopologySnapshotValidator(
        pureCrypto = pureCrypto,
        store = topologyStore,
        BatchingConfig().copy(
          topologyCacheAggregator = BatchAggregatorConfig.defaultsForTesting
        ),
        TopologyConfig.forTesting.copy(validateInitialTopologySnapshot = true),
        Some(defaultStaticSynchronizerParameters),
        timeouts,
        futureSupervisor = futureSupervisor,
        loggerFactory = loggerFactory,
      )

      val callback = new StoreBasedSynchronizerTopologyInitializationCallback()

      val topologyClient = mock[SynchronizerTopologyClientWithInit]

      when(
        topologyClient.updateKnownTimestampsDuringStartup(
          any[Option[SequencedTime]],
          any[Option[SequencedTime]],
          any[Option[CantonTimestamp]],
        )(anyTraceContext)
      ).thenReturn(FutureUnlessShutdown.pure(()))

      val result = callback
        .downloadWithRetries(
          validator,
          topologyClient,
          mkTopologyTransactionSource,
        )
        .futureValueUS

      result shouldBe Right(())
    }
  }
}
