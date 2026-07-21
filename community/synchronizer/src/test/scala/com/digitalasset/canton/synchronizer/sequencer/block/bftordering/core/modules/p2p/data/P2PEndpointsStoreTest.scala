// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  PlainTextP2PEndpoint,
  TlsP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.P2PEndpointConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import org.scalatest.wordspec.AsyncWordSpec

trait P2PEndpointsStoreTest extends AsyncWordSpec {
  this: AsyncWordSpec & BftSequencerBaseTest =>

  import P2PEndpointsStoreTest.*

  private[bftordering] def p2pEndpointsStore(
      createStore: () => P2PEndpointsStore[PekkoEnv]
  ): Unit =
    "P2pEndpointsStore" should {
      "create and retrieve endpoints" in {
        val store = createStore()
        for {
          updated1 <- store.addEndpoint(endpoint1)
          updated2 <- store.addEndpoint(endpoint1)
          list1 <- store.listEndpoints()

          updated3 <- store.addEndpoint(endpoint3)
          list2 <- store.listEndpoints()
        } yield {
          list1 should contain only endpoint1 -> None
          list2 should contain theSameElementsInOrderAs Seq(endpoint1 -> None, endpoint3 -> None)
          updated1 shouldBe true
          updated2 shouldBe false
          updated3 shouldBe true
        }
      }

      "associate a node ID to an endpoint" in {
        val store = createStore()
        for {
          updated1 <- store.addEndpoint(endpoint1)
          updated2 <- store.addEndpoint(endpoint2)
          list1 <- store.listEndpoints()
          updated3 <- store.associate(endpoint3, BftNodeId("node3"))
          updated4 <- store.associate(endpoint1, BftNodeId("node1"))
          updated5 <- store.associate(endpoint1, BftNodeId("node2"))
          updated6 <- store.associate(endpoint1, BftNodeId("node2"))
          list2 <- store.listEndpoints()
        } yield {
          updated1 shouldBe true
          updated2 shouldBe true
          list1 should contain theSameElementsInOrderAs Seq(endpoint1 -> None, endpoint2 -> None)
          updated3 shouldBe false
          updated4 shouldBe true
          updated5 shouldBe true
          updated6 shouldBe true
          list2 should contain theSameElementsInOrderAs Seq(
            endpoint1 -> Some(BftNodeId("node2")),
            endpoint2 -> None,
          )
        }
      }

      "remove endpoints" in {
        val store = createStore()
        for {
          updated1 <- store.addEndpoint(endpoint1)
          list1 <- store.listEndpoints()
          updated2 <- store.removeEndpoint(endpoint1.id)
          list2 <- store.listEndpoints()
          updated3 <- store.removeEndpoint(endpoint1.id)
          list3 <- store.listEndpoints()
        } yield {
          list1 should contain only endpoint1 -> None
          list2 should be(empty)
          list3 should be(empty)
          updated1 shouldBe true
          updated2 shouldBe true
          updated3 shouldBe false
        }
      }

      "clear endpoints" in {
        val store = createStore()
        for {
          updated1 <- store.addEndpoint(endpoint1)
          updated2 <- store.addEndpoint(endpoint2)
          list1 <- store.listEndpoints()
          _ <- store.clearAllEndpoints()
          list2 <- store.listEndpoints()
          _ <- store.clearAllEndpoints()
          list3 <- store.listEndpoints()
        } yield {
          list1 should contain theSameElementsInOrderAs Seq(endpoint1 -> None, endpoint2 -> None)
          list2 should be(empty)
          list3 should be(empty)
          updated1 shouldBe true
          updated2 shouldBe true
        }
      }
    }
}

object P2PEndpointsStoreTest {

  private val endpoint1 = PlainTextP2PEndpoint("host1", Port.tryCreate(1001))
  private val endpoint2 = TlsP2PEndpoint(P2PEndpointConfig("host2", Port.tryCreate(1002)))
  private val endpoint3 = PlainTextP2PEndpoint("host3", Port.tryCreate(1003))
}
