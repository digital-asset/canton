// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.immutable.SortedSet

class LeaderSelectionPolicyTest extends AsyncWordSpec with BaseTest {
  implicit val pv: ProtocolVersion = testedProtocolVersion

  "LeaderSelectionPolicy" should {
    "rotate leaders" in {
      forAll(
        Table[Set[Int], Long, Seq[BftNodeId]](
          ("node indexes", "epoch number", "expected rotated leaders"),
          (Set(1), 13, Seq(BftNodeId(s"node1"))),
          (
            Set(1, 2, 3),
            0,
            Seq(
              BftNodeId("node1"),
              BftNodeId("node2"),
              BftNodeId("node3"),
            ),
          ),
          (
            Set(1, 2, 3),
            1,
            Seq(
              BftNodeId("node2"),
              BftNodeId("node3"),
              BftNodeId("node1"),
            ),
          ),
          (
            Set(1, 2, 3),
            2,
            Seq(
              BftNodeId("node3"),
              BftNodeId("node1"),
              BftNodeId("node2"),
            ),
          ),
          (
            Set(1, 2, 3),
            3,
            Seq(
              BftNodeId("node1"),
              BftNodeId("node2"),
              BftNodeId("node3"),
            ),
          ),
          (
            Set(1, 2, 3),
            4,
            Seq(
              BftNodeId("node2"),
              BftNodeId("node3"),
              BftNodeId("node1"),
            ),
          ),
        )
      ) { case (nodeIndexes, epochNumber, expectedRotatedLeaders) =>
        val nodes = nodeIndexes.map { index =>
          BftNodeId(s"node$index")
        }
        val rotatedLeaders =
          LeaderSelectionPolicy.rotateLeaders(SortedSet.from(nodes), EpochNumber(epochNumber))

        rotatedLeaders should contain theSameElementsInOrderAs expectedRotatedLeaders
      }
    }
  }
}
