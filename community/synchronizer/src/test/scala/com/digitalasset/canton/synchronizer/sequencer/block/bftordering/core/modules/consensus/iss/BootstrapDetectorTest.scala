// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.TestEssentials
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.BootstrapDetector.BootstrapKind
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  Bootstrap,
  EpochStore,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.{
  NodeActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.TestBootstrapTopologyActivationTime
import org.scalatest.wordspec.AnyWordSpec

class BootstrapDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  import BootstrapDetectorTest.*

  "detect bootstrap kind" in {
    forAll(
      Table[Option[SequencerSnapshotAdditionalInfo], Membership, EpochStore.Epoch, BootstrapKind](
        (
          "sequencer snapshot additional info",
          "membership",
          "latest completed epoch",
          "expected should state transfer true/false",
        ),
        // No sequencer snapshot
        (
          None,
          aMembershipWith2Nodes,
          aBootstrapEpoch,
          BootstrapKind.RegularStartup,
        ),
        // Only 1 node
        (
          Some(aSequencerSnapshot),
          Membership.forTesting(myId),
          aBootstrapEpoch,
          BootstrapKind.RegularStartup,
        ),
        // Non-zero starting epoch
        (
          Some(aSequencerSnapshot),
          aMembershipWith2Nodes,
          Epoch(
            EpochInfo.forTesting(
              EpochNumber(7L),
              BlockNumber(70L),
              EpochLength(10L),
            ),
            lastBlockCommits = Seq.empty,
          ),
          BootstrapKind.RegularStartup,
        ),
        // Onboarding
        (
          Some(aSequencerSnapshot),
          aMembershipWith2Nodes,
          aBootstrapEpoch,
          BootstrapKind.Onboarding(
            EpochInfo.forTesting(
              EpochNumber(1500L),
              BlockNumber(15000L),
              DefaultEpochLength,
            )
          ),
        ),
      )
    ) { (snapshotAdditionalInfo, membership, latestCompletedEpoch, expectedShouldStateTransfer) =>
      BootstrapDetector.detect(
        DefaultEpochLength,
        snapshotAdditionalInfo,
        membership,
        latestCompletedEpoch,
      )(fail(_)) shouldBe expectedShouldStateTransfer
    }
  }

  "fail on missing start epoch number" in {
    a[RuntimeException] shouldBe thrownBy(
      BootstrapDetector.detect(
        DefaultEpochLength,
        Some(SequencerSnapshotAdditionalInfo(Map.empty /* boom! */ )),
        aMembershipWith2Nodes,
        aBootstrapEpoch,
      )(_ => throw new RuntimeException("aborted"))
    )
  }
}

object BootstrapDetectorTest extends TestEssentials {

  private val myId = BftNodeId("self")
  private val otherId = BftNodeId("other")
  private val aBootstrapEpoch = Bootstrap.bootstrapEpoch(TestBootstrapTopologyActivationTime)
  private val aMembershipWith2Nodes =
    Membership.forTesting(myId, Set(otherId))
  private val aSequencerSnapshot = SequencerSnapshotAdditionalInfo(
    // Minimal data required for the test
    Map(
      myId -> NodeActiveAt(
        TopologyActivationTime(CantonTimestamp.Epoch),
        Some(EpochNumber(1500L)),
        firstBlockNumberInStartEpoch = Some(BlockNumber(15000L)),
        startEpochTopologyQueryTimestamp = Some(TestBootstrapTopologyActivationTime),
        startEpochCouldAlterOrderingTopology = None,
        previousBftTime = None,
        previousEpochTopologyQueryTimestamp = None,
        leaderSelectionPolicyState = None,
      )
    )
  )
}
