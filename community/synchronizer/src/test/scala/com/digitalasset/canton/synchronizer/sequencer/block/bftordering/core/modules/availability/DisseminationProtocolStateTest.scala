// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.crypto.Signature.noSignature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.sequencing.protocol.MaxRequestSizeToDeserialize
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatchStats
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import org.scalatest.wordspec.AnyWordSpec

class DisseminationProtocolStateTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  import DisseminationProtocolStateTest.*

  "Reviewing a batch ready for ordering" when {

    "the topology is unchanged" should {
      "do nothing" in {
        val orderingTopology =
          orderingTopologyWith(
            ANodeId,
            BftKeyId(noSignature.authorizingLongTermKey.toProtoPrimitive),
          )
        val disseminatedBatchMetadata =
          disseminatedBatchMetadataWith(AnEpochNumber, ANodeId, ABatchId, noSignature, SomeStats)
        DisseminationProgress.reviewReadyForOrdering(
          disseminatedBatchMetadata,
          Node0,
          orderingTopology,
        ) shouldBe empty
      }
    }

    "an acking node is removed" should {
      "remove the ack from the node that has been removed" in {
        val orderingTopology =
          orderingTopologyWith(
            ANodeId,
            BftKeyId(noSignature.authorizingLongTermKey.toProtoPrimitive),
          )
        val disseminatedBatchMetadata =
          disseminatedBatchMetadataWith(AnEpochNumber, ANodeId, ABatchId, noSignature, SomeStats)
        val newTopology = orderingTopology.copy(nodesTopologyInfo = Map.empty)
        DisseminationProgress.reviewReadyForOrdering(
          disseminatedBatchMetadata,
          Node0,
          newTopology,
        ) shouldBe
          Some(
            DisseminationProgress(
              newTopology,
              InProgressBatchMetadata(
                ABatchId,
                AnEpochNumber,
                SomeStats,
                regressionsToSigning = 1,
              ),
              Set.empty,
            )
          )
      }
    }

    "an acking node's signing key is changed" should {
      "remove the ack from the node that has changed its signing key" in {
        val orderingTopology =
          orderingTopologyWith(
            ANodeId,
            BftKeyId(noSignature.authorizingLongTermKey.toProtoPrimitive),
          )
        val disseminatedBatchMetadata =
          disseminatedBatchMetadataWith(AnEpochNumber, ANodeId, ABatchId, noSignature, SomeStats)
        val newTopology =
          orderingTopology.copy(nodesTopologyInfo = orderingTopology.nodesTopologyInfo.map {
            case (nodeId, _) =>
              nodeId -> NodeTopologyInfo(
                AnActivationTime,
                Set(BftKeyId("newKey")),
              )
          })
        DisseminationProgress.reviewReadyForOrdering(
          disseminatedBatchMetadata,
          Node0,
          newTopology,
        ) shouldBe
          Some(
            DisseminationProgress(
              newTopology,
              InProgressBatchMetadata(
                ABatchId,
                AnEpochNumber,
                SomeStats,
                regressionsToSigning = 1,
              ),
              Set.empty,
            )
          )
      }
    }
  }
}

object DisseminationProtocolStateTest {

  private val AnActivationTime: TopologyActivationTime = TopologyActivationTime(
    CantonTimestamp.MinValue
  )

  private val SomeStats = OrderingRequestBatchStats(0, 0)
  private val AnEpochNumber = EpochNumber.First
  private val ANodeId = BftNodeId("node1")

  private def orderingTopologyWith(nodeId: BftNodeId, keyId: BftKeyId): OrderingTopology =
    OrderingTopology(
      nodesTopologyInfo = Map(
        nodeId -> NodeTopologyInfo(
          AnActivationTime,
          Set(keyId),
        )
      ),
      SequencingParameters.Default, // irrelevant for this test
      MaxRequestSizeToDeserialize.Limit(
        DynamicSynchronizerParameters.defaultMaxRequestSize.value
      ), // irrelevant for this test
      AnActivationTime, // irrelevant for this test
      areTherePendingCantonTopologyChanges = false, // irrelevant for this test
    )

  private def disseminatedBatchMetadataWith(
      epochNumber: EpochNumber,
      nodeId: BftNodeId,
      batchId: BatchId,
      signature: Signature,
      stats: OrderingRequestBatchStats,
  ): DisseminatedBatchMetadata =
    DisseminatedBatchMetadata(
      ProofOfAvailability(
        batchId,
        Seq(AvailabilityAck(nodeId, signature)),
        epochNumber,
      ),
      epochNumber,
      stats,
    )
}
