// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.BaseTest.defaultMaxBytesToDecompress
import com.digitalasset.canton.crypto.Signature.noSignature
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftKeyId,
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatchStats
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.TestBootstrapTopologyActivationTime
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class DisseminationStatusTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  import DisseminationStatusTest.*

  "The set of nodes to send a batch" when {

    "the dissemination is complete" should {

      "be the recipients if not sent to anybody yet, then empty" in {
        val membership = createMembership(Node0, Node1, Node2)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )

        disseminationCompletion.sendBatchTo shouldBe Set(Node1, Node2)

        disseminationCompletion.copy(batchSentTo = Set(Node1)).sendBatchTo shouldBe empty
      }
    }

    "the dissemination is in progress" should {

      "be the recipients minus the acks if not-yet-sent" in {
        val membership = createMembership(Node0, Node1, Node2)
        val disseminationInProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          )

        disseminationInProgress.sendBatchTo shouldBe Set(Node1, Node2)
      }

      "be the recipients, minus" +
        "ourselves, minus" +
        "the acks, minus " +
        "the nodes already sent to, plus " +
        "the other nodes from which acks were lost that are still in the topology" in {
          val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
          val disseminationInProgress =
            createDisseminationProgress(
              ABatchId,
              membership,
              acks = Set(Node0, Node2),
              AnEpochNumber,
              SomeStats,
            ).addSends(additionalSends = Set(Node1))
              .asInProgress
              .getOrElse(fail("should be in progress"))

          disseminationInProgress.sendBatchTo shouldBe EightNodes.diff(Set(Node0, Node1, Node2))

          disseminationInProgress
            .copy(
              acks = Set(AvailabilityAck(Node0, noSignature)),
              previousAcks = Some(
                Set(AvailabilityAck(Node2, noSignature), AvailabilityAck(node(300), noSignature))
              ),
            )
            .sendBatchTo shouldBe EightNodes.diff(Set(Node0, Node1))
        }

      "remember nodes already sent to across completion and regression" in {
        val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
        val disseminationInProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          ).addSends(additionalSends = Set(Node3))

        disseminationInProgress.sendBatchTo shouldBe EightNodes.diff(Set(Node0, Node3))

        val completed =
          disseminationInProgress
            .addAck(AvailabilityAck(Node1, noSignature))
            .addAck(AvailabilityAck(Node2, noSignature))

        completed shouldBe a[DisseminationStatus.Complete]

        val newMembership = createMembership(Node0, EightNodes.diff(Set(Node0, Node1)).toSeq*)
        val inProgress = completed.changeMembership(newMembership)

        inProgress shouldBe a[DisseminationStatus.InProgress]
        inProgress.sendBatchTo shouldBe EightNodes.diff(Set(Node0, Node1, Node2, Node3))
      }
    }
  }

  "Updating an in-progress dissemination" when {

    "it lacks nothing" should {
      "become complete" in {
        val membership = createMembership(Node0, Node1)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val aReadyForOrderingInstant = Instant.now()

        val updated =
          disseminationProgress.update() match {
            case c: DisseminationStatus.Complete =>
              c.copy(readyForOrderingInstant = Some(aReadyForOrderingInstant))
            case _ => fail("Expected the dissemination to be completed")
          }

        updated shouldBe DisseminationStatus.Complete(
          membership,
          Traced(
            ProofOfAvailability(
              ABatchId,
              Seq(
                AvailabilityAck(Node0, noSignature),
                AvailabilityAck(Node1, noSignature),
              ),
              AnEpochNumber,
            )
          ),
          epochNumber = AnEpochNumber,
          stats = SomeStats,
          availabilityEnterInstant = disseminationProgress.availabilityEnterInstant,
          readyForOrderingInstant = Some(aReadyForOrderingInstant),
          regressionsToSigning = disseminationProgress.regressionsToSigning,
          disseminationRegressions = disseminationProgress.disseminationRegressions,
        )
      }
    }

    "it is not complete" should {
      "stay unchanged" in {
        val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )

        disseminationProgress.update() shouldBe disseminationProgress
      }
    }
  }

  "Attempting to update a complete dissemination" when {

    "the dissemination is complete" should {
      "change nothing" in {
        val membership = createMembership(Node0, Node1)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )

        disseminationCompletion.update() shouldBe disseminationCompletion
      }
    }

    "the dissemination lacks something" should {
      "become in progres" in {
        val membership = createMembership(Node0, Node1)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set.empty,
            AnEpochNumber,
            SomeStats,
          )

        disseminationCompletion.update() shouldBe createDisseminationProgress(
          ABatchId,
          membership,
          acks = Set.empty,
          AnEpochNumber,
          SomeStats,
        )
      }
    }
  }

  "Adding an ack to a complete dissemination" when {

    "the ack is not already present" should {
      "add it to the proof of availability" in {
        val membership = createMembership(Node0, Node1)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          )

        disseminationCompletion.addAck(AvailabilityAck(Node1, noSignature)) shouldBe
          disseminationCompletion.copy(tracedProofOfAvailability =
            Traced(
              disseminationCompletion.tracedProofOfAvailability.value.copy(acks =
                Seq(AvailabilityAck(Node0, noSignature), AvailabilityAck(Node1, noSignature))
              )
            )
          )
      }
    }

    "the ack is already present" should {
      "change nothing" in {
        val membership = createMembership(Node0, Node1)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          )

        disseminationCompletion.addAck(AvailabilityAck(Node0, noSignature)) shouldBe
          disseminationCompletion
      }
    }

    "the ack is from a node not in the current membership" should {
      "change nothing" in {
        val membership = createMembership(Node0, Node1)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          )

        disseminationCompletion.addAck(AvailabilityAck(Node3, noSignature)) shouldBe
          disseminationCompletion
      }
    }
  }

  "Adding an ack to an in-progress dissemination" when {

    "the ack is not already present and there are still acks missing" should {
      "just add it" in {
        val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          )

        disseminationProgress.addAck(AvailabilityAck(Node1, noSignature)) shouldBe
          disseminationProgress.copy(acks =
            Set(AvailabilityAck(Node0, noSignature), AvailabilityAck(Node1, noSignature))
          )
      }
    }

    "the ack is not already present and a dissemination quorum is reached" should {
      "add the ack and complete the dissemination" in {
        val membership = createMembership(Node0, (Node0To3 - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          )

        disseminationProgress
          .addAck(AvailabilityAck(Node1, noSignature))
          .toEither
          .getOrElse(fail("should be complete"))
          .copy(readyForOrderingInstant = None) shouldBe
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
      }
    }

    "the ack is already present" should {
      "change nothing" in {
        val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          )

        disseminationProgress.addAck(AvailabilityAck(Node0, noSignature)) shouldBe
          disseminationProgress
      }
    }

    "the ack is from a node not in the current membership" should {
      "change nothing" in {
        val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0),
            AnEpochNumber,
            SomeStats,
          )

        disseminationProgress.addAck(AvailabilityAck(node(300), noSignature)) shouldBe
          disseminationProgress
      }
    }
  }

  "Updating the membership of an in-progress dissemination" when {

    "the topology has changed by removing a node but there is no regression" should {
      "only review the acks and recipients" in {
        val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership =
          Membership.forTesting(
            myId = Node0,
            EightNodes.diff(Set(Node0, Node1)),
          )

        val updated = disseminationProgress.changeMembership(newMembership)

        updated.membership shouldBe newMembership
        updated.recipients shouldBe newMembership.otherNodes
        updated.acks shouldBe Set(AvailabilityAck(Node0, noSignature))
      }
    }

    "the topology has changed by changing the acknowledgement key of a node but there is no regression" should {
      "only update the acks and recipients" in {
        val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership =
          Membership.forTesting(
            myId = Node0,
            membership.orderingTopology.copy(nodesTopologyInfo =
              membership.orderingTopology.nodesTopologyInfo.map { case (nodeId, nodeTopologyInfo) =>
                nodeId -> (if (nodeId == Node2)
                             NodeTopologyInfo(
                               Set(BftKeyId("newKey"))
                             )
                           else nodeTopologyInfo)
              }
            ),
          )

        val updated = disseminationProgress.changeMembership(newMembership)

        updated.membership shouldBe newMembership
        updated.recipients shouldBe newMembership.otherNodes
      }
    }

    "a node that acknowledged is removed and there is a regression" should {
      "regress and remove the ack from the node that has been removed" in {
        val membership = createMembership(Node0, Node1)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership =
          createMembership(Node0, FiveNodes.diff(Set(Node0, Node1)).toSeq*)

        disseminationProgress.changeMembership(newMembership) shouldBe
          DisseminationStatus.InProgress(
            newMembership,
            Traced(ABatchId),
            acks = Set(AvailabilityAck(Node0, noSignature)),
            epochNumber = AnEpochNumber,
            stats = SomeStats,
            disseminationRegressions = 1,
            previousAcks = Some(disseminationProgress.acks),
          )
      }
    }

    "the signing key of a node that acknowledged the batch has changed and the PoA is not valid anymore" should {
      "regress and remove the ack from the node that has changed its signing key" in {
        val membership = createMembership(Node0, (FiveNodes - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership =
          Membership.forTesting(
            myId = Node0,
            membership.orderingTopology.copy(nodesTopologyInfo =
              membership.orderingTopology.nodesTopologyInfo.map { case (nodeId, nodeTopologyInfo) =>
                nodeId -> (if (nodeId == Node1)
                             NodeTopologyInfo(
                               Set(BftKeyId("newKey"))
                             )
                           else nodeTopologyInfo)
              }
            ),
          )

        disseminationProgress.changeMembership(newMembership) shouldBe
          DisseminationStatus.InProgress(
            newMembership,
            Traced(ABatchId),
            acks = Set(AvailabilityAck(Node0, noSignature)),
            epochNumber = AnEpochNumber,
            stats = SomeStats,
            disseminationRegressions = 1,
            previousAcks = Some(disseminationProgress.acks),
          )
      }
    }

    "the signing key of the disseminating node has changed and the PoA is not valid anymore" should {
      "regress and remove the ack from the node that has changed its signing key" in {
        val membership = createMembership(Node0, (EightNodes - Node0).toSeq*)
        val disseminationProgress =
          createDisseminationProgress(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership =
          Membership.forTesting(
            myId = Node0,
            membership.orderingTopology.copy(nodesTopologyInfo =
              membership.orderingTopology.nodesTopologyInfo.map { case (nodeId, nodeTopologyInfo) =>
                nodeId -> (if (nodeId == Node0)
                             NodeTopologyInfo(
                               Set(BftKeyId("newKey"))
                             )
                           else nodeTopologyInfo)
              }
            ),
          )

        val expected =
          DisseminationStatus.InProgress(
            newMembership,
            Traced(ABatchId),
            acks = Set(AvailabilityAck(Node1, noSignature)),
            epochNumber = AnEpochNumber,
            stats = SomeStats,
            regressionsToSigning = 1,
            previousAcks = Some(disseminationProgress.acks),
          )
        disseminationProgress.changeMembership(newMembership) shouldBe expected
        expected.needsSigning shouldBe true
      }
    }
  }

  "Updating the membership of a complete dissemination" when {

    "the topology has changed by removing a node but the PoA is still valid" should {
      "only review the PoA" in {
        val membership = createMembership(Node0, Node1)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership = Membership.forTesting(myId = Node0, OrderingTopologyNode0)

        disseminationCompletion.changeMembership(newMembership) shouldBe
          disseminationCompletion.copy(
            membership = newMembership,
            tracedProofOfAvailability = disseminationCompletion.tracedProofOfAvailability.map(
              _.copy(acks = Seq(AvailabilityAck(Node0, noSignature)))
            ),
            previousAcks = Some(disseminationCompletion.acks),
          )
      }
    }

    "the topology has changed by changing the acknowledgement key of a node but the PoA is still valid" should {
      "only review the PoA and recipients" in {
        val membership = createMembership(Node0, Node1)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership = Membership.forTesting(
          myId = Node0,
          membership.orderingTopology.copy(nodesTopologyInfo =
            membership.orderingTopology.nodesTopologyInfo.map { case (nodeId, nodeTopologyInfo) =>
              nodeId -> (if (nodeId == Node1)
                           NodeTopologyInfo(
                             Set(BftKeyId("newKey"))
                           )
                         else nodeTopologyInfo)
            }
          ),
        )

        disseminationCompletion.changeMembership(newMembership) shouldBe
          disseminationCompletion.copy(
            membership = newMembership,
            tracedProofOfAvailability = disseminationCompletion.tracedProofOfAvailability.map(
              _.copy(acks = Seq(AvailabilityAck(Node0, noSignature)))
            ),
            previousAcks = Some(disseminationCompletion.acks),
          )
      }
    }

    "a node that acknowledged is removed and the PoA is not valid anymore" should {
      "regress and remove the ack from the node that has been removed" in {
        val membership = createMembership(Node0, Node1)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership =
          createMembership(Node0, FiveNodes.diff(Set(Node0, Node1)).toSeq*)

        disseminationCompletion.changeMembership(newMembership) shouldBe
          DisseminationStatus.InProgress(
            newMembership,
            Traced(ABatchId),
            acks = Set(AvailabilityAck(Node0, noSignature)),
            epochNumber = AnEpochNumber,
            stats = SomeStats,
            disseminationRegressions = 1,
            previousAcks = Some(disseminationCompletion.acks),
          )
      }
    }

    "the signing key of a node that acknowledged the batch has changed and the PoA is not valid anymore" should {
      "regress and remove the ack from the node that has changed its signing key" in {
        val membership = createMembership(Node0, (FiveNodes - Node0).toSeq*)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership =
          Membership.forTesting(
            myId = Node0,
            membership.orderingTopology.copy(nodesTopologyInfo =
              membership.orderingTopology.nodesTopologyInfo.map { case (nodeId, nodeTopologyInfo) =>
                nodeId -> (if (nodeId == Node1)
                             NodeTopologyInfo(
                               Set(BftKeyId("newKey"))
                             )
                           else nodeTopologyInfo)
              }
            ),
          )

        disseminationCompletion.changeMembership(newMembership) shouldBe
          DisseminationStatus.InProgress(
            newMembership,
            Traced(ABatchId),
            acks = Set(AvailabilityAck(Node0, noSignature)),
            epochNumber = AnEpochNumber,
            stats = SomeStats,
            disseminationRegressions = 1,
            previousAcks = Some(disseminationCompletion.acks),
          )
      }
    }

    "the signing key of the disseminating node has changed and the PoA is not valid anymore" should {
      "regress and remove the ack from the node that has changed its signing key" in {
        val membership = createMembership(Node0, (FiveNodes - Node0).toSeq*)
        val disseminationCompletion =
          createDisseminationCompletion(
            ABatchId,
            membership,
            acks = Set(Node0, Node1),
            AnEpochNumber,
            SomeStats,
          )
        val newMembership =
          Membership.forTesting(
            myId = Node0,
            membership.orderingTopology.copy(nodesTopologyInfo =
              membership.orderingTopology.nodesTopologyInfo.map { case (nodeId, nodeTopologyInfo) =>
                nodeId -> (if (nodeId == Node0)
                             NodeTopologyInfo(
                               Set(BftKeyId("newKey"))
                             )
                           else nodeTopologyInfo)
              }
            ),
          )

        val expected =
          DisseminationStatus.InProgress(
            newMembership,
            Traced(ABatchId),
            acks = Set(AvailabilityAck(Node1, noSignature)),
            epochNumber = AnEpochNumber,
            stats = SomeStats,
            regressionsToSigning = 1,
            previousAcks = Some(disseminationCompletion.acks),
          )
        disseminationCompletion.changeMembership(newMembership) shouldBe expected
        expected.needsSigning shouldBe true
      }
    }
  }
}

object DisseminationStatusTest {

  private val AnActivationTime = TestBootstrapTopologyActivationTime

  private val SomeStats = OrderingRequestBatchStats(0, 0)
  private val AnEpochNumber = EpochNumber.First
  private val FiveNodes = (0 to 4).map(i => BftNodeId(s"node$i")).toSet
  private val EightNodes = (0 to 7).map(i => BftNodeId(s"node$i")).toSet

  private def createDisseminationProgress(
      batchId: BatchId,
      membership: Membership,
      acks: Set[BftNodeId],
      epochNumber: EpochNumber,
      stats: OrderingRequestBatchStats,
  ): DisseminationStatus.InProgress =
    DisseminationStatus.InProgress(
      membership,
      Traced(batchId)(TraceContext.empty),
      acks.map(AvailabilityAck(_, noSignature)),
      epochNumber = epochNumber,
      stats = stats,
    )

  private def createDisseminationCompletion(
      batchId: BatchId,
      membership: Membership,
      acks: Set[BftNodeId],
      epochNumber: EpochNumber,
      stats: OrderingRequestBatchStats,
  ): DisseminationStatus.Complete =
    DisseminationStatus.Complete(
      membership,
      Traced(
        ProofOfAvailability(
          batchId,
          acks.map(AvailabilityAck(_, noSignature)).toSeq.sortBy(_.from),
          epochNumber,
        )
      )(TraceContext.empty),
      epochNumber = epochNumber,
      stats = stats,
    )

  private def createMembership(myId: BftNodeId, nodeIds: BftNodeId*): Membership =
    Membership.forTesting(
      myId,
      OrderingTopology(
        nodesTopologyInfo = (myId +: nodeIds)
          .map(
            _ -> NodeTopologyInfo(
              Set(BftKeyId(noSignature.authorizingLongTermKey.toProtoPrimitive))
            )
          )
          .toMap,
        SequencingParameters.Default, // irrelevant for this test
        defaultMaxBytesToDecompress, // irrelevant for this test
        AnActivationTime, // irrelevant for this test
        areTherePendingCantonTopologyChanges = Some(false), // irrelevant for this test
      ),
    )
}
