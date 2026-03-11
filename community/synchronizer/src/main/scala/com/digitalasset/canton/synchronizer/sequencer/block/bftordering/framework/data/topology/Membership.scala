// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.google.common.annotations.VisibleForTesting

final case class Membership(
    myId: BftNodeId,
    orderingTopology: OrderingTopology,
    leaders: Seq[BftNodeId],
) extends PrettyPrinting {
  val otherNodes: Set[BftNodeId] = orderingTopology.nodes - myId
  lazy val sortedNodes: Seq[BftNodeId] = orderingTopology.sortedNodes

  override protected def pretty: Pretty[Membership.this.type] =
    prettyOfClass(
      param("myId", _.myId.doubleQuoted),
      param("orderingTopology", _.orderingTopology),
      param("leaders", _.leaders.map(_.doubleQuoted)),
    )
}

object Membership {

  // Simple constructors for tests so that we don't have to provide a full ordering topology.

  @VisibleForTesting
  private[bftordering] def forTesting(
      myId: BftNodeId,
      otherNodes: Set[BftNodeId] = Set.empty,
      sequencingParameters: SequencingParameters = SequencingParameters.Default,
      leaders: Option[Seq[BftNodeId]] = None,
      nodesTopologyInfos: Map[BftNodeId, NodeTopologyInfo] = Map.empty,
  ): Membership = {
    val orderingTopology = OrderingTopology.forTesting(
      otherNodes + myId,
      sequencingParameters,
      nodesTopologyInfos = nodesTopologyInfos,
    )
    val nodes = orderingTopology.sortedNodes
    Membership(myId, orderingTopology, leaders.getOrElse(nodes))
  }

  @VisibleForTesting
  private[bftordering] def forTesting(
      myId: BftNodeId,
      orderingTopology: OrderingTopology,
  ): Membership =
    Membership(myId, orderingTopology, orderingTopology.sortedNodes)
}
