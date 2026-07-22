// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.network

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.network.NetworkPartitionFault.NoMorePartitions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  BrokenLink,
  NetworkPartitionFaultSettings,
}
import com.digitalasset.canton.time.Clock

import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Random

sealed trait NetworkPartitionFault {

  def tick(
      nodes: Set[BftNodeId],
      settings: NetworkPartitionFaultSettings,
      clock: Clock,
      random: Random,
  ): NetworkPartitionFault

  def partitionExists(node1: BftNodeId, node2: BftNodeId): Boolean

  def makeHealthy: NetworkPartitionFault = NoMorePartitions
}

object NetworkPartitionFault {

  case object NoMorePartitions extends NetworkPartitionFault {

    override def tick(
        nodes: Set[BftNodeId],
        settings: NetworkPartitionFaultSettings,
        clock: Clock,
        random: Random,
    ): NetworkPartitionFault = this

    override def partitionExists(node1: BftNodeId, node2: BftNodeId): Boolean = false
  }

  final case class NoCurrentPartition(started: CantonTimestamp) extends NetworkPartitionFault {

    private def shouldCreatePartition(
        settings: NetworkPartitionFaultSettings,
        clock: Clock,
        random: Random,
    ): Boolean =
      clock.now.isAfter(
        started.plus(settings.unPartitionStability.toJava)
      ) && settings.partitionProbability.flipCoin(random)

    private def makePartition(
        nodes: Set[BftNodeId],
        settings: NetworkPartitionFaultSettings,
        random: Random,
    ): Set[BrokenLink] =
      settings.partitionMode.makePartition(nodes, settings.partitionSymmetry, random)

    override def tick(
        nodes: Set[BftNodeId],
        settings: NetworkPartitionFaultSettings,
        clock: Clock,
        random: Random,
    ): NetworkPartitionFault =
      if (shouldCreatePartition(settings, clock, random)) {
        NetworkPartitionFault.ActivePartition(
          makePartition(nodes, settings, random),
          clock.now,
        )
      } else { this }

    override def partitionExists(node1: BftNodeId, node2: BftNodeId): Boolean = false
  }

  private final case class ActivePartition(partition: Set[BrokenLink], started: CantonTimestamp)
      extends NetworkPartitionFault {

    private def shouldRemovePartition(
        settings: NetworkPartitionFaultSettings,
        clock: Clock,
        random: Random,
    ): Boolean =
      clock.now.isAfter(
        started.plus(settings.partitionStability.toJava)
      ) && settings.unPartitionProbability.flipCoin(random)

    override def tick(
        nodes: Set[BftNodeId],
        settings: NetworkPartitionFaultSettings,
        clock: Clock,
        random: Random,
    ): NetworkPartitionFault =
      if (shouldRemovePartition(settings, clock, random)) {
        NetworkPartitionFault.NoCurrentPartition(clock.now)
      } else { this }

    override def partitionExists(node1: BftNodeId, node2: BftNodeId): Boolean =
      partition.contains(BrokenLink(node1, node2))
  }
}
