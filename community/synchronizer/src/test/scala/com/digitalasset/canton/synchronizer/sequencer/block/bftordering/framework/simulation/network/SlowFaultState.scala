// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.network

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.network.SlowFaultState.NoMoreSlowness
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  BrokenLink,
  SlowFaultSettings,
}
import com.digitalasset.canton.time.Clock

import scala.util.Random

/** [[SlowFaultState]] is a fault that when active will add extra delay for some nodes. If it is
  * enabled there will periods where a selection of nodes (based on [[PartitionMode]] ) will
  * experience the delay.
  */
sealed trait SlowFaultState {
  def tick(
      nodes: Set[BftNodeId],
      settings: SlowFaultSettings,
      clock: Clock,
      random: Random,
  ): SlowFaultState

  def haveSlowConnection(from: BftNodeId, to: BftNodeId): Boolean

  def makeHealthy: SlowFaultState = NoMoreSlowness
}

object SlowFaultState {
  final case class NoCurrentSlowness(endTime: CantonTimestamp) extends SlowFaultState {

    override def tick(
        nodes: Set[BftNodeId],
        settings: SlowFaultSettings,
        clock: Clock,
        random: Random,
    ): SlowFaultState =
      if (clock.now.isAfter(endTime)) {
        ActiveSlowness(
          settings.partitionMode
            .makePartition(nodes, settings.partitionSymmetry, random),
          clock.now.add(
            settings.faultDistribution.generateRandomDuration(random)
          ),
        )
      } else { this }

    override def haveSlowConnection(from: BftNodeId, to: BftNodeId): Boolean = false
  }

  object NoCurrentSlowness {
    def apply(settings: SlowFaultSettings, clock: Clock, random: Random): SlowFaultState =
      if (settings.enabled) {
        NoCurrentSlowness(
          clock.now.add(
            settings.noFaultsDistribution.generateRandomDuration(random)
          )
        )
      } else {
        NoMoreSlowness
      }
  }

  final case class ActiveSlowness(nodes: Set[BrokenLink], endTime: CantonTimestamp)
      extends SlowFaultState {
    override def tick(
        nodes: Set[BftNodeId],
        settings: SlowFaultSettings,
        clock: Clock,
        random: Random,
    ): SlowFaultState = if (clock.now.isAfter(endTime)) {
      NoCurrentSlowness(settings, clock, random)
    } else { this }

    override def haveSlowConnection(from: BftNodeId, to: BftNodeId): Boolean =
      nodes.contains(BrokenLink(from, to))
  }

  case object NoMoreSlowness extends SlowFaultState {
    override def tick(
        nodes: Set[BftNodeId],
        settings: SlowFaultSettings,
        clock: Clock,
        random: Random,
    ): SlowFaultState = this

    override def haveSlowConnection(from: BftNodeId, to: BftNodeId): Boolean = false
  }
}
