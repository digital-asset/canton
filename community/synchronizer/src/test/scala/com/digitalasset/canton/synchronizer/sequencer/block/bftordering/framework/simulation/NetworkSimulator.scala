// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.PlainTextP2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.P2PConnectionEventListener
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.network.{
  NetworkPartitionFault,
  SlowFaultState,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.DurationInt
import scala.util.Random

class NetworkSimulator(
    settings: NetworkSettings,
    topology: Topology[?, ?, ?, ?],
    agenda: Agenda,
    clock: Clock,
) {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private val random = new Random(settings.randomSeed)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var partitionState: NetworkPartitionFault =
    NetworkPartitionFault.NoCurrentPartition(clock.now)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var slowState: SlowFaultState =
    SlowFaultState.NoCurrentSlowness(settings.slowFaultSettings, clock, random)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var canUseFaults = true

  def tick(): Unit = if (canUseFaults) {
    partitionState = partitionState.tick(
      topology.activeNodes,
      settings.networkPartitionFaultSettings,
      clock,
      random,
    )
    slowState = slowState.tick(topology.activeNodes, settings.slowFaultSettings, clock, random)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  def scheduleNetworkEvent(
      from: BftNodeId,
      to: BftNodeId,
      msg: Any,
  ): Unit = {

    if (canUseFaults && settings.packetLoss.flipCoin(random)) {
      // this got dropped
      return
    }

    if (canUseFaults && partitionState.partitionExists(from, to)) {
      // there is a current partition, so drop
      return
    }

    val sendCount = 1 + (if (canUseFaults && settings.packetReplay.flipCoin(random)) 1 else 0)

    val delayFromSlowness = if (canUseFaults && slowState.haveSlowConnection(from, to)) {
      settings.slowFaultSettings.messageDelay.generateRandomDuration(random)
    } else {
      0.seconds
    }

    1 to sendCount foreach { _ =>
      val delay = settings.oneWayDelay
        .generateRandomDuration(random)
        .plus(delayFromSlowness)
      agenda.addOne(
        ReceiveNetworkMessage(to, msg),
        delay,
      )
    }
  }

  def scheduleEstablishConnection(
      from: BftNodeId,
      to: BftNodeId,
      maybeP2PEndpoint: Option[PlainTextP2PEndpoint],
      p2pConnectionEventListener: P2PConnectionEventListener,
      traceContext: TraceContext,
  ): Unit = {
    val delay = settings.establishConnectionDelay.generateRandomDuration(random)
    agenda.addOne(
      EstablishConnection(
        from,
        to,
        maybeP2PEndpoint,
        p2pConnectionEventListener,
        traceContext,
      ),
      delay,
    )
  }

  def makeHealthy(): Unit = {
    canUseFaults = false
    partitionState = partitionState.makeHealthy
    slowState = slowState.makeHealthy
  }
}
