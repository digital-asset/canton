// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.crypto.FingerprintKeyId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.{
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.endpointToTestBftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochLength,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.NodeTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.future.SimulationFuture
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.TestBootstrapTopologyActivationTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.util.Success

class SimulationOrderingTopologyProvider(
    thisNode: BftNodeId,
    epochLength: EpochLength, // TODO(#24184) make this dynamic sequencing parameter
    getEndpointsToTopologyData: () => Map[P2PEndpoint, NodeSimulationTopologyData],
    loggerFactory: NamedLoggerFactory,
)(implicit synchronizerProtocolVersion: ProtocolVersion)
    extends OrderingTopologyProvider[SimulationEnv] {

  override def getOrderingTopologyAt(
      activationTimeO: Option[TopologyActivationTime],
      checkPendingChanges: Boolean,
  )(implicit
      traceContext: TraceContext
  ): SimulationFuture[Option[(OrderingTopology, CryptoProvider[SimulationEnv])]] = {
    val activationTime = activationTimeO.getOrElse(TestBootstrapTopologyActivationTime)
    SimulationFuture(s"getOrderingTopologyAt($activationTime)") { () =>
      val activeSequencerTopologyData = getActiveSequencerTopologyData(activationTime)

      val topology =
        OrderingTopology(
          activeSequencerTopologyData.view.mapValues { simulationTopologyData =>
            NodeTopologyInfo(
              keyIds = simulationTopologyData
                .keysForTimestamp(activationTime.value)
                .view
                .map(keyPair => FingerprintKeyId.toBftKeyId(keyPair.publicKey.id))
                .toSet
            )
          }.toMap,
          epochLength, // TODO(#24184) make this dynamic sequencing parameter
          SequencingParameters.Default,
          BaseTest.defaultMaxBytesToDecompress,
          activationTime,
          // Switch the value deterministically so that we trigger all code paths.
          areTherePendingCantonTopologyChanges =
            Option.when(checkPendingChanges)(activationTime.value.toMicros % 2 == 0),
        )
      Success(
        Some(
          topology -> SimulationCryptoProvider.create(
            thisNode,
            activeSequencerTopologyData,
            activationTime.value,
            loggerFactory,
          )
        )
      )
    }
  }

  override def getFirstKnownAt(activationTime: TopologyActivationTime)(implicit
      traceContext: TraceContext
  ): SimulationFuture[Option[Map[BftNodeId, TopologyActivationTime]]] =
    SimulationFuture(s"getFirstKnownAt($activationTime)") { () =>
      Success(
        Some(
          getActiveSequencerTopologyData(activationTime).view.mapValues { simulationTopologyData =>
            simulationTopologyData.onboardingTime
          }.toMap
        )
      )
    }

  private def getActiveSequencerTopologyData(
      activationTime: TopologyActivationTime
  ): Map[BftNodeId, NodeSimulationTopologyData] =
    getEndpointsToTopologyData().view
      .filter { case (_, topologyData) =>
        topologyData.onboardingTime.value <= activationTime.value
        && topologyData.offboardingTime.forall(activationTime.value <= _)
      }
      .map { case (endpoint, topologyData) =>
        endpointToTestBftNodeId(endpoint) -> topologyData
      }
      .toMap
}
