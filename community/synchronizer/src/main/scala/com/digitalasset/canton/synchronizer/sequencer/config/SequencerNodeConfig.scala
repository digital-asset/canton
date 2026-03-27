// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.*
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.config.{DeclarativeSequencerConfig, PublicServerConfig}
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.SequencerHighAvailabilityConfig
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.synchronizer.sequencer.{SequencerConfig, SequencerHealthConfig}
import monocle.macros.syntax.lens.*

/** Configuration parameters for a single sequencer node
  *
  * @param init
  *   determines how this node is initialized
  * @param publicApi
  *   The configuration for the public sequencer API
  * @param adminApi
  *   parameters of the interface used to administrate the sequencer
  * @param storage
  *   determines how the sequencer stores this state
  * @param crypto
  *   determines the algorithms used for signing, hashing, and encryption
  * @param sequencer
  *   determines the type of sequencer
  * @param health
  *   Health check related sequencer config
  * @param monitoring
  *   Monitoring configuration for a canton node.
  * @param parameters
  *   general sequencer node parameters
  * @param replication
  *   replication configuration used for node startup
  * @param topology
  *   configuration for the topology service of the sequencer
  * @param trafficConfig
  *   Configuration for the traffic purchased entry manager.
  */
final case class SequencerNodeConfig(
    override val init: InitConfig = InitConfig(),
    publicApi: PublicServerConfig = PublicServerConfig(),
    override val adminApi: AdminServerConfig = AdminServerConfig(),
    override val storage: StorageConfig = StorageConfig.Memory(),
    override val crypto: CryptoConfig = CryptoConfig(),
    sequencer: SequencerConfig = SequencerConfig.default,
    timeTracker: SynchronizerTimeTrackerConfig = SynchronizerTimeTrackerConfig(),
    override val sequencerClient: SequencerClientConfig = SequencerClientConfig(),
    override val parameters: SequencerNodeParameterConfig = SequencerNodeParameterConfig(),
    health: SequencerHealthConfig = SequencerHealthConfig(),
    override val monitoring: NodeMonitoringConfig = NodeMonitoringConfig(),
    replication: Option[ReplicationConfig] = None,
    override val topology: TopologyConfig = TopologyConfig(),
    trafficConfig: SequencerTrafficConfig = SequencerTrafficConfig(),
    // 45 seconds before the default ack interval on participants is 1 minute
    // So slightly below to maximize the chance of them going through
    acknowledgementsConflateWindow: Option[PositiveFiniteDuration] = Some(
      PositiveFiniteDuration.ofSeconds(45)
    ),
    declarative: DeclarativeSequencerConfig = DeclarativeSequencerConfig(),
) extends LocalNodeConfig
    with ConfigDefaults[Option[DefaultPorts], SequencerNodeConfig] {

  override def clientAdminApi: ClientConfig = adminApi.clientConfig

  override def nodeTypeName: String = "sequencer"

  def toRemoteConfig: RemoteSequencerConfig =
    RemoteSequencerConfig(
      adminApi.clientConfig,
      publicApi.clientConfig,
      monitoring.grpcHealthServer.map(_.toRemoteConfig),
    )

  override def withDefaults(
      ports: Option[DefaultPorts]
  ): SequencerNodeConfig =
    ports
      .fold(this)(ports =>
        this
          .focus(_.publicApi.internalPort)
          .modify(ports.sequencerPublicApiPort.setDefaultPort)
          .focus(_.adminApi.internalPort)
          .modify(ports.sequencerAdminApiPort.setDefaultPort)
      )
      .focus(_.sequencer)
      .modify {
        case db: SequencerConfig.Database =>
          val enabled =
            ReplicationConfig.withDefault(storage, db.highAvailability.flatMap(_.enabled))
          db
            .focus(_.highAvailability)
            .modify(
              _.map(_.copy(enabled = enabled))
                .orElse(
                  enabled.map(enabled => SequencerHighAvailabilityConfig(enabled = Some(enabled)))
                )
            )
        case other => other

      }
      .focus(_.replication)
      // Even though the block sequencer does not support replicas, want to turn on replication
      // to take care of cases when more than one instance of the sequencer is started with the same
      // database setup and identity (typical in Kubernetes setups) and allow simultaneous startup of one instance
      // while another is shutting down. The exception is the reference sequencer, which relies on multiple nodes
      // using the same database for ordering blocks and handles that on a different level of the code.
      .modify(replication =>
        sequencer match {
          // TODO(#29603): remove this logic once reference sequencer is gone. The reference sequencer does not
          case SequencerConfig.External(sequencerType, _, _) if sequencerType == "reference" => None
          case _ => ReplicationConfig.withDefaultO(storage, replication)
        }
      )

}
