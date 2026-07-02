// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

/** Base configuration for the user traffic enforcement in the participant node. Currently, only the
  * internal, in-process server variant is supported, allowing for later an external TEA server to
  * be configured as well.
  *
  * @param enabled
  *   Whether to enable the traffic enforcement feature. Note: This feature is experimental,
  *   unstable and is disabled by default. Enabling it in production environments is not
  *   recommended.
  * @param trafficEnforcementServer
  *   The configuration for the connection to the traffic server. Currently, only the internal,
  *   in-process server variant is supported.
  */
final case class TrafficEnforcementConfig(
    enabled: Boolean = false,
    trafficEnforcementServer: TrafficEnforcementServerConfig =
      TrafficEnforcementServerConfig.Internal(),
)

sealed trait TrafficEnforcementServerConfig

object TrafficEnforcementServerConfig {

  /** Configuration for the internal, in-process traffic enforcement server.
    *
    * @param inProcessTeaServerName
    *   The name of the in-process gRPC serving the traffic service API of the traffic enforcement
    *   server
    */
  final case class Internal(
      inProcessTeaServerName: String = "TeaGrpcInProcServer"
  ) extends TrafficEnforcementServerConfig
}
