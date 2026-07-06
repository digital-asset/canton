// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.resource.{DbStorageSingle, Storage}
import com.typesafe.config.{Config, ConfigFactory}

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
      inProcessTeaServerName: String = "TeaGrpcInProcServer",
      projection: ProjectionConfig = ProjectionConfig(),
  ) extends TrafficEnforcementServerConfig {
    def pekkoConfig(storage: Storage): Config = storage match {
      case storage: DbStorageSingle =>
        storage.profile match {
          case _: Profile.H2 =>
            // H2 creates upper case tables but pekko expects lower case by default
            // this configures pekko to look for upper case ones
            ConfigFactory.parseString(
              """pekko.projection.slick.offset-store {
                |  use-lowercase-schema = false
                |  table = "PEKKO_PROJECTION_OFFSET_STORE"
                |  management-table = "PEKKO_PROJECTION_MANAGEMENT"
                |}""".stripMargin
            )
          case _ => ConfigFactory.empty()
        }
      case _ => ConfigFactory.empty()
    }
  }

  /** Config of the pekko projection in the internal traffic enforcement component
    * @param maxRetries
    *   max number of retries on projection failure
    * @param retryDelay
    *   delay between restarts of the projection when failing
    */
  final case class ProjectionConfig(
      maxRetries: PositiveInt = PositiveInt.MaxValue,
      retryDelay: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(1),
  )
}
