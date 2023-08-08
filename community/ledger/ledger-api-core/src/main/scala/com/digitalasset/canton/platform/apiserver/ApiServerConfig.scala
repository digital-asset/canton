// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.daml.ports.Port
import com.digitalasset.canton.ledger.api.tls.TlsConfiguration
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.config.{CommandServiceConfig, UserManagementServiceConfig}
import com.digitalasset.canton.platform.localstore.IdentityProviderManagementConfig
import com.digitalasset.canton.platform.services.time.TimeProviderType

import scala.concurrent.duration.*

final case class ApiServerConfig(
    address: Option[String] =
      ApiServerConfig.DefaultAddress, // This defaults to "localhost" when set to `None`.
    apiStreamShutdownTimeout: Duration = ApiServerConfig.DefaultApiStreamShutdownTimeout,
    command: CommandServiceConfig = ApiServerConfig.DefaultCommandServiceConfig,
    configurationLoadTimeout: Duration = ApiServerConfig.DefaultConfigurationLoadTimeout,
    managementServiceTimeout: FiniteDuration = ApiServerConfig.DefaultManagementServiceTimeout,
    maxInboundMessageSize: Int = ApiServerConfig.DefaultMaxInboundMessageSize,
    port: Port = ApiServerConfig.DefaultPort,
    rateLimit: Option[RateLimitingConfig] = ApiServerConfig.DefaultRateLimitingConfig,
    seeding: Seeding = ApiServerConfig.DefaultSeeding,
    timeProviderType: TimeProviderType = ApiServerConfig.DefaultTimeProviderType,
    tls: Option[TlsConfiguration] = ApiServerConfig.DefaultTls,
    userManagement: UserManagementServiceConfig = ApiServerConfig.DefaultUserManagement,
    identityProviderManagement: IdentityProviderManagementConfig =
      ApiServerConfig.DefaultIdentityProviderManagementConfig,
)

object ApiServerConfig {
  val DefaultPort: Port = Port(6865)
  val DefaultAddress: Option[String] = None
  val DefaultTls: Option[TlsConfiguration] = None
  val DefaultMaxInboundMessageSize: Int = 64 * 1024 * 1024
  val DefaultConfigurationLoadTimeout: Duration = 10.seconds
  val DefaultSeeding: Seeding = Seeding.Strong
  val DefaultManagementServiceTimeout: FiniteDuration = 2.minutes
  val DefaultUserManagement: UserManagementServiceConfig =
    UserManagementServiceConfig.default(enabled = false)
  val DefaultIdentityProviderManagementConfig: IdentityProviderManagementConfig =
    IdentityProviderManagementConfig()
  val DefaultCommandServiceConfig: CommandServiceConfig = CommandServiceConfig.Default
  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock
  val DefaultApiStreamShutdownTimeout: FiniteDuration = FiniteDuration(5, "seconds")
  val DefaultRateLimitingConfig: Option[RateLimitingConfig] = Some(RateLimitingConfig.Default)
}
