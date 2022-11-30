// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.DeprecatedConfigUtils.DeprecatedFieldsFor
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.version.ProtocolVersion

trait NodeConfig {
  def clientAdminApi: ClientConfig
}

object LocalNodeConfig {

  // TODO(i10108): remove when backwards compatibility can be discarded
  /** Deprecations for LocalNodeConfig.
    */
  trait LocalNodeConfigDeprecationImplicits {
    implicit def deprecatedLocalNodeConfig[X <: LocalNodeConfig]: DeprecatedFieldsFor[X] =
      new DeprecatedFieldsFor[LocalNodeConfig] {
        override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
          DeprecatedConfigUtils.MovedConfigPath(
            "init.startup-fail-fast",
            "storage.parameters.fail-fast-on-startup",
          ),
          DeprecatedConfigUtils.MovedConfigPath(
            "storage.fail-fast-on-startup",
            "storage.parameters.fail-fast-on-startup",
          ),
          DeprecatedConfigUtils.MovedConfigPath(
            "storage.max-connections",
            "storage.parameters.max-connections",
          ),
          DeprecatedConfigUtils.MovedConfigPath(
            "storage.ledger-api-jdbc-url",
            "storage.parameters.ledger-api-jdbc-url",
          ),
          DeprecatedConfigUtils.MovedConfigPath(
            "storage.connection-timeout",
            "storage.parameters.connection-timeout",
          ),
        )
      }
  }
}
trait LocalNodeConfig extends NodeConfig {

  /** Human readable name for the type of node used for displaying config error messages */
  val nodeTypeName: String

  def init: InitConfigBase
  def adminApi: AdminServerConfig
  def storage: StorageConfig
  def crypto: CryptoConfig
  def sequencerClient: SequencerClientConfig

  /** Various cache sizes */
  def caching: CachingConfigs

}

trait CommunityLocalNodeConfig extends LocalNodeConfig {
  override def storage: CommunityStorageConfig
}

trait LocalNodeParameters {

  def tracing: TracingConfig
  def delayLoggingThreshold: NonNegativeFiniteDuration
  def logQueryCost: Option[QueryCostMonitoringConfig]
  def loggingConfig: LoggingConfig
  def enableAdditionalConsistencyChecks: Boolean
  def enablePreviewFeatures: Boolean
  def processingTimeouts: ProcessingTimeout
  def sequencerClient: SequencerClientConfig
  def cachingConfigs: CachingConfigs
  def nonStandardConfig: Boolean
  def devVersionSupport: Boolean
  def dontWarnOnDeprecatedPV: Boolean

  /** The initial protocol version before connected to any domain, e.g., when creating the initial topology transactions. */
  def initialProtocolVersion: ProtocolVersion

}
