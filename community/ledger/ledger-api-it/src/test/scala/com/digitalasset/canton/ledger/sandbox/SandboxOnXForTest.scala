// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.sandbox

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ParticipantId
import com.daml.lf.engine.{EngineConfig as _EngineConfig}
import com.daml.lf.language.LanguageVersion
import com.daml.metrics.Metrics
import com.daml.ports.Port
import com.digitalasset.canton.ledger.api.auth.{AuthService, AuthServiceWildcard, JwtVerifierLoader}
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.ledger.runner.common.Config
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.platform.apiserver.{
  ApiServerConfig as _ApiServerConfig,
  AuthServiceConfig,
}
import com.digitalasset.canton.platform.config.MetricsConfig.MetricRegistryType
import com.digitalasset.canton.platform.config.{
  MetricsConfig,
  ParticipantConfig as _ParticipantConfig,
}
import com.digitalasset.canton.platform.configuration.InitialLedgerConfiguration
import com.digitalasset.canton.platform.indexer.{IndexerConfig as _IndexerConfig}
import com.digitalasset.canton.platform.localstore.UserManagementConfig
import com.digitalasset.canton.platform.store.DbSupport.ParticipantDataSourceConfig

import java.time.Duration
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

object SandboxOnXForTest {
  val EngineConfig: _EngineConfig = _EngineConfig(
    allowedLanguageVersions = LanguageVersion.StableVersions,
    profileDir = None,
    stackTraceMode = true,
    forbidV0ContractId = true,
  )
  val DevEngineConfig: _EngineConfig = EngineConfig.copy(
    allowedLanguageVersions = LanguageVersion.DevVersions
  )

  val ApiServerConfig = _ApiServerConfig().copy(
    initialLedgerConfiguration = Some(
      InitialLedgerConfiguration(
        maxDeduplicationDuration = Duration.ofMinutes(30L),
        avgTransactionLatency =
          Configuration.reasonableInitialConfiguration.timeModel.avgTransactionLatency,
        minSkew = Configuration.reasonableInitialConfiguration.timeModel.minSkew,
        maxSkew = Configuration.reasonableInitialConfiguration.timeModel.maxSkew,
        delayBeforeSubmitting = Duration.ZERO,
      )
    ),
    userManagement = UserManagementConfig.default(true),
    maxInboundMessageSize = 4194304,
    configurationLoadTimeout = 10000.millis,
    seeding = Seeding.Strong,
    port = Port.Dynamic,
    managementServiceTimeout = 120000.millis,
  )

  val IndexerConfig = _IndexerConfig(
    inputMappingParallelism = 512
  )

  val ParticipantId: Ref.ParticipantId = Ref.ParticipantId.assertFromString("sandbox-participant")
  val ParticipantConfig: _ParticipantConfig =
    _ParticipantConfig(
      apiServer = ApiServerConfig,
      indexer = IndexerConfig,
    )

  def singleParticipant(
      apiServerConfig: _ApiServerConfig = ApiServerConfig,
      indexerConfig: _IndexerConfig = IndexerConfig,
      authentication: AuthServiceConfig = AuthServiceConfig.Wildcard,
  ) = Map(
    ParticipantId -> ParticipantConfig.copy(
      apiServer = apiServerConfig,
      indexer = indexerConfig,
      authentication = authentication,
    )
  )

  def dataSource(jdbcUrl: String): Map[ParticipantId, ParticipantDataSourceConfig] = Map(
    ParticipantId -> ParticipantDataSourceConfig(jdbcUrl)
  )

  val Default: Config = Config(
    engine = EngineConfig,
    dataSource = Config.Default.dataSource.map { case _ -> value => (ParticipantId, value) },
    participants = singleParticipant(),
    metrics = MetricsConfig.DefaultMetricsConfig.copy(registryType = MetricRegistryType.New),
  )

  class ConfigAdaptor(
      authServiceOverwrite: Option[AuthService],
      jwtVerifierLoaderOverwrite: Option[JwtVerifierLoader],
  ) extends BridgeConfigAdaptor {
    override def authService(participantConfig: _ParticipantConfig): AuthService = {
      authServiceOverwrite.getOrElse(AuthServiceWildcard)
    }

    override def jwtVerifierLoader(
        participantConfig: _ParticipantConfig,
        metrics: Metrics,
        executionContext: ExecutionContext,
    ): JwtVerifierLoader = jwtVerifierLoaderOverwrite.getOrElse(
      super.jwtVerifierLoader(participantConfig, metrics, executionContext)
    )
  }
  object ConfigAdaptor {
    def apply(
        authServiceOverwrite: Option[AuthService],
        jwtVerifierLoaderOverwrite: Option[JwtVerifierLoader] = None,
    ) = new ConfigAdaptor(authServiceOverwrite, jwtVerifierLoaderOverwrite)
  }

  def defaultH2SandboxJdbcUrl() =
    s"jdbc:h2:mem:sandbox-${UUID.randomUUID().toString};db_close_delay=-1"

}
