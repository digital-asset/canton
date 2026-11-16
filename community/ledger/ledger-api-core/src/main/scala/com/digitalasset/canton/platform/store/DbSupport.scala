// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.config.{DbParametersConfig, QueryCostMonitoringConfig}
import com.digitalasset.canton.health.ReportsHealth
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{DbQueueMetrics, LedgerApiServerMetrics}
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.platform.store.backend.h2.H2StorageBackendFactory
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.backend.{
  DataSourceStorageBackend,
  StorageBackendFactory,
  VerifiedDataSource,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

final case class DbSupport(
    dbDispatcher: DbDispatcher with ReportsHealth,
    storageBackendFactory: StorageBackendFactory,
)

object DbSupport {

  final case class ParticipantDataSourceConfig(jdbcUrl: String)

  final case class DataSourceProperties(
      connectionPool: ConnectionPoolConfig,
      postgres: PostgresDataSourceConfig = PostgresDataSourceConfig(),
  ) {
    def createDbConfig(config: ParticipantDataSourceConfig): DbConfig = DbConfig(
      jdbcUrl = config.jdbcUrl,
      connectionPool = connectionPool,
      postgres = postgres,
    )
  }

  final case class ConnectionPoolConfig(
      connectionPoolSize: Int,
      connectionTimeout: FiniteDuration,
  )

  final case class DbConfig(
      jdbcUrl: String,
      connectionPool: ConnectionPoolConfig,
      postgres: PostgresDataSourceConfig = PostgresDataSourceConfig(),
  ) {
    def dataSourceConfig: DataSourceStorageBackend.DataSourceConfig =
      DataSourceStorageBackend.DataSourceConfig(
        jdbcUrl = jdbcUrl,
        postgresConfig = postgres,
      )
  }

  def owner(
      dbConfig: DbConfig,
      serverRole: ServerRole,
      metrics: LedgerApiServerMetrics,
      disableMergeAndHashJoins: Boolean,
      dbQueueMetricsOpt: Option[DbQueueMetrics],
      logQueryCostOpt: Option[QueryCostMonitoringConfig],
      dbParametersConfigOpt: Option[DbParametersConfig],
      schedulerOpt: Option[ScheduledExecutorService],
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): ResourceOwner[DbSupport] = {
    val dbType = DbType.jdbcType(dbConfig.jdbcUrl)
    val storageBackendFactory = StorageBackendFactory.of(
      dbType = dbType,
      disableMergeAndHashJoins = disableMergeAndHashJoins,
      loggerFactory = loggerFactory,
    )
    val dataSourceStorageBackend = storageBackendFactory.createDataSourceStorageBackend
    for {
      dataSource <- ResourceOwner.forFuture(() =>
        VerifiedDataSource(dataSourceStorageBackend, dbConfig.dataSourceConfig, loggerFactory)
      )
      dbDispatcher <- DbDispatcher
        .owner(
          dataSource = dataSource,
          serverRole = serverRole,
          connectionPoolSize =
            if (dbType.supportsParallelWrites) dbConfig.connectionPool.connectionPoolSize
            else 1,
          connectionTimeout = dbConfig.connectionPool.connectionTimeout,
          metrics = metrics,
          dbQueueMetricsOpt = dbQueueMetricsOpt,
          logQueryCostOpt = logQueryCostOpt,
          dbParametersConfigOpt = dbParametersConfigOpt,
          schedulerOpt = schedulerOpt,
          loggerFactory = loggerFactory,
        )
    } yield DbSupport(
      dbDispatcher = dbDispatcher,
      storageBackendFactory = storageBackendFactory,
    )
  }

  def forH2DbStorage(
      h2DbStorage: DbStorage,
      metrics: LedgerApiServerMetrics,
      loggerFactory: NamedLoggerFactory,
  ): DbSupport =
    DbSupport(
      dbDispatcher = DbDispatcher.ofDbStorage(
        dbStorage = h2DbStorage,
        overallWaitTimer = metrics.index.db.waitAll,
        overallExecutionTimer = metrics.index.db.execAll,
        loggerFactory = loggerFactory,
      ),
      storageBackendFactory = H2StorageBackendFactory(loggerFactory),
    )
}
