// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.lifecycle.HasSynchronizeWithClosing
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.Success

import javax.sql.DataSource
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Using}

/** Returns a DataSource that is guaranteed to be connected to a responsive, compatible database. */
object VerifiedDataSource {

  private val MaxInitialConnectRetryAttempts: Int = 600

  def apply(jdbcUrl: String, loggerFactory: NamedLoggerFactory)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DataSource] = {
    val dataSourceStorageBackend =
      StorageBackendFactory
        .of(dbType = DbType.jdbcType(jdbcUrl), loggerFactory = loggerFactory)
        .createDataSourceStorageBackend
    apply(
      dataSourceStorageBackend,
      DataSourceStorageBackend.DataSourceConfig(jdbcUrl),
      loggerFactory,
    )
  }

  def apply(
      dataSourceStorageBackend: DataSourceStorageBackend,
      dataSourceConfig: DataSourceStorageBackend.DataSourceConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[DataSource] = {
    val logger = TracedLogger(loggerFactory.getLogger(getClass))
    implicit val success: Success[Any] = retry.Success.always
    for {
      dataSource <- retry
        .Pause(
          logger = logger,
          operationName = "Connect to database",
          maxRetries = MaxInitialConnectRetryAttempts,
          delay = 1.second,
          hasSynchronizeWithClosing = HasSynchronizeWithClosing.NeverClosing,
        )
        .applyFut(
          Future {
            val createdDatasource =
              dataSourceStorageBackend.createDataSource(dataSourceConfig, loggerFactory)
            logger.info("Attempting to connect to the database")
            Using.resource(createdDatasource.getConnection)(
              dataSourceStorageBackend.checkDatabaseAvailable
            )
            createdDatasource
          }.thereafterP { case Failure(exception) =>
            logger.warn(exception.getMessage)
          },
          retry.AllExceptionRetryPolicy,
        )
      _ <- Future {
        Using.resource(dataSource.getConnection)(
          dataSourceStorageBackend.checkCompatibility
        )
      }
    } yield dataSource
  }

}
