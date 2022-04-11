// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout, QueryCostMonitoringConfig}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.resource.DbStorage.{DbAction, DbStorageCreationException}
import com.digitalasset.canton.tracing.TraceContext
import slick.jdbc.JdbcBackend.Database

import scala.concurrent.{ExecutionContext, Future}

/** DB Storage implementation that assumes a single process accessing the underlying database. */
class DbStorageSingle private (
    override val profile: DbStorage.Profile,
    override val dbConfig: DbConfig,
    db: Database,
    override val metrics: DbStorageMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends DbStorage
    with FlagCloseable
    with NamedLogging {

  override protected[canton] def runRead[A](
      action: DbAction.ReadTransactional[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    run(operationName, maxRetries)(db.run(action))

  override protected[canton] def runWrite[A](
      action: DbAction.All[A],
      operationName: String,
      maxRetries: Int,
  )(implicit traceContext: TraceContext, closeContext: CloseContext): Future[A] =
    run(operationName, maxRetries)(db.run(action))

  override def onClosed(): Unit = {
    db.close()
  }

  override def isActive: Boolean = true
}

object DbStorageSingle {
  def tryCreate(
      config: DbConfig,
      connectionPoolForParticipant: Boolean,
      logQueryCost: Option[QueryCostMonitoringConfig],
      metrics: DbStorageMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      retryConfig: DbStorage.RetryConfig = DbStorage.RetryConfig.failFast,
  )(implicit ec: ExecutionContext, closeContext: CloseContext): DbStorageSingle =
    create(
      config,
      connectionPoolForParticipant,
      logQueryCost,
      metrics,
      timeouts,
      loggerFactory,
      retryConfig,
    )
      .valueOr(err => throw new DbStorageCreationException(err))
      .onShutdown(throw new DbStorageCreationException("Shutdown during creation"))

  def create(
      config: DbConfig,
      connectionPoolForParticipant: Boolean,
      logQueryCost: Option[QueryCostMonitoringConfig],
      metrics: DbStorageMetrics,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      retryConfig: DbStorage.RetryConfig = DbStorage.RetryConfig.failFast,
  )(implicit
      ec: ExecutionContext,
      closeContext: CloseContext,
  ): EitherT[UnlessShutdown, String, DbStorageSingle] =
    for {
      db <- DbStorage.createDatabase(
        config,
        connectionPoolForParticipant,
        withWriteConnectionPool = false,
        withMainConnection = false,
        Some(metrics.queue),
        logQueryCost,
        retryConfig = retryConfig,
      )(loggerFactory)
      profile = DbStorage.profile(config)
      storage = new DbStorageSingle(profile, config, db, metrics, timeouts, loggerFactory)
    } yield storage

}
