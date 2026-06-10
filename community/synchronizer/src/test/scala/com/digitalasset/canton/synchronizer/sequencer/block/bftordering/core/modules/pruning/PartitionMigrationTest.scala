// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning

import cats.data.EitherT
import com.digitalasset.canton.config.{
  DbConfig,
  DbParametersConfig,
  PartitionConfig,
  ProcessingTimeout,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbMigrations, DbStorage}
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.db.{MigrationMode, PostgresTest}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.PartitionMigrationTest.DbMigrationsTargeted
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ResourceUtil
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class PartitionMigrationTest extends AsyncWordSpec with BftSequencerBaseTest with PostgresTest {
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  protected var migration: DbMigrationsTargeted = _

  override protected def cleanDb(storage: DbStorage)(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  override def mkDbConfig(basicConfig: DbBasicConfig): DbConfig.Postgres = {
    val defaultDbConfig = super.mkDbConfig(basicConfig)
    defaultDbConfig.copy(parameters =
      DbParametersConfig(partitions = PartitionConfig(initialBftOrdererTablesPartitionSize = 100))
    )
  }

  override def beforeAll(): Unit =
    try {
      setup = createSetup().initialized()
      migration = {
        new DbMigrationsTargeted(
          setup.config,
          migrationMode == MigrationMode.DevVersion,
          timeouts,
          loggerFactory,
        )
      }
      val migrationResult = migration.migrateDatabaseAtTargetVersion("5.0")
      migrationResult
        .valueOr(err => fail(s"Failed to migrate database: $err"))
        .onShutdown(fail("DB migration interrupted due to shutdown"))
      // we deliberately do not call super.beforeAll(), to avoid the regular migration logic to be called
    } catch {
      case NonFatal(e) =>
        logger.error("beforeAll failed", e)
        throw e
    }

  "Partition Migration" should {
    "create partitions and transfer data to partitioned tables" in {
      import storage.api.*
      import cats.syntax.parallel.*
      import com.digitalasset.canton.util.FutureInstances.*

      val epochs = Seq(100, 200, 300, 400, 599)

      for {
        _ <- epochs.parTraverse { epoch =>
          storage
            .update(
              sqlu"""insert into ord_metadata_output_blocks(epoch_number, block_number, bft_ts) values ($epoch, $epoch,$epoch) on conflict (block_number) do nothing""",
              "add block",
            )
            .failOnShutdown
        }
        _ = migration
          .migrateDatabaseAtTargetVersion("latest")
          .valueOr(err => fail(s"Failed to migrate database: $err"))
          .failOnShutdown

      } yield succeed
    }
  }

}

object PartitionMigrationTest {

  class DbMigrationsTargeted(
      dbConfig: DbConfig,
      alphaVersionSupport: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, closeContext: CloseContext)
      extends DbMigrations(dbConfig, alphaVersionSupport, timeouts, loggerFactory) {

    def migrateDatabaseAtTargetVersion(
        target: String
    ): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
      TraceContext.withNewTraceContext("migrate_database") { implicit traceContext =>
        withDb() { createdDb =>
          ResourceUtil.withResource(createdDb) { db =>
            val flyway =
              createFlywayConfig(DbMigrations.createDataSource(db.source)).target(target).load()
            migrateDatabaseInternal(flyway)
          }
        }
      }
  }
}
