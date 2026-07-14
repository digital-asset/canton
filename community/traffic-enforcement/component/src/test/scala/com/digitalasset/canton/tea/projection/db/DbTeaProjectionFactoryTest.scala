// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, MigrationMode, PostgresTest}
import com.digitalasset.canton.tea.projection.{
  EventSource,
  TeaProjectionFactory,
  TeaProjectionTest,
  TeaTrafficStore,
}
import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.config.Config
import org.apache.pekko.actor.typed.ActorSystem
import org.scalatest.wordspec.AnyWordSpec

trait DbTeaProjectionFactoryTest extends AnyWordSpec with BaseTest with TeaProjectionTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_traffic_enforcement_event",
        sqlu"truncate table par_traffic_enforcement_balance",
        sqlu"truncate table pekko_projection_offset_store",
        sqlu"truncate table pekko_projection_management",
      ),
      functionFullName,
    )
  }

  // The DB backends persist the offset in the pekko offset store, so a restarted projection
  // resumes exactly where the previous one left off.
  override protected def offsetsArePersisted: Boolean = true

  override def additionalPekkoConfig: Config =
    TrafficEnforcementServerConfig.Internal().pekkoConfig(storage.underlying)

  // The store is a stateless query wrapper; data isolation between tests is provided by cleanDb.
  // The execution context comes from HasExecutionContext (mixed in via DbTest).
  private lazy val dbStore: TeaDbTrafficStore =
    new TeaDbTrafficStore(storage, loggerFactory, timeouts)

  override protected def createBackend()(implicit system: ActorSystem[?]): Backend =
    new Backend {
      override val store: TeaTrafficStore = dbStore
      // The slick projection needs the raw single/multi storage, not the idempotency wrapper.
      override def newProjection(): TeaProjectionFactory =
        new TeaDbProjectionFactory(
          storage.underlying,
          loggerFactory,
          dbStore,
          EventSource.LedgerAPI,
          ProjectionConfig(),
        )
    }

  "DbTeaProjection" should {
    behave like teaProjection()
  }
}

class DbTeaProjectionFactoryPostgresTest extends DbTeaProjectionFactoryTest with PostgresTest {
  // TODO(i33278): remove when migrations are stable
  override def migrationMode: MigrationMode = MigrationMode.DevVersion
}

class DbTeaProjectionFactoryH2Test extends DbTeaProjectionFactoryTest with H2Test {
  // TODO(i33278): remove when migrations are stable
  override def migrationMode: MigrationMode = MigrationMode.DevVersion
}
