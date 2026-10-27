// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.canton

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.{DbParameterUtils, DbStorage}
import com.digitalasset.canton.store.db.PostgresTest
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AnyWordSpec
import slick.jdbc.GetResult

final class DefaultTableSettingsMigrationTestPostgres
    extends AnyWordSpec
    with BaseTest
    with PostgresTest {

  override protected def cleanDb(
      storage: DbStorage
  )(implicit tc: TraceContext): FutureUnlessShutdown[?] = FutureUnlessShutdown.unit

  private implicit val getResultTableSettings: GetResult[(String, Seq[String])] =
    GetResult(r => (r.nextString(), DbParameterUtils.getStringArrayResultsDb(r).toSeq))

  private val tables = Seq(
    "lapi_filter_activate_stakeholder",
    "lapi_filter_activate_witness",
    "lapi_filter_achs_stakeholder",
    "lapi_events_activate_contract",
    "lapi_filter_deactivate_stakeholder",
    "lapi_filter_deactivate_witness",
    "lapi_events_deactivate_contract",
    "lapi_filter_various_witness",
    "lapi_events_various_witnessed",
    "lapi_update_meta",
    "lapi_events_acs_commitments",
    "lapi_command_completions",
    "par_contracts",
  )

  private val expectedSettings = Seq(
    "autovacuum_vacuum_scale_factor=0.0",
    "autovacuum_vacuum_threshold=10000",
    "autovacuum_vacuum_cost_limit=2000",
    "autovacuum_vacuum_cost_delay=5",
    "autovacuum_vacuum_insert_scale_factor=0.0",
    "autovacuum_vacuum_insert_threshold=100000",
    "autovacuum_analyze_scale_factor=0.0",
    "autovacuum_analyze_threshold=1000000",
  )

  "DefaultTableSettingsMigration" should {
    "set the autovacuum and autoanalyze storage parameters" in {
      import storage.api.*

      val settingsByTable = storage
        .query(
          sql"select relname, reloptions from pg_class where reloptions is not null"
            .as[(String, Seq[String])],
          "read storage parameters",
        )
        .futureValueUS
        .toMap
        .withDefaultValue(Seq.empty)

      forEvery(tables) { table =>
        clue(s"Checking storage parameters of $table") {
          settingsByTable(table) should contain allElementsOf expectedSettings
        }
      }
    }
  }
}
