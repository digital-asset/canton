// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.BeforeAndAfterAll

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter

/** Goal of this test is to provide a light-weight approach to ingest synthetic Index DB data for
  * load-testing, benchmarking purposes. This test is not supposed to run in CI.
  *
  * Tests are separated to classes to ensure that testOnly finds only the one needed, sbt
  * Perf/testOnly *PerfTest -- -z "test name" seems to be broken when multiple tests are in the same
  * class.
  */


abstract class PerfTestBase
    extends IndexComponentLoadTestBase
    with BeforeAndAfterAll {

  def measure[T](block: => T): T = {
    val runtime = Runtime.getRuntime

    // clean memory
    System.gc()

    val memBefore = runtime.freeMemory()

    val result = block

    val memAfter: Long = runtime.freeMemory()

    val memUsed: Long = math.max(0L, memAfter - memBefore)

    reportMetric("memory used GB", (memUsed / math.pow(2, 30)).toFloat)

    result
  }

  private val reportStore: Report = Report()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneId.of("UTC"))
  val timestamp = formatter.format(Instant.now())
  val filenameSuffix = "lapi_perf_test" + timestamp

  override def reportMetric(key: String, value: Float): Unit = reportStore.add(key, value)

  override def afterAll(): Unit = {
    logger.warn("Performance test finished. Report:\n" + reportStore.show())

    val targetPath = sys.env.getOrElse("LAPI_REPORT_TARGET_PATH", "/tmp")
    reportStore.saveAsCsv(filenameSuffix, targetPath)
    super.afterAll()
  }

  override val dbConfig: com.digitalasset.canton.config.DbConfig =
    DbBasicConfig(
      username = "postgres",
      password = "password",
      dbName = sys.env.getOrElse("LAPI_DB_NAME", "lapi_load_test"),
      host = "localhost",
      port = 5432,
      connectionPoolEnabled = true,
    ).toPostgresDbConfig

}

class AcsFetchPerfTest extends PerfTestBase {

  override val filenameSuffix =
    "lapi_acs_fetch_perf_test_" + timestamp

  override implicit val traceContext: TraceContext =
    TraceContext.createNew("lapi-acs-fetch-perf-test")

  it should "fetch ACS" in TraceContext.withNewTraceContext("ACS fetch") { implicit traceContext =>
    measure {
      fetchAcs()
    }
  }
}

class AcsFetchWithIncreasedArchivalRatePerfTest extends PerfTestBase {

  override val filenameSuffix =
    "lapi_high_archive_fetch_perf_test_" + timestamp

  override implicit val traceContext: TraceContext =
    TraceContext.createNew("lapi-high-archive-fetch-perf-test")

  it should "fetch ACS with high archival rate" in TraceContext.withNewTraceContext("ACS fetch") { implicit traceContext =>
    measure {
      fetchAcs()
    }
  }
}


class InsertWithIncreasedArchivalRatePerfTest extends PerfTestBase {
  override val filenameSuffix =
    "lapi_insert_high_archive_perf_test_" + timestamp

  override implicit val traceContext: TraceContext =
    TraceContext.createNew("lapi-insert-high-archive-rate-perf-test")

  it should "20% CN NFR insert with higher archival rate" in {
    measure { cnNFRIngestionFixture(
      passes = 864,
      activeTxsPerPass = 13,
      yesIReallyWantToRunIt = true,
      actionName = "ingesting 20% CN NFR with higher archival rate",
    )
    }
  }
}

class InsertPerfTest extends PerfTestBase {
  override val filenameSuffix =
    "lapi_insert_perf_test_" + timestamp

  override implicit val traceContext: TraceContext = TraceContext.createNew("lapi-insert-perf-test")

  // contracts created total = 864 * 2023 * 5
  // archives = 864 * 1823 * 5
  // active contracts = 864 * 200 * 5
  it should "20% CN NFR insert" in {
    measure {
    cnNFRIngestionFixture(
      passes = 864,
      activeTxsPerPass = 23, // default
      yesIReallyWantToRunIt = true,
      actionName = "ingesting 20% CN NFR",
    )
  }
  }

}
