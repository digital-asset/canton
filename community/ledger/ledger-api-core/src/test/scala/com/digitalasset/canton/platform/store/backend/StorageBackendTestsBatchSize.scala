// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.store.dao.DatabaseSelfServiceError
import com.digitalasset.daml.lf.data.Ref.Party
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.event

/** This is used only with Postgres, it doesn't make sense to run with H2 */
private[backend] trait StorageBackendTestsBatchSize
    extends Matchers
    with Inside
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  behavior of "StorageBackend (batch size)"

  private def prepareExercises(
      numberOfExercises: Int,
      numberOfInformees: Int,
  ): Vector[DbDto] =
    (1 to numberOfExercises).flatMap { i =>
      val informees = (1 to numberOfInformees).map(j => s"informee-$j").map(Party.assertFromString)
      dtosConsumingExercise(
        event_offset = 1L,
        stakeholders = Set.empty,
        additional_witnesses = informees.toSet,
        event_sequential_id = i.toLong,
        template_id = someTemplateId,
      )
    }.toVector

  private def runBatchSizeTest(numOfDtos: Int, numOfInformees: Int) = {
    val dtos: Vector[DbDto] = prepareExercises(numOfDtos, numOfInformees)
    dtos.size shouldBe (numOfDtos * (numOfInformees + 1))
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    val directEc = DirectExecutionContext(noTracingLogger)
    val startTime = System.currentTimeMillis()
    tryWithConnections(1) {
      case List(connection) =>
        connection.setAutoCommit(false)
        connection.setNetworkTimeout(
          directEc,
          IndexerConfig.DefaultPostgresDataSourceConfig.networkTimeout.value.duration.toMillis.toInt,
        )
        try {
          ingest(dtos, connection)
        } catch {
          case e: Exception => // converting the raw SQL exception to IndexDbException
            throw DatabaseSelfServiceError(e)
        }
        updateLedgerEnd(offset(1), numOfDtos.toLong)(connection)
        connection.commit()
        val durationMs = System.currentTimeMillis() - startTime
        logger.info(
          s"Ingested $numOfDtos exercises with $numOfInformees informees in $durationMs ms"
        )
      case _ => fail("Not possible")
    }
  }

  // This test replicates the huge transaction of the mentioned sev1 issue. It is expected not to complete
  // within the default network timeout, but if the timeout or the environment changes the transaction size
  // may need adjustment. Note that in the sev1 2670 exercises and 1580 informees were present.
  it should "ingest a transaction with 2670 exercises and 1580 informee (sev1 #4433)" in {
    loggerFactory.assertEventuallyLogsSeq(
      SuppressionRule.Level(event.Level.ERROR)
    )(
      within = runBatchSizeTest(
        numOfDtos = 2670,
        numOfInformees = 1580,
      ),
      assertion = logs => {
        val logMessages = logs.map(_.message)
        logMessages.size shouldBe 1
        logMessages.forall(_.contains("INDEX_DB_SQL_NETWORK_TIMEOUT_ERROR")) shouldBe true
      },
    )
  }

  it should "ingest a small batch size within the default timeout" in runBatchSizeTest(
    numOfDtos = 100,
    numOfInformees = 100,
  ).success
}
