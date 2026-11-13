// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.dao.DatabaseSelfServiceError
import com.digitalasset.daml.lf.data.Ref.Party
import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Connection
import scala.util.Using

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
          20 * 1000, // 20 seconds, the original limit causing the Sev1
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

  // Regression test for sev1 #4433: a transaction too large to be ingested within the network timeout must surface
  // as INDEX_DB_SQL_NETWORK_TIMEOUT_ERROR. The timeout is injected deterministically: another connection holds a lock
  // conflicting with the INSERTs, so the ingestion cannot finish however fast the database is.
  it should "report INDEX_DB_SQL_NETWORK_TIMEOUT_ERROR if ingesting a batch exceeds the network timeout (sev1 #4433)" in {
    val dtos = prepareExercises(numberOfExercises = 10, numberOfInformees = 10)
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    val directEc = DirectExecutionContext(noTracingLogger)
    withTwoConnections { (lockingConnection, ingestingConnection) =>
      lockingConnection.setAutoCommit(false)
      runStatement(lockingConnection, "LOCK TABLE lapi_events_deactivate_contract IN SHARE MODE")
      val ingestingBackendPid = queryInt(ingestingConnection, "SELECT pg_backend_pid()")
      ingestingConnection.setAutoCommit(false)
      ingestingConnection.setNetworkTimeout(directEc, 1000)
      try {
        loggerFactory
          .assertLogs(
            DatabaseSelfServiceError(intercept[Exception](ingest(dtos, ingestingConnection))),
            _.warningMessage should include("INDEX_DB_SQL_NETWORK_TIMEOUT_ERROR"),
          )
          .discard
      } finally {
        // the client gave up on the ingesting connection, but its server process may still wait for the lock:
        // terminate it before releasing the lock, so that its INSERTs never run
        runStatement(lockingConnection, s"SELECT pg_terminate_backend($ingestingBackendPid)")
        lockingConnection.rollback()
      }
    }
  }

  private def runStatement(connection: Connection, sql: String): Unit =
    Using.resource(connection.createStatement())(_.execute(sql)).discard

  private def queryInt(connection: Connection, sql: String): Int =
    Using.resource(connection.createStatement()) { statement =>
      Using.resource(statement.executeQuery(sql)) { rs =>
        rs.next() shouldBe true
        rs.getInt(1)
      }
    }

  it should "ingest a small batch size within the default timeout" in runBatchSizeTest(
    numOfDtos = 100,
    numOfInformees = 100,
  ).success
}
