// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import anorm.SqlParser.{byteArray, long, scalar}
import anorm.{SqlParser, ~}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.CannotAcquireAllRowLocksException
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy.withoutNetworkTimeout
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.`SimpleSql ops`
import com.digitalasset.canton.platform.store.backend.postgresql.{
  PostgresDataSourceConfig,
  PostgresDataSourceStorageBackend,
  PostgresQueryStrategy,
}
import org.scalatest.Inside
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.{Connection, SQLException}
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

final class StorageBackendSpecPostgres
    extends AnyFlatSpec
    with StorageBackendProviderPostgres
    with StorageBackendSuite
    with Inside {

  behavior of "StorageBackend (Postgres)"

  it should "find the Postgres version" in {
    val version = executeSql(PostgresDataSourceStorageBackend(loggerFactory).getPostgresVersion)

    inside(version) { case Some(versionNumbers) =>
      // Minimum Postgres version used in tests
      versionNumbers._1 should be >= 9
      versionNumbers._2 should be >= 0
    }
  }

  it should "correctly parse a Postgres version" in {
    val backend = PostgresDataSourceStorageBackend(loggerFactory)
    backend.parsePostgresVersion("1.2") shouldBe Some((1, 2))
    backend.parsePostgresVersion("1.2.3") shouldBe Some((1, 2))
    backend.parsePostgresVersion("1.2.3-alpha.4.5") shouldBe Some((1, 2))
    backend.parsePostgresVersion("10.11") shouldBe Some((10, 11))
  }

  it should "fail the compatibility check for Postgres versions lower than minimum" in {
    val version = executeSql(PostgresDataSourceStorageBackend(loggerFactory).getPostgresVersion)
    val currentlyUsedMajorVersion = inside(version) { case Some((majorVersion, _)) =>
      majorVersion
    }
    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = currentlyUsedMajorVersion + 1,
        loggerFactory = loggerFactory,
      )

    loggerFactory.assertThrowsAndLogs[PostgresDataSourceStorageBackend.UnsupportedPostgresVersion](
      within = executeSql(
        backend.checkCompatibility
      ),
      assertions = _.errorMessage should include(
        "Deprecated Postgres version."
      ),
    )
  }

  it should "throw an exception if network timeout has been violated" in {
    import anorm.SqlStringInterpolation

    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = 14,
        loggerFactory = loggerFactory,
      )

    val dataSource = backend.createDataSource(
      DataSourceStorageBackend
        .DataSourceConfig(
          jdbcUrl = jdbcUrl,
          postgresConfig = PostgresDataSourceConfig(
            networkTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(1))
          ),
        ),
      loggerFactory,
    )

    val connection = dataSource.getConnection
    val startTime = System.nanoTime()
    val thrown = intercept[SQLException] {
      // sleep for 5 seconds to simulate a long-running query
      SQL"SELECT pg_sleep(5);".execute()(connection)
    }
    thrown.getMessage should include("An I/O error occurred while sending to the backend.")
    thrown.getCause.getMessage should include("Read timed out")
    val endTime = System.nanoTime()
    val timedOutAfterMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)

    // the network timeout should have occurred after approximately 1 second
    timedOutAfterMillis should be < 1500L
    timedOutAfterMillis should be >= 1000L

    connection.close()

    // when the network timeout is disabled, the long-running query should complete successfully
    val connection2 = dataSource.getConnection
    withoutNetworkTimeout { connection =>
      SQL"SELECT pg_sleep(5);".execute()(connection)
    }(connection2, noTracingLogger)

    // and when re-enabling the network timeout, it should again throw after the specified time
    val thrown2 = intercept[SQLException] {
      SQL"SELECT pg_sleep(5);".execute()(connection2)
    }
    thrown2.getMessage should include("An I/O error occurred while sending to the backend.")
    thrown2.getCause.getMessage should include("Read timed out")

    connection2.close()
  }

  it should "wait for a long running query when clientConnectionCheckInterval is not set" in {
    import anorm.SqlStringInterpolation

    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = 14,
        loggerFactory = loggerFactory,
      )

    val dataSource = backend.createDataSource(
      DataSourceStorageBackend
        .DataSourceConfig(
          jdbcUrl = jdbcUrl,
          postgresConfig = PostgresDataSourceConfig(
            clientConnectionCheckInterval = None,
            networkTimeout = None,
          ),
        ),
      loggerFactory,
    )

    val connection1 = dataSource.getConnection
    val connection2 = dataSource.getConnection

    // acquire advisory lock
    val acquired1 = SQL"SELECT pg_try_advisory_lock(123456);".as(
      SqlParser.bool("pg_try_advisory_lock").single
    )(connection1)
    acquired1 shouldBe true

    // blocking acquire advisory lock in another connection
    val acquired2 = Future(
      SQL"SELECT pg_advisory_lock(123456);".execute()(connection2)
    )(parallelExecutionContext)

    // long-running query
    val longRunning = Future(
      SQL"SELECT pg_sleep(5);".execute()(connection1)
    )(parallelExecutionContext)

    Threading.sleep(500) // let some time for db to start the long-running query
    acquired2.isCompleted shouldBe false
    // close the connection while the long-running query is still running simulating a lost client connection
    // since there is no clientConnectionCheckInterval the server will not interrupt the long-running query and will
    // only release the lock after it
    val thrown = intercept[TestFailedException] {
      connection1.close()
      longRunning.futureValue
    }
    thrown.getCause shouldBe a[org.postgresql.util.PSQLException]

    val startTime = System.nanoTime()
    acquired2.futureValue
    val endTime = System.nanoTime()
    val getLockDurationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)
    // should take almost 5 seconds to acquire the lock after client is disconnected since there is no clientConnectionCheckInterval
    // and the long-running query continues to run until completion and then releases the lock
    getLockDurationMillis should be > 4000L

    connection2.close()
  }

  it should "abort a long running query when clientConnectionCheckInterval is set" in {
    import anorm.SqlStringInterpolation

    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = 14,
        loggerFactory = loggerFactory,
      )

    val dataSource = backend.createDataSource(
      DataSourceStorageBackend
        .DataSourceConfig(
          jdbcUrl = jdbcUrl,
          postgresConfig = PostgresDataSourceConfig(
            clientConnectionCheckInterval = Some(config.NonNegativeFiniteDuration.ofSeconds(2)),
            networkTimeout = None,
          ),
        ),
      loggerFactory,
    )

    val connection1 = dataSource.getConnection
    val connection2 = dataSource.getConnection
    // acquire advisory lock
    val acquired1 = SQL"SELECT pg_try_advisory_lock(123456);"
      .as(SqlParser.bool("pg_try_advisory_lock").single)(connection1)
    acquired1 shouldBe true

    // blocking acquire advisory lock in another connection
    val acquired2 = Future(
      SQL"SELECT pg_advisory_lock(123456);".execute()(connection2)
    )(parallelExecutionContext)

    // long-running query
    val longRunning = Future(
      SQL"SELECT pg_sleep(5);".execute()(connection1)
    )(parallelExecutionContext)

    Threading.sleep(500) // let some time for db to start the long-running query
    acquired2.isCompleted shouldBe false
    // close the connection while the long-running query is still running simulating a lost client connection
    // when clientConnectionCheckInterval kicks in the server should interrupt the long-running query and release the lock
    val thrown = intercept[TestFailedException] {
      connection1.close()
      longRunning.futureValue
    }
    thrown.getCause shouldBe a[org.postgresql.util.PSQLException]

    val startTime = System.nanoTime()
    acquired2.futureValue
    val endTime = System.nanoTime()
    val getLockDurationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)

    // should take at most 2 seconds for the server to check that the client is disconnected, abort the long-running query
    // and release the lock so that the second connection can acquire it
    getLockDurationMillis should be < 2000L

    connection2.close()
  }

  it should "throw an exception when a long-running query violates network timeout" in {
    import anorm.SqlStringInterpolation

    val backend =
      new PostgresDataSourceStorageBackend(
        minMajorVersionSupported = 14,
        loggerFactory = loggerFactory,
      )

    val clientConnectionCheckInterval = config.NonNegativeFiniteDuration.ofSeconds(1)
    val networkTimeout = config.NonNegativeFiniteDuration.ofSeconds(2)

    val postgresConfig = PostgresDataSourceConfig(
      clientConnectionCheckInterval = Some(clientConnectionCheckInterval),
      networkTimeout = Some(networkTimeout),
    )

    val dataSource = backend.createDataSource(
      DataSourceStorageBackend
        .DataSourceConfig(
          jdbcUrl = jdbcUrl,
          postgresConfig = postgresConfig,
        ),
      loggerFactory,
    )

    val connection1 = dataSource.getConnection
    val connection2 = dataSource.getConnection()
    // setting a high timeout for the second connection
    connection2.setNetworkTimeout(
      parallelExecutionContext,
      60000, // 60 seconds
    )
    // acquire advisory lock
    val acquired1 = SQL"SELECT pg_try_advisory_lock(123456);"
      .as(SqlParser.bool("pg_try_advisory_lock").single)(connection1)
    acquired1 shouldBe true

    // blocking acquire advisory lock in another connection
    val acquired2 = Future(
      SQL"SELECT pg_advisory_lock(123456);".execute()(connection2)
    )(parallelExecutionContext)

    // long-running query that will violate the network timeout
    val longRunning = Future(
      SQL"SELECT pg_sleep(60);".execute()(connection1)
    )(parallelExecutionContext)

    Threading.sleep(500L) // let some time for db to start the long-running query
    acquired2.isCompleted shouldBe false
    val startTime = System.nanoTime()

    // long-running query should throw after network timeout, however the server continues to execute it
    val thrown = intercept[TestFailedException] {
      longRunning.futureValue
    }
    thrown.getCause shouldBe a[org.postgresql.util.PSQLException]
    thrown.getCause.getCause.getMessage should include("Read timed out")

    val networkTimeoutTime = System.nanoTime()
    val timeUntilNetworkTimeout = TimeUnit.NANOSECONDS.toMillis(networkTimeoutTime - startTime)
    // should take at most networkTimeout for the client to check that the network timeout has been violated
    timeUntilNetworkTimeout should be < networkTimeout.duration.toMillis

    acquired2.futureValue
    val endTime = System.nanoTime()

    val leewayForSchedulingDelaysMillis = 500L
    // should take at most clientConnectionCheckInterval more for the server to check that the client is disconnected, abort the long-running query
    // and release the lock so that the second connection can acquire it
    val timeToAbortAfterTimeout = TimeUnit.NANOSECONDS.toMillis(endTime - networkTimeoutTime)
    timeToAbortAfterTimeout should be < clientConnectionCheckInterval.duration.toMillis + leewayForSchedulingDelaysMillis

    val getLockDurationMillis = TimeUnit.NANOSECONDS.toMillis(endTime - startTime)
    getLockDurationMillis should be < (postgresConfig.networkTimeout.value.duration.toMillis + postgresConfig.clientConnectionCheckInterval.value.duration.toMillis + leewayForSchedulingDelaysMillis)

    connection1.close()
    connection2.close()
  }

  behavior of "exclusive table locking"

  it should "block other exclusive lock in lockExclusivelyPruningProcessingTable" in withConnections(
    2
  ) {
    case List(c1, c2) =>
      c1.setAutoCommit(false)
      c2.setAutoCommit(false)
      backend.event.lockExclusivelyPruningProcessingTable(c1)
      val blockedCall =
        Future(backend.event.lockExclusivelyPruningProcessingTable(c2))(parallelExecutionContext)
      Threading.sleep(1000)
      blockedCall.value shouldBe None
      c1.commit()
      c1.close()
      blockedCall.futureValue
      c2.close()

    case unexpected => fail(s"Incorrect amount of connections: ${unexpected.size}")
  }

  it should "block other exclusive locks in lockExclusivelyContractPruningProcessingTable" in withConnections(
    2
  ) {
    case List(c1, c2) =>
      c1.setAutoCommit(false)
      c2.setAutoCommit(false)
      backend.event.lockExclusivelyContractPruningProcessingTable(c1)
      val blockedCall =
        Future(backend.event.lockExclusivelyContractPruningProcessingTable(c2))(
          parallelExecutionContext
        )
      Threading.sleep(1000)
      blockedCall.value shouldBe None
      c1.commit()
      c1.close()
      blockedCall.futureValue
      c2.close()

    case unexpected => fail(s"Incorrect amount of connections: ${unexpected.size}")
  }

  it should "block other operations accessing contract candidate table (locked by lockExclusivelyContractPruningProcessingTable)" in withConnections(
    2
  ) {
    case List(c1, c2) =>
      c1.setAutoCommit(false)
      c2.setAutoCommit(false)
      backend.event.lockExclusivelyContractPruningProcessingTable(c1)
      val blockedCall =
        Future(backend.event.cleanPruningCandidates()(c2, implicitly))(parallelExecutionContext)
      Threading.sleep(1000)
      blockedCall.value shouldBe None
      c1.commit()
      c1.close()
      blockedCall.futureValue
      c2.close()

    case unexpected => fail(s"Incorrect amount of connections: ${unexpected.size}")
  }

  it should "block other operations inserting into contract candidate table (locked by lockExclusivelyContractPruningProcessingTable)" in withConnections(
    2
  ) {
    case List(c1, c2) =>
      c1.setAutoCommit(false)
      c2.setAutoCommit(false)
      backend.event.lockExclusivelyContractPruningProcessingTable(c1)
      val blockedCall = Future(
        SQL"INSERT INTO lapi_pruning_contract_candidate(internal_contract_id) VALUES (11)"
          .execute()(c2)
      )(parallelExecutionContext)
      Threading.sleep(1000)
      blockedCall.value shouldBe None
      c1.commit()
      c1.close()
      blockedCall.futureValue
      c2.close()

    case unexpected => fail(s"Incorrect amount of connections: ${unexpected.size}")
  }

  it should "be block by other operations accessing contract candidate table (attempt to lock by lockExclusivelyContractPruningProcessingTable)" in withConnections(
    2
  ) {
    case List(c1, c2) =>
      c1.setAutoCommit(false)
      c2.setAutoCommit(false)
      backend.event.cleanPruningCandidates()(c1, implicitly)
      val blockedCall =
        Future(backend.event.lockExclusivelyContractPruningProcessingTable(c2))(
          parallelExecutionContext
        )
      Threading.sleep(1000)
      blockedCall.value shouldBe None
      c1.commit()
      c1.close()
      blockedCall.futureValue
      c2.close()

    case unexpected => fail(s"Incorrect amount of connections: ${unexpected.size}")
  }

  it should "be block by other operations inserting into contract candidate table (attempt to lock by lockExclusivelyContractPruningProcessingTable)" in withConnections(
    2
  ) {
    case List(c1, c2) =>
      c1.setAutoCommit(false)
      c2.setAutoCommit(false)
      SQL"INSERT INTO lapi_pruning_contract_candidate(internal_contract_id) VALUES (11)"
        .execute()(c1)
      val blockedCall =
        Future(backend.event.lockExclusivelyContractPruningProcessingTable(c2))(
          parallelExecutionContext
        )
      Threading.sleep(1000)
      blockedCall.value shouldBe None
      c1.commit()
      c1.close()
      blockedCall.futureValue
      c2.close()

    case unexpected => fail(s"Incorrect amount of connections: ${unexpected.size}")
  }

  behavior of "read/write row locking of internal contract IDs"

  it should "row lock blocking leads to exception" in testRowLocking { env =>
    import env.*
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c1)
    assertThrows[CannotAcquireAllRowLocksException] {
      backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid2, cid3)))(c2)
    }
  }

  it should "disjoint locking is not blocking" in testRowLocking { env =>
    import env.*
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c1)
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid3, cid4)))(c2)
  }

  it should "write should block read" in testRowLocking { env =>
    import env.*
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c1)
    val f = Future(
      backend.event.readLockInternalContractIds(Set(cid2, cid3, -15))(c2) shouldBe Set(-15)
    )(parallelExecutionContext)
    Threading.sleep(1000)
    f.value shouldBe None
    c1.commit()
    f.futureValue
  }

  it should "read should block write" in testRowLocking { env =>
    import env.*
    backend.event.readLockInternalContractIds(Set(cid2, cid3, -15))(c1) shouldBe Set(-15)
    assertThrows[CannotAcquireAllRowLocksException] {
      backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c2)
    }
    c2.rollback()
    c1.commit()
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c2)
  }

  it should "read should not block read" in testRowLocking { env =>
    import env.*
    backend.event.readLockInternalContractIds(Set(cid2, cid3, -15))(c1) shouldBe Set(-15)
    backend.event.readLockInternalContractIds(Set(cid2, cid3, -15))(c2) shouldBe Set(-15)
  }

  it should "write should block write" in testRowLocking { env =>
    import env.*
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid2, -15)))(c1)
    assertThrows[CannotAcquireAllRowLocksException] {
      backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c2)
    }
    c2.rollback()
    c1.commit()
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c2)
  }

  it should "read lock should block write lock, write lock also might be starved by further read locks, and also the blocked writer progressively locking all entries as soon as possible" in testRowLocking {
    env =>
      import env.*
      backend.event.readLockInternalContractIds(Set(cid2, cid3, -15))(c1) shouldBe Set(-15)
      // write is blocked by the read for cid2, but cid1 is write locked
      val writeF = Future(
        blockingWriteLockInternalContractIds(
          PostgresQueryStrategy.anyOf(List(cid1, cid2))
        )(c2)
      )(parallelExecutionContext)
      Threading.sleep(500)
      writeF.value shouldBe None
      // another read is not blocked by the current read on cid2, so it executes immediately
      backend.event.readLockInternalContractIds(Set(cid2, cid4, -18))(c3) shouldBe Set(-18)
      Threading.sleep(500)
      writeF.value shouldBe None
      c1.commit()
      Threading.sleep(500)
      // although c1 already released it's cid2 read lock, c3 still has an open tx with cid2 read lock
      writeF.value shouldBe None
      // another tx incoming for cid2, c3 still hold the read lock, but this one can execute immediately
      backend.event.readLockInternalContractIds(Set(cid2, cid3, -19))(c1) shouldBe Set(-19)
      c3.commit()
      Threading.sleep(500)
      // although c3 released its cid2 read lock, c1 still holds it o writer is still blocked (starving)
      writeF.value shouldBe None
      // c3 now blocked as cid1 held by c2 with a write lock
      val readF = Future(
        backend.event.readLockInternalContractIds(Set(cid1, cid3, -19))(c3) shouldBe Set(-19)
      )(parallelExecutionContext)
      Threading.sleep(500)
      // c1 still holds the read lock for cid2
      writeF.value shouldBe None
      readF.value shouldBe None
      c1.commit()
      // finally c2 can get the write lock for cid2 as c1 released the read lock
      writeF.futureValue
      Threading.sleep(500)
      readF.value shouldBe None
      c2.commit()
      readF.futureValue
  }

  it should "read lock should not block contract upsert" in testRowLocking { env =>
    import env.*
    backend.event.readLockInternalContractIds(Set(cid2, cid3, -15))(c1) shouldBe Set(-15)
    upsertParContracts(c2)
  }

  it should "contract upsert should not block read lock" in testRowLocking { env =>
    import env.*
    upsertParContracts(c1)
    backend.event.readLockInternalContractIds(Set(cid2, cid3, -15))(c2) shouldBe Set(-15)
  }

  it should "write lock should not block contract upsert" in testRowLocking { env =>
    import env.*
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c1)
    upsertParContracts(c2)
  }

  it should "contract upsert should not block write lock" in testRowLocking { env =>
    import env.*
    upsertParContracts(c1)
    backend.event.writeLockInternalContractIds(PostgresQueryStrategy.anyOf(List(cid1, cid2)))(c2)
  }

  case class TestRowLockingEnv(
      c1: Connection,
      c2: Connection,
      c3: Connection,
      cid1: Long,
      cid2: Long,
      cid3: Long,
      cid4: Long,
  )

  private def testRowLocking(test: TestRowLockingEnv => Unit): Unit =
    withConnections(3) {
      case cs @ List(c1, c2, c3) =>
        cs.foreach { c =>
          c.setAutoCommit(false) // operations running in a transaction
          c.setNetworkTimeout(null, 5000) // in 5 second all JDBC calls should finish
        }
        test(
          TestRowLockingEnv(
            c1,
            c2,
            c3,
            insertParContract("first"),
            insertParContract("second"),
            insertParContract("third"),
            insertParContract("fourth"),
          )
        )

      case unexpected => fail(s"Incorrect amount of connections: ${unexpected.size}")
    }

  private def insertParContract(contractId: String): Long = {
    val contractIdBytes = contractId.getBytes
    executeSql(
      SQL"""
      INSERT INTO par_contracts (contract_id, instance, package_id, template_id)
      VALUES ($contractIdBytes, $contractIdBytes, 'pid', 'tid')
      RETURNING internal_contract_id"""
        .executeInsert(scalar[Long].single)(_)
    )
  }

  // Upserting "first" "second" from the fixture and "new" which does exist yet with on commit do nothing
  // also querying the contracts which have been found to simulate contract store DB behavior
  private def upsertParContracts(connection: Connection): Unit = {
    val contractIdBytes1 = "second".getBytes
    val contractIdBytes2 = "first".getBytes
    val contractIdBytes3 = "new".getBytes
    SQL"""
    INSERT INTO par_contracts (contract_id, instance, package_id, template_id)
    VALUES
    ($contractIdBytes1, $contractIdBytes1, 'pid', 'tid'),
    ($contractIdBytes2, $contractIdBytes2, 'pid', 'tid'),
    ($contractIdBytes3, $contractIdBytes3, 'pid', 'tid')
    ON CONFLICT(contract_id) DO NOTHING
    RETURNING internal_contract_id, contract_id"""
      .asVectorOf(long("internal_contract_id") ~ byteArray("contract_id") map {
        case _ ~ contractId => contractId.toList
      })(connection) shouldBe List("new".getBytes.toList)
    SQL"""
    SELECT contract_id
    FROM par_contracts
    WHERE contract_id ${PostgresQueryStrategy.anyOfBinary(
        List("second", "first").map(_.getBytes)
      )}"""
      .asVectorOf(byteArray("contract_id"))(connection)
      .map(_.toList)
      .toSet shouldBe List("second", "first").map(_.getBytes.toList).toSet
  }

  private def blockingWriteLockInternalContractIds(whereInternalContractIdExprs: CompositeSql)(
      connection: Connection
  ): Unit =
    SQL"""
      SELECT internal_contract_id
      FROM par_contracts
      WHERE internal_contract_id $whereInternalContractIdExprs
      ORDER BY internal_contract_id
      FOR UPDATE
      """.execute()(connection).discard
}
