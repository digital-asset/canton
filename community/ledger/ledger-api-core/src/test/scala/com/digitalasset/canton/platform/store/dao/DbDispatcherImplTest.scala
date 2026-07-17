// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.executors.InstrumentedExecutors
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.health.HealthStatus
import com.digitalasset.canton.logging.LoggingContextWithTrace
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Connection

class DbDispatcherImplTest
    extends AnyFlatSpec
    with MockitoSugar
    with BaseTest
    with Matchers
    with BeforeAndAfterAll {
  private def connectionProvider(connection: Connection) = new JdbcConnectionProvider {
    override def runSQL[T](block: Connection => T): T = block(connection)

    override def currentHealth(): HealthStatus = ???
  }
  private val metrics = DatabaseMetrics.ForTesting("concurrent change control")
  private lazy val executionContextService =
    InstrumentedExecutors.newFixedThreadPool("test", 2, _ => ())

  override protected def afterAll(): Unit = {
    super.afterAll()
    executionContextService.shutdown()
  }

  behavior of "DbDispatcherImpl"

  it should "close in case of no exceptions" in {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(mock[java.sql.Statement])
    when(connection.createStatement().executeQuery("SELECT 1")).thenReturn(mock[java.sql.ResultSet])

    val sut = new DbDispatcherImpl(
      connectionProvider = connectionProvider(connection),
      executorService = executionContextService,
      overallWaitTimer = metrics.waitTimer,
      overallExecutionTimer = metrics.executionTimer,
      loggerFactory = loggerFactory,
    )
    val exec = sut.executeSql(metrics) { conn =>
      val v = conn.createStatement()
      v.executeQuery("SELECT 1")
    }(LoggingContextWithTrace.ForTesting)
    exec.futureValue
    verify(connection).commit()
    verify(connection).close()
  }

  it should "report the exception when failing" in {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(mock[java.sql.Statement])
    when(connection.createStatement().executeQuery("SELECT 1"))
      .thenThrow(new RuntimeException("initial error"))

    val sut = new DbDispatcherImpl(
      connectionProvider = connectionProvider(connection),
      executorService = executionContextService,
      overallWaitTimer = metrics.waitTimer,
      overallExecutionTimer = metrics.executionTimer,
      loggerFactory = loggerFactory,
    )
    val exec = sut.executeSql(metrics) { conn =>
      val v = conn.createStatement()
      v.executeQuery("SELECT 1")
    }(LoggingContextWithTrace.ForTesting)
    val exception = exec.failed.futureValue
    exception.getMessage should include("initial error")
    verify(connection).rollback()
    verify(connection).close()
  }

  it should "report the original exception when close() is also failing (close should not suppress the original exception)" in {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(mock[java.sql.Statement])
    when(connection.createStatement().executeQuery("SELECT 1"))
      .thenThrow(new RuntimeException("initial error"))
    when(connection.close()).thenThrow(new RuntimeException("close error"))

    val sut = new DbDispatcherImpl(
      connectionProvider = connectionProvider(connection),
      executorService = executionContextService,
      overallWaitTimer = metrics.waitTimer,
      overallExecutionTimer = metrics.executionTimer,
      loggerFactory = loggerFactory,
    )
    val exec = sut.executeSql(metrics) { conn =>
      val v = conn.createStatement()
      v.executeQuery("SELECT 1")
    }(LoggingContextWithTrace.ForTesting)
    val exception = exec.failed.futureValue
    exception.getMessage should include("initial error")
    verify(connection).rollback()
    verify(connection).close()
  }

  it should "report the original exception when rollback() is also failing (close should not suppress the original exception)" in {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(mock[java.sql.Statement])
    when(connection.createStatement().executeQuery("SELECT 1"))
      .thenThrow(new RuntimeException("initial error"))
    when(connection.rollback()).thenThrow(new RuntimeException("rollback error"))

    val sut = new DbDispatcherImpl(
      connectionProvider = connectionProvider(connection),
      executorService = executionContextService,
      overallWaitTimer = metrics.waitTimer,
      overallExecutionTimer = metrics.executionTimer,
      loggerFactory = loggerFactory,
    )
    val exec = sut.executeSql(metrics) { conn =>
      val v = conn.createStatement()
      v.executeQuery("SELECT 1")
    }(LoggingContextWithTrace.ForTesting)
    val exception = exec.failed.futureValue
    exception.getMessage should include("initial error")
    verify(connection).rollback()
    verify(connection).close()
  }

  it should "report the original exception when both rollback() and close() are also failing (close should not suppress the original exception)" in {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(mock[java.sql.Statement])
    when(connection.createStatement().executeQuery("SELECT 1"))
      .thenThrow(new RuntimeException("initial error"))
    when(connection.rollback()).thenThrow(new RuntimeException("rollback error"))
    when(connection.close()).thenThrow(new RuntimeException("close error"))

    val sut = new DbDispatcherImpl(
      connectionProvider = connectionProvider(connection),
      executorService = executionContextService,
      overallWaitTimer = metrics.waitTimer,
      overallExecutionTimer = metrics.executionTimer,
      loggerFactory = loggerFactory,
    )
    val exec = sut.executeSql(metrics) { conn =>
      val v = conn.createStatement()
      v.executeQuery("SELECT 1")
    }(LoggingContextWithTrace.ForTesting)
    val exception = exec.failed.futureValue
    exception.getMessage should include("initial error")
    verify(connection).rollback()
    verify(connection).close()
  }

  it should "report the original exception when commit() and close() are failing (close should not suppress the original exception)" in {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(mock[java.sql.Statement])
    when(connection.createStatement().executeQuery("SELECT 1")).thenReturn(mock[java.sql.ResultSet])
    when(connection.commit()).thenThrow(new RuntimeException("commit error"))
    when(connection.close()).thenThrow(new RuntimeException("close error"))

    val sut = new DbDispatcherImpl(
      connectionProvider = connectionProvider(connection),
      executorService = executionContextService,
      overallWaitTimer = metrics.waitTimer,
      overallExecutionTimer = metrics.executionTimer,
      loggerFactory = loggerFactory,
    )
    val exec = sut.executeSql(metrics) { conn =>
      val v = conn.createStatement()
      v.executeQuery("SELECT 1")
    }(LoggingContextWithTrace.ForTesting)
    val exception = exec.failed.futureValue
    exception.getMessage should include("commit error")
    verify(connection).rollback()
    verify(connection).close()
  }

  it should "report the original exception when commit() and rollback() are also failing (close should not suppress the original exception)" in {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(mock[java.sql.Statement])
    when(connection.createStatement().executeQuery("SELECT 1")).thenReturn(mock[java.sql.ResultSet])
    when(connection.commit()).thenThrow(new RuntimeException("commit error"))
    when(connection.rollback()).thenThrow(new RuntimeException("rollback error"))

    val sut = new DbDispatcherImpl(
      connectionProvider = connectionProvider(connection),
      executorService = executionContextService,
      overallWaitTimer = metrics.waitTimer,
      overallExecutionTimer = metrics.executionTimer,
      loggerFactory = loggerFactory,
    )
    val exec = sut.executeSql(metrics) { conn =>
      val v = conn.createStatement()
      v.executeQuery("SELECT 1")
    }(LoggingContextWithTrace.ForTesting)
    val exception = exec.failed.futureValue
    exception.getMessage should include("commit error")
    verify(connection).rollback()
    verify(connection).close()
  }

  it should "report the original exception when commit() and both rollback() and close() are also failing (close should not suppress the original exception)" in {
    val connection = mock[Connection]
    when(connection.createStatement()).thenReturn(mock[java.sql.Statement])
    when(connection.createStatement().executeQuery("SELECT 1")).thenReturn(mock[java.sql.ResultSet])
    when(connection.commit()).thenThrow(new RuntimeException("commit error"))
    when(connection.rollback()).thenThrow(new RuntimeException("rollback error"))
    when(connection.close()).thenThrow(new RuntimeException("close error"))

    val sut = new DbDispatcherImpl(
      connectionProvider = connectionProvider(connection),
      executorService = executionContextService,
      overallWaitTimer = metrics.waitTimer,
      overallExecutionTimer = metrics.executionTimer,
      loggerFactory = loggerFactory,
    )
    val exec = sut.executeSql(metrics) { conn =>
      val v = conn.createStatement()
      v.executeQuery("SELECT 1")
    }(LoggingContextWithTrace.ForTesting)
    val exception = exec.failed.futureValue
    exception.getMessage should include("commit error")
    verify(connection).rollback()
    verify(connection).close()
  }
}
