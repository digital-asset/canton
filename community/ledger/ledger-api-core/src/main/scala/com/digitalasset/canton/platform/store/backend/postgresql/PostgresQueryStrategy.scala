// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import anorm.ToStatement
import com.daml.metrics.Timed
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.IndexErrors.DatabaseErrors.DbLockTimeoutError
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.{
  CompositeSql,
  SqlStringInterpolation,
}
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy.DbLockMeta
import com.digitalasset.canton.platform.store.dao.DatabaseSelfServiceError
import org.postgresql.util.PSQLException

import java.sql.{Connection, PreparedStatement}

object PostgresQueryStrategy extends QueryStrategy {
  implicit object ArrayByteaToStatement extends ToStatement[Array[Array[Byte]]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Array[Byte]]): Unit =
      s.setObject(index, v)
  }

  override def anyOf(longs: Iterable[Long]): CompositeSql = {
    val longArray: Array[java.lang.Long] =
      longs.view.map(Long.box).toArray
    cSQL"= ANY($longArray::bigint[])"
  }

  override def anyOfSmallInts(ints: Iterable[Int]): CompositeSql = {
    val intArray: Array[java.lang.Integer] =
      ints.view.map(Int.box).toArray
    cSQL"= ANY($intArray::smallint[])"
  }

  override def anyOfStrings(strings: Iterable[String]): CompositeSql = {
    val stringArray: Array[String] =
      strings.toArray
    cSQL"= ANY($stringArray::text[])"
  }

  /** ANY SQL clause generation for a number of Binary values
    */
  override def anyOfBinary(binaries: Iterable[Array[Byte]]): CompositeSql = {
    val binaryArray: Array[Array[Byte]] =
      binaries.toArray
    cSQL"= ANY($binaryArray::bytea[])"
  }

  override def analyzeTable(tableName: String): CompositeSql =
    cSQL"ANALYZE (SKIP_LOCKED true) #$tableName"

  override def forceSynchronousCommitForCurrentTransactionForPostgreSQL(
      connection: Connection
  ): Unit = SQL"SET LOCAL synchronous_commit TO ON".execute()(connection).discard

  override def cleanTable(tableName: String)(implicit connection: Connection): Unit =
    SQL"DELETE FROM #$tableName".execute().discard

  override def vacuumTable(tableName: String)(implicit connection: Connection): Unit =
    withAutoCommit {
      SQL"VACUUM (ANALYZE true, INDEX_CLEANUP off, TRUNCATE false) #$tableName"
        .execute()
        .discard
    }

  override def reindexTableIfPossible(tableName: String)(implicit connection: Connection): Boolean =
    handleNowaitException {
      SQL"LOCK TABLE #$tableName IN ACCESS EXCLUSIVE MODE NOWAIT".execute().discard
      SQL"REINDEX TABLE #$tableName".execute().discard
      true
    }(uponLockNotAvailable = false)

  override def vacuumAndReindexTable(
      tableName: String
  )(implicit connection: Connection, errorLoggingContext: ErrorLoggingContext): Unit = {
    errorLoggingContext.info(s"Vacuuming $tableName table")
    vacuumTable(tableName)
    errorLoggingContext.info(s"Vacuumed $tableName table")
    if (reindexTableIfPossible(tableName))
      errorLoggingContext.info(s"Successfully reindexed $tableName table")
    else
      errorLoggingContext.info(
        s"Unable to reindex $tableName table due to concurrent access of the table."
      )
  }

  def handleNowaitException[T](t: => T)(uponLockNotAvailable: => T): T =
    try {
      t
    } catch {
      case pSQLException: PSQLException
          // 55P03 	lock_not_available (reference from https://www.postgresql.org/docs/current/errcodes-appendix.html)
          if Option(pSQLException.getServerErrorMessage).map(_.getSQLState).contains("55P03") =>
        uponLockNotAvailable
    }

  def acquireLock[T](dbLockMeta: DbLockMeta)(lockF: => T)(implicit
      connection: Connection,
      errorLoggingContext: ErrorLoggingContext,
  ): T = {
    assert(!connection.getAutoCommit)
    assert(dbLockMeta.timeoutMillis > 0)

    def lock(): T =
      Timed.value(
        timer = dbLockMeta.timer,
        value = lockF,
      )

    QueryStrategy.withNetworkTimeout(dbLockMeta.timeoutMillis) {
      try {
        lock()
      } catch {
        case e: PSQLException if DatabaseSelfServiceError.isNetworkTimeoutException(e) =>
          throw DbLockTimeoutError
            .Reject(
              lockDescription = dbLockMeta.lockDescription,
              timeoutConfig = dbLockMeta.timeoutConfig,
              timeoutMillis = dbLockMeta.timeoutMillis,
            )(errorLoggingContext)
            .asGrpcError
      }
    }(implicitly, errorLoggingContext.noTracingLogger)
  }

  private def withAutoCommit[T](f: => T)(implicit connection: Connection): T =
    if (!connection.getAutoCommit) {
      try {
        connection.setAutoCommit(true)
        f
      } finally {
        connection.setAutoCommit(false)
      }
    } else f
}
