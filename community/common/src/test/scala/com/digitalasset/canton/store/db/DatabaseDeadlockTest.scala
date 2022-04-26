// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import io.functionmeta.functionFullName
import org.scalatest.{Assertion, BeforeAndAfterAll}
import slick.jdbc.PositionedParameters
import slick.sql.SqlAction

import java.sql.SQLException
import scala.concurrent.Future
import scala.util.{Failure, Random, Success, Try}

trait DatabaseDeadlockTest
    extends BaseTestWordSpec
    with BeforeAndAfterAll
    with HasExecutionContext {
  this: DbTest =>

  lazy val rawStorage: DbStorage = storage.underlying
  import rawStorage.api._

  val batchSize = 100
  val roundsNegative = 10
  val roundsPositive = 1
  val maxRetries = 2

  def createTableAction: SqlAction[Int, NoStream, Effect.Write]

  override def beforeAll(): Unit = {
    super.beforeAll()

    rawStorage
      .queryAndUpdate(
        DBIO.seq(
          sqlu"drop table database_deadlock_test".asTry, // Try to drop, in case it already exists.
          createTableAction,
        ),
        functionFullName,
      )
      .futureValue
  }

  override def cleanDb(storage: DbStorage): Future[Unit] =
    rawStorage.update_(
      sqlu"truncate table database_deadlock_test",
      functionFullName,
    )

  case class DbBulkCommand(sql: String, setParams: PositionedParameters => Int => Unit) {
    def run(
        ascending: Boolean,
        maxRetries: Int,
    ): Future[Array[Int]] = {
      rawStorage.queryAndUpdate(
        DbStorage.bulkOperation(
          sql,
          if (ascending) 0 until batchSize else (0 until batchSize).reverse,
          storage.profile,
        )(setParams),
        s"$functionFullName-${sql.take(10)}",
        maxRetries,
      )
    }
  }

  def setIdValue(sp: PositionedParameters)(id: Int): Unit = {
    sp >> id
    sp >> Random.nextInt()
  }
  def setValueId(sp: PositionedParameters)(id: Int): Unit = {
    sp >> Random.nextInt()
    sp >> id
  }
  def setId(sp: PositionedParameters)(id: Int): Unit = {
    sp >> id
  }

  def upsertCommand: DbBulkCommand

  def updateCommand: DbBulkCommand =
    DbBulkCommand("update database_deadlock_test set v = ? where id = ?", setValueId)

  def deleteCommand: DbBulkCommand =
    DbBulkCommand("delete from database_deadlock_test where id = ?", setId)

  "The storage" when {
    // Test upserts
    testQuery("bulk inserts", upsertCommand, upsertCommand)

    testQueryWithSetup(
      // insert rows first to test the update part of the upsert
      setup = upsertCommand.run(ascending = true, 0).futureValue,
      "bulk upserts",
      upsertCommand,
      upsertCommand,
    )

    // Test updates
    testQueryWithSetup(
      setup = upsertCommand.run(ascending = true, 0).futureValue,
      "bulk updates",
      updateCommand,
      updateCommand,
    )

    // Test deletes
    testQuery(
      "bulk delete + insert",
      deleteCommand,
      upsertCommand,
    )
  }

  def testQuery(
      description: String,
      command1: DbBulkCommand,
      command2: DbBulkCommand,
  ): Unit = {
    testQueryWithSetup((), description, command1, command2)
  }

  def testQueryWithSetup(
      setup: => Unit,
      description: String,
      command1: DbBulkCommand,
      command2: DbBulkCommand,
  ): Unit = {
    if (dbCanDeadlock) {
      s"running conflicting $description" can {
        "abort with a deadlock" in {
          setup
          assertSQLException(runWithConflictingRowOrder(command1, command2, 0))
        }
      }
    }

    s"running conflicting $description with retry" must {
      "succeed" in {
        setup
        assertNoException(runWithConflictingRowOrder(command1, command2, maxRetries))
      }
    }
  }

  def dbCanDeadlock: Boolean = true

  def assertSQLException(body: => Try[_]): Assertion = {
    // TODO(i9169): Test that at least one round ends with a deadlock
    forAll(0 until roundsNegative) { _ =>
      inside(body) {
        case Success(_) => succeed
        case Failure(e: SQLException) => assertDeadlock(e)
      }
    }
  }

  def assertDeadlock(e: SQLException): Assertion

  def assertNoException(body: => Try[_]): Assertion =
    forAll(0 until roundsPositive) { _ =>
      inside(body) { case Success(_) => succeed }
    }

  def runWithConflictingRowOrder(
      command1: DbBulkCommand,
      command2: DbBulkCommand,
      maxRetries: Int,
  ): Try[Seq[Array[Int]]] =
    Future
      .sequence(
        Seq(
          command1.run(ascending = true, maxRetries),
          command2.run(ascending = false, maxRetries),
        )
      )
      .transform(Try(_))
      .futureValue
}

class DatabaseDeadlockTestH2 extends DatabaseDeadlockTest with H2Test {
  import rawStorage.api._

  // H2 cannot deadlock at the moment, because we are enforcing a single connection.
  // Therefore disabling negative tests.
  override def dbCanDeadlock: Boolean = false

  override lazy val createTableAction: SqlAction[Int, NoStream, Effect.Write] =
    sqlu"create table database_deadlock_test(id bigint primary key, v bigint not null)"

  override lazy val upsertCommand: DbBulkCommand = DbBulkCommand(
    """merge into database_deadlock_test
      |using (select cast(? as bigint) id, cast(? as bigint) v from dual) as input
      |on (database_deadlock_test.id = input.id)
      |when not matched then
      |  insert(id, v)
      |  values(input.id, input.v)
      |when matched then
      |  update set v = input.v""".stripMargin,
    setIdValue,
  )

  override def assertDeadlock(e: SQLException): Assertion = fail("unimplemented")
}

class DatabaseDeadlockTestPostgres extends DatabaseDeadlockTest with PostgresTest {
  import rawStorage.api._

  override lazy val createTableAction: SqlAction[Int, NoStream, Effect.Write] =
    sqlu"create table database_deadlock_test(id bigint primary key, v bigint not null)"

  override lazy val upsertCommand: DbBulkCommand = DbBulkCommand(
    """insert into database_deadlock_test(id, v)
      |values (?, ?)
      |on conflict (id) do 
      |update set v = excluded.v""".stripMargin,
    setIdValue,
  )

  override def assertDeadlock(e: SQLException): Assertion = e.getSQLState shouldBe "40P01"
}