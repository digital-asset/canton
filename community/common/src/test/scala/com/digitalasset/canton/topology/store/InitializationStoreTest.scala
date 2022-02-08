// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.{NodeId, UniqueIdentifier}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait InitializationStoreTest extends AsyncWordSpec with BaseTest with BeforeAndAfterAll {

  val uid = UniqueIdentifier.tryFromProtoPrimitive("da::default")
  val uid2 = UniqueIdentifier.tryFromProtoPrimitive("two::default")
  val nodeId = NodeId(uid)
  val nodeId2 = NodeId(uid2)

  def initializationStore(mk: () => InitializationStore): Unit = {
    "when storing the unique identifier" should {
      "be able to set the value of the id" in {
        val store = mk()
        for {
          emptyId <- store.id
          _ = emptyId shouldBe None
          _ <- store.setId(nodeId)
          id <- store.id
        } yield id shouldBe Some(nodeId)
      }
      "fail when trying to set two different ids" in {
        val store = mk()
        for {
          _ <- store.setId(nodeId)
          _ <- loggerFactory.assertInternalErrorAsync[IllegalArgumentException](
            store.setId(nodeId2),
            _.getMessage shouldBe s"Unique id of node is already defined as $nodeId and can't be changed to $nodeId2!",
          )
        } yield succeed
      }
    }
  }
}

trait DbInitializationStoreTest extends InitializationStoreTest {
  this: DbTest =>

  def cleanDb(storage: DbStorage): Future[Int] = {
    import storage.api._
    storage.update(
      sqlu"truncate table node_id",
      operationName = s"${this.getClass}: truncate table node_id",
    )
  }

  "DbInitializationStore" should {
    behave like initializationStore(() => new DbInitializationStore(storage, loggerFactory))
  }
}
class DbInitializationStoreTestH2 extends DbInitializationStoreTest with H2Test

class DbInitializationStoreTestPostgres extends DbInitializationStoreTest with PostgresTest

class InitializationStoreTestInMemory extends InitializationStoreTest {
  "InMemoryInitializationStore" should {
    behave like initializationStore(() => new InMemoryInitializationStore(loggerFactory))
  }
}
