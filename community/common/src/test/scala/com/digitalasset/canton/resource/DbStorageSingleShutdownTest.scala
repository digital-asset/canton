// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, UnlessShutdown}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.store.db.DbStorageSetup
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level
import slick.dbio.DBIO

import java.sql.SQLTransientConnectionException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}

class DbStorageSingleShutdownTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  private def withStorageAndOpenCaller(name: String)(
      test: (DbStorage, CloseContext) => Future[Assertion]
  ): Future[Assertion] = {
    // A separate in-memory database per test case
    val setup =
      DbStorageSetup.h2(
        loggerFactory,
        mkDbConfig = cfg => cfg.copy(dbName = s"${cfg.dbName}-$name").toH2DbConfig,
      )
    // The caller's close context stays open throughout the test: only the storage gets closed.
    val caller = FlagCloseable(logger, timeouts)
    test(setup.storage, CloseContext(caller)).thereafter { _ =>
      caller.close()
      setup.close()
    }
  }

  "DbStorageSingle" should {

    "abort an operation issued after close without retrying or warning" in withStorageAndOpenCaller(
      "issued-after-close"
    ) { (storage, callerCC) =>
      val attempts = new AtomicInteger(0)
      storage.close()

      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        storage
          .query(DBIO.successful(()).map(_ => attempts.incrementAndGet()), "query-after-close")(
            traceContext,
            callerCC,
          )
          .unwrap
          .map { result =>
            result shouldBe UnlessShutdown.AbortedDueToShutdown
            attempts.get() shouldBe 0
          },
        _ shouldBe empty,
      )
    }

    "stop retrying an operation when the storage closes, even if the caller stays open" in withStorageAndOpenCaller(
      "retrying-during-close"
    ) { (storage, callerCC) =>
      val attempts = new AtomicInteger(0)
      val retrying = Promise[Unit]()
      val alwaysFailing = DBIO.successful(()).flatMap { _ =>
        if (attempts.incrementAndGet() >= 2) retrying.trySuccess(()).discard
        DBIO.failed(new SQLTransientConnectionException("test transient failure"))
      }

      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
        {
          val resultF =
            storage.update(alwaysFailing, "update-retrying-during-close")(traceContext, callerCC)
          for {
            _ <- retrying.future
            _ = storage.close()
            result <- resultF.unwrap
          } yield result shouldBe UnlessShutdown.AbortedDueToShutdown
        },
        _ shouldBe empty,
      )
    }
  }
}
