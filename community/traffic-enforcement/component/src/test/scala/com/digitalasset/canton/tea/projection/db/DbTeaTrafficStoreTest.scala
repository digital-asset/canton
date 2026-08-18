// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficEnforcementError
import com.digitalasset.canton.tea.projection.{AccountId, TeaTrafficStoreTest, TrafficDelta}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

trait DbTeaTrafficStoreTest extends AsyncWordSpec with BaseTest with TeaTrafficStoreTest {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_traffic_enforcement_event",
        sqlu"truncate table par_traffic_enforcement_balance",
        sqlu"truncate table pekko_projection_offset_store",
        sqlu"truncate table pekko_projection_management",
      ),
      functionFullName,
    )
  }

  "TeaTrafficStore" should {
    behave like teaTrafficStore(() =>
      new TeaDbTrafficStore(
        storage,
        loggerFactory,
        timeouts,
        TrafficEnforcementServerConfig.Internal().databaseQueryTimeout,
      )
    )

    "classify a malformed query as fatal" in {
      import storage.api.*
      val malformedQuery =
        sql"select nonexistent_column from par_traffic_enforcement_balance".as[Long]
      val classified: Future[TrafficEnforcementError] = loggerFactory.suppressWarningsAndErrors {
        storage.query(malformedQuery, "malformed_query").unwrap.transform {
          case Failure(ex) => Success(TeaDbTrafficStore.classifyFailure(ex))
          case Success(_) => Failure(new RuntimeException("expected the malformed query to fail"))
        }
      }
      classified.map(_ shouldBe a[TrafficEnforcementErrors.FatalFailure.Reject])
    }

    "classify a persist failure due to an infrastructure exception as fatal" in {
      import storage.api.*
      val accountId = AccountId.tryCreate("classify-persist-account")
      val trafficDelta = TrafficDelta.creditBalanceDelta(1L)
      val classify = TeaDbTrafficStore.classifyPersistFailure(accountId, trafficDelta)

      val malformedQuery =
        sql"select nonexistent_column from par_traffic_enforcement_balance".as[Long]
      loggerFactory
        .suppressWarningsAndErrors {
          storage
            .query(malformedQuery, "malformed_query")
            .unwrap
            .transform {
              case Failure(ex) => Success(ex)
              case Success(_) =>
                Failure(new RuntimeException("expected the malformed query to fail"))
            }
            // classify constructs the error, which logs it, so it must stay inside the
            // suppressed block.
            .map(classify)
        }
        .map(_ shouldBe a[TrafficEnforcementErrors.FatalFailure.Reject])
    }

    // Postgres only: H2 has no transaction-scoped statement timeout to fire.
    "classify a fired statement_timeout as transient" in {
      storage.profile match {
        case _: Profile.Postgres =>
          import storage.api.*
          val lockAcquired = Promise[Unit]()
          val releaseLock = Promise[Unit]()

          // Run on the raw (non-idempotency-wrapping) storage: the wrapper re-runs every write
          // once more after the first completes, which would re-acquire this lock forever.
          val lockHeld = storage.underlying.queryAndUpdate(
            (for {
              _ <- sqlu"lock table par_traffic_enforcement_balance in access exclusive mode"
              _ = lockAcquired.success(())
              _ <- DBIO.from(releaseLock.future)
            } yield ()).transactionally,
            "hold_table_lock",
          )

          val timedOutStore = new TeaDbTrafficStore(
            storage,
            loggerFactory,
            timeouts,
            PositiveFiniteDuration.ofMillis(50),
          )

          for {
            _ <- FutureUnlessShutdown.outcomeF(lockAcquired.future)
            // Released here rather than as a later step, so a failed lookup cannot leave the
            // access exclusive lock held and block every remaining case in this suite.
            result <- timedOutStore
              .getBalance(AccountId.tryCreate("lock-test-account"))
              .value
              .thereafter(_ => releaseLock.trySuccess(()).discard)
            _ <- lockHeld
          } yield result.left.value shouldBe a[TrafficEnforcementErrors.TransientFailure.Reject]

        case _: Profile.H2 =>
          cancel("statement_timeout enforcement only applies to PostgreSQL")
      }
    }
  }
}

class DbTeaTrafficStorePostgresTest extends DbTeaTrafficStoreTest with PostgresTest

class DbTeaTrafficStoreH2Test extends DbTeaTrafficStoreTest with H2Test
