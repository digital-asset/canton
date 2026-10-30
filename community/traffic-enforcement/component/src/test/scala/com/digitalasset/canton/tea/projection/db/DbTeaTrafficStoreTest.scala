// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.projection.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  DbConfig,
  DbParametersConfig,
  PositiveFiniteDuration,
  ProcessingTimeout,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig
import com.digitalasset.canton.resource.DbLockedConnectionPool.NoActiveConnectionAvailable
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.DbAction.All
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.store.db.{DbStorageIdempotency, DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tea.TrafficEnforcementErrors.TrafficEnforcementError
import com.digitalasset.canton.tea.projection.{
  CorrelationAccount,
  CorrelationKey,
  EventSource,
  SubmissionCorrelation,
  TeaTrafficStoreTest,
  TrafficDelta,
}
import com.digitalasset.canton.tea.{AccountId, TrafficEnforcementErrors}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.{BaseTest, DefaultDamlValues}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait DbTeaTrafficStoreTest extends AsyncWordSpec with BaseTest with TeaTrafficStoreTest {
  this: DbTest =>

  private val defaultConfig = TrafficEnforcementServerConfig.Internal()

  private def newStore(
      db: DbStorage = storage,
      databaseQueryTimeout: PositiveFiniteDuration = defaultConfig.databaseQueryTimeout,
      accountLookupTimeout: PositiveFiniteDuration = defaultConfig.accountLookupTimeout,
  ): TeaDbTrafficStore =
    new TeaDbTrafficStore(
      db,
      loggerFactory,
      timeouts,
      databaseQueryTimeout,
      accountLookupTimeout,
    )

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_traffic_enforcement_event",
        sqlu"truncate table par_traffic_enforcement_correlations",
        sqlu"truncate table par_traffic_enforcement_uncharged_completions",
        sqlu"truncate table par_traffic_enforcement_balance",
        sqlu"truncate table par_traffic_enforcement_state",
        // Double quotes to force H2 to use lower case table names
        sqlu"""truncate table "pekko_projection_offset_store"""",
        sqlu"""truncate table "pekko_projection_management"""",
      ),
      functionFullName,
    )
  }

  "TeaTrafficStore" should {
    behave like teaTrafficStore(
      () => newStore(),
      (store: TeaDbTrafficStore, event) => {
        import storage.api.*
        storage
          .queryAndUpdate(
            store
              .applyCompletionDBIO(
                event,
                "test",
                EventSource.LedgerAPICompletions,
                completionPolicy,
              )
              .transactionally,
            "apply_completion",
          )
      },
    )

    "keep a correlation that replaced a pruned one with the same key" in {
      val store = newStore()
      val pruned = SubmissionCorrelation(
        CorrelationKey(DefaultDamlValues.lfhash(), "submission"),
        CorrelationAccount.Account(AccountId.tryCreate("alice")),
        CantonTimestamp.Epoch,
      )
      val replacement = pruned.copy(insertedAt = pruned.insertedAt.plusSeconds(1))

      for {
        _ <- store.insertCorrelation(pruned).valueOrFail("insert pruned")
        _ <- store.prune(pruned.insertedAt).valueOrFail("prune")
        _ <- store.insertCorrelation(replacement).valueOrFail("insert replacement")
        deleted <- storage.update(store.deleteCorrelationDBIO(pruned), "delete_pruned")
        remaining <- store.lookupCorrelation(pruned.key).valueOrFail("lookup")
      } yield {
        deleted shouldBe 0
        remaining shouldBe Some(replacement)
      }
    }

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
          // NOTE: `lockHeld` holds a JDBC connection open until `releaseLock` is fulfilled. The storage
          // configures the connection pool to have at most as many connections as there are threads.
          // We therefore need to ensure that the test runs with at least 2 threads so that the `getBalance`
          // call below does not block forever trying to obtain a connection.
          val lockHeld = storage.underlying.queryAndUpdate(
            (for {
              _ <- sqlu"lock table par_traffic_enforcement_balance in access exclusive mode"
              _ = lockAcquired.success(())
              _ <- DBIO.from(releaseLock.future)
            } yield ()).transactionally,
            "hold_table_lock",
          )

          val timedOutStore = newStore(databaseQueryTimeout = PositiveFiniteDuration.ofMillis(50))

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

    def storeWithFullPool(
        databaseQueryTimeout: PositiveFiniteDuration = defaultConfig.databaseQueryTimeout,
        accountLookupTimeout: PositiveFiniteDuration = defaultConfig.accountLookupTimeout,
    ): (DbTeaTrafficStoreTest.FullConnectionPoolStorage, TeaDbTrafficStore) = {
      val fullPoolStorage = new DbTeaTrafficStoreTest.FullConnectionPoolStorage(
        storage.underlying,
        timeouts,
        loggerFactory,
      )
      (fullPoolStorage, newStore(fullPoolStorage, databaseQueryTimeout, accountLookupTimeout))
    }
    val fullPoolCorrelation = SubmissionCorrelation(
      CorrelationKey(DefaultDamlValues.lfhash(), "submission"),
      CorrelationAccount.Account(AccountId.tryCreate("alice")),
      CantonTimestamp.Epoch,
    )

    "retry a write while the pool has no free connection" in {
      val (fullPoolStorage, store) = storeWithFullPool()

      fullPoolStorage.failNextAttempts(2)
      for {
        _ <- store.insertCorrelation(fullPoolCorrelation).valueOrFail("insert")
        stored <- store.lookupCorrelation(fullPoolCorrelation.key).valueOrFail("lookup")
      } yield {
        fullPoolStorage.attempts shouldBe 3
        stored shouldBe Some(fullPoolCorrelation)
      }
    }

    "give up with a transient failure when the pool stays full" in {
      // The 1ms query timeout caps every delay after the first 50ms one, so the retries take
      // about 60ms in total.
      val (fullPoolStorage, store) =
        storeWithFullPool(databaseQueryTimeout = PositiveFiniteDuration.ofMillis(1))

      fullPoolStorage.failNextAttempts(Int.MaxValue)
      store.insertCorrelation(fullPoolCorrelation).value.map { result =>
        result.left.value shouldBe a[TrafficEnforcementErrors.TransientFailure.Reject]
        fullPoolStorage.attempts shouldBe TeaDbTrafficStore.NoActiveConnectionMaxRetries + 1
      }
    }

    "stop retrying in time to answer before the client times out" in {
      // Retries stop 150ms in. Every delay is at least 25ms, so all 10 retries would need 275ms.
      val (fullPoolStorage, store) = storeWithFullPool(
        databaseQueryTimeout = PositiveFiniteDuration.ofMillis(50),
        accountLookupTimeout = PositiveFiniteDuration.ofMillis(300),
      )

      fullPoolStorage.failNextAttempts(Int.MaxValue)
      store.insertCorrelation(fullPoolCorrelation).value.map { result =>
        result.left.value shouldBe a[TrafficEnforcementErrors.TransientFailure.Reject]
        fullPoolStorage.attempts should be > 1
        fullPoolStorage.attempts should be < TeaDbTrafficStore.NoActiveConnectionMaxRetries + 1
      }
    }

    // Postgres only. H2 has no transaction-scoped statement timeout to fire.
    "cancel a correlation insert that runs past the statement timeout" in {
      storage.profile match {
        case _: Profile.Postgres =>
          import storage.api.*
          val lockAcquired = Promise[Unit]()
          val releaseLock = Promise[Unit]()

          // Same setup as the balance lookup case above.
          val lockHeld = storage.underlying.queryAndUpdate(
            (for {
              _ <- sqlu"lock table par_traffic_enforcement_correlations in access exclusive mode"
              _ = lockAcquired.success(())
              _ <- DBIO.from(releaseLock.future)
            } yield ()).transactionally,
            "hold_table_lock",
          )
          val store = newStore(databaseQueryTimeout = PositiveFiniteDuration.ofMillis(50))

          for {
            _ <- FutureUnlessShutdown.outcomeF(lockAcquired.future)
            result <- store
              .insertCorrelation(fullPoolCorrelation)
              .value
              .thereafter(_ => releaseLock.trySuccess(()).discard)
            _ <- lockHeld
            stored <- store.lookupCorrelation(fullPoolCorrelation.key).valueOrFail("lookup")
          } yield {
            result.left.value shouldBe a[TrafficEnforcementErrors.TransientFailure.Reject]
            stored shouldBe None
          }

        case _: Profile.H2 =>
          cancel("statement_timeout enforcement only applies to PostgreSQL")
      }
    }
  }
}

object DbTeaTrafficStoreTest {

  /** Fails writes with `NoActiveConnectionAvailable` before they reach the underlying storage, for
    * the requested number of attempts.
    */
  final class FullConnectionPoolStorage(
      underlying: DbStorage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext)
      extends DbStorageIdempotency(underlying, timeouts, loggerFactory) {
    private val remainingFailures = new AtomicInteger(0)
    private val attemptCount = new AtomicInteger(0)

    def failNextAttempts(count: Int): Unit = remainingFailures.set(count)
    def attempts: Int = attemptCount.get()

    override protected[canton] def runWrite[A](
        action: All[A],
        operationName: String,
        maxRetries: Int,
    )(implicit traceContext: TraceContext, closeContext: CloseContext): FutureUnlessShutdown[A] = {
      attemptCount.incrementAndGet().discard
      if (remainingFailures.getAndUpdate(n => math.max(n - 1, 0)) > 0)
        FutureUnlessShutdown.failed(NoActiveConnectionAvailable())
      else super.runWrite(action, operationName, maxRetries)
    }
  }
}

class DbTeaTrafficStorePostgresTest extends DbTeaTrafficStoreTest with PostgresTest {
  override def mkDbConfig(basicConfig: DbBasicConfig): DbConfig.Postgres = {
    // Ensure that the DB has at least 2 connections (see NOTE above)
    val nbThreads = Threading.detectNumberOfThreads(NamedLogging.noopNoTracingLogger)
    val nbConnections = nbThreads.max(PositiveInt.two)

    val defaultDbConfig = super.mkDbConfig(basicConfig)
    defaultDbConfig.copy(parameters = DbParametersConfig(maxConnections = Some(nbConnections)))
  }
}

class DbTeaTrafficStoreH2Test extends DbTeaTrafficStoreTest with H2Test
