// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.tracing.TraceContext
import org.slf4j.event.Level
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

class HttpExtensionOAuthTokenManagerTest extends AsyncWordSpec with BaseTest {

  implicit val tc: TraceContext = TraceContext.empty

  private final class FakeRuntime(
      initialNowMillis: Long = 1000L,
      requestIds: Seq[String] = Seq("req-1", "req-2", "req-3", "req-4", "req-5"),
  ) {
    private val ids = mutable.Queue.from(requestIds)
    private var now = initialNowMillis

    def nowMillis(): Long = this.synchronized(now)

    def advanceMillis(millis: Long): Unit = this.synchronized {
      now += millis
    }

    def newRequestId(): String = this.synchronized {
      if (ids.nonEmpty) {
        ids.dequeue()
      } else {
        fail("No request IDs left in fake runtime")
      }
    }
  }

  private def token(
      value: String,
      nowMillis: Long,
      expiresInSeconds: Option[Long],
  ): HttpExtensionOAuthAccessToken =
    HttpExtensionOAuthAccessToken(
      value = value,
      expiresAtMillis = expiresInSeconds.map(seconds => nowMillis + (seconds * 1000L)),
    )

  private def requestTimeoutForRemainingBudget(
      runtime: FakeRuntime,
      requestTimeoutMillis: Long,
  )(deadlineMs: Long): Either[ExtensionCallError, Duration] = {
    val remainingBudgetMs = deadlineMs - runtime.nowMillis()
    if (remainingBudgetMs <= 0) {
      Left(ExtensionCallError(504, "Total timeout exceeded", None))
    } else {
      Right(Duration.ofMillis(math.min(requestTimeoutMillis, remainingBudgetMs)))
    }
  }

  private def makeManager(
      runtime: FakeRuntime,
      requestTimeoutMillis: Long = 10000L,
  )(
      acquireToken: (Duration, String) => Either[ExtensionCallError, HttpExtensionOAuthAccessToken]
  ): HttpExtensionOAuthTokenManager =
    new HttpExtensionOAuthTokenManager(
      extensionId = "test-ext",
      requestTimeoutForRemainingBudget = requestTimeoutForRemainingBudget(runtime, requestTimeoutMillis),
      acquireToken = acquireToken,
      nowMillis = () => runtime.nowMillis(),
      newRequestId = () => runtime.newRequestId(),
      loggerFactory = loggerFactory,
    )

  private def deadlineFrom(runtime: FakeRuntime, extraMillis: Long = 25000L): Long =
    runtime.nowMillis() + extraMillis

  "HttpExtensionOAuthTokenManager" should {

    "reuse an unexpired cached token without starting a second acquisition" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val acquireCalls = new AtomicInteger(0)
      val manager = makeManager(runtime) { (_, _) =>
        acquireCalls.incrementAndGet()
        Right(token("token-1", runtime.nowMillis(), expiresInSeconds = Some(120L)))
      }

      val deadlineMs = deadlineFrom(runtime)
      val first = manager.currentBearerToken(deadlineMs)
      val second = manager.currentBearerToken(deadlineMs)

      first shouldBe Right("token-1")
      second shouldBe Right("token-1")
      acquireCalls.get() shouldBe 1
    }

    "avoid cache reuse when the acquired token has no expiry" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val issuedTokens = Iterator("token-1", "token-2")
      val manager = makeManager(runtime) { (_, _) =>
        Right(token(issuedTokens.next(), runtime.nowMillis(), expiresInSeconds = None))
      }

      val deadlineMs = deadlineFrom(runtime)
      val first = manager.currentBearerToken(deadlineMs)
      val second = manager.currentBearerToken(deadlineMs)

      first shouldBe Right("token-1")
      second shouldBe Right("token-2")
    }

    "reacquire a token after the cached token expires" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val issuedTokens = Iterator(
        token("token-1", runtime.nowMillis(), expiresInSeconds = Some(1L)),
        token("token-2", runtime.nowMillis() + 5000L, expiresInSeconds = Some(120L)),
      )
      val acquireCalls = new AtomicInteger(0)
      val manager = makeManager(runtime) { (_, _) =>
        acquireCalls.incrementAndGet()
        Right(issuedTokens.next())
      }

      val first = manager.currentBearerToken(deadlineFrom(runtime))
      runtime.advanceMillis(5000L)
      val second = manager.currentBearerToken(deadlineFrom(runtime))

      first shouldBe Right("token-1")
      second shouldBe Right("token-2")
      acquireCalls.get() shouldBe 2
    }

    "log token acquisition, cache reuse, and reacquisition events" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val issuedTokens = Iterator(
        token("token-1", runtime.nowMillis(), expiresInSeconds = Some(1L)),
        token("token-2", runtime.nowMillis() + 1500L, expiresInSeconds = Some(120L)),
      )
      val manager = makeManager(runtime) { (_, _) =>
        Right(issuedTokens.next())
      }

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          Future.successful {
            manager.currentBearerToken(deadlineFrom(runtime)) shouldBe Right("token-1")
            manager.currentBearerToken(deadlineFrom(runtime)) shouldBe Right("token-1")
            runtime.advanceMillis(1500L)
            manager.currentBearerToken(deadlineFrom(runtime)) shouldBe Right("token-2")
            succeed
          },
          logs => {
            val messages = logs.map(_.message)
            messages.exists(_.contains("Starting OAuth token acquisition")) shouldBe true
            messages.exists(_.contains("OAuth token acquisition succeeded")) shouldBe true
            messages.exists(_.contains("Reusing cached OAuth token")) shouldBe true
            messages.exists(_.contains("Reacquiring OAuth token")) shouldBe true
          },
        )
        .map(_ => succeed)
    }

    "serialize concurrent cold-cache misses so waiters share one successful acquisition" in {
      val executor = Executors.newFixedThreadPool(2)
      val managerEc = ExecutionContext.fromExecutorService(executor)
      val acquisitionStarted = Promise[Unit]()
      val releaseAcquisition = Promise[Unit]()
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val acquireCalls = new AtomicInteger(0)
      val manager = makeManager(runtime) { (_, _) =>
        acquireCalls.incrementAndGet()
        acquisitionStarted.trySuccess(())
        Await.result(releaseAcquisition.future, 5.seconds)
        Right(token("token-1", runtime.nowMillis(), expiresInSeconds = Some(120L)))
      }
      val firstF = Future(manager.currentBearerToken(deadlineFrom(runtime)))(managerEc)
      val secondF = Future(manager.currentBearerToken(deadlineFrom(runtime)))(managerEc)

      val resultF = for {
        first <- firstF
        second <- secondF
      } yield {
        first shouldBe Right("token-1")
        second shouldBe Right("token-1")
        acquireCalls.get() shouldBe 1
      }

      val gated = acquisitionStarted.future.flatMap { _ =>
        releaseAcquisition.success(())
        resultF
      }(executionContext)

      gated.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "propagate one failed shared acquisition to waiters and allow a later retry" in {
      val executor = Executors.newFixedThreadPool(2)
      val managerEc = ExecutionContext.fromExecutorService(executor)
      val acquisitionStarted = Promise[Unit]()
      val releaseFailure = Promise[Unit]()
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"))
      val acquireCalls = new AtomicInteger(0)
      val manager = makeManager(runtime) { (_, requestId) =>
        acquireCalls.incrementAndGet() match {
          case 1 =>
            acquisitionStarted.trySuccess(())
            Await.result(releaseFailure.future, 5.seconds)
            Left(ExtensionCallError(500, "Unexpected error: issuer-boom", Some(requestId)))
          case 2 =>
            Right(token("token-2", runtime.nowMillis(), expiresInSeconds = Some(120L)))
          case other =>
            fail(s"Unexpected acquisition number: $other")
        }
      }
      val firstF = Future(manager.currentBearerToken(deadlineFrom(runtime)))(managerEc)
      val secondF = Future(manager.currentBearerToken(deadlineFrom(runtime)))(managerEc)

      val sharedFailureF = Future.sequence(Seq(firstF, secondF))

      val resultF = for {
        _ <- acquisitionStarted.future.map(_ => releaseFailure.success(()))(executionContext)
        sharedFailures <- sharedFailureF
        laterResult = manager.currentBearerToken(deadlineFrom(runtime))
      } yield {
        sharedFailures shouldBe Seq(
          Left(ExtensionCallError(500, "Unexpected error: issuer-boom", Some("req-1"))),
          Left(ExtensionCallError(500, "Unexpected error: issuer-boom", Some("req-1"))),
        )
        laterResult shouldBe Right("token-2")
        acquireCalls.get() shouldBe 2
      }

      resultF.transform { result =>
        executor.shutdownNow()
        result
      }(executionContext)
    }

    "invalidate only the matching cached token value" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2", "req-3"))
      val acquireCalls = new AtomicInteger(0)
      val issuedTokens = Iterator(
        token("token-1", runtime.nowMillis(), expiresInSeconds = Some(1L)),
        token("token-2", runtime.nowMillis() + 5000L, expiresInSeconds = Some(120L)),
        token("token-3", runtime.nowMillis() + 5000L, expiresInSeconds = Some(120L)),
      )
      val manager = makeManager(runtime) { (_, _) =>
        acquireCalls.incrementAndGet()
        Right(issuedTokens.next())
      }

      val initial = manager.currentBearerToken(deadlineFrom(runtime))
      runtime.advanceMillis(5000L)
      val refreshed = manager.currentBearerToken(deadlineFrom(runtime))
      manager.invalidateCachedTokenIfMatches("token-1")
      val reusedFresh = manager.currentBearerToken(deadlineFrom(runtime))
      manager.invalidateCachedTokenIfMatches("token-2")
      val reacquired = manager.currentBearerToken(deadlineFrom(runtime))

      initial shouldBe Right("token-1")
      refreshed shouldBe Right("token-2")
      reusedFresh shouldBe Right("token-2")
      reacquired shouldBe Right("token-3")
      acquireCalls.get() shouldBe 3
    }

    "log matching token invalidation" in {
      val runtime = new FakeRuntime(requestIds = Seq("req-1", "req-2"))
      val manager = makeManager(runtime) { (_, _) =>
        Right(token("token-1", runtime.nowMillis(), expiresInSeconds = Some(120L)))
      }

      loggerFactory
        .assertLogsSeq(SuppressionRule.LevelAndAbove(Level.DEBUG))(
          Future.successful {
            manager.currentBearerToken(deadlineFrom(runtime)) shouldBe Right("token-1")
            manager.invalidateCachedTokenIfMatches("token-1")
            manager.currentBearerToken(deadlineFrom(runtime)) shouldBe Right("token-1")
            succeed
          },
          logs => {
            val messages = logs.map(_.message)
            messages.exists(_.contains("Invalidated cached OAuth token")) shouldBe true
          },
        )
        .map(_ => succeed)
    }
  }
}
