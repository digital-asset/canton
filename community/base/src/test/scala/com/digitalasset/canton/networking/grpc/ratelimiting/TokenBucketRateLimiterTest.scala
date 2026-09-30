// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.RateLimitConfig
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.RateLimitMetrics
import com.digitalasset.canton.util.PekkoUtil
import com.typesafe.scalalogging.Logger
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** Ported from splice's
  * `apps/common/src/test/scala/org/lfdecentralizedtrust/splice/util/SpliceRateLimiterTest.scala`.
  */
class TokenBucketRateLimiterTest
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with MetricValues
    with BeforeAndAfterAll {

  // the real-elapsed-time tests below run for several seconds
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(Span(30, Seconds))

  // Can't mix in canton's own HasActorSystem/HasExecutionContext here: both live in
  // community-testing, which depends on community-base, so community-base depending back on
  // either (even in Test scope) would be a circular module dependency. This reproduces just the
  // handful of lines HasActorSystem itself needs, using primitives (PekkoUtil, Materializer)
  // already in community-base's own main sources.
  private implicit val ec: DirectExecutionContext =
    DirectExecutionContext(Logger(LoggerFactory.getLogger(getClass)))
  private implicit lazy val actorSystem: ActorSystem =
    PekkoUtil.createActorSystem(getClass.getSimpleName)
  private implicit lazy val materializer: Materializer = Materializer(actorSystem)

  override def afterAll(): Unit =
    try actorSystem.terminate().discard
    finally super.afterAll()

  private def limiter(config: RateLimitConfig): TokenBucketRateLimiter =
    new TokenBucketRateLimiter(
      "test",
      config,
      RateLimitMetrics(
        NoOpMetricsFactory,
        TracedLogger(classOf[TokenBucketRateLimiterTest], NamedLoggerFactory.root),
      )(MetricsContext.Empty),
    )

  "TokenBucketRateLimiter" should {

    "accept requests under limit" in {
      val elementsToRun = 100
      withRateLimiter() { case (rateLimitMetrics, rateLimiter) =>
        runThroughRateLimiter(rateLimiter, 9, elementsToRun).reduce(_ && _) shouldBe true

        rateLimitMetrics.meter.valueFilteredOnLabels(
          LabelFilter("result", "accepted"),
          LabelFilter("limiter", "test"),
        ) shouldBe elementsToRun
      }
    }

    "reject requests that are over the limit" in {
      withRateLimiter() { case (rateLimitMetrics, rateLimiter) =>
        val results = runThroughRateLimiter(rateLimiter, 100, 1000)

        val (accepted, rejected) = results.partition(identity)

        // estimate for running 10 seconds, with some overhead for slower execution
        accepted.length should (be > 85 and be < 150)

        rateLimitMetrics.meter.valueFilteredOnLabels(
          LabelFilter("result", "accepted"),
          LabelFilter("limiter", "test"),
        ) should be(accepted.length)

        rateLimitMetrics.meter.valueFilteredOnLabels(
          LabelFilter("result", "rejected"),
          LabelFilter("limiter", "test"),
        ) should be(rejected.length)
      }
    }

    "start with the configured rate per second worth of permits" in {
      val l = limiter(RateLimitConfig(ratePerSecond = 10))

      // the limiter must not have to warm up first: it holds its configured rate worth of permits
      // (10) right from its creation, plus guava's deferred payment for the next one
      val results = Seq.fill(50)(l.markRun())
      results.take(11) should contain only true
      results.count(!_) should be > 35
    }

    "not create any limiter if disabled" in {
      // a disabled limiter must not fail even for a rate that guava would reject
      val l = limiter(RateLimitConfig(enabled = false, ratePerSecond = 0))
      Seq.fill(100)(l.markRun()) should contain only true
    }

    "reject everything if the rate is zero" in {
      withRateLimiter(RateLimitConfig(ratePerSecond = 0)) { case (rateLimitMetrics, rateLimiter) =>
        Seq.fill(100)(rateLimiter.markRun()) should contain only false

        rateLimitMetrics.meter.valueFilteredOnLabels(
          LabelFilter("limiter", "test"),
          LabelFilter("result", "rejected"),
        ) should be(100)
      }
    }

    "reject everything if the sustained rate is zero" in {
      val l = limiter(RateLimitConfig(ratePerSecond = 100, sustainedRatePerSecond = Some(0)))
      Seq.fill(100)(l.markRun()) should contain only false
    }

    // Not ported from splice. Additional deterministic coverage of the burst/sustained
    // interaction, complementing "throttle to the sustained rate..." below without needing real
    // elapsed time.
    "combine the burst and sustained limits, taking whichever is more restrictive" in {
      // the burst limiter alone would allow far more than 3 immediate calls. The sustained
      // limiter's own initial budget (rounded to whole permits, capped at 1 second worth) is the
      // binding constraint here since no time elapses between calls in this test
      val l = limiter(
        RateLimitConfig(
          ratePerSecond = 1000,
          sustainedRatePerSecond = Some(3),
          sustainedWindowSeconds = 60,
        )
      )

      val results = Seq.fill(20)(l.markRun())
      results.take(4) should contain only true
      results.count(!_) should be > 10
    }

    "let the burst limit be the binding constraint when it is smaller than the sustained one" in {
      val l = limiter(
        RateLimitConfig(
          ratePerSecond = 2,
          sustainedRatePerSecond = Some(1000),
          sustainedWindowSeconds = 60,
        )
      )

      val results = Seq.fill(20)(l.markRun())
      results.take(3) should contain only true
      results.count(!_) should be > 10
    }
  }

  "TokenBucketRateLimiter with a sustained limit" should {

    "throttle to the sustained rate once the burst budget is drained" in {
      withRateLimiter(
        RateLimitConfig(ratePerSecond = 1000, sustainedRatePerSecond = Some(10))
      ) { case (_, rateLimiter) =>
        val results = TokenBucketRateLimiterTest
          .runRateLimited(40, 120) {
            rateLimiter.runWithLimit(Future.successful(true))
          }
          .futureValue
        // ~3 seconds of runtime at 10 permits/s, with generous slack
        results.count(identity) should (be >= 10 and be <= 60)
        results.count(!_) should be > 0
      }
    }
  }

  private def runThroughRateLimiter(
      rateLimiter: TokenBucketRateLimiter,
      runsPerSecond: Int,
      runFor: Int,
  ) =
    TokenBucketRateLimiterTest
      .runRateLimited(runsPerSecond, runFor) {
        rateLimiter.runWithLimit(Future.successful(true))
      }
      .futureValue

  private def withRateLimiter[A](
      config: RateLimitConfig = RateLimitConfig(ratePerSecond = 10)
  )(f: (RateLimitMetrics, TokenBucketRateLimiter) => A): A = {
    val metricsFactory = new InMemoryMetricsFactory()
    val rateLimitMetrics = RateLimitMetrics(
      metricsFactory,
      TracedLogger(classOf[TokenBucketRateLimiterTest], NamedLoggerFactory.root),
    )(MetricsContext.Empty)
    val rateLimiter = new TokenBucketRateLimiter("test", config, rateLimitMetrics)
    try f(rateLimitMetrics, rateLimiter)
    finally rateLimitMetrics.close()
  }
}

object TokenBucketRateLimiterTest {

  /** Ported from `runRateLimited` in splice's
    * `apps/common/src/test/scala/org/lfdecentralizedtrust/splice/util/SpliceRateLimiterTest.scala`,
    * dropping the HttpCommandException/CommandFailure recover cases: those are splice
    * console/HTTP-client concepts that don't apply here, and the StatusRuntimeException case
    * already covers what TokenBucketRateLimiter.runWithLimit actually throws.
    */
  def runRateLimited(runRate: Int, elementsToRun: Int)(
      run: => Future[?]
  )(implicit
      mat: Materializer
  ): Future[Seq[Boolean]] = {
    import mat.executionContext
    Source
      .repeat(())
      .take(elementsToRun.longValue())
      .throttle(runRate, 1.second)
      .mapAsync(elementsToRun)(_ =>
        run
          .map(_ => true)
          .recover {
            case rejection: StatusRuntimeException
                if rejection.getStatus.getCode == Status.Code.RESOURCE_EXHAUSTED =>
              false
          }
      )
      // throttle after as well to ensure that even for runs that take a while to execute we still keep the rate
      .throttle(runRate, 1.second)
      .runWith(Sink.seq)
  }
}
