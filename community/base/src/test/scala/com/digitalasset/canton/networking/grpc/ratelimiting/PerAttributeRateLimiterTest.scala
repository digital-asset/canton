// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.config.{PerAttributeRateLimitConfig, RateLimitConfig}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.RateLimitMetrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Ported from splice's
  * `apps/common/src/test/scala/org/lfdecentralizedtrust/splice/util/SpliceRateLimiterTest.scala`
  * ("the per attribute rate limiter" and "... with IP CIDR overrides" sections), adapted to this
  * rate limiter's rename.
  */
class PerAttributeRateLimiterTest extends AnyWordSpec with Matchers {

  private def metrics: RateLimitMetrics =
    RateLimitMetrics(
      NoOpMetricsFactory,
      TracedLogger(classOf[PerAttributeRateLimiterTest], NamedLoggerFactory.root),
    )(MetricsContext.Empty)

  private def limiter(
      config: PerAttributeRateLimitConfig,
      attributeMatcherFactory: PerAttributeRateLimitConfig => String => Option[
        RateLimitConfig.Simple
      ] = PerAttributeRateLimiter.exactMatch,
  ): PerAttributeRateLimiter =
    new PerAttributeRateLimiter(
      "test",
      "test_attribute",
      config,
      metrics,
      TracedLogger(classOf[PerAttributeRateLimiterTest], NamedLoggerFactory.root),
      attributeMatcherFactory,
    )

  "the per attribute rate limiter" should {

    "limit each attribute value separately" in {
      val l = limiter(PerAttributeRateLimitConfig(limit = RateLimitConfig(ratePerSecond = 1)))

      val ip1 = Seq.fill(20)(l.markRun(Some("1.1.1.1")))
      ip1.count(identity) shouldBe 2
      ip1.count(!_) shouldBe 18

      // a different attribute value is not affected by the limiter of the first one
      l.markRun(Some("2.2.2.2")) shouldBe true
      l.markRun(Some("2.2.2.2")) shouldBe true
      l.markRun(Some("2.2.2.2")) shouldBe false
    }

    "not limit requests with an unknown attribute value" in {
      val l = limiter(PerAttributeRateLimitConfig(limit = RateLimitConfig(ratePerSecond = 1)))

      // requests without an attribute value are not rate limited here; the overall/global rate
      // limiter is relied upon to bound them instead
      Seq.fill(20)(l.markRun(None)) should contain only true
      // requests with a known attribute value are still limited
      l.markRun(Some("1.1.1.1")) shouldBe true
    }

    "not limit anything if disabled" in {
      val l = limiter(PerAttributeRateLimitConfig.disabled)
      Seq.fill(100)(l.markRun(Some("1.1.1.1"))) should contain only true
      Seq.fill(100)(l.markRun(None)) should contain only true
    }

    "reject all requests with an attribute value if the per attribute rate is zero" in {
      val l = limiter(PerAttributeRateLimitConfig(limit = RateLimitConfig(ratePerSecond = 0)))
      Seq.fill(20)(l.markRun(Some("1.1.1.1"))) should contain only false
      Seq.fill(20)(l.markRun(Some("2.2.2.2"))) should contain only false
      // requests without an attribute value are still not limited here
      Seq.fill(20)(l.markRun(None)) should contain only true
    }
  }

  "the per attribute rate limiter with attribute overrides" should {

    "use the custom limit of a matching attribute value" in {
      val l = limiter(
        PerAttributeRateLimitConfig(
          limit = RateLimitConfig(ratePerSecond = 1),
          attributeOverrides = Map("1.1.1.1" -> RateLimitConfig(ratePerSecond = 3)),
        )
      )

      Seq.fill(20)(l.markRun(Some("1.1.1.1"))).count(identity) shouldBe 4
      Seq.fill(20)(l.markRun(Some("2.2.2.2"))).count(identity) shouldBe 2
    }

    "not apply overrides if per attribute limiting is disabled" in {
      val l = limiter(
        PerAttributeRateLimitConfig(
          enabled = false,
          attributeOverrides = Map("1.1.1.1" -> RateLimitConfig(ratePerSecond = 1)),
        )
      )
      Seq.fill(100)(l.markRun(Some("1.1.1.1"))) should contain only true
    }
  }

  "the per attribute rate limiter with IP CIDR overrides" should {

    "use the custom limit for IPs of a matching network" in {
      val l = limiter(
        PerAttributeRateLimitConfig(
          limit = RateLimitConfig(ratePerSecond = 1),
          attributeOverrides = Map("10.0.0.0/8" -> RateLimitConfig(ratePerSecond = 3)),
        ),
        IpCidrRateLimits.matchClientIp,
      )

      Seq.fill(20)(l.markRun(Some("10.1.2.3"))).count(identity) shouldBe 4
      Seq.fill(20)(l.markRun(Some("10.4.5.6"))).count(identity) shouldBe 4
      // non-matching IPs use the default limit
      Seq.fill(20)(l.markRun(Some("11.1.2.3"))).count(identity) shouldBe 2
    }

    "use the most specific matching network" in {
      val l = limiter(
        PerAttributeRateLimitConfig(
          limit = RateLimitConfig(ratePerSecond = 100),
          attributeOverrides = Map(
            "10.0.0.0/8" -> RateLimitConfig(ratePerSecond = 3),
            "10.1.2.3" -> RateLimitConfig(ratePerSecond = 1),
          ),
        ),
        IpCidrRateLimits.matchClientIp,
      )

      Seq.fill(20)(l.markRun(Some("10.1.2.3"))).count(identity) shouldBe 2
      Seq.fill(20)(l.markRun(Some("10.1.2.4"))).count(identity) shouldBe 4
    }
  }
}
