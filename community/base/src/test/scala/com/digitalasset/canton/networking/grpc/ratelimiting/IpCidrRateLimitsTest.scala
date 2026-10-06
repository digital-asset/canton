// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.digitalasset.canton.config.{PerAttributeRateLimitConfig, RateLimitConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Ported from splice's
  * `apps/common/src/test/scala/org/lfdecentralizedtrust/splice/util/IpCidrRateLimitsTest.scala`.
  */
class IpCidrRateLimitsTest extends AnyWordSpec with Matchers {

  private val networkLimit = RateLimitConfig(ratePerSecond = 5)
  private val hostLimit = RateLimitConfig(ratePerSecond = 50)

  private def perClientIp(
      overrides: (String, RateLimitConfig.Simple)*
  ): PerAttributeRateLimitConfig =
    PerAttributeRateLimitConfig(attributeOverrides = overrides.toMap)

  private def limitFor(
      config: PerAttributeRateLimitConfig,
      clientIp: String,
  ): Option[RateLimitConfig.Simple] =
    IpCidrRateLimits.matchClientIp(config)(clientIp)

  "IpCidrRateLimits.matchClientIp" should {

    "match addresses within the configured IPv4 network" in {
      val overrides = perClientIp("10.0.0.0/8" -> networkLimit)
      Seq("10.1.2.3", "10.255.255.255", "10.0.0.0").foreach(ip =>
        limitFor(overrides, ip) shouldBe Some(networkLimit)
      )
      Seq("11.0.0.1", "9.255.255.255").foreach(ip => limitFor(overrides, ip) shouldBe empty)
    }

    "match a bare IP address as a single host" in {
      val overrides = perClientIp("10.1.2.3" -> hostLimit)
      limitFor(overrides, "10.1.2.3") shouldBe Some(hostLimit)
      limitFor(overrides, "10.1.2.4") shouldBe empty
    }

    "use the most specific match" in {
      val overrides = perClientIp("10.0.0.0/8" -> networkLimit, "10.1.2.3" -> hostLimit)
      limitFor(overrides, "10.1.2.3") shouldBe Some(hostLimit)
      limitFor(overrides, "10.1.2.4") shouldBe Some(networkLimit)
    }

    "not match values that are not IP addresses" in {
      val overrides = perClientIp("10.0.0.0/8" -> networkLimit)
      limitFor(overrides, "not-an-ip") shouldBe empty
      limitFor(overrides, "") shouldBe empty
      // must not do a DNS lookup
      limitFor(overrides, "localhost") shouldBe empty
    }

    "not match anything without overrides" in {
      limitFor(PerAttributeRateLimitConfig(), "10.1.2.3") shouldBe empty
    }

    "group IPv6 clients by their /64 prefix" in {
      val overrides = perClientIp("2001:db8::/32" -> networkLimit)
      limitFor(overrides, "2001:db8:1:2:3:4:5:6") shouldBe Some(networkLimit)
      limitFor(overrides, "2001:db8:0:0:0:0:0:0/64") shouldBe Some(networkLimit)
      limitFor(overrides, "2001:db9::1") shouldBe empty
    }

    "return a reusable matcher for a config" in {
      val matcher = IpCidrRateLimits.matchClientIp(perClientIp("10.0.0.0/8" -> networkLimit))
      matcher("10.1.2.3") shouldBe Some(networkLimit)
      matcher("11.1.2.3") shouldBe empty
    }
  }

  "IpCidrRateLimits.tryValidate" should {

    "accept a config whose overrides are all valid CIDRs" in {
      IpCidrRateLimits.tryValidate(perClientIp("10.0.0.0/8" -> networkLimit))
    }

    "reject a config with a malformed CIDR override" in {
      an[IllegalArgumentException] should be thrownBy IpCidrRateLimits.tryValidate(
        perClientIp("not-a-cidr" -> networkLimit)
      )
    }
  }
}
