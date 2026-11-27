// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{
  PerAttributeRateLimitConfig,
  PerClientIpRateLimitConfig,
  RateLimitConfig,
  RateLimitersConfig,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.RateLimitMetrics
import com.digitalasset.canton.networking.grpc.GrpcAddressHelper
import com.digitalasset.canton.networking.grpc.ratelimiting.IpAwareRateLimitInterceptor.{
  AdmissionResult,
  RejectionReason,
}
import io.grpc.Metadata
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class IpAwareRateLimitInterceptorTest
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks {

  private def metrics: RateLimitMetrics =
    RateLimitMetrics(
      NoOpMetricsFactory,
      TracedLogger(classOf[IpAwareRateLimitInterceptorTest], NamedLoggerFactory.root),
    )(MetricsContext.Empty)

  private def interceptor(config: RateLimitersConfig): IpAwareRateLimitInterceptor =
    new IpAwareRateLimitInterceptor(
      config,
      maxLoggingRatePerSecond = NonNegativeInt.tryCreate(10),
      metrics,
      NamedLoggerFactory.root,
    )

  private val method = "com.example.Service/Method"

  // a generous, effectively-unbounded rate to use where a tier isn't the one under test
  private val ample = PerClientIpRateLimitConfig(ratePerSecond = 1000)

  "IpAwareRateLimitInterceptor.admit" should {

    "admit requests under the global limit" in {
      val i = interceptor(RateLimitersConfig(default = ample, global = ample))
      i.admit(method, clientIp = None) shouldBe AdmissionResult.Admitted
    }

    "reject requests once the (per-method) default limit is exhausted" in {
      // Each method gets its own limiter, configured from `rateLimiters` if that method is listed
      // there and from `default` otherwise. Only `global` is shared across methods, so with a
      // generous global rate the per-method limiter is what runs out and reports the rejection
      val i = interceptor(
        RateLimitersConfig(default = PerClientIpRateLimitConfig(ratePerSecond = 1), global = ample)
      )
      i.admit(method, None) shouldBe AdmissionResult.Admitted
      i.admit(method, None) shouldBe AdmissionResult.Admitted
      i.admit(method, None) shouldBe AdmissionResult.Rejected(RejectionReason.PerMethod(method))
    }

    "reject requests once the aggregate global limit is exhausted across different methods" in {
      val i = interceptor(
        RateLimitersConfig(default = ample, global = PerClientIpRateLimitConfig(ratePerSecond = 1))
      )
      i.admit("com.example.Service/MethodA", None) shouldBe AdmissionResult.Admitted
      i.admit("com.example.Service/MethodB", None) shouldBe AdmissionResult.Admitted
      i.admit("com.example.Service/MethodC", None) shouldBe AdmissionResult.Rejected(
        RejectionReason.Global
      )
    }

    "reject requests once a method-specific override is exhausted, even under generous default/global limits" in {
      val i = interceptor(
        RateLimitersConfig(
          default = ample,
          rateLimiters = Map(method -> PerClientIpRateLimitConfig(ratePerSecond = 1)),
          global = ample,
        )
      )
      i.admit(method, None) shouldBe AdmissionResult.Admitted
      i.admit(method, None) shouldBe AdmissionResult.Admitted
      i.admit(method, None) shouldBe AdmissionResult.Rejected(RejectionReason.PerMethod(method))
      // a different, unconfigured method uses `default` and is unaffected
      i.admit("com.example.Service/OtherMethod", None) shouldBe AdmissionResult.Admitted
    }

    "reject requests once a client IP's per-method limit is exhausted, checked ahead of the method/global limits" in {
      val i = interceptor(
        RateLimitersConfig(
          default = ample.copy(perClientIp =
            PerAttributeRateLimitConfig(limit = RateLimitConfig(ratePerSecond = 1))
          ),
          global = ample,
        )
      )
      i.admit(method, Some("1.1.1.1")) shouldBe AdmissionResult.Admitted
      i.admit(method, Some("1.1.1.1")) shouldBe AdmissionResult.Admitted
      i.admit(method, Some("1.1.1.1")) shouldBe AdmissionResult.Rejected(
        RejectionReason.PerClientIpMethod(method)
      )

      // a different client IP has its own, unexhausted budget
      i.admit(method, Some("2.2.2.2")) shouldBe AdmissionResult.Admitted
    }

    "reject requests once a client IP's global limit is exhausted, checked ahead of the method/global limits" in {
      val i = interceptor(
        RateLimitersConfig(
          default = ample,
          global = ample.copy(perClientIp =
            PerAttributeRateLimitConfig(limit = RateLimitConfig(ratePerSecond = 1))
          ),
        )
      )
      i.admit(method, Some("1.1.1.1")) shouldBe AdmissionResult.Admitted
      i.admit(method, Some("1.1.1.1")) shouldBe AdmissionResult.Admitted
      i.admit(method, Some("1.1.1.1")) shouldBe AdmissionResult.Rejected(
        RejectionReason.PerClientIpGlobal
      )
    }

    "not rate limit a request whose client IP could not be determined" in {
      val i = interceptor(
        RateLimitersConfig(
          default = ample.copy(perClientIp =
            PerAttributeRateLimitConfig(limit = RateLimitConfig(ratePerSecond = 1))
          ),
          global = ample,
        )
      )
      (1 to 20).foreach(_ => i.admit(method, clientIp = None) shouldBe AdmissionResult.Admitted)
    }

    "not rate limit anything by client IP when per-client-IP limiting is not configured" in {
      val i = interceptor(
        RateLimitersConfig(
          default = ample.copy(perClientIp = PerAttributeRateLimitConfig.disabled),
          global = ample.copy(perClientIp = PerAttributeRateLimitConfig.disabled),
        )
      )
      (1 to 20).foreach(_ => i.admit(method, Some("1.1.1.1")) shouldBe AdmissionResult.Admitted)
    }

    "apply the default RateLimitersConfig's global per-client-IP protection out of the box" in {
      // With nothing configured, the four limiters are set up like this, in the order `admit`
      // checks them:
      //
      //   per-client-ip <method>   off
      //   per-client-ip global     10 requests per second (PerAttributeRateLimitConfig.DefaultLimit)
      //   <method>                 200 requests per second
      //   global                   200 requests per second
      //
      // So one client IP is capped at 10 requests per second, and that cap comes from the global
      // tier alone. The two method tiers do not separate clients from each other at all, their
      // 200 is a budget that all clients share. These defaults come from splice unchanged.
      // 11 requests pass because the limiter starts with one second worth of permits, see
      // BurstyRateLimiterFactory.
      val i = interceptor(RateLimitersConfig())
      val results = Seq.fill(20)(i.admit(method, Some("1.1.1.1")))
      results.count(_ == AdmissionResult.Admitted) shouldBe 11
      results.count(
        _ == AdmissionResult.Rejected(RejectionReason.PerClientIpGlobal)
      ) shouldBe 9
    }
  }

  "IpAwareRateLimitInterceptor.rateLimitKey" should {

    "give every spelling of one address the same key" in {
      val cases = Table(
        ("address", "key"),
        // IPv4 is one address per client already, so it is kept as it is
        ("203.0.113.5", "203.0.113.5"),
        // a client on a dual stack socket is reported as an IPv4-mapped IPv6 address, which must
        // not get a limiter of its own
        ("::ffff:203.0.113.5", "203.0.113.5"),
        ("0:0:0:0:0:ffff:cb00:7105", "203.0.113.5"),
        // all addresses of one /64 share a limiter, whichever way the address is written
        ("2001:db8:1:2::1", "2001:db8:1:2:0:0:0:0/64"),
        ("2001:db8:1:2::dead", "2001:db8:1:2:0:0:0:0/64"),
        ("2001:0DB8:0001:0002:0000:0000:0000:0001", "2001:db8:1:2:0:0:0:0/64"),
        // a zone id names a local interface and is not part of the address
        ("fe80::1%1", "fe80:0:0:0:0:0:0:0/64"),
        // a value that is not an IP literal is used as it is, it only ever acts as a limiter key
        ("not-an-ip", "not-an-ip"),
        // formats no caller is expected to produce, listed so that a change of behaviour shows up
        ("203.0.113.5:443", "203.0.113.5:443"),
        ("[2001:db8::1]", "[2001:db8::1]"),
        ("[2001:db8::1]:443", "[2001:db8::1]:443"),
        ("010.1.2.3", "010.1.2.3"),
        ("1.2.3.4.5", "1.2.3.4.5"),
        ("unknown", "unknown"),
        ("", ""),
      )
      forAll(cases) { (address, key) =>
        IpAwareRateLimitInterceptor.rateLimitKey(address) shouldBe key
      }
    }
  }

  "GrpcAddressHelper.extractHeaderIpLiteral" should {

    def key(name: String): Metadata.Key[String] =
      Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER)

    def metadata(entries: (String, String)*): Metadata = {
      val md = new Metadata()
      entries.foreach { case (name, value) => md.put(key(name), value) }
      md
    }

    val forwardedFor = key("x-forwarded-for")
    val realIp = key("x-real-ip")

    "ignore every header when no header is trusted" in {
      GrpcAddressHelper.extractHeaderIpLiteral(
        metadata("x-forwarded-for" -> "1.1.1.1"),
        headerKeys = Seq.empty,
      ) shouldBe None
    }

    "ignore a header that is not trusted" in {
      GrpcAddressHelper.extractHeaderIpLiteral(
        metadata("x-forwarded-for" -> "1.1.1.1"),
        Seq(realIp),
      ) shouldBe None
    }

    "read a trusted header" in {
      GrpcAddressHelper.extractHeaderIpLiteral(
        metadata("x-forwarded-for" -> "1.1.1.1"),
        Seq(forwardedFor),
      ) shouldBe Some("1.1.1.1")
    }

    "use the first trusted header that is present, in the configured order" in {
      val headers = metadata("x-forwarded-for" -> "1.1.1.1", "x-real-ip" -> "2.2.2.2")
      GrpcAddressHelper.extractHeaderIpLiteral(headers, Seq(realIp, forwardedFor)) shouldBe Some(
        "2.2.2.2"
      )
      GrpcAddressHelper.extractHeaderIpLiteral(headers, Seq(forwardedFor, realIp)) shouldBe Some(
        "1.1.1.1"
      )
    }

    "take the original client from a comma separated list of proxy hops" in {
      GrpcAddressHelper.extractHeaderIpLiteral(
        metadata("x-forwarded-for" -> "1.1.1.1, 10.0.0.1, 10.0.0.2"),
        Seq(forwardedFor),
      ) shouldBe Some("1.1.1.1")
    }

    "skip a trusted header whose value is not an IP literal" in {
      // a client controlled header can hold anything, including a host name that must not be
      // resolved, so such a value is ignored rather than used as a rate limiting key
      GrpcAddressHelper.extractHeaderIpLiteral(
        metadata("x-forwarded-for" -> "not-an-ip", "x-real-ip" -> "2.2.2.2"),
        Seq(forwardedFor, realIp),
      ) shouldBe Some("2.2.2.2")
    }

    "ignore values that are not plain IP literals" in {
      // a proxy is expected to write a bare IP, these are the shapes that would otherwise end up
      // as a limiter key, and an address with a port or in brackets is not one
      forAll(
        Table(
          "value",
          "203.0.113.5:443",
          "[2001:db8::1]:443",
          "[2001:db8::1]",
          "example.com",
          "unknown",
          "_hidden",
          "",
        )
      ) { value =>
        GrpcAddressHelper.extractHeaderIpLiteral(
          metadata("x-forwarded-for" -> value),
          Seq(forwardedFor),
        ) shouldBe None
      }
    }

    "ignore an address naming a network interface" in {
      // such a value cannot be used as a key at all, parsing an interface name that exists on no
      // host throws, see IpCidr
      an[IllegalArgumentException] should be thrownBy
        IpAwareRateLimitInterceptor.rateLimitKey("fe80::1%eth999")

      // every scope id is rejected, whether or not the host has an interface of that name, so
      // that the verdict does not depend on the host the node runs on. Were a scope id accepted,
      // a client could name a different interface on every request and get a fresh rate limit
      // budget each time.
      forAll(
        Table("value", "fe80::1%eth0", "fe80::1%lo0", "fe80::1%lo", "fe80::1%9", "fe80::1%eth999")
      ) { value =>
        GrpcAddressHelper.extractHeaderIpLiteral(
          metadata("x-forwarded-for" -> value),
          Seq(forwardedFor),
        ) shouldBe None
      }
    }

    "fall back to the next header when the first one names an interface" in {
      GrpcAddressHelper.extractHeaderIpLiteral(
        metadata("x-forwarded-for" -> "fe80::1%eth0", "x-real-ip" -> "2.2.2.2"),
        Seq(forwardedFor, realIp),
      ) shouldBe Some("2.2.2.2")
    }

    "find no IP when none of the trusted headers holds one" in {
      // the caller then uses the IP of the TCP connection instead, see
      // IpAwareRateLimitInterceptor.resolveClientIp
      GrpcAddressHelper.extractHeaderIpLiteral(
        metadata("x-forwarded-for" -> "unknown"),
        Seq(forwardedFor, realIp),
      ) shouldBe None
    }
  }
}
