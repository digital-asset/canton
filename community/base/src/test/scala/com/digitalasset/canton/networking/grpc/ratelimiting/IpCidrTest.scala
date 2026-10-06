// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class IpCidrTest extends AnyWordSpec with Matchers {

  "IpCidr.parse" should {

    "normalize IPv4 networks and hosts" in {
      IpCidr.tryParse("10.1.2.3/8").toString shouldBe "10.0.0.0/8"
      IpCidr.tryParse("10.1.2.3").toString shouldBe "10.1.2.3/32"
      IpCidr.tryParse("10.1.2.3/32").toString shouldBe "10.1.2.3/32"
      IpCidr.tryParse("10.1.2.3/0").toString shouldBe "0.0.0.0/0"
      IpCidr.tryParse("10.1.2.3/23").toString shouldBe "10.1.2.0/23"
    }

    "normalize IPv6 networks and hosts, including across the 64 bit word boundary" in {
      IpCidr.tryParse("2001:db8:1:2:3:4:5:6/32").toString shouldBe "2001:db8:0:0:0:0:0:0/32"
      IpCidr.tryParse("2001:db8:1:2:3:4:5:6/96").toString shouldBe "2001:db8:1:2:3:4:0:0/96"
      IpCidr.tryParse("2001:db8:1:2:3:4:5:6/0").toString shouldBe "0:0:0:0:0:0:0:0/0"
      IpCidr.tryParse("2001:db8:1:2:3:4:5:6").toString shouldBe "2001:db8:1:2:3:4:5:6/128"
      IpCidr.tryParse("2001:db8:1:2:3:4:5:6/64").toString shouldBe "2001:db8:1:2:0:0:0:0/64"
    }

    "treat an IPv4-mapped IPv6 address/network as the plain IPv4 one" in {
      IpCidr.tryParse("::ffff:192.0.2.1").addressBits shouldBe 32
      IpCidr.tryParse("::ffff:192.0.2.1").toString shouldBe "192.0.2.1/32"
      // an IPv6 prefix length is therefore out of range for a mapped address
      an[IllegalArgumentException] should be thrownBy IpCidr.tryParse("::ffff:192.0.2.0/120")
    }

    "reject invalid IPv4 CIDRs" in {
      Seq(
        "not-an-ip/8",
        "10.0.0.0/33",
        "10.0.0.0/-1",
        "10.0.0.0/eight",
        "10.0.0.0/8/16",
        "10.0.0.256/8",
        "",
      )
        .foreach(cidr => IpCidr.parse(cidr) shouldBe empty)
    }

    "reject invalid IPv6 CIDRs" in {
      Seq(
        "not-an-ip/32",
        "2001:db8::/129",
        "2001:db8::/-1",
        "2001:db8::/thirtytwo",
        "2001:db8::/32/64",
        "2001:db8:::1/32",
        "",
      ).foreach(cidr => IpCidr.parse(cidr) shouldBe empty)
    }

    "not do a DNS lookup" in {
      IpCidr.parse("localhost") shouldBe empty
    }
  }

  "IpCidr.contains" should {

    "match addresses within an IPv4 network, not those outside it" in {
      val network = IpCidr.tryParse("10.0.0.0/8")
      Seq("10.1.2.3", "10.255.255.255", "10.0.0.0").foreach(ip =>
        network.contains(IpCidr.tryParse(ip)) shouldBe true
      )
      Seq("11.0.0.1", "9.255.255.255").foreach(ip =>
        network.contains(IpCidr.tryParse(ip)) shouldBe false
      )
    }

    "match everything for a zero length prefix, but not the other IP version" in {
      IpCidr.tryParse("0.0.0.0/0").contains(IpCidr.tryParse("8.8.8.8")) shouldBe true
      IpCidr.tryParse("0.0.0.0/0").contains(IpCidr.tryParse("::1")) shouldBe false
      IpCidr.tryParse("::/0").contains(IpCidr.tryParse("::1")) shouldBe true
      IpCidr.tryParse("::/0").contains(IpCidr.tryParse("8.8.8.8")) shouldBe false
    }

    "match IPv4 prefixes that are not on a byte boundary" in {
      val network = IpCidr.tryParse("10.1.2.0/23")
      network.contains(IpCidr.tryParse("10.1.2.255")) shouldBe true
      network.contains(IpCidr.tryParse("10.1.3.255")) shouldBe true
      network.contains(IpCidr.tryParse("10.1.4.1")) shouldBe false
    }

    "match IPv6 prefixes across the 64 bit word boundary" in {
      val at64 = IpCidr.tryParse("2001:db8:0:1::/64")
      at64.contains(IpCidr.tryParse("2001:db8:0:1:ffff:ffff:ffff:ffff")) shouldBe true
      at64.contains(IpCidr.tryParse("2001:db8:0:2::1")) shouldBe false

      val beyond64 = IpCidr.tryParse("2001:db8:0:1:2:3::/96")
      beyond64.contains(IpCidr.tryParse("2001:db8:0:1:2:3:4:5")) shouldBe true
      beyond64.contains(IpCidr.tryParse("2001:db8:0:1:2:4:0:0")) shouldBe false

      val host = IpCidr.tryParse("2001:db8:0:1:2:3:4:5")
      host.contains(IpCidr.tryParse("2001:db8:0:1:2:3:4:5")) shouldBe true
      host.contains(IpCidr.tryParse("2001:db8:0:1:2:3:4:6")) shouldBe false
    }

    "not match a client network that is wider than the configured one" in {
      val configured = IpCidr.tryParse("2001:db8:0:0:1::/80")
      // the /64 the client is grouped into is not fully covered by the configured /80
      configured.contains(IpCidr.tryParse("2001:db8:0:0:0:0:0:0/64")) shouldBe false
    }
  }
}
