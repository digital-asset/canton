// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.NonEmptyList
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.util.ReleaseUtils.shard
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors}

final class ReleaseUtilsTest extends AnyFlatSpec with Matchers with Inspectors with Inside {

  private val one = PositiveInt.one
  private val two = PositiveInt.two
  private val three = PositiveInt.tryCreate(3)
  private val four = PositiveInt.tryCreate(4)

  behavior of "sharding"

  it should "shard a single item" in {
    shard(NonEmptyList.of(1), one) shouldBe List(List(1))
  }
  it should "pad with empty lists (shards) when fewer items than requested shards are available" in {
    shard(NonEmptyList.of(1), two) shouldBe List(List(1), List())
  }
  it should "put 1 item per shard when the number of items equals the requested shards" in {
    shard(NonEmptyList.of(1, 2, 3), three) shouldBe List(List(1), List(2), List(3))
  }
  it should "deal items out round-robin across the shards" in {
    // The element at 0-based index i lands in shard i % n, so consecutive items go to different shards.
    shard(NonEmptyList.of(1, 2, 3, 4), two) shouldBe List(List(1, 3), List(2, 4))

    shard(NonEmptyList.of(1, 2, 3, 4), three) shouldBe List(List(1, 4), List(2), List(3))
  }
  it should "spread items evenly across the shards" in {
    shard(NonEmptyList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), four) shouldBe List(
      List(1, 5, 9),
      List(2, 6, 10),
      List(3, 7),
      List(4, 8),
    )
  }

  behavior of "ignored bundle detection"

  it should "reject the auxiliary bundles published alongside a release" in {
    forAll(
      List(
        "3.6.0-snapshot.20260929.20331.0.v07b3f95b-api",
        "3.6.0-snapshot.20260929.20331.0.v07b3f95b-protobuf",
        "3.6.0-snapshot.20260929.20331.0.v07b3f95b-docs",
        "3.5.2-api",
      )
    )(ReleaseUtils.isReleaseName(_) shouldBe false)
  }

  it should "accept actual releases" in {
    forAll(
      List(
        "3.6.0-snapshot.20260929.20331.0.v07b3f95b",
        "3.5.2",
        "3.5.3-rc1",
        "3.4.12-ad-hoc.100000000.20240215.12560.0.vb11aba02",
      )
    )(ReleaseUtils.isReleaseName(_) shouldBe true)
  }

}
