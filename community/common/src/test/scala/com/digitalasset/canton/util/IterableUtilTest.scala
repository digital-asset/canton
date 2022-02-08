// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class IterableUtilTest extends AnyWordSpec with BaseTest {
  "spansBy" should {
    "work on a simple example" in {
      val example = List(1, 1, 1, 2, 2, 4, 5, 5).zipWithIndex
      IterableUtil
        .spansBy(example)(_._1)
        .map { case (i, it) => i -> it.map(_._2).toList }
        .toList shouldBe List(
        (1, List(0, 1, 2)),
        (2, List(3, 4)),
        (4, List(5)),
        (5, List(6, 7)),
      )
    }
  }

  "subzipBy" should {
    "stop when empty" in {
      IterableUtil.subzipBy(Iterator(1, 2), Iterator.empty: Iterator[Int]) { (_, _) =>
        Some(1)
      } shouldBe Seq.empty

      IterableUtil.subzipBy(Iterator.empty: Iterator[Int], Iterator(1, 2)) { (_, _) =>
        Some(1)
      } shouldBe Seq.empty
    }

    "skip elements in the second argument" in {
      IterableUtil.subzipBy(Iterator(1, 2, 3), Iterator(0, 1, 1, 2, 0, 2, 3, 4)) { (x, y) =>
        if (x == y) Some(x -> y) else None
      } shouldBe Seq(1 -> 1, 2 -> 2, 3 -> 3)
    }

    "not skip elements in the first argument" in {
      IterableUtil.subzipBy(Iterator(1, 2, 3), Iterator(2, 3, 1)) { (x, y) =>
        if (x == y) Some(x -> y) else None
      } shouldBe Seq(1 -> 1)
    }
  }
}
