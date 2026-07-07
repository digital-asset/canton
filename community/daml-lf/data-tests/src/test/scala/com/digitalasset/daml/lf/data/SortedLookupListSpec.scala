// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SortedLookupListSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  "SortMap.fromImmArray should fails if the input list contians duplicate keys" in {

    val negativeTestCases = Table(
      "list",
      ImmArray.Empty,
      ImmArray("1" -> 1),
      ImmArray("1" -> 1, "2" -> 2, "3" -> 3),
      ImmArray("2" -> 2, "3" -> 3, "1" -> 1),
    )

    val positiveTestCases =
      Table("list", ImmArray("1" -> 1, "1" -> 2), ImmArray("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2))

    forAll(negativeTestCases)(l => SortedLookupList.fromImmArray(l) shouldBe a[Right[?, ?]])

    forAll(positiveTestCases)(l => SortedLookupList.fromImmArray(l) shouldBe a[Left[?, ?]])

  }

  "SortMap.fromSortedImmArray should fails if the input list is not sorted" in {

    val negativeTestCases =
      Table(
        "list",
        ImmArray.empty[(String, Int)],
        ImmArray("1" -> 1),
        ImmArray("1" -> 1, "2" -> 2, "3" -> 3),
      )

    val positiveTestCases = Table(
      "list",
      ImmArray("1" -> 1, "1" -> 2),
      ImmArray("1" -> 1, "2" -> 2, "3" -> 3, "1" -> 2),
      ImmArray("2" -> 2, "3" -> 3, "1" -> 1),
    )

    forAll(negativeTestCases)(l => SortedLookupList.fromOrderedImmArray(l) shouldBe a[Right[?, ?]])

    forAll(positiveTestCases)(l => SortedLookupList.fromOrderedImmArray(l) shouldBe a[Left[?, ?]])

  }

  "traverse" should {

    "work with Either as applicative" in {
      import cats.syntax.traverse.*

      type F[A] = Either[Int, A]

      val sll = SortedLookupList.from(List("1" -> 1, "2" -> 2, "3" -> 3))

      sll.traverse[F, Int](n => Right(n)) shouldBe
        Right(SortedLookupList.from(List("1" -> 1, "2" -> 2, "3" -> 3)))

      sll.traverse[F, Int](n => if (n >= 2) Left(n) else Right(n)) shouldBe Left(2)
    }

    "preserve keys and order" in {
      import cats.syntax.traverse.*

      val sll = SortedLookupList.from(List("3" -> 3, "1" -> 1, "2" -> 2))

      // SortedLookupList sorts entries by key on construction, so traverse
      // operates on (and preserves) the sorted order, not the insertion order.
      sll.toImmArray shouldBe ImmArray("1" -> 1, "2" -> 2, "3" -> 3)

      sll.traverse[Option, Int](n => Some(n * 10)).map(_.toImmArray) shouldBe
        Some(ImmArray("1" -> 10, "2" -> 20, "3" -> 30))
    }

    "work with List as applicative" in {
      import cats.syntax.traverse.*

      SortedLookupList
        .from(List("a" -> 1, "b" -> 2))
        .traverse(n => (0 to n).toList)
        .map(_.toImmArray) shouldBe
        List(
          ImmArray("a" -> 0, "b" -> 0),
          ImmArray("a" -> 0, "b" -> 1),
          ImmArray("a" -> 0, "b" -> 2),
          ImmArray("a" -> 1, "b" -> 0),
          ImmArray("a" -> 1, "b" -> 1),
          ImmArray("a" -> 1, "b" -> 2),
        )
    }
  }

}
