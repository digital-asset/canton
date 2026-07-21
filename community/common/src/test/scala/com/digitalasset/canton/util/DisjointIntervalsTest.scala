// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import org.scalatest.wordspec.AnyWordSpec

class DisjointIntervalsTest extends AnyWordSpec with BaseTest {
  import DisjointIntervalsTest.*

  "empty" should {
    "be empty" in {
      val empty = DisjointIntervals.empty[TestInterval]
      empty.intervals shouldBe Map.empty
    }
  }

  "from" should {
    "sort the intervals by interval end" in {
      val intervals = Seq(
        TestInterval(6, 10),
        TestInterval(12, 15),
        TestInterval(1, 5),
      )
      val disjointIntervals = DisjointIntervals.tryFrom(intervals)
      disjointIntervals.intervals.values.toSeq shouldBe intervals.sortBy(_.end)
    }
  }

  "overlappingWith" should {
    "return overlapping intervals" in {
      val intervals = Seq(
        TestInterval(1, 5),
        TestInterval(6, 10),
        TestInterval(12, 15),
      )
      val disjointIntervals = DisjointIntervals.tryFrom(intervals)

      disjointIntervals.overlappingWith(2, 3) shouldBe Seq(TestInterval(1, 5))
      disjointIntervals.overlappingWith(0, 2) shouldBe Seq(TestInterval(1, 5))
      disjointIntervals.overlappingWith(0, 1) shouldBe Seq.empty
      disjointIntervals.overlappingWith(5, 12) shouldBe Seq(TestInterval(6, 10))
      disjointIntervals.overlappingWith(4, 12) shouldBe Seq(TestInterval(1, 5), TestInterval(6, 10))
      disjointIntervals.overlappingWith(4, 13) shouldBe intervals
      disjointIntervals.overlappingWith(Int.MinValue, Int.MaxValue) shouldBe intervals
      disjointIntervals.overlappingWith(10, 12) shouldBe empty
    }
  }

  "remove" should {
    "remove the period" in {
      val intervals = Seq(
        TestInterval(1, 5, Set("first")),
        TestInterval(6, 10, Set("second")),
        TestInterval(12, 15, Set("third")),
      )
      val disjointIntervals = DisjointIntervals.tryFrom(intervals)

      disjointIntervals.remove(11, 12).intervals.values.toSeq shouldBe intervals
      disjointIntervals.remove(1, 5).intervals.values.toSeq shouldBe intervals.drop(1)
      disjointIntervals.remove(0, 6).intervals.values.toSeq shouldBe intervals.drop(1)
      disjointIntervals.remove(0, 16).intervals.values.toSeq shouldBe Seq.empty
      disjointIntervals.remove(1, 10).intervals.values.toSeq shouldBe Seq(
        TestInterval(12, 15, Set("third"))
      )
      disjointIntervals.remove(2, 8).intervals.values.toSeq shouldBe
        Seq(
          TestInterval(1, 2, Set("first")),
          TestInterval(8, 10, Set("second")),
          TestInterval(12, 15, Set("third")),
        )
      disjointIntervals.remove(3, 13).intervals.values.toSeq shouldBe
        Seq(TestInterval(1, 3, Set("first")), TestInterval(13, 15, Set("third")))
    }
  }

  "equals" should {
    "ignore mergeability" in {
      DisjointIntervals.empty[TestInterval] shouldBe MergeableDisjointIntervals.empty[TestInterval]
      MergeableDisjointIntervals.empty[TestInterval] shouldBe DisjointIntervals.empty[TestInterval]

      val intervals = Seq(
        TestInterval(1, 2),
        TestInterval(3, 5),
      )
      DisjointIntervals.tryFrom(intervals) shouldBe MergeableDisjointIntervals.tryFrom(intervals)
      MergeableDisjointIntervals.tryFrom(intervals) shouldBe DisjointIntervals.tryFrom(intervals)
    }
  }

  "hashcode" should {
    "respect equality" in {
      DisjointIntervals.empty[TestInterval].hashCode() shouldBe
        MergeableDisjointIntervals.empty[TestInterval].hashCode()
    }
  }

  "mergeAdjacent" should {
    "merge adjacent intervals" in {
      val intervals = Seq(
        TestInterval(1, 5, Set("a")),
        TestInterval(5, 10, Set("b")),
        TestInterval(11, 12, Set("c")),
        TestInterval(13, 15, Set("d")),
        TestInterval(15, 16, Set("e")),
        TestInterval(16, 20, Set("f")),
      )
      MergeableDisjointIntervals.tryFrom(intervals).mergeAdjacent shouldBe
        DisjointIntervals.tryFrom(
          Seq(
            TestInterval(1, 10, Set("a", "b")),
            TestInterval(11, 12, Set("c")),
            TestInterval(13, 20, Set("d", "e", "f")),
          )
        )
    }
  }

  "add" should {
    "insert non-overlapping intervals" in {
      val intervals = MergeableDisjointIntervals.tryFrom(
        Seq(TestInterval(1, 10, Set("a")), TestInterval(20, 30, Set("b")))
      )
      val addedMiddle = intervals.add(TestInterval(11, 19, Set("c")), mergeAdjacent = false)
      addedMiddle.intervals.values.toSeq shouldBe
        Seq(
          TestInterval(1, 10, Set("a")),
          TestInterval(11, 19, Set("c")),
          TestInterval(20, 30, Set("b")),
        )

      val addedAdjacent = intervals.add(TestInterval(10, 20, Set("c")), mergeAdjacent = false)
      addedAdjacent.intervals.values.toSeq shouldBe
        Seq(
          TestInterval(1, 10, Set("a")),
          TestInterval(10, 20, Set("c")),
          TestInterval(20, 30, Set("b")),
        )
    }

    "merge overlapping intervals" in {
      val intervals = MergeableDisjointIntervals.tryFrom(
        Seq(TestInterval(1, 10, Set("a")), TestInterval(20, 30, Set("b")))
      )
      val addedOverlapLeft = intervals.add(TestInterval(5, 15, Set("c")), mergeAdjacent = false)
      addedOverlapLeft.intervals.values.toSeq shouldBe Seq(
        TestInterval(1, 15, Set("a", "c")),
        TestInterval(20, 30, Set("b")),
      )
      val addedOverlapRight = intervals.add(TestInterval(15, 25, Set("c")), mergeAdjacent = false)
      addedOverlapRight.intervals.values.toSeq shouldBe Seq(
        TestInterval(1, 10, Set("a")),
        TestInterval(15, 30, Set("b", "c")),
      )

      val addedOverlapBoth = intervals.add(TestInterval(5, 25, Set("c")), mergeAdjacent = false)
      addedOverlapBoth.intervals.values.toSeq shouldBe Seq(TestInterval(1, 30, Set("a", "b", "c")))
    }

    "merge with adjacent intervals" in {
      val intervals = MergeableDisjointIntervals.tryFrom(
        Seq(TestInterval(1, 10, Set("a")), TestInterval(20, 30, Set("b")))
      )
      val addedAdjacentLeft = intervals.add(TestInterval(10, 15, Set("c")), mergeAdjacent = true)
      addedAdjacentLeft.intervals.values.toSeq shouldBe Seq(
        TestInterval(1, 15, Set("a", "c")),
        TestInterval(20, 30, Set("b")),
      )

      val addedAdjacentRight = intervals.add(TestInterval(15, 20, Set("c")), mergeAdjacent = true)
      addedAdjacentRight.intervals.values.toSeq shouldBe Seq(
        TestInterval(1, 10, Set("a")),
        TestInterval(15, 30, Set("b", "c")),
      )

      val addedAdjacentBoth = intervals.add(TestInterval(10, 20, Set("c")), mergeAdjacent = true)
      addedAdjacentBoth.intervals.values.toSeq shouldBe Seq(TestInterval(1, 30, Set("a", "b", "c")))
    }
  }

}

object DisjointIntervalsTest {
  private final case class TestInterval(
      start: Int,
      end: Int,
      associatedData: Set[String] = Set.empty,
  ) extends PrettyPrinting {
    override protected def pretty: Pretty[TestInterval.this.type] = TestInterval.prettyTestInterval
  }
  private object TestInterval {
    implicit val prettyTestInterval: Pretty[TestInterval] = {
      import Pretty.*
      prettyOfClass[TestInterval](
        param("start", _.start),
        param("end", _.end),
        unnamedParam(_.associatedData.map(_.unquoted)),
      )
    }

    implicit val intervalOps: MergeableIntervalOps.Aux[TestInterval, Int] =
      new MergeableIntervalOps[TestInterval] {
        override type Point = Int

        override def ordering: Ordering[Point] = implicitly
        override def pretty: Pretty[TestInterval] = implicitly

        override def startExclusive(interval: TestInterval): Int = interval.start
        override def endInclusive(interval: TestInterval): Int = interval.end

        override def split(
            interval: TestInterval,
            startExclusive: Int,
            endInclusive: Int,
        ): Seq[TestInterval] = {
          val leftRemainder = Option.when(startExclusive > interval.start)(
            interval.copy(end = startExclusive)
          )
          val rightRemainder = Option.when(endInclusive < interval.end)(
            interval.copy(start = endInclusive)
          )
          leftRemainder.toList ++ rightRemainder.toList
        }

        override def merge(x: TestInterval, y: TestInterval): TestInterval =
          TestInterval(x.start min y.start, x.end max y.end, x.associatedData ++ y.associatedData)
      }
  }
}
