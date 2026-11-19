// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.util.BestFittingBatcherTest.TestItems
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.IterableOps

class BestFittingBatcherTest extends AnyWordSpec with BaseTest {
  "BestFitter" should {
    "pack items optimally" in {
      val maxSize = PositiveInt.tryCreate(10)
      val fitter =
        new BestFittingBatcher[PositiveInt, TestItems](maxBatchSize = maxSize)

      fitter.extractFullestBatch() shouldBe None

      fitter.add(TestItems(3)) shouldBe true
      fitter.extractFullestBatch() shouldBe Some(Vector(TestItems(3)))

      (1 to 3).foreach { _ =>
        fitter.add(TestItems(4)) shouldBe true
      }

      fitter.extractFullestBatch() shouldBe Some(Vector.fill(2)(TestItems(4)))
      fitter.extractFullestBatch() shouldBe Some(Vector(TestItems(4)))
      fitter.extractFullestBatch() shouldBe None

      (1 to 3).foreach { _ =>
        fitter.add(TestItems(5)) shouldBe true
      }

      fitter.extractFullestBatch() shouldBe Some(Vector.fill(2)(TestItems(5)))
      fitter.extractFullestBatch() shouldBe Some(Vector(TestItems(5)))
      fitter.extractFullestBatch() shouldBe None

      (6.to(4, -1)).foreach { size =>
        fitter.add(TestItems(size)) shouldBe true
      }

      fitter.extractFullestBatch() shouldBe Some(Vector(TestItems(6), TestItems(4)))
      fitter.extractFullestBatch() shouldBe Some(Vector(TestItems(5)))
      fitter.extractFullestBatch() shouldBe None

      (4 to 6).foreach { size =>
        fitter.add(TestItems(size)) shouldBe true
      }

      fitter.extractFullestBatch() shouldBe Some(Vector(TestItems(4), TestItems(5)))
      fitter.extractFullestBatch() shouldBe Some(Vector(TestItems(6)))
      fitter.extractFullestBatch() shouldBe None

      fitter.add(TestItems(maxSize.value + 1)) shouldBe false
    }

    "sum individual item estimates while keeping groups together" in {
      val fitter = new BestFittingBatcher[PositiveInt, TestItems](
        maxBatchSize = PositiveInt.tryCreate(10),
        maxDataSizeHintO = Some(PositiveInt.tryCreate(10)),
        estimateDataSizeOfItem = identity,
      )
      val large = TestItems.withDataSizes(3, 5)
      val small = TestItems.withDataSizes(1, 1)

      Seq(large, large, small, small).foreach { items =>
        fitter.add(items) shouldBe true
      }

      fitter.extractFullestBatch() shouldBe Some(Vector(large, small))
      fitter.extractFullestBatch() shouldBe Some(Vector(large, small))
      fitter.extractFullestBatch() shouldBe None
    }

    "respect the item count limit even when data capacity remains" in {
      val fitter = new BestFittingBatcher[PositiveInt, TestItems](
        maxBatchSize = PositiveInt.tryCreate(3),
        maxDataSizeHintO = Some(PositiveInt.tryCreate(100)),
        estimateDataSizeOfItem = identity,
      )
      val items = TestItems(2)

      fitter.add(items) shouldBe true
      fitter.add(items) shouldBe true
      fitter.extractFullestBatch() shouldBe Some(Vector(items))
      fitter.extractFullestBatch() shouldBe Some(Vector(items))
      fitter.extractFullestBatch() shouldBe None
    }

    "skip count-compatible batches without enough data capacity" in {
      val fitter = new BestFittingBatcher[PositiveInt, TestItems](
        maxBatchSize = PositiveInt.tryCreate(10),
        maxDataSizeHintO = Some(PositiveInt.tryCreate(10)),
        estimateDataSizeOfItem = identity,
      )
      val countFullest = TestItems.withDataSizes(2, 1, 1, 1, 1, 1, 1, 1)
      val dataFullest = TestItems.withDataSizes(3, 3, 3)
      val available = TestItems.withDataSizes(2, 1, 1)
      val addition = TestItems.withDataSizes(3, 3)

      Seq(countFullest, dataFullest, available, addition).foreach { items =>
        fitter.add(items) shouldBe true
      }

      fitter.extractFullestBatch() shouldBe Some(Vector(countFullest))
      fitter.extractFullestBatch() shouldBe Some(Vector(available, addition))
      fitter.extractFullestBatch() shouldBe Some(Vector(dataFullest))
      fitter.extractFullestBatch() shouldBe None
    }

    "prefer the smallest remaining data capacity for equally count-full batches" in {
      val fitter = new BestFittingBatcher[PositiveInt, TestItems](
        maxBatchSize = PositiveInt.tryCreate(4),
        maxDataSizeHintO = Some(PositiveInt.tryCreate(10)),
        estimateDataSizeOfItem = identity,
      )
      val smaller = TestItems.withDataSizes(1, 1, 2)
      val larger = TestItems.withDataSizes(2, 2, 3)
      val addition = TestItems.withDataSizes(3)

      Seq(smaller, larger, addition).foreach { items =>
        fitter.add(items) shouldBe true
      }

      fitter.extractFullestBatch() shouldBe Some(Vector(larger, addition))
      fitter.extractFullestBatch() shouldBe Some(Vector(smaller))
      fitter.extractFullestBatch() shouldBe None
    }

    "preserve data-capacity ordering and reuse emptied capacity buckets" in {
      val maximumDataSize = 10
      val fitter = new BestFittingBatcher[PositiveInt, TestItems](
        maxBatchSize = PositiveInt.tryCreate(3),
        maxDataSizeHintO = Some(PositiveInt.tryCreate(maximumDataSize)),
        estimateDataSizeOfItem = identity,
      )
      val largeItems = Seq(6, 8, 7).map(size => TestItems.withDataSizes(size))

      // No pair fits. Poll by least remaining data capacity, not insertion order.
      largeItems.foreach(items => fitter.add(items) shouldBe true)
      Seq(8, 7, 6).foreach { size =>
        val items = TestItems.withDataSizes(size)
        fitter.extractFullestBatch() shouldBe Some(Vector(items))
      }
      fitter.extractFullestBatch() shouldBe None

      // Reusing the emptied index must find the exact data-capacity match for every small item.
      largeItems.foreach(items => fitter.add(items) shouldBe true)
      val complements = largeItems.map { items =>
        TestItems.withDataSizes(maximumDataSize - items.data.head.value)
      }
      complements.foreach(items => fitter.add(items) shouldBe true)
      largeItems.zip(complements).foreach { case (large, small) =>
        fitter.extractFullestBatch() shouldBe Some(Vector(large, small))
      }
      fitter.extractFullestBatch() shouldBe None
    }

    "keep oversized data groups alone without overflowing" in {
      val fitter = new BestFittingBatcher[PositiveInt, TestItems](
        maxBatchSize = PositiveInt.tryCreate(10),
        maxDataSizeHintO = Some(PositiveInt.tryCreate(Int.MaxValue)),
        estimateDataSizeOfItem = identity,
      )
      val accepted = TestItems.withDataSizes(Int.MaxValue - 1, 1)
      val oversizedData = TestItems.withDataSizes(Int.MaxValue, Int.MaxValue)

      fitter.add(accepted) shouldBe true
      fitter.add(oversizedData) shouldBe true
      fitter.add(TestItems(11)) shouldBe false
      fitter.extractFullestBatch() shouldBe Some(Vector(accepted))
      fitter.extractFullestBatch() shouldBe Some(Vector(oversizedData))
      fitter.extractFullestBatch() shouldBe None
    }

    "default to one data-size unit per individual item" in {
      val fitter = new BestFittingBatcher[PositiveInt, TestItems](
        maxBatchSize = PositiveInt.tryCreate(10),
        maxDataSizeHintO = Some(PositiveInt.tryCreate(4)),
      )
      val items = TestItems.withDataSizes(100, 200)

      (1 to 3).foreach(_ => fitter.add(items) shouldBe true)

      fitter.extractFullestBatch() shouldBe Some(Vector(items, items))
      fitter.extractFullestBatch() shouldBe Some(Vector(items))
      fitter.extractFullestBatch() shouldBe None
    }

    "not evaluate the estimator when the data-size limit is disabled" in {
      val fitter = new BestFittingBatcher[PositiveInt, TestItems](
        maxBatchSize = PositiveInt.tryCreate(10),
        estimateDataSizeOfItem = _ => fail("Estimator must not be evaluated"),
      )
      val items = TestItems(5)

      fitter.add(items) shouldBe true
      fitter.add(items) shouldBe true
      fitter.extractFullestBatch() shouldBe Some(Vector(items, items))
      fitter.extractFullestBatch() shouldBe None
    }
  }
}

object BestFittingBatcherTest {

  final case class TestItems(data: Vector[PositiveInt])
      extends BestFittingBatcher.Sized[PositiveInt] {
    override val size: PositiveInt = PositiveInt.tryCreate(data.size)
    override def sizeIs: IterableOps.SizeCompareOps = data.sizeIs
    override def iterator: Iterator[PositiveInt] = data.iterator
  }
  object TestItems {
    def apply(size: Int): TestItems = TestItems(Vector.fill(size)(PositiveInt.one))
    def withDataSizes(sizes: Int*): TestItems =
      TestItems(sizes.iterator.map(PositiveInt.tryCreate).toVector)
  }
}
