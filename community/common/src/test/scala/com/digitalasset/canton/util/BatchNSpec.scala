// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import org.apache.pekko.stream.Attributes.InputBuffer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{Attributes, DelayOverflowStrategy}
import org.scalatest.Inspectors
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class BatchNSpec extends AsyncFlatSpec with Matchers with PekkoBeforeAndAfterAll with Inspectors {

  private val MaxBatchSize = 10
  private val MaxBatchCount = 5

  behavior of s"BatchN.forMaxConcurrency with equally weighed items"

  it should "form batches of size 1 under no load" in {
    val inputSize = 10
    val input = 1 to inputSize
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .batchNForMaxConcurrency(MaxBatchSize, MaxBatchCount)
        .runWith(Sink.seq[Iterable[Int]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize)(1)
    }
  }

  it should "form maximally-sized batches if downstream is slower than upstream" in {
    val inputSize = 100
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchNForMaxConcurrency(MaxBatchSize, MaxBatchCount)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize / MaxBatchSize)(
        MaxBatchSize
      )
    }
  }

  it should "form even-sized batches under downstream back-pressure" in {
    val inputSize = 15
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchNForMaxConcurrency(MaxBatchSize, MaxBatchCount)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(5)(
        3
      )
    }
  }

  behavior of s"BatchN.forMaxBatchSize}"

  it should "form batches of size 1 under no load" in {
    val inputSize = 10
    val input = 1 to inputSize
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .batchNForMaxBatchSize(MaxBatchSize, MaxBatchCount)
        .runWith(Sink.seq[Iterable[Int]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize)(1)
    }
  }

  it should "form maximally-sized batches if downstream is slower than upstream" in {
    val inputSize = 100
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchNForMaxBatchSize(MaxBatchSize, MaxBatchCount)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize / MaxBatchSize)(
        MaxBatchSize
      )
    }
  }

  it should "form maximally-sized batches under downstream back-pressure" in {
    val inputSize = 25
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchNForMaxBatchSize(MaxBatchSize, MaxBatchCount)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs (Array.fill(inputSize / MaxBatchSize)(
        MaxBatchSize // fill as many full batches as possible
      ) :+ inputSize % MaxBatchSize) // and the last batch is whatever is left-over
    }
  }

  it should "form a single maximally-sized batch under downstream back-pressure" in {
    val inputSize = MaxBatchSize - 1
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchNForMaxBatchSize(MaxBatchSize, MaxBatchCount)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array(MaxBatchSize - 1)
    }
  }

  behavior of s"BatchN with cost function"

  val intCostFn: ((Option[Int], Vector[Int])) => Long = {
    case (Some(_), _) => 1L
    case (None, v) => 0L
  }

  it should "form batches of size 1 under no load" in {
    val inputSize = 10
    val input = (1 to inputSize).map(i => (Some(i), if (i % 3 == 0) Vector(i) else Vector.empty))
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .via(
          BatchN.apply(
            MaxBatchSize,
            MaxBatchCount,
            minBatchSize = 1,
            costFn = intCostFn,
          )
        )
        .runWith(Sink.seq[Iterable[(Option[Int], Vector[Int])]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize)(1)
    }
  }

  it should "form maximally-sized batches if downstream is slower than upstream" in {
    val inputSize = 500
    val input = (1 to inputSize).map(i => (if (i % 2 == 0) None else Some(i), Vector(i)))

    val batchesF =
      Source(input)
        .via(
          BatchN.apply(
            MaxBatchSize,
            MaxBatchCount,
            minBatchSize = 1,
            costFn = intCostFn,
          )
        )
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      // each batch would contain same number of cost=1 and cost=0 items
      val expectedBatchSize = MaxBatchSize * 2
      val expectedNumberOfBatches = inputSize / MaxBatchSize / 2
      batches.map(_.size) should contain theSameElementsAs Array.fill(expectedNumberOfBatches)(
        expectedBatchSize
      )
    }
  }

  it should "respect minBatchSize" in {
    val inputSize = 20
    val minBatchSize = 5
    val input = (1 to inputSize).map(i => (if (i % 2 == 0) None else Some(i), Vector(i)))
    val batchesF =
      Source(input).async
        .via(
          BatchN.apply(
            MaxBatchSize,
            MaxBatchCount,
            minBatchSize = minBatchSize,
            costFn = intCostFn,
          )
        )
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq[Iterable[(Option[Int], Vector[Int])]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      // each batch would contain same number of cost=1 and cost=0 items
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize / minBatchSize / 2)(
        minBatchSize * 2
      )
    }
  }

  it should "cut variable sized, but same cost batches" in {
    val inputSize = 100
    val input = (1 to inputSize).map(i =>
      (if (i % 2 == 0 || i % 40 == 3 || i % 40 == 5) None else Some(i), Vector(i))
    )
    val totalCost = input.count(_._1.isDefined)
    val targetBatchSize = totalCost / MaxBatchCount + (if (totalCost % MaxBatchCount > 0) 1 else 0)
    val batchesF =
      Source(input).async
        .via(
          BatchN.apply(
            MaxBatchSize,
            MaxBatchCount,
            minBatchSize = 1,
            costFn = intCostFn,
          )
        )
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq[Iterable[(Option[Int], Vector[Int])]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      targetBatchSize should be(9)
      batches.map(_.size) should contain theSameElementsAs Array(
        targetBatchSize * 2 + 4, // evaluates to 22: up to 22, there are 11 odd numbers (cost 1), but 3 and 5 are None, so the total cost of this is 9
        targetBatchSize * 2, // no extra Nones are going into this batch (only the even numbers)
        targetBatchSize * 2 + 4, // 2 extra Nones (43 and 45)
        targetBatchSize * 2,
        targetBatchSize * 2 + 2,
      )
    }
  }

  it should "handle weights bigger than MaxBatchSize in the front" in {
    val inputSize = 18
    val input = (1 to inputSize).map(i => (Some(i), Vector(i)))
    val batchesF =
      Source(input).async
        .via(
          BatchN.apply(
            MaxBatchSize,
            MaxBatchCount,
            minBatchSize = 1,
            costFn = in => if (in._1.contains(1)) 15L else 1L,
          )
        )
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq[Iterable[(Option[Int], Vector[Int])]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      // the first batch is a single element of big weight
      batches.head.size should be(1)
      // the rest should have more than 1 element
      forAll(batches.tail.map(_.size))(_ should be > 1)
    }
  }

  it should "handle weights bigger than MaxBatchSize at the end" in {
    val inputSize = 18
    val input = (1 to inputSize).map(i => (Some(i), Vector(i)))
    val batchesF =
      Source(input).async
        .via(
          BatchN.apply(
            MaxBatchSize,
            MaxBatchCount,
            minBatchSize = 1,
            costFn = in => if (in._1.contains(18)) 15L else 1L,
          )
        )
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq[Iterable[(Option[Int], Vector[Int])]])

    batchesF.map { batches =>
      // all input is successfully processed
      batches.flatten should contain theSameElementsInOrderAs input
    }
  }
}
