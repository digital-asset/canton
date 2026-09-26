// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.collection.mutable.ArrayBuffer

/** Forms dynamically-sized batches based on downstream backpressure.
  *   - Under light load, this flow emits batches of size 1.
  *   - Under moderate load, this flow emits batches according to the batch mode:
  *     - forMaxConcurrency: emits batches of even sizes
  *     - forMaxBatchSize: emits fewer but full batches
  *   - Under heavy load (dowstream saturated), this flow emits batches of `maxBatchSize`.
  *
  * moderate load: short intermittent backpressure from downstream that doesn't fill up the maximum
  * batch capacity (maxBatchSize * maxBatchCount) of BatchN.
  *
  * heavy load: downstream backpressure causes the full batch capacity to fill up and BatchN to
  * exert backpressure to upstream.
  *
  * Under heavy load or when maxBatchCount == 1, forMaxBatchSize and forMaxConcurrency behave the
  * same way, i.e. full batches are emitted.
  */
object BatchN {

  /** BatchN variant to favor a smaller number of large batches when catching up after backpressure
    */
  def forMaxBatchSize[In](maxBatchSize: Int, maxBatchCount: Int): Flow[In, Iterable[In], NotUsed] =
    apply(maxBatchSize, maxBatchCount, minBatchSize = maxBatchSize, costFn = (_: Any) => 1L)

  /** BatchN variant to favor a higher number of small batches when catching up after backpressure
    */
  def forMaxConcurrency[In](
      maxBatchSize: Int,
      maxBatchCount: Int,
  ): Flow[In, Iterable[In], NotUsed] =
    apply(maxBatchSize, maxBatchCount, minBatchSize = 1, costFn = (_: Any) => 1L)

  def apply[In](
      maxBatchSize: Int,
      maxBatchCount: Int,
      minBatchSize: Int,
      costFn: In => Long,
  ): Flow[In, Iterable[In], NotUsed] = {
    assert(maxBatchSize > 0, s"maxBatchSize ($maxBatchSize) must be greater than 0")
    assert(
      minBatchSize <= maxBatchSize,
      s"minBatchSize ($minBatchSize) must be less than or equal to maxBatchSize ($maxBatchSize)",
    )
    val totalBatchSize = maxBatchSize * maxBatchCount
    Flow[In]
      .batchWeighted[ArrayBuffer[In]](
        totalBatchSize.toLong,
        costFn,
        newBatch(totalBatchSize, _),
      )(_ addOne _)
      .mapConcat { totalBatch =>
        val (targetBatchWeight, remainder) =
          if (maxBatchSize == minBatchSize)
            minBatchSize.toLong -> 0L
          else {
            val totalCost = totalBatch.view.map(costFn).sum
            val baseBatchSize = totalCost / maxBatchCount
            if (baseBatchSize >= minBatchSize)
              baseBatchSize -> totalCost % maxBatchCount
            else
              minBatchSize.toLong -> 0L
          }

        // imperative code to effectively split the totalBatch into batches of targetBatchWeight factoring in the remainder
        val result = ArrayBuffer[ArrayBuffer[In]]()
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var currentBatch = ArrayBuffer[In]()
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var currentBatchWeightRemaining = 0L
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var currentRemainder = remainder

        def initNewBatch(): Unit = {
          currentBatch = ArrayBuffer[In]()
          currentBatch.sizeHint(targetBatchWeight.toInt + 1)
          currentBatchWeightRemaining = if (remainder > 0) {
            currentRemainder = currentRemainder - 1
            targetBatchWeight + 1
          } else targetBatchWeight
        }

        initNewBatch()
        totalBatch.foreach { in =>
          val cost = costFn(in)
          if (currentBatchWeightRemaining - cost < 0 && currentBatch.nonEmpty) {
            result.addOne(currentBatch)
            initNewBatch()
          }
          currentBatch.addOne(in)
          currentBatchWeightRemaining = currentBatchWeightRemaining - cost
        }
        if (currentBatch.nonEmpty) result.addOne(currentBatch)
        result
      }
  }

  private def newBatch[In](maxBatchSize: Int, newElement: In) = {
    val newBatch = ArrayBuffer.empty[In]
    newBatch.sizeHint(maxBatchSize)
    newBatch.addOne(newElement)
    newBatch
  }

}
