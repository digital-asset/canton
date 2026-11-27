// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.BestFittingBatcher.{CapacityLeft, Sized}
import com.digitalasset.nonempty.NonEmpty

import java.util.concurrent.atomic.AtomicReference
import scala.collection.IterableOps
import scala.collection.immutable.SortedMap

/** A mutable, thread-safe (and lock-free) batcher that groups items into batches such that the
  * batches are as full as possible and a set of items is always kept together in a batch.
  *
  * It uses a best-fitting algorithm (https://en.wikipedia.org/wiki/Best-fit_bin_packing) to place
  * items into existing batches or create new batches as needed.
  *
  * @param maxBatchSize
  *   The maximum number of items in a batch
  * @param maxDataSizeHintO
  *   An optional target estimated data size per batch. Groups exceeding the hint are kept alone.
  * @param estimateDataSizeOfItem
  *   Estimates the data size of a single item, defaulting to one; only evaluated when
  *   `maxDataSizeHintO` is defined
  * @tparam T
  *   The type of individual items
  * @tparam ItemsT
  *   The type of items to be batched. Must implement `Sized` to provide size information
  */
class BestFittingBatcher[T, ItemsT <: Sized[T]](
    maxBatchSize: PositiveInt,
    maxDataSizeHintO: Option[PositiveInt] = None,
    estimateDataSizeOfItem: T => PositiveInt = (_: T) => PositiveInt.one,
) {

  private type BatchT = NonEmpty[Vector[ItemsT]]

  // Search data capacities within each count bucket, so a failed fit skips the bucket's entire
  //  backlog in logarithmic time. The number of count buckets is bounded by maxBatchSize.
  private type BatchesByDataSize = SortedMap[Option[Long], NonEmpty[Vector[BatchT]]]

  private type Batches = SortedMap[Int, BatchesByDataSize]

  private val maximumCapacity =
    CapacityLeft(maxBatchSize.value, maxDataSizeHintO.map(_.value.toLong))

  // The state is a sorted map of batches, ordered first by lowest available items and then by
  //  lowest remaining data capacity (if defined, else there is a single-entry map with key `None`).
  private val stateRef: AtomicReference[Batches] =
    new AtomicReference(SortedMap.empty[Int, BatchesByDataSize])

  /** Inserts a set of items together into the fullest batch that can contain them.
    *
    * @param items
    *   The items to be inserted
    * @return
    *   `true` if the items were inserted successfully, `false` if the items exceed the maximum
    *   batch item count
    */
  def add(items: ItemsT): Boolean =
    // Refuse to add items only if they exceed the maximum batch size,
    //  but still add them if they exceed the maximum data size hint.
    if (items.sizeIs > maxBatchSize.value) {
      false
    } else {
      val totalItemsDataSizeO =
        maxDataSizeHintO.map(_ => items.iterator.map(estimateDataSizeOfItem(_).value.toLong).sum)
      val requiredCapacity = CapacityLeft(items.size.value, totalItemsDataSizeO)
      val exceedsDataSizeHint =
        requiredCapacity.dataSizeO.zip(maximumCapacity.dataSizeO).exists {
          case (required, maximum) => required > maximum
        }
      stateRef.updateAndGet { state =>
        val fullestFittingBatchesWithCapacityLeftO =
          Option
            .when(!exceedsDataSizeHint) {
              // Look up the batches with the minimum fitting size and capacity.
              state
                .iteratorFrom(requiredCapacity.batchSize)
                .flatMap { case (batchSize, batchesByDataSize) =>
                  batchesByDataSize.iteratorFrom(totalItemsDataSizeO).nextOption().map {
                    case (remainingDataSizeO, batchesWithCapacityLeft) =>
                      (batchesWithCapacityLeft, CapacityLeft(batchSize, remainingDataSizeO))
                  }
                }
                .nextOption()
            }
            .flatten
        fullestFittingBatchesWithCapacityLeftO match {
          case Some((fullestFittingBatches, capacityLeft)) =>
            // Found a batch that can fit the items: extract it from the state
            val (head, stateWithoutTheFittingBatch) =
              extractBatch(fullestFittingBatches, state, capacityLeft)
            // Add the items to it
            val updatedBatch = head :+ items
            // Compute the new capacity left
            val newCapacityLeft = capacityLeft - requiredCapacity
            // Add it back to the state
            addBatch(stateWithoutTheFittingBatch, newCapacityLeft, updatedBatch)

          case None =>
            // No existing batch can fit the items, so create a new batch and add it to the state.
            //  Items that exceed the maximum data size hint are still added to a new batch,
            //  but they will be kept alone.
            val newBatch = NonEmpty(Vector, items)
            val newCapacityLeft = maximumCapacity - requiredCapacity
            addBatch(state, newCapacityLeft, newBatch)
        }
      }.discard
      true
    }

  def extractFullestBatch(): Option[BatchT] =
    AtomicUtil.updateAndGetComputed(stateRef) { state =>
      val fullestBatchesWithCapacityLeftO =
        state.headOption.flatMap { case (batchSize, batchesByDataSize) =>
          batchesByDataSize.headOption.map { case (dataSizeO, batches) =>
            (batches, CapacityLeft(batchSize, dataSizeO))
          }
        }
      fullestBatchesWithCapacityLeftO.fold(state -> Option.empty[BatchT]) {
        case (fullestBatches, capacityLeft) =>
          val (head, newState) =
            extractBatch(fullestBatches, state, capacityLeft)
          newState -> Some(head)
      }
    }

  // `batches` can be retrieved from the state by `capacityLeft` but the caller has already
  //   looked it up in the state, so we can avoid a second lookup by passing it in.
  private def extractBatch(
      alreadyLookedUpBatches: NonEmpty[Vector[BatchT]],
      state: Batches,
      capacityLeft: CapacityLeft,
  ): (BatchT, Batches) =
    alreadyLookedUpBatches.head1 -> {
      state.updatedWith(capacityLeft.batchSize) { batchesByDataSizesO =>
        batchesByDataSizesO
          .map(
            _.updatedWith(capacityLeft.dataSizeO)(_ => NonEmpty.from(alreadyLookedUpBatches.tail1))
          )
          .filter(_.nonEmpty)
      }
    }

  private def addBatch(state: Batches, capacityLeft: CapacityLeft, batch: BatchT): Batches =
    state.updatedWith(capacityLeft.batchSize) { existingDataSizes =>
      val batchesByDataSize =
        existingDataSizes.getOrElse(SortedMap.empty[Option[Long], NonEmpty[Vector[BatchT]]])
      Some(batchesByDataSize.updatedWith(capacityLeft.dataSizeO) {
        case Some(existingBatches) => Some(existingBatches :+ batch)
        case None => Some(NonEmpty(Vector, batch))
      })
    }
}

object BestFittingBatcher {

  /** Batch capacity, ordered first by item count and then by estimated data size.
    *
    * @param batchSize
    *   The number of individual items
    * @param dataSizeO
    *   The estimated data size, or `None` when no data-size limit is configured.
    */
  final case class CapacityLeft(batchSize: Int, dataSizeO: Option[Long]) {

    def -(other: CapacityLeft): CapacityLeft =
      CapacityLeft(
        batchSize - other.batchSize,
        dataSizeO.zip(other.dataSizeO).map { case (remaining, required) =>
          if (required > remaining) 0L else remaining - required
        },
      )
  }

  object CapacityLeft {

    implicit val ordering: Ordering[CapacityLeft] =
      Ordering.by(capacity => (capacity.batchSize, capacity.dataSizeO))
  }

  trait Sized[+T] {
    def size: PositiveInt
    def sizeIs: IterableOps.SizeCompareOps
    def iterator: Iterator[T]
  }
}
