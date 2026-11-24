// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.collection.BoundedQueue
import com.digitalasset.canton.util.collection.BoundedQueue.DropStrategy
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class SizeLimitedBoundedQueue[ItemType](
    totalItemSizeLimit: Option[PositiveInt],
    maxNumberOfItems: Int,
    dropStrategy: BoundedQueue.DropStrategy = BoundedQueue.DropStrategy.DropOldest,
    itemSizeOf: ItemType => Int = (_: ItemType) => 0,
) {
  private val innerQueue = mutable.Queue[ItemType]()
  private var currentTotalItemSize: Long = 0L

  def contains(item: ItemType): Boolean = innerQueue.contains(item)

  def size: Int = innerQueue.size

  def enqueue(item: ItemType): Option[Int] = {
    val itemSize = itemSizeOf(item)

    innerQueue.enqueue(item)
    currentTotalItemSize += itemSize
    trim()
  }

  def dequeue(): ItemType = {
    val item = innerQueue.dequeue()
    currentTotalItemSize -= itemSizeOf(item)
    item
  }

  def removeHead(): Option[ItemType] = {
    val itemO = innerQueue.removeHeadOption()
    itemO.foreach { item =>
      currentTotalItemSize -= itemSizeOf(item)
    }
    itemO
  }

  def removeLast(): Option[ItemType] = {
    val itemO = innerQueue.removeLastOption()
    itemO.foreach { item =>
      currentTotalItemSize -= itemSizeOf(item)
    }
    itemO
  }

  def isEmpty: Boolean = innerQueue.isEmpty

  def toSeq: Seq[ItemType] = innerQueue.toSeq

  @VisibleForTesting
  def getCurrentTotalItemSize: Int = currentTotalItemSize.toInt

  @SuppressWarnings(Array("org.wartremover.warts.While", "org.wartremover.warts.Var"))
  private def trim(): Option[Int] = {
    def overCapacity: Boolean =
      totalItemSizeLimit.exists(
        currentTotalItemSize > _.value
      ) || innerQueue.sizeIs > maxNumberOfItems

    var howManyWeHaveDropped = 0

    dropStrategy match {
      case DropStrategy.DropOldest =>
        while (overCapacity) {
          howManyWeHaveDropped += 1
          removeHead().discard
        }
      case DropStrategy.DropNewest =>
        while (overCapacity) {
          howManyWeHaveDropped += 1
          removeLast().discard
        }
    }

    if (howManyWeHaveDropped > 0) Some(howManyWeHaveDropped) else None
  }
}
