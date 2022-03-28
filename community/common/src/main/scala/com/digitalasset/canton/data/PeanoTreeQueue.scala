// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data
import com.digitalasset.canton.data.PeanoQueue.{
  AssociatedValue,
  BeforeHead,
  InsertedValue,
  NotInserted,
}
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable
import scala.concurrent.blocking

/** Implementation of [[PeanoQueue]] for [[java.lang.Long]] keys based on a tree map.
  *
  * This implementation is not thread safe.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class PeanoTreeQueue[V](initHead: Long) extends PeanoQueue[Long, V] {

  private val elems: mutable.TreeMap[Long, V] = mutable.TreeMap.empty[Long, V]

  private var headV: Long = initHead

  override def head: Long = headV

  private var frontV: Long = initHead

  override def front: Long = frontV

  override def insert(key: Long, value: V): Boolean = {
    require(key != Long.MaxValue, s"The maximal key value ${Long.MaxValue} cannot be inserted.")

    def associationChanged(oldValue: V): Nothing =
      throw new IllegalArgumentException(
        s"New value $value for key $key differs from old value $oldValue."
      )

    if (key >= frontV) {
      elems.put(key, value) match {
        case None => if (key == frontV) cleanup()
        case Some(oldValue) =>
          if (oldValue != value) {
            elems.put(key, oldValue) // undo the changes
            associationChanged(oldValue)
          }
      }
      true
    } else if (key >= headV) {
      val oldValue =
        elems
          .get(key)
          .getOrElse(
            throw new IllegalStateException("Unreachable code by properties of the PeanoQueue")
          )
      if (value != oldValue)
        associationChanged(oldValue)
      true
    } else false
  }

  override def alreadyInserted(key: Long): Boolean = {
    if (key >= frontV) {
      elems.contains(key)
    } else {
      true
    }
  }

  /** Update `front` as long as the [[elems]] contain consecutive key-value pairs starting at `front`.
    */
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def cleanup(): Unit = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var next = frontV
    val iter = elems.keysIteratorFrom(next)
    while (iter.hasNext && iter.next() == next) {
      next += 1
    }
    frontV = next
  }

  def get(key: Long): AssociatedValue[V] = {
    if (key < headV) BeforeHead
    else
      elems.get(key) match {
        case None =>
          val floor = elems.rangeImpl(None, Some(key)).lastOption.map(_._2)
          val ceiling = elems.rangeImpl(Some(key), None).headOption.map(_._2)
          NotInserted(floor, ceiling)
        case Some(value) => InsertedValue(value)
      }
  }

  override def poll(): Option[(Long, V)] = {
    if (headV >= frontV) None
    else {
      val key = headV
      val value =
        elems
          .remove(key)
          .getOrElse(
            throw new IllegalStateException("Unreachable code by properties of the PeanoQueue")
          )
      headV = key + 1
      Some((key, value))
    }
  }

  @VisibleForTesting
  def invariant: Boolean = {
    headV <= frontV &&
    elems.rangeImpl(None, Some(frontV + 1)).toSeq.map(_._1) == (headV until frontV)
  }

  override def toString: String = {
    val builder = new StringBuilder("PeanoQueue(front = ").append(frontV)
    elems.foreach { case (k, v) =>
      builder.append(", ").append(k).append("->").append(v.toString)
    }
    builder.append(")")
    builder.toString
  }
}

object PeanoTreeQueue {
  def apply[V](init: Long) = new PeanoTreeQueue[V](init)
}

/** A thread-safe [[PeanoTreeQueue]] thanks to synchronizing all methods */
class SynchronizedPeanoTreeQueue[V](initHead: Long) extends PeanoQueue[Long, V] {
  private[this] val queue: PeanoQueue[Long, V] = new PeanoTreeQueue(initHead)

  override def head: Long = blocking { queue synchronized queue.head }

  override def front: Long = blocking { queue synchronized queue.front }

  override def insert(key: Long, value: V): Boolean =
    blocking { queue synchronized queue.insert(key, value) }

  override def alreadyInserted(key: Long): Boolean =
    blocking { queue synchronized queue.alreadyInserted(key) }

  override def get(key: Long): AssociatedValue[V] = blocking { queue synchronized queue.get(key) }

  override def poll(): Option[(Long, V)] = blocking { queue synchronized queue.poll() }

}
