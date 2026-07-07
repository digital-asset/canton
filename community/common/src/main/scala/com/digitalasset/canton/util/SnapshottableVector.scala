// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.pretty.Pretty

import java.util.concurrent.atomic.AtomicReference

/** A mutable vector to which elements can be appended and where snapshots can be taken atomically.
  * Taking a snapshot is constant-time, appending elements is effectively constant-time. Thread
  * safe.
  */
class SnapshottableVector[A] {

  private val vector: AtomicReference[Vector[A]] = new AtomicReference[Vector[A]](Vector.empty)

  /** Obtains a snapshot of the current vector. Subsequent modifications of this vector do not show
    * up in the snapshot.
    */
  def snapshot: Vector[A] = vector.get()

  /** Appends an element to the vector. */
  def add(x: A): Unit =
    vector.getAndUpdate(init => init.appended(x)).discard

  /** Appends all elements to the vector.
    *
    * @note
    *   If `addAll` for a large value of `xs` interleaves with many calls to `add`, the call to
    *   `addAll` is likely starving, because it needs to be re-executed due to the faster concurrent
    *   change via `add`.
    */
  def addAll(xs: IterableOnce[A]): Unit =
    vector.getAndUpdate(init => init.appendedAll(xs)).discard

  override def toString: String = vector.get.toString
}

object SnapshottableVector {
  def empty[A]: SnapshottableVector[A] = new SnapshottableVector[A]

  def from[A](xs: IterableOnce[A]): SnapshottableVector[A] = {
    val e = empty[A]
    e.addAll(xs)
    e
  }

  implicit def prettySnapshottableVector[A: Pretty]: Pretty[SnapshottableVector[A]] = {
    import Pretty.*
    prettyOfParam(_.snapshot)
  }
}
