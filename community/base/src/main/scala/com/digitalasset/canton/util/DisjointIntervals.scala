// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.either.*
import com.digitalasset.canton.checked
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.daml.lf.data.TreeMap
import com.digitalasset.nonempty.NonEmpty
import pprint.Tree

import scala.collection.immutable
import scala.collection.immutable.SortedMap
import scala.math.Ordered.orderingToOrdered
import scala.util.hashing.MurmurHash3

/** Type class for intervals with associated data. Each interval is exclusive on the start and
  * inclusive on the end.
  */
trait IntervalOps[A] {

  /** The type of points over which the intervals are formed */
  type Point

  /** The ordering of points */
  def ordering: Ordering[Point]

  /** The exclusive start of the given interval */
  def startExclusive(interval: A): Point

  /** The inclusive end of the given interval */
  def endInclusive(interval: A): Point

  /** Computes the remainder of `interval` when `startExclusive` to `endInclusive` are removed. This
    * can result in 0 to 2 new intervals. Only called if (`startExclusive`, `endInclusive`] overlaps
    * with the given interval.
    */
  def split(interval: A, startExclusive: Point, endInclusive: Point): Seq[A]

  def pretty: Pretty[A]
}

object IntervalOps {
  type Aux[A, P] = IntervalOps[A] { type Point = P }
}

/** Type class for intervals with associated data where adjacent or overlapping intervals can be
  * merged
  */
trait MergeableIntervalOps[A] extends IntervalOps[A] {

  /** Merges two adjacent or overlapping intervals into one */
  def merge(x: A, y: A): A
}

object MergeableIntervalOps {
  type Aux[A, P] = MergeableIntervalOps[A] { type Point = P }
}

/** Represents a set of pairwise disjoint intervals.
  */
sealed trait DisjointIntervals[A] extends PrettyPrinting with Product with Serializable {
  type Point

  protected def instance: IntervalOps.Aux[A, Point]

  protected implicit def ordering: Ordering[Point] = instance.ordering

  /** The disjoint intervals indexed by [[IntervalOps.endInclusive]] */
  def intervals: SortedMap[Point, A]

  /** Returns all the intervals in `intervals` that overlap with the given `interval` in order.
    */
  def overlappingWith(interval: A): immutable.Iterable[A] =
    overlappingWith(instance.startExclusive(interval), instance.endInclusive(interval))

  def overlappingWith(startExclusive: Point, endInclusive: Point): Seq[A] =
    intervals
      .iteratorFrom(startExclusive)
      .dropWhile { case (end, _) => end <= startExclusive }
      .takeWhile { case (_, interval) => instance.startExclusive(interval) < endInclusive }
      .map { case (_, interval) => interval }
      .to(Seq)

  /** Removes all intervals fully covered by the given range and splits the partly covered intervals
    * so that the given range is removed from
    */
  def remove(startExclusive: Point, endInclusive: Point): DisjointIntervals.Aux[A, Point] = {
    val overlapping = overlappingWith(startExclusive, endInclusive)
    NonEmpty.from(overlapping) match {
      case None => this
      case Some(overlappingNE) =>
        if (overlappingNE.sizeIs == 1) {
          // The given argument is contained in a single interval. So we just have to split this one.
          val overlap = overlappingNE.head1
          val replacements = instance.split(overlap, startExclusive, endInclusive)
          val newIntervals = intervals - instance.endInclusive(overlap) ++
            replacements.map(i => instance.endInclusive(i) -> i)
          DisjointIntervals.impl(instance)(newIntervals)
        } else {
          // The given argument overlaps with multiple intervals. So we have to split the first and last
          // interval and remove all the others.
          val first = overlappingNE.head1
          val last = overlappingNE.last1
          val firstReplacements = instance.split(first, startExclusive, endInclusive)
          val lastReplacements = instance.split(last, startExclusive, endInclusive)
          val newIntervals = intervals -- overlapping.iterator.map(instance.endInclusive) ++
            firstReplacements.map(asMapEntry) ++ lastReplacements.map(asMapEntry)
          DisjointIntervals.impl(instance)(newIntervals)
        }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  final override def equals(other: Any): Boolean =
    if (this eq other.asInstanceOf[AnyRef]) true
    else
      other match {
        case that: DisjointIntervals[?] =>
          that.canEqual(this) && this.intervals == that.intervals
        case _ => false
      }

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  final def canEqual(other: Any): Boolean =
    other.isInstanceOf[DisjointIntervals[?]]

  final override def hashCode(): Int =
    MurmurHash3.productHash(this)

  override def productArity: Int = 1
  override def productElement(n: Int): Any = intervals
  override def productPrefix: String = "DisjointIntervals"

  override protected val pretty: Pretty[DisjointIntervals.this.type] = { inst =>
    Tree.Apply(
      "DisjointIntervals",
      inst.intervals.valuesIterator.map(instance.pretty.treeOf),
    )
  }

  protected final def asMapEntry(interval: A): (Point, A) =
    instance.endInclusive(interval) -> interval
}

sealed trait DisjointIntervalsCompanion[DI[X] <: DisjointIntervals[X]] {
  type Aux[A, P] = DI[A] { type Point = P }
  type Ops[A] <: IntervalOps[A]

  protected def impl[A](
      inst: Ops[A]
  )(ivals: SortedMap[inst.Point, A]): Aux[A, inst.Point]

  def empty[A](implicit instance: Ops[A]): Aux[A, instance.Point] =
    impl[A](instance)(SortedMap.empty(instance.ordering))

  def from[A](intervals: IterableOnce[A])(implicit
      instance: Ops[A]
  ): Either[String, Aux[A, instance.Point]] = {
    val sortedIntervals =
      SortedMap.from(intervals.iterator.map(i => instance.endInclusive(i) -> i))(instance.ordering)
    checkDisjointSorted(sortedIntervals.valuesIterator)
      .map((_: Unit) => impl[A](instance)(sortedIntervals))
  }

  def tryFrom[A](intervals: IterableOnce[A])(implicit instance: Ops[A]): Aux[A, instance.Point] =
    from(intervals).valueOr(err => throw new IllegalArgumentException(err))

  def checkDisjoint[A](intervals: immutable.Iterable[A])(implicit
      instance: IntervalOps[A]
  ): Either[String, Unit] =
    checkDisjointSorted(intervals.toSeq.sortBy(instance.endInclusive)(instance.ordering))

  private def checkDisjointSorted[A](sortedIntervals: IterableOnce[A])(implicit
      instance: IntervalOps[A]
  ): Either[String, Unit] =
    MonadUtil.foldLeftM((), sortedIntervals.iterator.sliding(2)) { (_, next) =>
      NonEmpty.from(next) match {
        case None => Either.unit[String]
        case Some(ne) =>
          if (ne.sizeIs <= 1) Either.unit[String]
          else {
            val first = ne.head1
            val second = ne.last1
            implicit val ordering: Ordering[instance.Point] = instance.ordering
            Either.cond(
              instance.endInclusive(first) <= instance.startExclusive(second),
              (),
              s"Overlapping intervals $first and $second",
            )
          }
      }
    }
}

object DisjointIntervals extends DisjointIntervalsCompanion[DisjointIntervals] {
  override type Ops[A] = IntervalOps[A]

  override protected def impl[A](
      inst: IntervalOps[A]
  )(ivals: SortedMap[inst.Point, A]): DisjointIntervals.Aux[A, inst.Point] =
    new DisjointIntervals[A] {
      override type Point = inst.Point
      override protected val instance: IntervalOps.Aux[A, Point] = inst
      override val intervals: SortedMap[Point, A] = ivals
    }
}

sealed trait MergeableDisjointIntervals[A] extends DisjointIntervals[A] {
  override protected def instance: MergeableIntervalOps.Aux[A, Point]

  /** Adds the given interval to this set of intervals. Removes all overlapping intervals and merges
    * them with the given interval.
    */
  def add(interval: A, mergeAdjacent: Boolean): MergeableDisjointIntervals.Aux[A, Point] = {
    val overlapping = overlappingWith(interval).toSeq
    val toMerge = if (mergeAdjacent) {
      val startExclusive = instance.startExclusive(interval)
      val endInclusive = instance.endInclusive(interval)
      val overlapExceedsStart = overlapping.headOption.exists(
        instance.startExclusive(_) < startExclusive
      )
      val adjacentStart = if (!overlapExceedsStart) {
        intervals.rangeTo(instance.startExclusive(interval)).lastOption.collect {
          case (`startExclusive`, ival) => ival
        }
      } else None
      val overlapExceedsEnd = overlapping.lastOption.exists(instance.endInclusive(_) > endInclusive)
      val adjacentEnd = if (!overlapExceedsEnd) {
        intervals
          .rangeFrom(endInclusive)
          .view
          .dropWhile { case (end, _) => end <= endInclusive }
          .headOption
          .collect {
            case (_, ival) if instance.startExclusive(ival) == endInclusive => ival
          }
      } else None
      adjacentStart.toList ++ overlapping ++ adjacentEnd.toList
    } else overlapping
    val newInterval = toMerge.foldLeft(interval)(instance.merge)
    val newIntervals = intervals -- toMerge.map(instance.endInclusive) + asMapEntry(newInterval)
    MergeableDisjointIntervals.impl(instance)(newIntervals)
  }

  def mergeAdjacent: MergeableDisjointIntervals[A] =
    NonEmpty.from(intervals) match {
      case None => this
      case Some(intervalsNE) =>
        val newIntervalsB = Seq.newBuilder[A]

        val (_, first) = intervalsNE.head1
        val last = intervalsNE.tail1.foldLeft(first) { case (current, (_, next)) =>
          if (instance.endInclusive(current) == instance.startExclusive(next)) {
            instance.merge(current, next)
          } else {
            newIntervalsB += current
            next
          }
        }
        newIntervalsB += last
        val newIntervals = newIntervalsB.result()
        // If nothing has been merged, we do not have to create a copy.
        if (newIntervals.sizeIs == intervals.size) this
        else {
          val newIntervalsMap = checked(
            // Entries in `newIntervals` are sorted and merged, so we can use the fast
            TreeMap.fromStrictlyOrderedEntries(
              newIntervals.view.map(interval => instance.endInclusive(interval) -> interval)
            )
          )
          MergeableDisjointIntervals.impl(instance)(newIntervalsMap)
        }
    }
}

object MergeableDisjointIntervals extends DisjointIntervalsCompanion[MergeableDisjointIntervals] {
  override type Ops[A] = MergeableIntervalOps[A]

  override protected def impl[A](
      inst: MergeableIntervalOps[A]
  )(ivals: SortedMap[inst.Point, A]): MergeableDisjointIntervals.Aux[A, inst.Point] =
    new MergeableDisjointIntervals[A] {
      override type Point = inst.Point
      override protected val instance: MergeableIntervalOps.Aux[A, Point] = inst
      override val intervals: SortedMap[Point, A] = ivals
    }
}
