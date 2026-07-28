// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.collection

import cats.FlatMap
import cats.data.Chain
import cats.kernel.Semigroup
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.util.{ChainUtil, Checked, ErrorUtil}

import scala.annotation.tailrec
import scala.collection.{concurrent, mutable}

object MapsUtil {

  /** Merges two maps of sets, combining values for intersecting keys via set union.
    *
    * @param big
    *   Used as the base map for fold optimization (should be the larger map).
    * @param small
    *   The map folded into `big`.
    */
  def mergeMapsOfSets[K, V](big: Map[K, Set[V]], small: Map[K, Set[V]]): Map[K, Set[V]] =
    small.foldLeft(big) { case (acc, (k, v)) =>
      acc.updatedWith(k)(opt => Some(opt.fold(v)(_ union v)))
    }

  /** Atomically modifies `map` at `key`.
    *
    * `notFound` and `f` may evaluate multiple times due to concurrent retries; their effects are
    * sequenced into the result.
    *
    * @param notFound
    *   Value to insert if the key is absent (`None` leaves the key absent).
    * @param f
    *   Function to transform the value found in the map (`None` removes the key).
    * @return
    *   The value associated with the key *before* the update.
    */
  def modifyWithConcurrentlyM[F[_], K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => F[Option[V]],
      f: V => F[Option[V]],
  )(implicit monad: FlatMap[F]): F[Option[V]] = {
    // Make sure that we evaluate `notFound` at most once
    lazy val notFoundV = notFound

    def step(): F[Either[Unit, Option[V]]] = map.get(key) match {
      case None =>
        monad.map(notFoundV) {
          case None => Either.right[Unit, Option[V]](None)
          case Some(newValue) =>
            map
              .putIfAbsent(key, newValue)
              .fold(Either.right[Unit, Option[V]](None))(_ => Either.left[Unit, Option[V]](()))
        }
      case Some(oldValue) =>
        monad.map(f(oldValue)) {
          case None => Either.cond(map.remove(key, oldValue), Some(oldValue), ())
          case Some(newValue) =>
            Either.cond(map.replace(key, oldValue, newValue), Some(oldValue), ())
        }
    }

    monad.tailRecM(())((_: Unit) => step())
  }

  /** Atomically updates or inserts into `map` at `key`.
    *
    * Note: Unlike [[modifyWithConcurrentlyM]], this method does not support removing elements or
    * preventing the insertion of elements when they are not originally in the map.
    *
    * @param notFound
    *   Value to insert if the key is absent.
    * @param f
    *   Function to update the value associated with the key. Does not support removals.
    * @return
    *   The value associated with the key *before* the update, or `None` if absent.
    */
  def updateWithConcurrentlyM[F[_], K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => F[V],
      f: V => F[V],
  )(implicit monad: FlatMap[F]): F[Option[V]] =
    modifyWithConcurrentlyM(
      map,
      key,
      monad.map(notFound)(Some(_)),
      (v: V) => monad.map(f(v))(Some(_)),
    )

  /** Same as [[modifyWithConcurrentlyM]] but discards the original value associated with the key.
    *
    * @param notFound
    *   Value to insert if the key is absent (`None` leaves the key absent).
    * @param f
    *   Function to transform the value found in the map (`None` removes the key).
    */
  def modifyWithConcurrentlyM_[F[_], K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => F[Option[V]],
      f: V => F[Option[V]],
  )(implicit monad: FlatMap[F]): F[Unit] =
    monad.void(modifyWithConcurrentlyM(map, key, notFound, f))

  /** Same as [[updateWithConcurrentlyM]] but discards the original value associated with the key.
    *
    * @param notFound
    *   Value to insert if the key is absent.
    * @param f
    *   Function to update the value associated with the key.
    */
  def updateWithConcurrentlyM_[F[_], K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => F[V],
      f: V => F[V],
  )(implicit monad: FlatMap[F]): F[Unit] =
    monad.void(updateWithConcurrentlyM(map, key, notFound, f))

  /** Specializes [[modifyWithConcurrentlyM_]] for the [[Checked]] monad.
    *
    * Non-aborts are only accumulated from the specific evaluation of `notFound` or `f` that either
    * successfully updates the map or causes the first [[Checked.Abort]].
    *
    * @param notFound
    *   Value to insert if the key is absent (`None` leaves absent).
    * @param f
    *   Function to update the value associated with the key (`None` removes).
    * @return
    *   Either [[Checked.Abort]] or [[Checked.Result]] monad completing with [[scala.Unit]].
    */
  def modifyWithConcurrentlyChecked_[A, N, K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => Checked[A, N, Option[V]],
      f: V => Checked[A, N, Option[V]],
  ): Checked[A, N, Unit] = {
    /* The update function `f` may execute several times for different values,
     * In that case, we may get several non-aborts reported
     * for the same contract update.
     *
     * To keep only those from the last update, we group the problems in `Chain`s and only keep the last one.
     */
    def lift(chain: Chain[N]): Chain[Chain[N]] = Chain(chain)

    MapsUtil
      .modifyWithConcurrentlyM_(
        map,
        key,
        notFound.mapNonaborts(lift),
        (x: V) => f(x).mapNonaborts(lift),
      )
      .mapNonaborts { chainsOfNonaborts =>
        ChainUtil.lastOption(chainsOfNonaborts).getOrElse(Chain.empty)
      }
  }

  /** Specializes [[updateWithConcurrentlyM_]] for the [[Checked]] monad.
    *
    * @param notFound
    *   Value to insert if the key is absent.
    * @param f
    *   Function to update the value associated with the key. Does not support removals.
    * @return
    *   Either [[Checked.Abort]] or [[Checked.Result]] monad completing with [[scala.Unit]].
    */
  def updateWithConcurrentlyChecked_[A, N, K, V](
      map: concurrent.Map[K, V],
      key: K,
      notFound: => Checked[A, N, V],
      f: V => Checked[A, N, V],
  ): Checked[A, N, Unit] =
    modifyWithConcurrentlyChecked_(map, key, notFound.map(Some(_)), (v: V) => f(v).map(Some.apply))

  /** Transforms a map by mapping each key to a set of new keys.
    *
    * Note: Generates a new map m' with the following property:
    * {{{If { (k,v) in m and k' in f(k) } then v is in the set m'[k']}}}
    *
    * See `MapsUtilTest` for an example.
    *
    * @param f
    *   Effectful function to map each of the input keys to a new set of keys ks.
    * @return
    *   A new map where each original value is grouped under all new keys returned by applying f to
    *   its original key.
    */
  def groupByMultipleM[M[_], K, K2, V](
      m: Map[K, V]
  )(f: K => M[Set[K2]])(implicit M: cats.Monad[M]): M[Map[K2, Set[V]]] =
    m.toList.foldM(Map.empty[K2, Set[V]]) { case (acc, (k, v)) =>
      M.map(f(k)) {
        _.foldLeft(acc) { (m_, k2) =>
          m_.updatedWith(k2)(opt => Some(opt.fold(Set(v))(_ + v)))
        }
      }
    }

  /** Atomically updates the value for `key` if present.
    *
    * The update function `f` may be evaluated multiple times. If `f` throws an exception, the
    * exception propagates and the map is not updated.
    *
    * @param f
    *   Function to update the value associated with the key. Bypassed if `f(current) eq current`.
    *   Retried on conflict.
    * @return
    *   `true` if the map was changed.
    */
  def updateWithConcurrently[K, V <: AnyRef](map: concurrent.Map[K, V], key: K)(
      f: V => V
  ): Boolean = {
    @tailrec def go(): Boolean = map.get(key) match {
      case None => false
      case Some(current) =>
        val next = f(current)
        if (current eq next)
          false // Do not modify the map if the transformation doesn't change anything
        else if (map.replace(key, current, next)) true
        else go() // concurrent modification, so let's retry
    }
    go()
  }

  /** Inserts `value` at `key` if absent.
    *
    * @throws java.lang.IllegalStateException
    *   if the key already exists with a different value.
    */
  def tryPutIdempotent[K, V](map: concurrent.Map[K, V], key: K, value: V)(implicit
      loggingContext: ErrorLoggingContext
  ): Unit =
    map.putIfAbsent(key, value).foreach { oldValue =>
      ErrorUtil.requireState(
        oldValue == value,
        s"Map key $key already has value $oldValue assigned to it. Cannot insert $value.",
      )
    }

  /** Merges two maps, combining values for intersecting keys.
    *
    * @param f
    *   Function resolving collisions.
    * @return
    *   New map containing all keys from both maps, with values combined using the provided
    *   function.
    */
  def mergeWith[K, V](map1: Map[K, V], map2: Map[K, V])(f: (V, V) => V): Map[K, V] = {
    // We don't need `f`'s associativity when we merge maps
    implicit val semigroupV: Semigroup[V] = (x: V, y: V) => f(x, y)
    Semigroup[Map[K, V]].combine(map1, map2)
  }

  /** Mutates `m` by adding elements from `extendWith`.
    *
    * @param extendWith
    *   Key-value pairs to insert.
    * @param merge
    *   Function resolving collisions for the same key.
    */
  def extendMapWith[K, V](m: mutable.Map[K, V], extendWith: IterableOnce[(K, V)])(
      merge: (V, V) => V
  ): Unit =
    extendWith.iterator.foreach { case (k, v) =>
      m.updateWith(k) {
        case None => Some(v)
        case Some(mv) => Some(merge(mv, v))
      }.discard[Option[V]]
    }

  /** Returns a new map containing elements from `m` and `extendWith`.
    *
    * @param extendWith
    *   Key-value pairs to insert.
    * @param merge
    *   Function resolving collisions for the same key.
    */
  def extendedMapWith[K, V](m: Map[K, V], extendWith: IterableOnce[(K, V)])(
      merge: (V, V) => V
  ): Map[K, V] =
    extendWith.iterator.foldLeft(m) { case (acc, (k, v)) =>
      acc.updatedWith(k) {
        case None => Some(v)
        case Some(mv) => Some(merge(mv, v))
      }
    }

  /** Converts a collection of pairs into a map, while checking for conflicting key bindings.
    *
    * @return
    *   `Right(Map)` if there are no collisions with distinct values. `Left(Map)` containing *only*
    *   the conflicting keys and their divergent value sets if collisions exist.
    */
  def toNonConflictingMap[K, V](it: Iterable[(K, V)]): Either[Map[K, Set[V]], Map[K, V]] = {
    val set = it.toSet
    val map = set.toMap
    if (map.sizeCompare(set) == 0) Right(map)
    else
      Left(set.groupBy { case (k, _) => k }.collect {
        case (k, v) if v.sizeIs > 1 => (k, v.map { case (_, v2) => v2 })
      })
  }

  /** Intersects two maps of sets.
    *
    * Retains keys present in both maps where the set intersection is non-empty.
    *
    * @return
    *   Map containing intersections of the values of the two input maps.
    */
  def intersectValues[K, V](m1: Map[K, Set[V]], m2: Map[K, Set[V]]): Map[K, Set[V]] =
    m1.flatMap { case (k, v1) =>
      for {
        v2 <- m2.get(k)
        intersection = v1.intersect(v2) if intersection.nonEmpty
      } yield k -> intersection
    }

  /** Transposes a map of `K -> Set[V]` to `V -> Set[K]`. */
  def transpose[K, V](original: Map[K, Set[V]]): Map[V, Set[K]] =
    original.foldLeft(Map.empty[V, Set[K]]) { case (acc, (k, vs)) =>
      vs.foldLeft(acc) { case (innerAcc, v) =>
        innerAcc.updatedWith(v) {
          case None => Some(Set(k))
          case Some(ks) => Some(ks + k)
        }
      }
    }

  /** Strips `None` values from the map and unwraps `Some`s. */
  def skipEmpty[K, V](original: Map[K, Option[V]]): Map[K, V] =
    original.collect { case (k, Some(v)) =>
      (k, v)
    }

  /** Inserts `newValue` at `key` if absent.
    *
    *   - Idempotency: Ignores the insertion if the key already exists and the existing value is
    *     equal to the new value.
    *   - Fails if the key already exists and the existing value differs from the new value.
    *
    * @param errorFn
    *   Takes the key, oldValue, newValue and returns an appropriate error if the key already exists
    *   with a different value.
    * @return
    *   `Right(())` on successful insert or if the existing value equals `newValue`. `Left(E)` on
    *   conflict.
    */
  def insertIfAbsent[K, V, E](
      map: concurrent.Map[K, V],
      key: K,
      newValue: V,
      errorFn: (K, V, V) => E,
  ): Either[E, Unit] =
    map.putIfAbsent(key, newValue) match {
      case None => Either.unit
      case Some(oldValue) =>
        Either.cond(oldValue == newValue, (), errorFn(key, oldValue, newValue))
    }

  /** Inserts `newValue` at `key` if absent.
    *
    *   - Idempotency: Ignores the insertion if the key already exists and the existing value is
    *     equal to the new value.
    *   - Fails if the key already exists and the existing value differs from the new value.
    *
    * @param staticErrorFn
    *   Error returned if the key already exists with a different value.
    * @return
    *   `Right(())` on successful insert or if the existing value equals `newValue`. `Left(E)` on
    *   conflict.
    */
  def insertIfAbsent[K, V, E](
      map: concurrent.Map[K, V],
      key: K,
      newValue: V,
      staticErrorFn: => E,
  ): Either[E, Unit] =
    MapsUtil.insertIfAbsent(map, key, newValue, (_: K, _: V, _: V) => staticErrorFn)
}
