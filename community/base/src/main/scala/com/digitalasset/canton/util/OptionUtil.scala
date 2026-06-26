// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString

/** Utility methods for `Option` tailored to specific domain requirements in Canton.
  *
  * Serves two specialized use cases:
  *   1. Conflict resolution and state merging (e.g., detecting contradictions via
  *      `Option[Option[A]]`).
  *   1. Bridging Protobuf default values (empty strings, zeros) to and from Scala `Option`s.
  *
  * For all other general-purpose `Option` manipulations or monadic flows, standard Scala library
  * methods or the Cats core library (e.g., `Apply`, `Semigroup` `|+|`, `OptionT`) should be used
  * instead.
  */
object OptionUtil {

  /** Merges two options, allowing the merge function to flag an unresolvable conflict by returning
    * `None`. Returns `None` if a conflict occurs, or `Some(Option[A])` containing the merged or
    * single value.
    */
  def mergeWithO[A](left: Option[A], right: Option[A])(f: (A, A) => Option[A]): Option[Option[A]] =
    (left, right) match {
      case (None, _) => Some(right)
      case (_, None) => Some(left)
      case (Some(x), Some(y)) => f(x, y).map(Some(_))
    }

  /** Detects conflicts between two options based on equality.
    *
    * Return `None` iff both `left` and `right` are defined and not equal. Otherwise, returns
    * `Some(Option[A])` containing whichever side was defined.
    */
  def mergeEqual[A](left: Option[A], right: Option[A]): Option[Option[A]] =
    if (left eq right) Some(left)
    else
      mergeWithO(left, right) { (x, y) =>
        if (x == y) left else None
      }

  /** Combines two options using a provided function if both are defined, or returns the defined
    * one.
    *
    * Note: If type `A` has a Cats `Semigroup` instance, prefer using `left |+| right` instead.
    */
  def mergeWith[A](left: Option[A], right: Option[A])(f: (A, A) => A): Option[A] =
    (left, right) match {
      case (Some(l), Some(r)) => Some(f(l, r))
      case (None, r) => r
      case (l, None) => l
    }

  /** Maps an empty string to `None`. Useful for handling Protobuf default values. */
  def emptyStringAsNone(str: String): Option[String] = if (str.isEmpty) None else Some(str)

  /** Maps an empty [[com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString]] to
    * `None`.
    */
  def emptyStringAsNone[S <: LengthLimitedString](str: S): Option[S] =
    if (str.unwrap.isEmpty) None else Some(str)

  /** Maps `None` to an empty string. Useful for populating Protobuf fields. */
  def noneAsEmptyString(strO: Option[String]): String = strO.getOrElse("")

  /** Maps a `0` integer to `None`. Useful for handling Protobuf default integer values. */
  def zeroAsNone(n: Int): Option[Int] = if (n == 0) None else Some(n)

}
