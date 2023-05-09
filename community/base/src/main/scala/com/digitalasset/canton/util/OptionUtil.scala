// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString

import scala.annotation.nowarn

object OptionUtil {

  def mergeWithO[A](left: Option[A], right: Option[A])(f: (A, A) => Option[A]): Option[Option[A]] =
    (left, right) match {
      case (None, _) => Some(right)
      case (_, None) => Some(left)
      case (Some(x), Some(y)) => f(x, y).map(Some(_))
    }

  /** Return None iff both `left` and `right` are defined and not equal.
    *
    * Otherwise, return
    *  - Some(left), if only left is defined
    *  - Some(right), if right is defined
    */
  def mergeEqual[A](left: Option[A], right: Option[A]): Option[Option[A]] = {
    if (left eq right) Some(left)
    else
      mergeWithO(left, right) { (x, y) =>
        if (x == y) left else None
      }
  }

  def mergeWith[A](left: Option[A], right: Option[A])(f: (A, A) => A): Option[A] = {
    @nowarn("msg=match may not be exhaustive") // mergeWithO is always defined
    val Some(result) = mergeWithO(left, right)((l, r) => Some(f(l, r)))
    result
  }

  def zipWith[A, B, C](left: Option[A], right: Option[B])(f: (A, B) => C): Option[C] =
    for {
      l <- left
      r <- right
    } yield f(l, r)

  def emptyStringAsNone(str: String): Option[String] = if (str.isEmpty) None else Some(str)
  def emptyStringAsNone[S <: LengthLimitedString](str: S): Option[S] =
    if (str.unwrap.isEmpty) None else Some(str)
  def noneAsEmptyString(strO: Option[String]): String = strO.getOrElse("")

  def zeroAsNone(n: Int): Option[Int] = if (n == 0) None else Some(n)

}
