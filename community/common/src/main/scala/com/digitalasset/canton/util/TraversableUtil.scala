// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

object TraversableUtil {

  /** Calculates the largest possible list ys of elements in an input traversable xs such that:
    * For all y in ys. y >= x for all x in xs.
    *
    * Informally, this gives the list of all highest elements of `xs`.
    *
    * See `TraversableUtilTest` for an example.
    */
  def maxList[A](xs: Iterable[A])(implicit order: Ordering[A]): List[A] = {
    xs.foldLeft(List.empty: List[A]) {
      case (Nil, x) => List(x)
      case (accs @ (acc :: _), x) =>
        val diff = order.compare(acc, x)
        if (diff < 0) List(x)
        else if (diff == 0)
          x :: accs
        else accs
    }
  }

  /** Calculates the largest possible list ys of elements in an input traversable xs such that:
    * For all y in ys. y <= x for all x in xs.
    *
    * Informally, this gives the list of all lowest elements of `xs`.
    */
  def minList[A](xs: Iterable[A])(implicit order: Ordering[A]): List[A] = {
    maxList[A](xs)(order.reverse)
  }
}
