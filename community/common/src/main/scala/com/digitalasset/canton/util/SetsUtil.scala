// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Applicative
import cats.syntax.functor.*
import cats.syntax.traverse.*
import monocle.Traversal
import monocle.function.Each

object SetsUtil {
  def requireDisjoint[A](xs: (Set[A], String), ys: (Set[A], String)): Unit = {
    val overlap = xs._1 intersect ys._1
    if (overlap.nonEmpty)
      throw new IllegalArgumentException(s"${xs._2} overlap with ${ys._2}. Overlap: $overlap")
  }

  object instances {

    /** Monocle Each for Set[A]
      */
    implicit def eachSet[A]: Each[Set[A], A] = Each {
      new Traversal[Set[A], A] {
        def modifyA[F[_]: Applicative](f: A => F[A])(s: Set[A]): F[Set[A]] =
          s.toList
            .traverse(f)
            .map(_.toSet)
      }
    }
  }
}
