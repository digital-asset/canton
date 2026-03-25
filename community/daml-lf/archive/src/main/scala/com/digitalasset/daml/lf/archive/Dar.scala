// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import scalaz.std.list.*
import scalaz.syntax.equal.*
import scalaz.{Applicative, Equal, Traverse}

final case class Dar[A](main: A, dependencies: List[A]) {
  lazy val all: List[A] = main :: dependencies
}

object Dar {
  implicit val darTraverse: Traverse[Dar] = new Traverse[Dar] {
    override def map[A, B](fa: Dar[A])(f: A => B): Dar[B] =
      Dar[B](main = f(fa.main), dependencies = fa.dependencies.map(f))

    override def traverseImpl[G[_]: Applicative, A, B](fa: Dar[A])(f: A => G[B]): G[Dar[B]] = {
      import scalaz.syntax.apply.*
      import scalaz.syntax.traverse.*
      import scalaz.std.list.*
      val gb: G[B] = f(fa.main)
      val gbs: G[List[B]] = fa.dependencies.traverse(f)
      ^(gb, gbs)((b, bs) => Dar(b, bs))
    }
  }

  implicit def darEqual[A: Equal]: Equal[Dar[A]] = new Equal[Dar[A]] {
    override def equal(a1: Dar[A], a2: Dar[A]): Boolean =
      a1.main === a2.main && a1.dependencies === a2.dependencies
  }
}
