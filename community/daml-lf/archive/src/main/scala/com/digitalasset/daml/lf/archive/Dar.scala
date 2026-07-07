// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

final case class Dar[A](main: A, dependencies: List[A]) {
  lazy val all: List[A] = main :: dependencies
  def map[B](f: A => B): Dar[B] = Dar(f(main), dependencies.map(f))
}
