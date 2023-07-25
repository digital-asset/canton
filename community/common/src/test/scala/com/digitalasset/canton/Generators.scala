// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.nonempty.NonEmpty
import org.scalacheck.{Arbitrary, Gen}

object Generators {
  private val nonEmptyMaxSize: Int = 4

  def nonEmptyListGen[T](implicit arb: Arbitrary[T]): Gen[NonEmpty[List[T]]] = for {
    size <- Gen.choose(1, nonEmptyMaxSize - 1)
    element <- arb.arbitrary
    elements <- Gen.containerOfN[List, T](size, arb.arbitrary)
  } yield NonEmpty(List, element, elements: _*)

  def nonEmptySetGen[T](implicit arb: Arbitrary[T]): Gen[NonEmpty[Set[T]]] =
    nonEmptyListGen[T].map(_.toSet)
  def nonEmptySet[T](implicit arb: Arbitrary[T]): Arbitrary[NonEmpty[Set[T]]] =
    Arbitrary(nonEmptyListGen[T].map(_.toSet))
}
