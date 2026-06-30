// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import com.daml.scalatest.WordSpecCheckLaws
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class FrontStackSpec
    extends AnyWordSpec
    with Matchers
    with ScalaCheckPropertyChecks
    with WordSpecCheckLaws {

  import DataArbitrary.*

  "apply" when {
    "1 element is provided" should {
      "behave the same as prepend" in forAll { x: Int =>
        FrontStack(x) should ===(x +: FrontStack.empty)
      }
    }

    "2 elements are provided" should {
      "behave the same as prepend" in forAll { (x: Int, y: Int) =>
        FrontStack(x, y) should ===(x +: y +: FrontStack.empty)
      }
    }
  }

  "++:" should {
    "yield equal results to +:" in forAll { (ia: ImmArray[Int], fs: FrontStack[Int]) =>
      (ia ++: fs) should ===(ia.toSeq.foldRight(fs)(_ +: _))
    }
  }

  "toImmArray" should {
    "yield same elements as iterator" in forAll { fs: FrontStack[Int] =>
      fs.toImmArray should ===(fs.iterator.to(ImmArray))
    }
  }

  "length" should {
    "be tracked accurately during building" in forAll { fs: FrontStack[Int] =>
      fs.length should ===(fs.iterator.length)
    }
  }

  "slowApply" should {
    "throw when out of bounds" in forAll { fs: FrontStack[Int] =>
      an[IndexOutOfBoundsException] should be thrownBy fs.slowApply(-1)
      an[IndexOutOfBoundsException] should be thrownBy fs.slowApply(fs.length)
    }

    "preserve Seq's apply" in forAll { fs: FrontStack[Int] =>
      val expected = Table(
        ("value", "index"),
        fs.toImmArray.toSeq.zipWithIndex*
      )
      forEvery(expected) { (value, index) =>
        fs.slowApply(index) should ===(value)
      }
    }
  }

  "toBackStack" should {
    "be retracted by toFrontStack" in forAll { fs: FrontStack[Int] =>
      fs.toBackStack.toFrontStack should ===(fs)
    }
  }

  "Traverse instance" should {
    import cats.Eq
    import cats.laws.discipline.TraverseTests
    import DataArbitrary.`arb FrontStack`

    implicit val eqFrontStack: Eq[FrontStack[Int]] = Eq.fromUniversalEquals

    // Equivalent of the former `checkLaws(ScalazProperties.traverse.laws[FrontStack])`,
    // expressed with cats-laws and registered as individual word-spec tests.
    TraverseTests[FrontStack]
      .traverse[Int, Int, Int, Int, Option, Option]
      .all
      .properties
      .foreach { case (id, prop) =>
        id in check(prop)
      }

    "reconstruct itself with foldRight" in forAll { fs: FrontStack[Int] =>
      cats
        .Foldable[FrontStack]
        .foldRight(fs, cats.Eval.now(FrontStack.empty[Int]))((i, acc) => acc.map(i +: _))
        .value should ===(fs)
    }
  }
}
