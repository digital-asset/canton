// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package org.scalactic

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrinting,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec

class PrettyPrettifierTest extends AnyWordSpec with BaseTest {
  import PrettyPrettifierTest.*

  "PrettyPrettifier" should {
    val sut = new PrettyPrettifier
    "use pretty-printing for case classes" in {
      sut(PrettyPrintingCaseClass(10)) shouldBe "PrettyPrintingCaseClass(x = 10)"
      sut(
        PrettyPrintingCaseClassCompanion(20)
      ) shouldBe "PrettyPrintingCaseClassCompanion(y = 20)"
    }

    "use pretty-printing in nested contexts" in {
      sut(
        Container(PrettyPrintingCaseClass(10))
      ) shouldBe "Container(PrettyPrintingCaseClass(x = 10))"

      val array = Array(PrettyPrintingCaseClassCompanion(20))
      sut(array) shouldBe "Array(PrettyPrintingCaseClassCompanion(y = 20))"

      val list = List(PrettyPrintingCaseClassCompanion(30))
      sut(list) shouldBe "List(PrettyPrintingCaseClassCompanion(y = 30))"
    }

    "use pretty-printing for diffs" in {
      sut(PrettyPrintingCaseClass(10), PrettyPrintingCaseClass(20)) shouldBe
        PrettyPair(
          "PrettyPrintingCaseClass(x = 10)",
          "PrettyPrintingCaseClass(x = 20)",
          Some("PrettyPrettifierTest$PrettyPrintingCaseClass(x: 10 -> 20)"),
        )
    }
  }

  "BaseTest" should {
    "prettify using PrettyPrettifier" in {
      val result = intercept[TestFailedException] {
        PrettyPrintingCaseClass(10) shouldBe PrettyPrintingCaseClass(20)
      }
      result.getMessage should (include("PrettyPrintingCaseClass(x = 10)")
        and include("PrettyPrintingCaseClass(x = 20)"))
    }
  }
}

object PrettyPrettifierTest {
  private final case class PrettyPrintingCaseClass(x: Int) extends PrettyPrinting {
    override protected def pretty: Pretty[PrettyPrintingCaseClass] = prettyOfClass(param("x", _.x))
  }

  private final case class PrettyPrintingCaseClassCompanion(y: Int)
      extends PrettyPrintingFromCompanion {
    override def prettyCompanion: PrettyPrintingCompanion[PrettyPrintingCaseClassCompanion] =
      PrettyPrintingCaseClassCompanion
  }
  private object PrettyPrintingCaseClassCompanion
      extends PrettyPrintingCompanion[PrettyPrintingCaseClassCompanion] {
    override protected val pretty: Pretty[PrettyPrintingCaseClassCompanion] =
      prettyOfClass(param("y", _.y))
  }

  private final case class Container[A](value: A) {
    override def toString: String = s"Container(value=$value)"
  }
}
