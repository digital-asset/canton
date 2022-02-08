// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.ShowUtil._
import org.mockito.exceptions.verification.SmartNullPointerException
import org.scalatest.wordspec.AnyWordSpec

class PrettyPrintingTest extends AnyWordSpec with BaseTest {

  case object ExampleSingleton extends PrettyPrinting {
    override def pretty: Pretty[ExampleSingleton.type] = prettyOfObject[ExampleSingleton.type]
  }

  val singletonInst: ExampleSingleton.type = ExampleSingleton
  val singletonStr: String = "ExampleSingleton"

  /** Example of a class where pretty printing needs to be implemented separately.
    */
  case class ExampleAlienClass(p1: String, p2: String)

  /** Enable pretty printing for [[ExampleAlienClass]].
    */
  implicit val prettyAlien: Pretty[ExampleAlienClass] = {
    import Pretty._
    prettyOfClass(
      param("p1", _.p1.doubleQuoted),
      unnamedParam(_.p2.doubleQuoted),
      customParam(inst =>
        show"allParams: {${Seq(inst.p1.singleQuoted, inst.p2.singleQuoted).mkShow()}}"
      ),
      paramWithoutValue("confidential"),
    )
  }

  val alienInst: ExampleAlienClass = ExampleAlienClass("p1Val", "p2Val")
  val alienStr: String =
    """ExampleAlienClass(p1 = "p1Val", "p2Val", allParams: {'p1Val', 'p2Val'}, confidential = ...)"""

  /** Example of a class that extends [[PrettyPrinting]].
    */
  case class ExampleCaseClass(alien: ExampleAlienClass, singleton: ExampleSingleton.type)
      extends PrettyPrinting {
    override def pretty: Pretty[ExampleCaseClass] =
      prettyOfClass(param("alien", _.alien), param("singleton", _.singleton))
  }

  val caseClassInst: ExampleCaseClass = ExampleCaseClass(alienInst, ExampleSingleton)
  val caseClassStr: String = s"ExampleCaseClass(alien = $alienStr, singleton = $singletonStr)"

  /** Example of a class that uses ad hoc pretty printing.
    */
  case class ExampleAdHocCaseClass(alien: ExampleAlienClass, caseClass: ExampleCaseClass)
      extends PrettyPrinting {
    override def pretty: Pretty[ExampleAdHocCaseClass] = adHocPrettyInstance
  }

  val adHocCaseClassInst: ExampleAdHocCaseClass = ExampleAdHocCaseClass(alienInst, caseClassInst)
  val adHocCaseClassStr: String =
    s"""ExampleAdHocCaseClass(
       |  ExampleAlienClass("p1Val", "p2Val"),
       |  $caseClassStr
       |)""".stripMargin

  case object ExampleAdHocObject extends PrettyPrinting {
    override def pretty: Pretty[this.type] = adHocPrettyInstance
  }

  val adHocObjectInst: ExampleAdHocObject.type = ExampleAdHocObject
  val adHocObjectStr: String = "ExampleAdHocObject"

  "show is pretty" in {
    singletonInst.show shouldBe singletonStr
    alienInst.show shouldBe alienStr
    caseClassInst.show shouldBe caseClassStr
    adHocCaseClassInst.show shouldBe adHocCaseClassStr
    adHocObjectInst.show shouldBe adHocObjectStr
  }

  "show interpolator is pretty" in {
    show"Showing $singletonInst" shouldBe s"Showing $singletonStr"
    show"Showing $alienInst" shouldBe s"Showing $alienStr"
    show"Showing $caseClassInst" shouldBe s"Showing $caseClassStr"
    show"Showing $adHocCaseClassInst" shouldBe s"Showing $adHocCaseClassStr"
    show"Showing $adHocObjectInst" shouldBe s"Showing $adHocObjectStr"
  }

  "toString is pretty" in {
    singletonInst.toString shouldBe singletonStr
    caseClassInst.toString shouldBe caseClassStr
    adHocCaseClassInst.toString shouldBe adHocCaseClassStr
    adHocObjectInst.toString shouldBe adHocObjectStr
  }

  "toString is not pretty" in {
    alienInst.toString shouldBe "ExampleAlienClass(p1Val,p2Val)"
  }

  "fail gracefully on a mock" in {
    val mockedInst = mock[ExampleCaseClass]

    (the[SmartNullPointerException] thrownBy mockedInst.toString).getMessage should
      endWith("exampleCaseClass.pretty();\n")
    (the[SmartNullPointerException] thrownBy mockedInst.show).getMessage should
      endWith("exampleCaseClass.pretty();\n")
    import Pretty.PrettyOps
    (the[SmartNullPointerException] thrownBy mockedInst.toPrettyString()).getMessage should
      endWith("exampleCaseClass.pretty();\n")
  }
}
