// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.syntax.{Parser, Pretty}
import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalacheck.Gen
import org.scalacheck.Prop.forAllShrink
import org.scalactic.Prettifier
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.Checkers

class SyntaxTestPVDev
    extends SyntaxTest(
      languageVersion = LanguageVersion.v2_dev,
      readOnlyRollbacks = true,
    )

class SyntaxTestPV34
    extends SyntaxTest(
      languageVersion = LanguageVersion.v2_2,
      readOnlyRollbacks = false,
    )

abstract class SyntaxTest(languageVersion: LanguageVersion, readOnlyRollbacks: Boolean)
    extends AnyWordSpec
    with Matchers
    with Checkers {

  // Used to pretty-print counter examples. Displays both the pretty version for readability and the raw version in
  // case the bug resides in the pretty-printer.
  private implicit val prettifier: Prettifier = Prettifier.apply {
    case scenario: Concrete.Scenario =>
      s"""|
          |=== toString ===
          |${scenario.toString}
          |=== prettyScenario ===
          |${Pretty.prettyScenario(scenario)}""".stripMargin
    case other =>
      Prettifier.default(other)
  }

  private val generators =
    new ConcreteGenerators(languageVersion, readOnlyRollbacks, generateQueryByKey = true)

  "The parser and the pretty-printer" should {
    "verify the roundtrip property" in {
      val scenarioGenerator = generators.validScenarioGenerator(
        numParties = 3,
        numPackages = 3,
        numParticipants = 3,
      )
      check {
        forAllShrink(
          Gen.delay(Gen.const(scenarioGenerator.generate(size = 30))),
          Shrinker.shrinkScenario.shrink,
        ) { scenario =>
          Parser.parseScenario(Pretty.prettyScenario(scenario)) match {
            case Left(error) => throw new AssertionError(s"Failed to parse scenario: $error")
            case Right(parsed) => parsed == scenario
          }
        }
      }
    }
  }
}
