// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.checker.{
  PropertyChecker,
  PropertyCheckerResultAssertions,
}
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.syntax.{Parser, Pretty}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SyntaxTestPVDev
    extends SyntaxTest(
      contractKeys = true,
      readOnlyRollbacks = true,
    )

class SyntaxTestPV34
    extends SyntaxTest(
      contractKeys = false,
      readOnlyRollbacks = false,
    )

abstract class SyntaxTest(contractKeys: Boolean, readOnlyRollbacks: Boolean)
    extends AnyWordSpec
    with Matchers
    with PropertyCheckerResultAssertions {

  private val generators =
    new ConcreteGenerators(contractKeys, readOnlyRollbacks)

  "The parser and the pretty-printer" should {
    "verify the roundtrip property" in {
      val scenarioGenerator = generators.validScenarioGenerator(
        numParties = 3,
        numPackages = 3,
        numParticipants = 3,
      )

      PropertyChecker
        .checkProperty(
          generate = () => scenarioGenerator.generate(size = 20),
          shrink = Shrinker.shrinkScenario,
          property = (scenario: Concrete.Scenario) =>
            Parser.parseScenario(Pretty.prettyScenario(scenario)) match {
              case Left(error) => Left(s"Failed to parse scenario: $error")
              case Right(parsed) =>
                if (parsed == scenario) Right(())
                else Left("Roundtrip failed: parsed scenario differs from original")
            },
          maxSamples = 50,
        )
        .assertPassed(Pretty.prettyScenario)
    }
  }
}
