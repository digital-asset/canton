// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.checker.{
  PropertyChecker,
  PropertyCheckerResultAssertions,
}
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.Pretty
import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GeneratorTestPVDev
    extends GeneratorTest(
      languageVersion = LanguageVersion.v2_dev,
      readOnlyRollbacks = true,
      generateQueryByKey = true,
      keyMode = KeyMode.NonUniqueContractKeys,
    )

class GeneratorTestPV34
    extends GeneratorTest(
      languageVersion = LanguageVersion.v2_2,
      readOnlyRollbacks = false,
      generateQueryByKey = false,
      keyMode = KeyMode.UniqueContractKeys,
    )

abstract class GeneratorTest(
    languageVersion: LanguageVersion,
    readOnlyRollbacks: Boolean,
    generateQueryByKey: Boolean,
    keyMode: KeyMode,
) extends AnyWordSpec
    with Matchers
    with PropertyCheckerResultAssertions {

  private val generators =
    new ConcreteGenerators(languageVersion, readOnlyRollbacks, generateQueryByKey, keyMode)

  "The symbolic solver" should {
    "synthesize valid scenarios" in {
      val numParties = 3
      val numPackages = 1

      val generator =
        generators.validScenarioGenerator(numParties, numPackages, numParticipants = 3)

      PropertyChecker
        .checkProperty(
          generate = () => generator.generate(size = 30),
          shrink = Shrinker.shrinkScenario,
          property = (scenario: Concrete.Scenario) =>
            if (SymbolicSolver.valid(scenario, numPackages, numParties)) Right(())
            else Left("Scenario failed validity check"),
          maxSamples = 10,
        )
        .assertPassed(Pretty.prettyScenario)
    }
  }
}
