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
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.ValidityResult.*
import com.digitalasset.canton.testing.modelbased.syntax.Pretty
import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger

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
      val maxSamples = 20

      val generator =
        generators.validScenarioGenerator(numParties, numPackages, numParticipants = 3)

      // Track how many samples the validator couldn't decide (z3 returned unknown/timeout).
      // We tolerate some unknowns but fail if more than half of the samples are inconclusive.
      val unknownCount = new AtomicInteger(0)

      val result = PropertyChecker
        .checkProperty(
          generate = () => generator.generate(size = 30),
          shrink = Shrinker.shrinkScenario,
          property = (scenario: Concrete.Scenario) =>
            SymbolicSolver.valid(scenario, numPackages, numParties) match {
              case Valid => Right(())
              case Invalid => Left("Expected Valid, got Invalid")
              // Unknown results (z3 timeout or inconclusive) are not treated as failures:
              // we can't shrink them meaningfully. We just count them and check below.
              case Unknown =>
                unknownCount.incrementAndGet()
                Right(())
            },
          maxSamples = maxSamples,
        )

      result.assertPassed(Pretty.prettyScenario)

      assert(
        unknownCount.get() <= maxSamples / 2,
        s"Too many inconclusive (unknown) results from z3: ${unknownCount.get()} out of $maxSamples. Check for possible regression.",
      )
    }
  }
}
