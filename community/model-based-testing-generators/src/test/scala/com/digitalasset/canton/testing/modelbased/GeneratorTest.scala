// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.digitalasset.canton.testing.modelbased.generators.ConcreteGenerators
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver
import com.digitalasset.canton.testing.modelbased.syntax.Pretty
import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class GeneratorTestPVDev
    extends GeneratorTest(
      languageVersion = LanguageVersion.v2_dev,
      readOnlyRollbacks = true,
    )

class GeneratorTestPV34
    extends GeneratorTest(
      languageVersion = LanguageVersion.v2_2,
      readOnlyRollbacks = false,
    )

abstract class GeneratorTest(languageVersion: LanguageVersion, readOnlyRollbacks: Boolean)
    extends AnyWordSpec
    with Matchers {

  private val generators = new ConcreteGenerators(languageVersion, readOnlyRollbacks)

  "The symbolic solver" should {
    "synthesize valid scenarios" in {
      val numParties = 3
      val numPackages = 1
      val scenario = generators
        .validScenarioGenerator(numParties, numPackages, numParticipants = 3)
        .generate(size = 50)
      assert(
        SymbolicSolver.valid(scenario, numPackages, numParties),
        Pretty.prettyScenario(scenario),
      )
    }
  }
}
