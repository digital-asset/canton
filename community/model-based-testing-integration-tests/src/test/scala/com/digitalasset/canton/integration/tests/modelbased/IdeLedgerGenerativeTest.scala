// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.modelbased

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.checker.{
  PropertyChecker,
  PropertyCheckerResultAssertions,
}
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.runner.ReferenceInterpreter
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.Pretty
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.NextGenContractStateMachine as ContractStateMachine
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt

/** Generative test for the IDE ledger: throws many random valid scenarios at the
  * ReferenceInterpreter (which wraps the IDE ledger) and checks that it doesn't crash.
  *
  * Lives in the `modelbased` package so that it is picked up by the `model_based_test` nightly CI
  * job.
  */
class IdeLedgerGenerativeTest
    extends AnyWordSpec
    with BaseTest
    with PropertyCheckerResultAssertions {

  "The IDE ledger" should {
    "not crash on valid scenarios" should {
      List(
        (
          "pv34",
          new ConcreteGenerators(
            languageVersion = LanguageVersion.v2_2,
            readOnlyRollbacks = false,
            keyMode = KeyMode.UniqueContractKeys,
            generateQueryByKey = false,
          ),
          ContractStateMachine.Mode.NoKey,
        ),
        (
          "pv35",
          new ConcreteGenerators(
            languageVersion = LanguageVersion.v2_3,
            readOnlyRollbacks = true,
            keyMode = KeyMode.NonUniqueContractKeys,
            generateQueryByKey = true,
          ),
          ContractStateMachine.Mode.NUCK,
        ),
      ).foreach { case (pv, generators, csmMode) =>
        s"for $pv transactions" in {
          val interpreter = ReferenceInterpreter(loggerFactory, csmMode)

          val generator =
            generators.validScenarioGenerator(
              numParties = 3,
              numPackages = 1,
              numParticipants = 3,
            )

          val result = PropertyChecker
            .checkProperty(
              generate = () => generator.generate(size = 40),
              shrink = Shrinker.shrinkScenario,
              property = (s: Concrete.Scenario) =>
                interpreter.runAndProject(s).flatMap { projections =>
                  if (projections.nonEmpty) Right(())
                  else Left("Expected non-empty projections")
                },
              maxSamples = Int.MaxValue,
              timeout = 5.minutes,
              bufferSize = 1000,
              generatorParallelism = Runtime.getRuntime.availableProcessors(),
            )

          logger.info(result.summary)
          result.assertPassed(Pretty.prettyScenario)
        }
      }
    }
  }
}
