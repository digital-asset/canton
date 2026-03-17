// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.daml.logging.LoggingContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.runner.ReferenceInterpreter
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.{Parser, Pretty}
import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.wordspec.AnyWordSpec

class ReferenceInterpreterTest extends AnyWordSpec with BaseTest {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "The reference interpreter" should {
    "run a scenario with two toplevel create commands" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      Create 0 sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[]
        |      Create 1 sigs={1} obs={}
        |""".stripMargin)

      ReferenceInterpreter().runAndProject(scenario) match {
        case Left(error) =>
          fail(error)
        case Right(projections) =>
          projections should not be empty
      }
    }

    "run a scenario where a sub-command retrieves a contract created in another subcommand by key" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      Create 0 sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      Exercise NonConsuming 0 ctl={1} cobs={}
        |        FetchByKey 1
        |        LookupByKey Success 1
        |        ExerciseByKey NonConsuming 1 ctl={1} cobs={}
        |""".stripMargin)

      ReferenceInterpreter().runAndProject(scenario) match {
        case Left(error) =>
          fail(error)
        case Right(projections) =>
          projections should not be empty
      }
    }

    // TODO(i30398): move non-regression tests like this to another place than the ReferenceInterpreterTest, which
    //  should ideally only contain tests for the interpreter itself and not for the IDE ledger.
    "run a scenario with a negative lookup" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      Create 0 sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[]
        |      Exercise NonConsuming 0 ctl={1} cobs={}
        |        LookupByKey Failure key=(0, {1})
        |""".stripMargin)

      ReferenceInterpreter().runAndProject(scenario) match {
        case Left(error) =>
          fail(error)
        case Right(projections) =>
          projections should not be empty
      }
    }

    "run a generated scenario and produce projections" should {
      List((LanguageVersion.v2_dev, true), (LanguageVersion.v2_2, false)).foreach {
        case (languageVersion, readOnlyRollbacks) =>
          s"with LF version $languageVersion and readOnlyRollbacks=$readOnlyRollbacks" in {
            val interpreter = ReferenceInterpreter()
            val generators = new ConcreteGenerators(
              languageVersion = languageVersion,
              readOnlyRollbacks = readOnlyRollbacks,
              // TODO(#30398): change to NUCK once NUCK state machine is implemented and plugged
              keyMode = KeyMode.UniqueContractKeys,
              // TODO(#30398): change to true once QueryNByKey is fully supported in the engine
              generateQueryByKey = false,
            )
            val scenario = generators
              .validScenarioGenerator(numParties = 3, numPackages = 1, numParticipants = 3)
              .generate(size = 50)
            logger.info(s"Running scenario:\n${Pretty.prettyScenario(scenario)}")

            interpreter.runAndProject(scenario) match {
              case Left(error) =>
                logger.error(s"Interpreter error: $error")
                logger.error("Shrinking scenario...")
                val shrinkResult =
                  Shrinker.shrinkToFailure[Concrete.Scenario](
                    scenario,
                    error,
                    s => interpreter.runAndProject(s).map(_ => ()),
                  )(Shrinker.shrinkScenario)
                logger.error(shrinkResult.summary)
                logger.error(s"Shrunk scenario:\n${Pretty.prettyScenario(shrinkResult.value)}")
                logger.error(s"Original scenario:\n${Pretty.prettyScenario(scenario)}")
                fail(shrinkResult.error)
              case Right(projections) =>
                projections should not be empty
            }
          }
      }
    }
  }
}
