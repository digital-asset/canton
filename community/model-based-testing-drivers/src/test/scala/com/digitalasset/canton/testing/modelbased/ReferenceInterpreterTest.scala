// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.checker.{
  PropertyChecker,
  PropertyCheckerResultAssertions,
}
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.runner.ReferenceInterpreter
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.{Parser, Pretty}
import com.digitalasset.daml.lf.language.LanguageVersion
import org.scalatest.wordspec.AnyWordSpec

class ReferenceInterpreterTest
    extends AnyWordSpec
    with BaseTest
    with PropertyCheckerResultAssertions {

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

      ReferenceInterpreter(loggerFactory).runAndProject(scenario) match {
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

      ReferenceInterpreter(loggerFactory).runAndProject(scenario) match {
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

      ReferenceInterpreter(loggerFactory).runAndProject(scenario) match {
        case Left(error) =>
          fail(error)
        case Right(projections) =>
          projections should not be empty
      }
    }

    "run a generated scenario and produce projections" should {
      List(
        "pv34" ->
          new ConcreteGenerators(
            languageVersion = LanguageVersion.v2_2,
            readOnlyRollbacks = false,
            keyMode = KeyMode.UniqueContractKeys,
            generateQueryByKey = false,
          ),
        "pvdev" ->
          new ConcreteGenerators(
            languageVersion = LanguageVersion.v2_dev,
            readOnlyRollbacks = true,
            // TODO(#30398): change to NUCK once NUCK state machine is implemented and plugged
            keyMode = KeyMode.UniqueContractKeys,
            // TODO(#30398): change to true once QueryNByKey is fully supported in the engine
            generateQueryByKey = false,
          ),
      ).foreach { case (pv, generators) =>
        s"for $pv transactions" in {
          val interpreter = ReferenceInterpreter(loggerFactory)

          val generator =
            generators.validScenarioGenerator(
              numParties = 3,
              numPackages = 1,
              numParticipants = 3,
            )

          PropertyChecker
            .checkProperty(
              generate = () => generator.generate(size = 30),
              shrink = Shrinker.shrinkScenario,
              property = (s: Concrete.Scenario) =>
                interpreter.runAndProject(s).flatMap { projections =>
                  if (projections.nonEmpty) Right(())
                  else Left("Expected non-empty projections")
                },
              maxSamples = 20,
            )
            .assertPassed(Pretty.prettyScenario)
        }
      }
    }
  }
}
