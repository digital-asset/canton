// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.testing.modelbased.checker.PropertyCheckerResultAssertions
import com.digitalasset.canton.testing.modelbased.runner.ReferenceInterpreter
import com.digitalasset.canton.testing.modelbased.syntax.Parser
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

    "sammy test" in {
      val scenario = Parser.assertParseScenario("""
          |Scenario
          |  Topology
          |    Participant 1 pkgs={0} parties={1,2}
          |  Ledger
          |    Commands participant=1 actAs={1,2} disclosures=[]
          |      CreateWithKey 0 key=(2, {1}) sigs={1} obs={}
          |      ExerciseByKey Consuming 0 ctl={2} cobs={}
          |        CreateWithKey 1 key=(1, {1,2}) sigs={1,2} obs={}
          |        QueryByKey [1] exhaustive=true
          |""".stripMargin)
      ReferenceInterpreter(loggerFactory).runAndProject(scenario) match {
        case Left(error) =>
          fail(error)
        case Right(projections) =>
          projections should not be empty
      }
    }
  }
}
