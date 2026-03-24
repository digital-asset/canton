// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased

import com.digitalasset.canton.testing.modelbased.ast.Concrete.Scenario
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.Parser
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValidatorTest extends AnyWordSpec with Matchers {

  private def validScenario(scenario: Scenario): Boolean = {
    val numParties = scenario.topology.flatMap(_.parties).toSet.size
    val maxPackageId =
      scenario.ledger.flatMap(_.commands.flatMap(_.packageId)).maxOption.getOrElse(0)
    SymbolicSolver.valid(scenario, maxPackageId, numParties, KeyMode.NonUniqueContractKeys)
  }

  "the validator" should {
    "validate a scenario where a sub-command retrieves a contract created in another subcommand by key" in {
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

      validScenario(scenario) shouldBe true
    }

    "validate a transaction that creates two contracts with the same key" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |""".stripMargin)

      validScenario(scenario) shouldBe true
    }

    "validate a scenario that creates two contracts with the same key in different transactions" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |""".stripMargin)

      validScenario(scenario) shouldBe true
    }

    "validate a transaction that creates two contracts with the same key then looks up the most recent one" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |      ExerciseByKey NonConsuming 2 ctl={1} cobs={}
        |""".stripMargin)

      validScenario(scenario) shouldBe true
    }

    "reject a transaction that creates two contracts with the same key then looks up the least recent one" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |      ExerciseByKey NonConsuming 1 ctl={1} cobs={}
        |""".stripMargin)

      validScenario(scenario) shouldBe false
    }

    "validate a scenario that creates two contracts with the same key then looks up the most recent active one" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[]
        |      Exercise Consuming 2 ctl={1} cobs={}
        |      ExerciseByKey NonConsuming 1 ctl={1} cobs={}
        |""".stripMargin)

      validScenario(scenario) shouldBe true
    }

    "validate a scenario that exercices by key a disclosure that is not the most recently created contract with that key" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[1]
        |      ExerciseByKey NonConsuming 1 ctl={1} cobs={}
        |""".stripMargin)

      validScenario(scenario) shouldBe true
    }

    "validate a scenario that prefers local contracts to disclosures in exerciseByKey" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[1]
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |      ExerciseByKey NonConsuming 2 ctl={1} cobs={}
        |""".stripMargin)
      validScenario(scenario) shouldBe true
    }

    "reject a scenario that prefers disclosures to local contracts in exerciseByKey" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[1]
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |      ExerciseByKey NonConsuming 1 ctl={1} cobs={}
        |""".stripMargin)
      validScenario(scenario) shouldBe false
    }

    "validate a scenario that performs an exhaustive query by key with local, disclosed and global contracts" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[1]
        |      CreateWithKey 3 key=(0, {1}) sigs={1} obs={}
        |      Exercise NonConsuming 1 ctl={1} cobs={}
        |        QueryByKey [3,1,2] exhaustive=true
        |""".stripMargin)
      validScenario(scenario) shouldBe true
    }

    "reject a scenario that performs an exhaustive but ill-ordered query by key with local, disclosed and global contracts" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[1]
        |      CreateWithKey 3 key=(0, {1}) sigs={1} obs={}
        |      Exercise NonConsuming 1 ctl={1} cobs={}
        |        QueryByKey [3,2,1] exhaustive=true
        |""".stripMargin)
      validScenario(scenario) shouldBe false
    }

    "reject a scenario that declares an exhaustive query by key that isn't exhaustive" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[]
        |      Exercise NonConsuming 1 ctl={1} cobs={}
        |        QueryByKey [2] exhaustive=true
        |""".stripMargin)
      validScenario(scenario) shouldBe false
    }

    "validate a scenario that performs a non-exhaustive query by key with local, disclosed and global contracts" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[1]
        |      CreateWithKey 3 key=(0, {1}) sigs={1} obs={}
        |      Exercise NonConsuming 1 ctl={1} cobs={}
        |        QueryByKey [3,1] exhaustive=false
        |""".stripMargin)
      validScenario(scenario) shouldBe true
    }

    "reject a scenario that performs a non-exhaustive but ill-ordered query by key with local, disclosed and global contracts" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[1]
        |      CreateWithKey 3 key=(0, {1}) sigs={1} obs={}
        |      Exercise NonConsuming 1 ctl={1} cobs={}
        |        QueryByKey [2,3] exhaustive=false
        |""".stripMargin)
      validScenario(scenario) shouldBe false
    }

    "validate a scenario that declares an non-exhaustive query by key that returns all active contracts" in {
      val scenario = Parser.assertParseScenario("""
        |Scenario
        |  Topology
        |    Participant 1 pkgs={0} parties={1}
        |  Ledger
        |    Commands participant=1 actAs={1} disclosures=[]
        |      CreateWithKey 1 key=(0, {1}) sigs={1} obs={}
        |      CreateWithKey 2 key=(0, {1}) sigs={1} obs={}
        |    Commands participant=1 actAs={1} disclosures=[]
        |      Exercise NonConsuming 1 ctl={1} cobs={}
        |        QueryByKey [2,1] exhaustive=false
        |""".stripMargin)
      validScenario(scenario) shouldBe true
    }
  }
}
