// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.{Parser, Pretty}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId, toIdentifier}
import com.digitalasset.daml.lf.value.{Value => V}
import org.scalatest.{AppendedClues, LoneElement}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec


class NextGenContractStateMachineGenerativeSpec
  extends AnyWordSpec
    with LoneElement
    with Matchers
    with AppendedClues {

  import NextGenContractStateMachineGenerativeSpec._

  "the contract state machine" should {
    "accept a valid scenario" in {
      val scenario = Parser.assertParseScenario(
        """
          |Scenario
          |  Topology
          |    Participant 0 pkgs={0} parties={1}
          |  Ledger
          |    Commands participant=0 actAs={1} disclosures=[]
          |      CreateWithKey 0 pkg=0 key=(5, {1}) sigs={1} obs={1}
          |      CreateWithKey 1 pkg=0 key=(4, {1}) sigs={1} obs={}
          |      ExerciseByKey Consuming 1 ctl={1} cobs={}
          |        CreateWithKey 2 key=(3, {1}) sigs={1} obs={}
          |        Fetch 2
          |        ExerciseByKey Consuming 0 ctl={1} cobs={1}
          |          Exercise NonConsuming 2 ctl={1} cobs={1}
          |            CreateWithKey 3 key=(1, {1}) sigs={1} obs={}
          |          FetchByKey 2
          |          Fetch 2
          |          ExerciseByKey NonConsuming 2 ctl={1} cobs={}
          |            CreateWithKey 4 key=(2, {1}) sigs={1} obs={}
          |            LookupByKey Success 2
          |          LookupByKey Success 3
          |        Exercise NonConsuming 2 ctl={1} cobs={}
          |          LookupByKey Success 2
          |          Fetch 4""".stripMargin)

      processScenario(scenario) shouldBe a[Right[_, _]]
    }

    "accept random valid scenarios" in {
      val numSamples = 100
      val size = 20
      for (_ <- 1 to numSamples) {
        val scenario = generators
          .validScenarioGenerator(
            numParties = 1,
            numPackages = 1,
            numParticipants = 1,
            numCommands = Some(1),
          )
          .generate(size)
        withClue(s"Original scenario ${Pretty.prettyScenario(scenario)}") {
          processScenario(scenario) match {
            case Left(error) =>
              val shrinkResult =
                Shrinker.shrinkToFailure(scenario, error, processScenario)(Shrinker.shrinkScenario)
              withClue(s"Shrunk scenario:\n${Pretty.prettyScenario(shrinkResult.value)}") {
                fail(shrinkResult.error)
              }
            case Right(_) => succeed
          }
        }
      }
    }
  }
}

object NextGenContractStateMachineGenerativeSpec {

  private val generators =
    new ConcreteGenerators(
      LanguageVersion.v2_dev,
      readOnlyRollbacks = true,
      generateQueryByKey = true,
      keyMode = KeyMode.NonUniqueContractKeys
    )

  private val pkgName: Ref.PackageName = Ref.PackageName.assertFromString("test-package")
  private val templateId: Ref.TypeConId = "Mod:T"

  private def toContractId(cid: Concrete.ContractId): V.ContractId = {
    val l = crypto.Hash.underlyingHashLength
    val bytes = Array.ofDim[Byte](l)
    bytes(l - 4) = (cid >> 24).toByte
    bytes(l - 3) = (cid >> 16).toByte
    bytes(l - 2) = (cid >> 8).toByte
    bytes(l - 1) = cid.toByte
    val hash = crypto.Hash.assertFromByteArray(bytes)
    V.ContractId.V1(hash)
  }

  private def toGlobalKey(keyId: Concrete.KeyId): GlobalKey =
    GlobalKey.assertBuild(
      templateId,
      pkgName,
      V.ValueInt64(keyId.toLong),
      Hash.hashPrivateKey(s"key-$keyId"),
    )

  private class TransactionProcessor {
    private var nodeCounter: Int = 0
    private def freshNodeId(): Int = {
      val nid = nodeCounter
      nodeCounter += 1
      nid
    }

    def processAction(
        state: NextGenContractStateMachine.LLState[Int],
        action: Concrete.Action,
    ): ErrOr[NextGenContractStateMachine.LLState[Int]] = {
      import NextGenContractStateMachine.HHState
      action match {
        case Concrete.Create(contractId, _, _) =>
          state.visitCreate(
            contractId = toContractId(contractId),
            mbKey = None)

        case Concrete.CreateWithKey(contractId, keyId, _, _, _) =>
          state.visitCreate(contractId = toContractId(contractId), mbKey = Some(toGlobalKey(keyId)))

        case Concrete.Exercise(kind, contractId, _, _, subTransaction) =>
          for {
            s <- state.visitExercise(
              nodeId = freshNodeId(),
              targetId = toContractId(contractId),
              mbKey = None,
              byKey = false,
              consuming = kind == Concrete.Consuming,
            )
            s <- processTransaction(s, subTransaction)
          } yield s

        case Concrete.ExerciseByKey(kind, contractId, keyId, _, _, _, subTransaction) =>
          for {
            s <- state.visitExercise(
              nodeId = freshNodeId(),
              targetId = toContractId(contractId),
              mbKey = Some(toGlobalKey(keyId)),
              byKey = true,
              consuming = kind == Concrete.Consuming,
            )
            s <- processTransaction(s, subTransaction)
          } yield s

        case Concrete.Fetch(contractId) =>
          state.visitFetch(
            contractId = toContractId(contractId),
            mbKey = None,
            byKey = false)

        case Concrete.FetchByKey(contractId, keyId, _) =>
          state.visitFetch(
            contractId = toContractId(contractId),
            mbKey = Some(toGlobalKey(keyId)),
            byKey = true)

        case Concrete.LookupByKey(contractIdOpt, keyId, _) =>
          contractIdOpt match {
            case Some(contractId) =>
              state.visitQueryByKey(
                gk = toGlobalKey(keyId),
                result = Vector(toContractId(contractId)),
                exhaustive = false,
              )
            case None =>
              state.visitQueryByKey(
                gk = toGlobalKey(keyId),
                result = Vector.empty,
                exhaustive = true)
          }

        case Concrete.QueryByKey(contractIds, keyId, _, exhaustive) =>
        state.visitQueryByKey(
          gk = toGlobalKey(keyId),
          result = contractIds.map(toContractId).toVector,
          exhaustive)

        case Concrete.Rollback(subTransaction) =>
          val s = state.beginRollback
          processTransaction(s, subTransaction).flatMap(_.endRollback)
      }
    }

    def processTransaction(
        state: NextGenContractStateMachine.LLState[Int],
        tx: Concrete.Transaction,
    ): ErrOr[NextGenContractStateMachine.LLState[Int]] =
      tx.foldLeft[ErrOr[NextGenContractStateMachine.LLState[Int]]](Right(state)) {
        case (Right(s), action) => processAction(s, action)
        case (left, _) => left
      }
  }

  private def processScenario(
      scenario: Concrete.Scenario
  ): Either[String, Unit] =
    new TransactionProcessor()
      .processTransaction(
        NextGenContractStateMachine.empty[Int](authorizeRollBack = false),
        scenario.ledger.headOption.map(_.commands.map(_.action)).getOrElse(List.empty),
      )
      .left
      .map(_.toString)
      .map(_ => ())
}
