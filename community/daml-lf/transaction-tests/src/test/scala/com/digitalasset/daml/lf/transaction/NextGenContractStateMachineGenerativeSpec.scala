// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.canton.testing.modelbased.ast.Concrete
import com.digitalasset.canton.testing.modelbased.ast.Implicits.*
import com.digitalasset.canton.testing.modelbased.checker.{
  PropertyChecker,
  PropertyCheckerResultAssertions,
}
import com.digitalasset.canton.testing.modelbased.generators.{ConcreteGenerators, Shrinker}
import com.digitalasset.canton.testing.modelbased.solver.SymbolicSolver.KeyMode
import com.digitalasset.canton.testing.modelbased.syntax.{Parser, Pretty}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.{
  defaultPackageId,
  toIdentifier,
}
import com.digitalasset.daml.lf.value.Value as V
import org.scalatest.LoneElement
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Try

// This test runs on every PR and therefore generates a small number of small samples in order to keep the runtime
// reasonable. For more extensive testing, see
// com.digitalasset.canton.integration.tests.modelbased.NextGenContractStateMachineGenerativeSpecLarge which runs
// nightly and generates a larger number of larger samples.
class NextGenContractStateMachineGenerativeSpecSmall
    extends NextGenContractStateMachineGenerativeSpec(
      sampleSize = 20,
      maxSamples = 200,
    )

/** Abstract base class for generative testing of the contract state machine. Subclasses control the
  * generation parameters (size, sample count, parallelism, etc.).
  */
abstract class NextGenContractStateMachineGenerativeSpec(
    sampleSize: Int,
    maxSamples: Int,
    timeout: FiniteDuration = 365.days,
    sampleBufferSize: Int = 100,
    generatorParallelism: Int = 1,
) extends AnyWordSpec
    with LoneElement
    with Matchers
    with PropertyCheckerResultAssertions {

  import NextGenContractStateMachineGenerativeSpec.*

  private def verifyQueriesMatchContractOrder(
      scenario: Concrete.Scenario,
      state: NextGenContractStateMachine.LLState,
  ): Either[String, Unit] =
    Try {
      val tx = scenario.ledger(1).commands.map(_.action)
      val contractOrder = state.contractOrder

      // assert that contractOrder has unique elements only, then convert to set
      val contractOrderSet: Set[V.ContractId] = contractOrder.toSet
      contractOrderSet should have size contractOrder.size.toLong

      // assert that contractOrder contains exactly the contract ids in the scenario
      val scenarioContractIds: Set[V.ContractId] = tx.flatMap(collectIds).toSet
      contractOrderSet shouldEqual scenarioContractIds

      // assert all queries have the order as specified by the contractOrder.
      tx.queryByKeyNodes.foreach { case Concrete.QueryByKey(contractIds, _, _, _) =>
        val queryResult = contractIds.map(toContractId)
        val queryResultSet = queryResult.toSet
        val filteredOrder = contractOrder.filter(queryResultSet)
        withClue(
          s"QueryByKey result order does not match contractOrder:\n  queryResult=$queryResult\n  contractOrder=$contractOrder\n"
        ) {
          filteredOrder shouldBe queryResult
        }
      }
    }.toEither.left.map(_.getMessage).map(_ => ())

  "the contract state machine" should {
    "accept a valid scenario" in {
      val scenario = Parser.assertParseScenario("""
          |Scenario
          |  Topology
          |    Participant 0 pkgs={0} parties={1}
          |  Ledger
          |    Commands participant=0 actAs={1} disclosures=[]
          |      CreateWithKey 0 key=(1, {1}) sigs={1} obs={}
          |      CreateWithKey 1 key=(1, {1}) sigs={1} obs={}
          |    Commands participant=0 actAs={1} disclosures=[0]
          |      Exercise NonConsuming 0 ctl={1} cobs={}
          |        QueryByKey [0] exhaustive=true
          |
          |""".stripMargin)
      processScenario(scenario) shouldBe a[Right[?, ?]]
    }

    "accept random valid scenarios" in {
      val generator = generators.validScenarioGenerator(
        numParties = 1,
        numPackages = 1,
        numParticipants = 1,
        numCommands = Some(2),
      )

      PropertyChecker
        .checkProperty(
          generate = () => generator.generate(size = sampleSize, distinctKeyToContractRatio = 0.4),
          shrink = Shrinker.shrinkScenario.suchThat(_.ledger.size == 2),
          property = processScenario(_, stateProp = verifyQueriesMatchContractOrder),
          maxSamples = maxSamples,
          timeout = timeout,
          bufferSize = sampleBufferSize,
          generatorParallelism = generatorParallelism,
        )
        .assertPassed(Pretty.prettyScenario)
    }
  }
}

object NextGenContractStateMachineGenerativeSpec {


  // TODO (#31844) The framework should pass the actual template ID
  private val dummyTmplId: Ref.TypeConId = Ref.TypeConId.assertFromString("-dummy-:Mod:T")

  private val generators =
    new ConcreteGenerators(
      LanguageVersion.v2_dev,
      readOnlyRollbacks = true,
      generateQueryByKey = true,
      keyMode = KeyMode.NonUniqueContractKeys,
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

  /** Collect a mapping from contract ID to key ID from all CreateWithKey nodes in a scenario. */
  private def collectKeys(scenario: Concrete.Scenario): Map[Concrete.ContractId, Concrete.KeyId] = {

    def fromTransaction(tx: Concrete.Transaction): Map[Concrete.ContractId, Concrete.KeyId] =
      tx.foldLeft(Map.empty[Concrete.ContractId, Concrete.KeyId]) { (acc, action) =>
        acc ++ fromAction(action)
      }

    def fromAction(action: Concrete.Action): Map[Concrete.ContractId, Concrete.KeyId] =
      action match {
        case Concrete.CreateWithKey(contractId, keyId, _, _, _) =>
          Map(contractId -> keyId)
        case Concrete.Exercise(_, _, _, _, subTransaction) =>
          fromTransaction(subTransaction)
        case Concrete.ExerciseByKey(_, _, _, _, _, _, subTransaction) =>
          fromTransaction(subTransaction)
        case Concrete.Rollback(subTransaction) =>
          fromTransaction(subTransaction)
        case _ => Map.empty
      }

    scenario.ledger
      .flatMap(_.actions.map(fromAction))
      .foldLeft(Map.empty[Concrete.ContractId, Concrete.KeyId])(_ ++ _)
  }

  private def collectIds(action: Concrete.Action): Set[V.ContractId] =
    action match {
      case Concrete.Create(contractId, _, _) => Set(toContractId(contractId))
      case Concrete.CreateWithKey(contractId, _, _, _, _) => Set(toContractId(contractId))
      case Concrete.Exercise(_, contractId, _, _, subTransaction) =>
        Set(toContractId(contractId)) ++ subTransaction.flatMap(collectIds)
      case Concrete.ExerciseByKey(_, contractId, _, _, _, _, subTransaction) =>
        Set(toContractId(contractId)) ++ subTransaction.flatMap(collectIds)
      case Concrete.Fetch(contractId) => Set(toContractId(contractId))
      case Concrete.FetchByKey(contractId, _, _) => Set(toContractId(contractId))
      case Concrete.LookupByKey(Some(contractId), _, _) => Set(toContractId(contractId))
      case Concrete.LookupByKey(None, _, _) => Set.empty
      case Concrete.QueryByKey(contractIds, _, _, _) => contractIds.map(toContractId).toSet
      case Concrete.Rollback(subTransaction) => subTransaction.flatMap(collectIds).toSet
    }

  private class TransactionProcessor(keyMap: Map[Concrete.ContractId, Concrete.KeyId]) {
    private var nodeCounter: Int = 0
    private def freshNodeId(): NodeId = {
      val nid = nodeCounter
      nodeCounter += 1
      NodeId(nid)
    }

    private def keyOf(contractId: Concrete.ContractId): Option[GlobalKey] =
      keyMap.get(contractId).map(toGlobalKey)

    def processAction(
        state: NextGenContractStateMachine.LLState,
        action: Concrete.Action,
    ): Either[TransactionError, NextGenContractStateMachine.LLState] = {
      import NextGenContractStateMachine.HHState
      action match {
        case Concrete.Create(contractId, _, _) =>
          state.visitCreate(
            nid = freshNodeId(),
            contractId = toContractId(contractId),
            mbKey = None,
          )

        case Concrete.CreateWithKey(contractId, keyId, _, _, _) =>
          state.visitCreate(
            nid = freshNodeId(),
            contractId = toContractId(contractId),
            mbKey = Some(toGlobalKey(keyId)),
          )

        case Concrete.Exercise(kind, contractId, _, _, subTransaction) =>
          for {
            s <- state.visitExercise(
              nodeId = freshNodeId(),
              tmplId = dummyTmplId,
              targetId = toContractId(contractId),
              mbKey = keyOf(contractId),
              byKey = false,
              consuming = kind == Concrete.Consuming,
            )
            s <- processTransaction(s, subTransaction)
          } yield s

        case Concrete.ExerciseByKey(kind, contractId, keyId, _, _, _, subTransaction) =>
          for {
            s <- state.visitExercise(
              nodeId = freshNodeId(),
              tmplId = dummyTmplId,
              targetId = toContractId(contractId),
              mbKey = Some(toGlobalKey(keyId)),
              byKey = true,
              consuming = kind == Concrete.Consuming,
            )
            s <- processTransaction(s, subTransaction)
          } yield s

        case Concrete.Fetch(contractId) =>
          state.visitFetch(
            tmplId = dummyTmplId,
            contractId = toContractId(contractId),
            mbKey = keyOf(contractId),
            byKey = false,
          )

        case Concrete.FetchByKey(contractId, keyId, _) =>
          state.visitFetch(
            tmplId = dummyTmplId,
            contractId = toContractId(contractId),
            mbKey = Some(toGlobalKey(keyId)),
            byKey = true,
          )

        case Concrete.LookupByKey(contractIdOpt, keyId, _) =>
          contractIdOpt match {
            case Some(contractId) =>
              state.visitQueryByKey(
                key = toGlobalKey(keyId),
                result = Vector(toContractId(contractId)),
                exhaustive = false,
              )
            case None =>
              state.visitQueryByKey(
                key = toGlobalKey(keyId),
                result = Vector.empty,
                exhaustive = true,
              )
          }

        case Concrete.QueryByKey(contractIds, keyId, _, exhaustive) =>
          state.visitQueryByKey(
            key = toGlobalKey(keyId),
            result = contractIds.map(toContractId).toVector,
            exhaustive,
          )

        case Concrete.Rollback(subTransaction) =>
          val s = state.beginRollback
          processTransaction(s, subTransaction).flatMap(x =>
            handleViaEmptyEffectfulRollback(x.endRollback)
          )
      }
    }

    def processTransaction(
        state: NextGenContractStateMachine.LLState,
        tx: Concrete.Transaction,
    ): Either[TransactionError, NextGenContractStateMachine.LLState] =
      tx.foldLeft[Either[TransactionError, NextGenContractStateMachine.LLState]](
        Right(state)
      ) {
        case (Right(s), action) => processAction(s, action)
        case (left, _) => left
      }
  }

  private def processScenario(
      scenario: Concrete.Scenario,
      stateProp: (Concrete.Scenario, NextGenContractStateMachine.LLState) => Either[String, Unit] =
        (_, _) => Right(()),
  ): Either[String, Unit] = {
    val keyMap = collectKeys(scenario)
    new TransactionProcessor(keyMap)
      .processTransaction(
        NextGenContractStateMachine.empty(authorizeRollBack = false),
        scenario.ledger(1).commands.map(_.action),
      )
      .left
      .map(_.toString)
      .flatMap(s => stateProp(scenario, s))
  }

  // TODO(#31454)
  private def handleViaEmptyEffectfulRollback[A](
      eOrA: Either[Set[NodeId], A]
  ): Either[TransactionError, A] =
    eOrA.left.map(_ => TransactionError.EffectfulRollback(Set()))

}
