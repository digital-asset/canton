// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.interpretation.{Error => IError}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.speedy.Speedy.ContractInfo
import com.digitalasset.daml.lf.transaction.{
  NextGenContractStateMachine => ContractStateMachine,
  Node,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq

class RollBackNodesInPartialTransactionSpec extends AnyWordSpec with Matchers with Inside {

  private[this] val txVersion = SerializationVersion.maxVersion
  private[this] val contractIdVersion = ContractIdVersion.V2

  private[this] val transactionSeed = crypto.Hash.hashPrivateKey("PartialTransactionSpec")
  private[this] val pkgName = data.Ref.PackageName.assertFromString("-package-name-")
  private[this] val templateId = data.Ref.Identifier.assertFromString("pkg:Mod:Template")
  private[this] val choiceId = data.Ref.Name.assertFromString("choice")
  private[this] val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("My contract"))
  private[this] val party = data.Ref.Party.assertFromString("Alice")
  private[this] val committers: Set[data.Ref.Party] = Set(party)

  private[this] val initialStateRollbacksAllowed = PartialTransaction.initial(
    ContractStateMachine.Mode.NoKey,
    InitialSeeding.TransactionSeed(transactionSeed),
    committers,
  )

  private[this] def contractIdsInOrder(ptx: PartialTransaction): List[Value.ContractId] = {
    ptx.finish.toOption.get._1
      .fold(List.empty[Value.ContractId]) {
        case (acc, (_, create: Node.Create)) => acc :+ create.coid
        case (acc, _) => acc
      }
  }

  private[this] implicit class PartialTransactionExtra(val ptx: PartialTransaction) {

    val contract = ContractInfo(
      version = txVersion,
      packageName = pkgName,
      templateId = templateId,
      value = SValue.SRecord(templateId, ImmArray.empty, ArraySeq.empty),
      signatories = Set(party),
      observers = Set.empty,
      keyOpt = None,
    )

    def insertFetch_(cid: Value.ContractId): PartialTransaction =
      ptx
        .insertFetch(
          coid = cid,
          contract = contract,
          optLocation = None,
          byKey = false,
          version = txVersion,
          interfaceId = None,
        ) match {
        case Left(e) => throw new RuntimeException(s"$e")
        case Right(r) => r
      }

    def insertCreateWithContractId_ : (Value.ContractId, PartialTransaction) =
      ptx
        .insertCreate(
          preparationTime = data.Time.Timestamp.Epoch,
          contract = contract,
          optLocation = None,
          contractIdVersion = contractIdVersion,
        ) match {
        case Left(e) => throw new RuntimeException(s"$e")
        case Right(r) => r
      }

    def insertCreate_ : PartialTransaction =
      insertCreateWithContractId_._2

    def beginExercises_(consuming: Boolean): PartialTransaction =
      ptx
        .beginExercises(
          packageName = pkgName,
          templateId = contract.templateId,
          targetId = cid,
          contract = contract,
          interfaceId = None,
          choiceId = choiceId,
          optLocation = None,
          consuming = consuming,
          actingParties = Set(party),
          choiceObservers = Set.empty,
          choiceAuthorizers = None,
          byKey = false,
          chosenValue = Value.ValueUnit,
          version = txVersion,
        )
        .toOption
        .get

    def beginConsumingExercises_ : PartialTransaction = beginExercises_(consuming = true)

    def beginNonConsumingExercises_ : PartialTransaction = beginExercises_(consuming = false)

    def endExercises_ : PartialTransaction =
      ptx.endExercises(Value.ValueNone)

    def rollbackTry_ : PartialTransaction =
      ptx.rollbackTry match {
        case Left(err) => throw new IErrorThrowable(err)
        case Right(ptx) => ptx
      }
  }

  case class IErrorThrowable(err: IError) extends Throwable

  private[this] val outputCidsEffectful =
    contractIdsInOrder(
      initialStateRollbacksAllowed //
        .insertCreate_ // create the contract cid_0
        .beginConsumingExercises_ // open an exercise context
        .insertCreate_ // create the contract cid_1_0
        .insertCreate_ // create the contract cid_1_2
        .insertCreate_ // create the contract cid_1_3
        .endExercises_ // close the exercise context normally
        .insertCreate_ // create the contract cid_2
    )

  val Seq(cid_0, cid_1_0, cid_1_1, cid_1_2, cid_2) = outputCidsEffectful

  "try context (effectful rollbacks allowed)" should {
    "be without effect when closed without exception" in {
      def run1 = contractIdsInOrder(
        initialStateRollbacksAllowed //
          .insertCreate_ // create the contract cid_0
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .beginTry // open a try context
          .insertCreate_ // create the contract cid_1_1
          .endTry // close the try context
          .insertCreate_ // create the contract cid_1_2
          .endExercises_ // close the exercise context normally
          .insertCreate_ // create the contract cid_2
      )

      def run2 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateRollbacksAllowed //
          .insertCreate_ // create the contract cid_0
          .beginTry // open a try context
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_2
          .insertCreate_ // create the contract cid_1_3
          .endExercises_ // close the exercise context normally
          .endTry // close the try context
          .insertCreate_ // create the contract cid_2
      )

      run1 shouldBe outputCidsEffectful
      run2 shouldBe outputCidsEffectful

    }

    "rollback the current transaction without resetting seed counter for contract IDs" in {
      def run1 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateRollbacksAllowed //
          .insertCreate_ // create the contract cid_0
          .beginTry // open a first try context
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_1
          .insertCreate_ // create the contract cid_1_2
          // an exception is thrown
          .abortExercises // close abruptly the exercise due to an uncaught exception
          .rollbackTry_ // the try context handles the exception
          .insertCreate_ // create the contract cid_2
      )

      def run2 = contractIdsInOrder(
        initialStateRollbacksAllowed //
          .insertCreate_ // create the contract cid_0
          .beginTry // open a first try context
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_1
          .beginTry // open a second try context
          .insertCreate_ // create the contract cid_1_2
          // an exception is thrown
          .rollbackTry_ // the second try context does not handle the exception
          .abortExercises // close abruptly the exercise due to an uncaught exception
          .rollbackTry_ // the first try context does handle the exception
          .insertCreate_ // create the contract cid_2
      )

      def run3 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateRollbacksAllowed //
          .insertCreate_ // create the contract cid_0
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .beginTry // open a try context
          .insertCreate_ // create the contract cid_1_2
          .rollbackTry_ // the try  context does handle the exception
          .insertCreate_ // create the contract cid_1_3
          .endExercises_ // close the exercise context normally
          .insertCreate_ // create the contract cid_2
      )

      run1 shouldBe outputCidsEffectful
      run2 shouldBe outputCidsEffectful
      run3 shouldBe outputCidsEffectful
    }
  }

  private[this] val initialStateEffectfulRollbacksDisallowed = PartialTransaction.initial(
    ContractStateMachine.Mode.NUCK,
    InitialSeeding.TransactionSeed(transactionSeed),
    committers,
  )

  private[this] val outputCidsWithPure = {
    contractIdsInOrder {
      val (cid, txIntermediate) =
        initialStateEffectfulRollbacksDisallowed //
          .insertCreate_ // create the contract cid_0
          .beginConsumingExercises_ // open an exercise context
          .insertCreateWithContractId_ // create the contract cid_1_0
      txIntermediate.insertCreate_ // create the contract cid_1_2
        .insertFetch_(cid) // fetch
        .insertCreate_ // create the contract cid_1_3
        .endExercises_ // close the exercise context normally
        .insertCreate_ // create the contract cid_2
    }
  }

  val Seq(_, with_pure_cid_1_0, _, _, _) = outputCidsWithPure

  "try context (effectful rollbacks disallowed)" should {
    "be without effect when closed without exception" in {
      // No issues from one create in a try that didn't roll back
      def run1 = contractIdsInOrder(
        initialStateEffectfulRollbacksDisallowed //
          .insertCreate_ // create the contract cid_0
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .beginTry // open a try context
          .insertCreate_ // create the contract cid_1_1
          .endTry // close the try context
          .insertFetch_(with_pure_cid_1_0) // fetch a contract
          .insertCreate_ // create the contract cid_1_2
          .endExercises_ // close the exercise context normally
          .insertCreate_ // create the contract cid_2
      )

      // No issues from consuming exercises, a fetch, and a create in a try that didn't roll back
      def run2 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateEffectfulRollbacksDisallowed //
          .insertCreate_ // create the contract cid_0
          .beginTry // open a try context
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_2
          .insertFetch_(with_pure_cid_1_0) // fetch a contract
          .insertCreate_ // create the contract cid_1_3
          .endExercises_ // close the exercise context normally
          .endTry // close the try context
          .insertCreate_ // create the contract cid_2
      )

      run1 shouldBe outputCidsWithPure
      run2 shouldBe outputCidsWithPure
    }

    "rollback the current transaction without resetting seed counter for contract IDs" in {
      // No issues from rolling back a fetch
      def run1 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateEffectfulRollbacksDisallowed //
          .insertCreate_ // create the contract cid_0
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_2
          .beginTry //
          .insertFetch_(with_pure_cid_1_0) // fetch a contract
          .rollbackTry_ //
          .insertCreate_ // create the contract cid_1_3
          .endExercises_ // close the exercise context normally
          .insertCreate_ // create the contract cid_2
      )

      run1 shouldBe outputCidsWithPure

      // No issues from rolling back a nonconsuming exercise
      def run2 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateEffectfulRollbacksDisallowed //
          .insertCreate_ // create the contract cid_0
          .beginTry //
          .beginNonConsumingExercises_ // open an exercise context
          .insertFetch_(with_pure_cid_1_0) // fetch a contract
          .endExercises_ // close the exercise context normally
          .rollbackTry_ //
          .insertCreate_ // create the contract cid_2
      )

      run2 shouldBe contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateEffectfulRollbacksDisallowed //
          .insertCreate_ // create the contract cid_0
          .beginNonConsumingExercises_ // open an exercise context
          .insertFetch_(with_pure_cid_1_0) // fetch a contract
          .endExercises_ // close the exercise context normally
          .insertCreate_ // create the contract cid_2
      )

      // Error thrown when rolling back a fetch and a create
      def run3 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateEffectfulRollbacksDisallowed //
          .insertCreate_ // create the contract cid_0
          .beginConsumingExercises_ // open an exercise context
          .insertCreate_ // create the contract cid_1_0
          .insertCreate_ // create the contract cid_1_2
          .beginTry //
          .insertFetch_(with_pure_cid_1_0) // fetch a contract
          .insertCreate_ // create the contract cid_1_3
          .rollbackTry_ //
          .endExercises_ // close the exercise context normally
          .insertCreate_ // create the contract cid_2
      )

      // Error thrown when rolling back a consuming exercise and a fetch
      def run4 = contractIdsInOrder(
        // the double slashes below tricks scalafmt
        initialStateEffectfulRollbacksDisallowed //
          .insertCreate_ // create the contract cid_0
          .beginTry //
          .beginConsumingExercises_ // open an exercise context
          .insertFetch_(with_pure_cid_1_0) // fetch a contract
          .endExercises_ // close the exercise context normally
          .rollbackTry_ //
          .insertCreate_ // create the contract cid_2
      )

      assert(intercept[IErrorThrowable](run3).err.isInstanceOf[IError.EffectfulRollback])
      assert(intercept[IErrorThrowable](run4).err.isInstanceOf[IError.EffectfulRollback])
    }
  }

}
