// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.syntax.either.*
import com.digitalasset.canton.participant.protocol.validation.InternalConsistencyChecker.{
  ErrorWithInternalConsistencyCheck,
  InconsistentContractKeyError,
}
import com.digitalasset.canton.protocol.ExampleContractFactory
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.NodeId
import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder.NodeOps
import com.digitalasset.daml.lf.transaction.test.{
  TestIdFactory,
  TestNodeBuilder,
  TransactionBuilder,
  TreeTransactionBuilder,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ValueRecord

import TransactionBuilder.Implicits.*

class NextGenInternalConsistencyCheckerTest extends InternalConsistencyCheckerTest {

  "Internal consistency checker" when {

    val participantId: ParticipantId = ParticipantId("test")
    val sut = new NextGenInternalConsistencyChecker(participantId, loggerFactory)

    "rollback scope order" should checkRollbackScopeOrder()

    "standard happy cases" should checkStandardHappyCases(sut)

    "key consistency cases" should checkKeyConsistencyCases(sut)

  }

  def checkKeyConsistencyCases(sut: InternalConsistencyChecker): Unit = {
    val ids: Iterator[NodeId] = Iterator.from(0).map(NodeId.apply)
    val txBuilder = new TreeTransactionBuilder with TestNodeBuilder with TestIdFactory {
      override def nextNodeId(): NodeId = ids.next()
    }

    val someCreate = txBuilder.create(
      id = txBuilder.newCid,
      templateId = "M:T",
      argument = Value.ValueUnit,
      signatories = List("signatory"),
      observers = List("observer"),
    )

    val someExercise = txBuilder.exercise(
      someCreate,
      choice = "C",
      consuming = false,
      actingParties = Set("A"),
      ValueRecord(None, ImmArray.empty),
      byKey = false,
    )

    val key = ExampleContractFactory.buildKeyWithMaintainers()

    val cId1 = txBuilder.newCid
    val cId2 = txBuilder.newCid
    val cId3 = txBuilder.newCid

    s"key consistency" must {
      "allow a non-exhaustive query followed by an exhaustive one" in {
        val tx = txBuilder.toTransaction(
          someExercise.withChildren(
            txBuilder.queryByKey(key = key, Vector(cId1, cId2), exhaustive = false),
            txBuilder.queryByKey(key = key, Vector(cId1, cId2), exhaustive = true),
          )
        )
        checkTransaction(sut, tx, Set(key.globalKey)) shouldBe Either.unit
      }
      "disallow the inconsistent contract ordering" in {
        val tx = txBuilder.toTransaction(
          someExercise.withChildren(
            txBuilder.queryByKey(key = key, Vector(cId1, cId2, cId3), exhaustive = false),
            txBuilder.queryByKey(key = key, Vector(cId2, cId3), exhaustive = false),
          )
        )
        inside(checkTransaction(sut, tx, Set(key.globalKey))) {
          case Left(ErrorWithInternalConsistencyCheck(InconsistentContractKeyError(actual))) =>
            actual shouldBe key.globalKey
        }
      }
      "allow inconsistent contract ordering if the key is not hosted" in {
        val tx = txBuilder.toTransaction(
          someExercise.withChildren(
            txBuilder.queryByKey(key = key, Vector(cId1, cId2, cId3), exhaustive = false),
            txBuilder.queryByKey(key = key, Vector(cId2, cId3), exhaustive = false),
          )
        )
        checkTransaction(sut, tx, Set.empty) shouldBe Either.unit
      }
      "disallow an exhaustive query followed by one that returns additional contracts" in {
        val tx = txBuilder.toTransaction(
          someExercise.withChildren(
            txBuilder.queryByKey(key = key, Vector(cId1, cId2), exhaustive = true),
            txBuilder.queryByKey(key = key, Vector(cId1, cId2, cId3), exhaustive = false),
          )
        )
        inside(checkTransaction(sut, tx, Set(key.globalKey))) {
          case Left(ErrorWithInternalConsistencyCheck(InconsistentContractKeyError(actual))) =>
            actual shouldBe key.globalKey
        }
      }
    }
  }

}
