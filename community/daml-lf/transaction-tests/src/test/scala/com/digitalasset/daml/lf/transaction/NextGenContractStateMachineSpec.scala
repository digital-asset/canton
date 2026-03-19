// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.ContractStateMachineSpec._
import com.digitalasset.daml.lf.transaction.TransactionError.{
  DuplicateContractId,
  DuplicateContractKey,
  InconsistentContractKey,
}
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder._
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.{
  defaultPackageId,
  toIdentifier,
  toName,
  toParty,
}
import com.digitalasset.daml.lf.value.{Value => V}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class NextGenContractStateMachineSpec
    extends AnyFreeSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks {

  def contractKeyInputs[Tx](tx: HasTxNodes[Tx]): ErrOr[Map[GlobalKey, Vector[V.ContractId]]] = {
    tx.foldInExecutionOrder[ErrOr[NextGenContractStateMachine.LLState[NodeId]]](
      Right(NextGenContractStateMachine.empty[NodeId]())
    )(
      exerciseBegin = (acc, nid, exe) =>
        (acc.flatMap(_.handleExercise(nid, exe)), Transaction.ChildrenRecursion.DoRecurse),
      exerciseEnd = (acc, _, _) => acc,
      rollbackBegin =
        (acc, _, _) => (acc.map(_.beginRollback), Transaction.ChildrenRecursion.DoRecurse),
      rollbackEnd = (acc, _, _) => acc.flatMap(_.endRollback),
      leaf = (acc, nid, leaf) => acc.flatMap(_.handleNode(nid, leaf)),
    ).map(_.keyInputs.transform((_, v) => v.queue))
  }

  "contractKeyInputs" - {
    val dummyBuilder = new TxBuilder()
    val parties = List(alice)
    val keyPkgName = Ref.PackageName.assertFromString("key-package-name")
    def keyValue(s: String) = V.ValueText(s)
    def keyHash(s: String) = crypto.Hash.hashPrivateKey(s)
    def globalKey(k: String) = GlobalKey.assertBuild(
      "Mod:T",
      keyPkgName,
      keyValue(k),
      keyHash(k),
    )
    def create(s: V.ContractId, k: String) = dummyBuilder
      .create(
        id = s,
        templateId = "Mod:T",
        argument = V.ValueUnit,
        signatories = parties.toSet,
        observers = parties.toSet,
        key = CreateKey.SignatoryMaintainerKey(keyValue(k), keyHash(k)),
        packageName = keyPkgName,
      )

    def exe(s: V.ContractId, k: String, consuming: Boolean, byKey: Boolean) =
      dummyBuilder
        .exercise(
          contract = create(s, k),
          choice = "Choice",
          actingParties = parties.toSet,
          consuming = consuming,
          argument = V.ValueUnit,
          byKey = byKey,
        )

    def fetch(s: V.ContractId, k: String, byKey: Boolean) =
      dummyBuilder.fetch(contract = create(s, k), byKey = byKey)

    def lookup(s: V.ContractId, k: String, found: Boolean) =
      dummyBuilder.lookupByKey(contract = create(s, k), found = found)

    "return None for create" in {
      val builder = new TxBuilder()
      val createNode = create(cid(0), "k0")
      builder.add(createNode)
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Right(
        Map()
      )
    }
    "return Some(_) for fetch and fetch-by-key" in {
      val builder = new TxBuilder()
      val fetchNode0 = fetch(cid(0), "k0", byKey = false)
      val fetchNode1 = fetch(cid(1), "k1", byKey = true)
      builder.add(fetchNode0)
      builder.add(fetchNode1)
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Right(
        Map(
          //          Disable until fetch-by-id populate contract key inputs.
          //          globalKey("k0") -> Vector(cid(0)),
          globalKey("k1") -> Vector(cid(1))
        )
      )
    }

    "return Some(_) for consuming/non-consuming exercise and exercise-by-key" in {
      val builder = new TxBuilder()
      val exe0 = exe(cid(0), "k0", consuming = false, byKey = false)
      val exe1 = exe(cid(1), "k1", consuming = true, byKey = false)
      val exe2 = exe(cid(2), "k2", consuming = false, byKey = true)
      val exe3 = exe(cid(3), "k3", consuming = true, byKey = true)
      builder.add(exe0)
      builder.add(exe1)
      builder.add(exe2)
      builder.add(exe3)
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Right(
        Map(
          //          globalKey("k0") -> Vector(cid(0)),
          //          globalKey("k1") -> Vector(cid(1)),
          globalKey("k2") -> Vector(cid(2)),
          globalKey("k3") -> Vector(cid(3)),
        )
      )
    }

    "return None for negative lookup by key" in {
      val builder = new TxBuilder()
      val lookupNode = lookup(cid(0), "k0", found = false)
      builder.add(lookupNode)
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Right(
        Map(globalKey("k0") -> Vector())
      )
    }

    "return Some(_) for positive lookup by key" in {
      val builder = new TxBuilder()
      val lookupNode = lookup(cid(0), "k0", found = true)
      builder.add(lookupNode)
      inside(lookupNode.result) { case Vector(contractId) =>
        contractId shouldBe cid(0)
        val tx = builder.build()
        contractKeyInputs(tx) shouldBe Right(
          Map(globalKey("k0") -> Vector(contractId))
        )
      }
    }
    "returns keys used under rollback nodes" ignore {
      val builder = new TxBuilder()
      val createNode = create(cid(0), "k0")
      val exerciseNode = exe(cid(1), "k1", consuming = false, byKey = false)
      val fetchNode = fetch(cid(2), "k2", byKey = false)
      val lookupNode = lookup(cid(3), "k3", found = false)
      val rollback = builder.add(builder.rollback())
      builder.add(createNode, rollback)
      builder.add(exerciseNode, rollback)
      builder.add(fetchNode, rollback)
      builder.add(lookupNode, rollback)
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Right(
        Map(
          //          globalKey("k1") -> Vector(exerciseNode.targetCoid),
          //          globalKey("k2") -> Vector(fetchNode.coid),
        )
      )
    }
    "fetch and create conflict for the same contract ID" in {
      val builder = new TxBuilder()
      builder.add(fetch(cid(0), "k0", byKey = false))
      builder.add(create(cid(0), "k1"))
      val tx = builder.build()
      tx.contractKeyInputs shouldBe Left(
        DuplicateContractId(cid(0))
      )
    }
    "lookup by key and create conflict for the same contract ID" in {
      val builder = new TxBuilder()
      builder.add(lookup(cid(0), "k0", found = true))
      builder.add(create(cid(0), "k1"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        DuplicateContractId(cid(0))
      )
    }
    "consuming exercise and create conflict for the same contract ID" in {
      val builder = new TxBuilder()
      builder.add(exe(cid(0), "k1", consuming = true, byKey = false))
      builder.add(create(cid(0), "k2"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        DuplicateContractId(cid(0))
      )
    }
    "non-consuming exercise and create conflict for the same contract ID" in {
      val builder = new TxBuilder()
      builder.add(exe(cid(0), "k1", consuming = false, byKey = false))
      builder.add(create(cid(0), "k2"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        DuplicateContractId(cid(0))
      )
    }
    "two creates conflict" ignore {
      val builder = new TxBuilder()
      builder.add(create(cid(0), "k0"))
      builder.add(create(cid(1), "k0"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        DuplicateContractKey(globalKey("k0"))
      )
    }
    "two creates do not conflict if interleaved with archive" in {
      val builder = new TxBuilder()
      builder.add(create(cid(0), "k0"))
      builder.add(exe(cid(0), "k0", consuming = true, byKey = false))
      builder.add(create(cid(1), "k0"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Right(Map())
    }
    "two creates do not conflict if one is in rollback" in {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(create(cid(0), "k0"), rollback)
      builder.add(create(cid(1), "k0"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Right(Map())
    }
    "negative lookup after create fails" in {
      val builder = new TxBuilder()
      builder.add(create(cid(0), "k0"))
      builder.add(lookup(cid(0), "k0", found = false))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        InconsistentContractKey(globalKey("k0"))
      )
    }
    "inconsistent lookups conflict" in {
      val builder = new TxBuilder()
      builder.add(lookup(cid(0), "k0", found = true))
      builder.add(lookup(cid(0), "k0", found = false))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        InconsistentContractKey(globalKey("k0"))
      )
    }
    "inconsistent lookups conflict across rollback" in {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(lookup(cid(0), "k0", found = true), rollback)
      builder.add(lookup(cid(0), "k0", found = false))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        InconsistentContractKey(globalKey("k0"))
      )
    }
    "positive lookup conflicts with create" ignore {
      val builder = new TxBuilder()
      builder.add(lookup(cid(0), "k0", found = true))
      builder.add(create(cid(1), "k0"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        DuplicateContractKey(globalKey("k0"))
      )
    }
    "positive lookup in rollback conflicts with create" ignore {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(lookup(cid(0), "k0", found = true), rollback)
      builder.add(create(cid(1), "k0"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        DuplicateContractKey(globalKey("k0"))
      )
    }
    "rolled back archive does not prevent conflict" ignore {
      val builder = new TxBuilder()
      builder.add(create(cid(0), "k0"))
      val rollback = builder.add(builder.rollback())
      builder.add(exe(cid(0), "k0", consuming = true, byKey = true), rollback)
      builder.add(create(cid(1), "k0"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        DuplicateContractKey(globalKey("k0"))
      )
    }
    "successful, inconsistent lookups conflict" in {
      val builder = new TxBuilder()
      val create0 = create(cid(0), "k0")
      val create1 = create(cid(1), "k0")
      builder.add(builder.lookupByKey(create0, found = true))
      builder.add(builder.lookupByKey(create1, found = true))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        InconsistentContractKey(globalKey("k0"))
      )
    }
    "first negative input wins" ignore {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      val create0 = create(cid(0), "k0")
      val lookup0 = builder.lookupByKey(create0, found = false)
      val create1 = create(cid(1), "k1")
      val lookup1 = builder.lookupByKey(create1, found = false)
      builder.add(create0, rollback)
      builder.add(lookup1, rollback)
      builder.add(lookup0)
      builder.add(create1)
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Right(
        Map(globalKey("k0") -> Vector(), globalKey("k1") -> Vector())
      )
    }
  }

  lazy val alice: Ref.Party = "Alice"
  val aliceS: Set[Ref.Party] = Set(alice)
  val templateId: Ref.TypeConId = "Template:Id"
  val choiceId: Ref.ChoiceName = "Choice"
  val pkgName: Ref.PackageName = Ref.PackageName.assertFromString("package-name")
  val txVersion: SerializationVersion = SerializationVersion.maxVersion
  val unit: V = V.ValueUnit

  def cid(coid: Int): V.ContractId = {
    val l = crypto.Hash.underlyingHashLength
    val bytes = Array.ofDim[Byte](l)
    bytes(l - 4) = (coid >> 24).toByte
    bytes(l - 3) = (coid >> 16).toByte
    bytes(l - 2) = (coid >> 8).toByte
    bytes(l - 1) = coid.toByte
    val hash = crypto.Hash.assertFromByteArray(bytes)
    V.ContractId.V1(hash)
  }

  private def toKeyWithMaintainers(
      templateId: Ref.TypeConId,
      key: String,
  ): GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers.assertBuild(
      templateId,
      V.ValueText(key),
      crypto.Hash.hashPrivateKey(key),
      aliceS,
      pkgName,
    )

  private def toOptKeyWithMaintainers(
      templateId: Ref.TypeConId,
      key: String,
  ): Option[GlobalKeyWithMaintainers] =
    if (key.isEmpty) None
    else Some(toKeyWithMaintainers(templateId, key))

  def gkey(key: String): GlobalKey =
    GlobalKey.assertBuild(
      templateId,
      pkgName,
      V.ValueText(key),
      crypto.Hash.hashPrivateKey(key),
    )

  def mkCreate(
      contractId: V.ContractId,
      key: String = "",
  ): Node.Create =
    Node.Create(
      coid = contractId,
      packageName = pkgName,
      templateId = templateId,
      arg = unit,
      signatories = aliceS,
      stakeholders = aliceS,
      keyOpt = toOptKeyWithMaintainers(templateId, key),
      version = txVersion,
    )

  def mkExercise(
      contractId: V.ContractId,
      consuming: Boolean = true,
      key: String = "",
      byKey: Boolean = false,
  ): Node.Exercise =
    Node.Exercise(
      targetCoid = contractId,
      packageName = pkgName,
      templateId = templateId,
      interfaceId = None,
      choiceId = choiceId,
      consuming = consuming,
      actingParties = aliceS,
      chosenValue = unit,
      stakeholders = aliceS,
      signatories = aliceS,
      choiceObservers = Set.empty,
      choiceAuthorizers = None,
      children = ImmArray.Empty,
      exerciseResult = None,
      keyOpt = toOptKeyWithMaintainers(templateId, key),
      byKey = byKey,
      version = txVersion,
    )

  def mkFetch(
      contractId: V.ContractId,
      key: String = "",
      byKey: Boolean = false,
  ): Node.Fetch =
    Node.Fetch(
      coid = contractId,
      packageName = pkgName,
      templateId = templateId,
      actingParties = aliceS,
      signatories = aliceS,
      stakeholders = aliceS,
      keyOpt = toOptKeyWithMaintainers(templateId, key),
      byKey = byKey,
      version = txVersion,
      interfaceId = None,
    )

}
