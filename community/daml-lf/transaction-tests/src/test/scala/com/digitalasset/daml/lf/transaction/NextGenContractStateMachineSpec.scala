// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import scala.language.implicitConversions
import com.digitalasset.daml.lf.transaction.NextGenContractStateMachine.{
  HHState,
  LLState,
  State,
  StateMachineResult,
}
import com.digitalasset.daml.lf.transaction.TransactionError.{
  AlreadyConsumed,
  DuplicateContractId,
  EffectfulRollback,
  InconsistentContractKey,
}
import com.digitalasset.daml.lf.transaction.test.TestNodeBuilder.*
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.{
  defaultPackageId,
  toIdentifier,
  toName,
  toParty,
}
import com.daml.scalautil.Statement.discard
import cats.syntax.foldable.*
import cats.syntax.option.*
import com.digitalasset.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestNodeBuilder}
import com.digitalasset.daml.lf.value.Value as V
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class NextGenContractStateMachineSpec
    extends AnyFreeSpec
    with Inside
    with Matchers
    with TableDrivenPropertyChecks {

  import NextGenContractStateMachineSpec.*

  type OptStateMachineResult = ErrOr[StateMachineResult]
  lazy val alice: Ref.Party = "Alice"
  val aliceS: Set[Ref.Party] = Set(alice)

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
        Map(globalKey("k0") -> Vector(cid(1), cid(0)))
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
        Map(globalKey("k0") -> Vector(cid(1), cid(0)))
      )
    }
    "positive lookup in rollback conflicts with create" ignore {
      val builder = new TxBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(lookup(cid(0), "k0", found = true), rollback)
      builder.add(create(cid(1), "k0"))
      val tx = builder.build()
      contractKeyInputs(tx) shouldBe Left(
        Map(globalKey("k0") -> Vector(cid(1), cid(0)))
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
        Map(globalKey("k0") -> Vector(cid(1), cid(0)))
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
  val tmplIdWithoutKey: Ref.TypeConId = "Template:WithoutKey"
  val tmplIdWithKey: Ref.TypeConId = "Template:WithKey"
  def getTmplId(key: Option[_]): Ref.TypeConId = key match {
    case Some(_) => tmplIdWithKey
    case None => tmplIdWithoutKey
  }
  val choiceId: Ref.ChoiceName = "Choice"
  val pkgName: Ref.PackageName = Ref.PackageName.assertFromString("package-name")
  val txVersion: SerializationVersion = SerializationVersion.maxVersion
  val unit: V = V.ValueUnit

  def contractKeyInputs[Tx](
      tx: HasTxNodes[Tx]
  ): Either[TransactionError, Map[GlobalKey, Vector[V.ContractId]]] =
    tx.foldInExecutionOrder[Either[TransactionError, NextGenContractStateMachine.LLState]](
      Right(NextGenContractStateMachine.empty())
    )(
      exerciseBegin = (acc, nid, exe) =>
        (acc.flatMap(_.handleExercise(nid, exe)), Transaction.ChildrenRecursion.DoRecurse),
      exerciseEnd = (acc, _, _) => acc,
      rollbackBegin =
        (acc, _, _) => (acc.map(_.beginRollback), Transaction.ChildrenRecursion.DoRecurse),
      rollbackEnd = (acc, _, _) => acc.flatMap(s => handleViaEmptyEffectfulRollback(s.endRollback)),
      leaf = (acc, nid, leaf) => acc.flatMap(_.handleNode(nid, leaf)),
    ).map(_.keyInputs.transform((_, v) => v.queue))

  // TODO(#31454)
  def handleViaEmptyEffectfulRollback[A](
      eOrA: Either[Set[NodeId], A]
  ): Either[TransactionError, A] =
    eOrA.left.map(_ => TransactionError.EffectfulRollback(Set()))

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

  implicit class GKeyStringOps(private val key: String) {
    def gkey: GlobalKey =
      GlobalKey.assertBuild(
        tmplIdWithKey,
        pkgName,
        V.ValueText(key),
        crypto.Hash.hashPrivateKey(key),
      )
  }

  implicit def stringToGKey(key: String): GlobalKey = key.gkey

  implicit def optStringToOptGkey(key: Option[String]): Option[GlobalKey] = key.map(_.gkey)

  def mkCreate(
      contractId: V.ContractId,
      key: Option[String] = None,
  ): Node.Create =
    Node.Create(
      coid = contractId,
      packageName = pkgName,
      templateId = getTmplId(key),
      arg = unit,
      signatories = aliceS,
      stakeholders = aliceS,
      keyOpt = toOptKeyWithMaintainers(key),
      version = txVersion,
    )

  private def toOptKeyWithMaintainers(
      key: Option[String],
  ): Option[GlobalKeyWithMaintainers] =
    key.map(toKeyWithMaintainers(_))

  private def toKeyWithMaintainers(
      key: String,
  ): GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers.assertBuild(
      tmplIdWithKey,
      V.ValueText(key),
      crypto.Hash.hashPrivateKey(key),
      aliceS,
      pkgName,
    )

  def mkExercise(
      contractId: V.ContractId,
      consuming: Boolean = true,
      key: Option[String] = None,
      byKey: Boolean = false,
  ): Node.Exercise =
    Node.Exercise(
      targetCoid = contractId,
      packageName = pkgName,
      templateId = getTmplId(key),
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
      keyOpt = toOptKeyWithMaintainers(key),
      byKey = byKey,
      version = txVersion,
    )

  def mkFetch(
      contractId: V.ContractId,
      key: Option[String] = None,
      byKey: Boolean = false,
  ): Node.Fetch =
    Node.Fetch(
      coid = contractId,
      packageName = pkgName,
      templateId = getTmplId(key),
      actingParties = aliceS,
      signatories = aliceS,
      stakeholders = aliceS,
      keyOpt = toOptKeyWithMaintainers(key),
      byKey = byKey,
      version = txVersion,
      interfaceId = None,
    )

  def mkQueryByKey(
      result: Vector[V.ContractId],
      key: String,
      exhaustive: Boolean,
  ): Node.QueryByKey =
    Node.QueryByKey(
      packageName = pkgName,
      templateId = tmplIdWithKey,
      exhaustive = exhaustive,
      key = toKeyWithMaintainers(key),
      result = result,
      version = txVersion,
    )

  implicit def nodeToTx(node: Node): HasTxNodes[?] = {
    val builder = new TxBuilder()
    val _ = builder.add(node)
    builder.build()
  }

  // Recursively replay a sub-transaction's nodes into a builder
  private def addSubTx(
      builder: TxBuilder,
      tx: HasTxNodes[?],
      nodeId: NodeId,
      parentId: Option[NodeId],
  ): Unit = {
    val node = tx.nodes(nodeId)
    val newId = parentId.fold(builder.add(node))(builder.add(node, _))
    node match {
      case rb: Node.Rollback => rb.children.foreach(addSubTx(builder, tx, _, Some(newId)))
      case ex: Node.Exercise => ex.children.foreach(addSubTx(builder, tx, _, Some(newId)))
      case _ => ()
    }
  }

  // add HasTxNodes* below a Rollback node (indicating that the passed nodes have been rolled back)
  def mkRollbackTx(nodes: HasTxNodes[?]*): HasTxNodes[?] = {
    val builder = new TxBuilder()
    val rollbackId = builder.add(Node.Rollback(ImmArray.empty))
    nodes.foreach(tx => tx.roots.foreach(addSubTx(builder, tx, _, Some(rollbackId))))
    builder.build()
  }

  def mkTx(nodes: HasTxNodes[?]*): HasTxNodes[?] = {
    val builder = new TxBuilder()
    nodes.foreach(tx => tx.roots.foreach(addSubTx(builder, tx, _, None)))
    builder.build()
  }

  "the contract state machine" - {

    def toContractId(cid: Int): V.ContractId = {
      val l = crypto.Hash.underlyingHashLength
      val bytes = Array.ofDim[Byte](l)
      bytes(l - 4) = (cid >> 24).toByte
      bytes(l - 3) = (cid >> 16).toByte
      bytes(l - 2) = (cid >> 8).toByte
      bytes(l - 1) = cid.toByte
      val hash = crypto.Hash.assertFromByteArray(bytes)
      V.ContractId.V1(hash)
    }

    def toGlobalKey(keyId: Int): GlobalKey =
      GlobalKey.assertBuild(
        tmplIdWithoutKey,
        pkgName,
        V.ValueInt64(keyId.toLong),
        crypto.Hash.hashPrivateKey(s"key-$keyId"),
      )

    val start = Right(NextGenContractStateMachine.empty(authorizeRollBack = false))
    val cid1 = toContractId(1)
    // val cid2 = toContractId(2)
    val key1 = toGlobalKey(1)

    "reject when a contract is return by a fetch-by-id and but not by subsequent queryByKey" in {
      val r = for {
        state <- start
        state <- state.visitFetchById(tmplIdWithKey, cid1, mbKey = Some(key1))
        state <- state.visitQueryByKey(key1, result = Vector(), exhaustive = true)
      } yield state
      r shouldBe a[Left[?, ?]]
    }

    "reject when a contract is not return by a queryByKey but by a subsequent fetch-by-id" in {
      val r = for {
        state <- start
        state <- state.visitQueryByKey(key1, result = Vector(), exhaustive = true)
        state <- state.visitFetchById(tmplIdWithKey, cid1, mbKey = Some(key1))
      } yield state
      r shouldBe a[Left[?, ?]]
    }

    "reject when a contract archive-by-id is then queryByKey" in {
      val r = for {
        state <- start
        state <- state.visitExercise(NodeId(1), tmplIdWithKey, cid1, mbKey = Some(key1), byKey = false, consuming = true)
        state <- state.visitQueryByKey(key1, Vector(cid1), exhaustive = false)
      } yield state
      r shouldBe a[Left[?, ?]]
    }
  }

  /* test structure:
   *    * null
   *
   *    * Rollback stuff
   *    ** begintry
   *    ** trycatch
   *    *** begintry endtry
   *    *** begintry rollback
   *
   *    * Create stuff
   *    ** create
   *    ** double-create
   *    *** double-create (same cid)
   *    *** double-create (varying cid)
   *    *** double-create (same cid, different key)
   *    ** create, trycatch
   *    *** try create endtry
   *    *** try create rollback
   *    *** try create(key) rollback, then create same key
   *
   *    * Archive stuff
   *    ** archive
   *    ** create archive
   *    ** double-create archive
   *    ** double archive
   *    ** create, archive, trycatch
   *    *** create try archive endtry
   *    *** create try archive rollback
   *    *** double-create try archive endtry
   *    *** double-create try archive rollback
   *    *** try create archive endtry
   *    *** try create archive rollback
   *    ** archive, rollback, archive
   *
   *    * QueryById stuff
   *    ** querybyid
   *    ** queryById same contract twice (caching)
   *    ** create, querybyid
   *    *** create then querybyid
   *    *** querybyid then create
   *    ** querybyid, trycatch
   *    *** try querybyid endtry
   *    *** try querybyid rollback
   *    *** try querybyid rollback (reads survive rollback)
   *    ** querybyid, archive
   *    *** querybyid then archive
   *    *** querybyid then archive then querybyid
   *    ** create, archive, querybyid
   *    ** create, archive, querybyid, trycatch
   *    *** create, archive, querybyid, endtry
   *    *** create, archive, querybyid, rollback
   *    *** try create, archive, rollback, querybyid
   *
   *    * QueryByKey stuff
   *    ** querybykey
   *    *** simple querybykey
   *    *** querybykey (varying n)
   *    *** negative querynbykey
   *    *** queryNByKey with n=0
   *    *** queryNByKey empty exhaustive search
   *    ** queryNByKey continuation
   *    *** queryNByKey with InProgress (multi-step)
   *    *** queryNByKey continuation returns more results than requested
   *    *** queryNByKey with duplicates across batches (dedup)
   *    *** queryNByKey with duplicate spanning two batches
   *    ** queryNByKey caching
   *    *** queryNByKey same key twice
   *    *** queryNByKey inside try, rollback, then same key (reads survive rollback)
   *    *** querynbykey twice but inconsistent results (HH-only)
   *    ** create, querybykey
   *    *** create then querybykey
   *    *** querybykey then create
   *    *** create then queryNByKey, ledger returns local cid
   *    ** querybykey, archive
   *    *** create then archive then querybykey
   *    *** querybykey then archive
   *    ** archive, querybykey
   *    ** querybykey, trycatch
   *    *** try querybykey endtry
   *    *** try querybykey rollback
   *    ** querybyid, querybykey
   *    *** queryById then queryByKey
   *    *** queryByKey then queryById
   *
   *    * Special scenarios
   *    ** queryNByKey
   *    *** inconsistent querybyid after queryNByKey
   *    **** positive case: queryById reports contract+key exists that queryNByKey did not find
   *    **** negative case: queryNByKey reports contract+key exists that queryById did not find
   */

  // TODO(#31454)
  // remove workaround
  def rollbackTryForTesting(s: LLState): ErrOr[LLState] =
    s.rollbackTry.left.map(_ => EffectfulRollback(Set.empty))

  def ignoreUnitTest(unitTest: UnitTest): Unit =
    unitTest.description - {
      unitTest.expected.foreach { case (mode, _) =>
        s"mode $mode" ignore {}
      }
    }

  def runUnitTestWithoutAndWithKey(
      key: String,
      mkUnitTest: Option[String] => UnitTest,
  ): Unit = {
    "with key unset" - {
      runUnitTest(mkUnitTest(None))
    }
    "with key set" - {
      runUnitTest(mkUnitTest(key.some))
    }
  }

  def runUnitTest(unitTest: UnitTest): Unit = {
    require(
      unitTest.interaction.isDefined || unitTest.transaction.isDefined,
      s"UnitTest '${unitTest.description}' must have at least one of interaction or transaction defined",
    )
    unitTest.description - {
      unitTest.expected.foreach { case (mode, expectedResult) =>
        s"mode $mode" - {
          unitTest.interaction.foreach { interaction =>
            "LL" in {
              val fresh = NextGenContractStateMachine.empty(mode)
              val result = interaction.apply(fresh).map(_.toStateMachineResult)
              compareResult(result, expectedResult)
            }
          }
          unitTest.transaction.foreach { tx =>
            "HH" in {
              val hhResult = walkTransactionOnHHState(tx, mode).map(_.toStateMachineResult)
              compareResult(hhResult, expectedResult)
            }
          }
        }
      }
    }
  }

  def walkTransactionOnHHState[Tx](
      tx: HasTxNodes[Tx],
      mode: NextGenContractStateMachine.Mode =  NextGenContractStateMachine.Mode.NUCK
  ): ErrOr[LLState] =
    tx.foldInExecutionOrder[ErrOr[LLState]](
      Right(NextGenContractStateMachine.empty(mode))
    )(
      exerciseBegin = (acc, nid, exe) =>
        (acc.flatMap(_.handleExercise(nid, exe)), Transaction.ChildrenRecursion.DoRecurse),
      exerciseEnd = (acc, _, _) => acc,
      rollbackBegin =
        (acc, _, _) => (acc.map(_.beginRollback), Transaction.ChildrenRecursion.DoRecurse),
      rollbackEnd = (acc, _, _) => acc.flatMap(s => handleViaEmptyEffectfulRollback(s.endRollback)),
      leaf = (acc, nid, leaf) => acc.flatMap(_.handleNode(nid, leaf)),
    )

  def compareResult(result: OptStateMachineResult, expectedResult: OptStateMachineResult): Unit =
    (result, expectedResult) match {
      case (Left(err1), Left(err2)) => discard(err1 shouldBe err2)
      case (Right(state), Right(res)) =>
        withClue("inputContractIds") {
          state.inputContractIds shouldBe res.inputContractIds
        }
        withClue("globalKeyInputs") {
          // TODO: remove cleaning
          cleanGlobalKeyInputs(state.globalKeyInputs) shouldBe res.globalKeyInputs
        }
        withClue("localKeys") {
          state.localKeys shouldBe res.localKeys
        }
        withClue("consumed") {
          discard(state.consumed shouldBe res.consumed)
        }
      case _ => fail(s"$result was not equal to $expectedResult")
    }

  /** Remove entries from globalKeyInputs where the queue is empty and exhaustive is false, so that
    * Map.empty, Map(key1 -> KeyMapping(Vector.empty, false)), etc. are all treated as equivalent.
    * Entries with exhaustive=true are kept even if the queue is empty, as they represent a
    * confirmed empty result from the ledger.
    */
  private def cleanGlobalKeyInputs(
      inputs: Map[GlobalKey, KeyMapping]
  ): Map[GlobalKey, KeyMapping] =
    inputs.filter { case (_, km) => km.queue.nonEmpty || km.exhaustive }

  def runUnitTestQueryToN(maxN: Int, mkUnitTest: Int => UnitTest): Unit =
    (1 to maxN).foreach { n =>
      s"query with n=$n" - {
        runUnitTest(mkUnitTest(n))
      }
    }

  // helper for queryNByKey tests with NeedKeys continuation
  // Catches common pattern with queryNByKey: call queryNByKey, expect the continuation, give the passed result,
  // expect the machine to return a result, return result
  def queryNByKeyAndReplyToNeedsKey(
      key: GlobalKey,
      n: Int,
      replyToNeedsKeyWith: Seq[V.ContractId],
      state: LLState,
  ): ErrOr[LLState] = {
    assert(
      replyToNeedsKeyWith.size < n,
      s"Finished passed but replyToNeedsKeyWith.size (${replyToNeedsKeyWith.size}) >= n ($n). " +
        s"Finished means caller of NeedsKeys (e.g. the ledger) tried to find more but ran out, so it must return " +
        s"fewer than requested.",
    )
    state.queryNByKey(key, n) match {
      case Right(res) => fail(s"Expected needsKeys but got unexpected result: $res")
      case Left(nk) =>
        nk.resume(replyToNeedsKeyWith.view, NeedKeyProgression.Finished) match {
          case Right(errOrResult) =>
            for {
              result <- errOrResult
              (mp, s) = result
              _ = mp.queue shouldBe replyToNeedsKeyWith
            } yield s
          case Left(nk) => fail(s"unexpected NeedKeys: $nk")
        }
    }
  }

  // * null
  "nullUnitTest" - {
    runUnitTest(
      UnitTest(
        description = "null (no interaction)",
        interaction = Right(_),
        transaction = mkTx(),
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  //  * Rollback stuff
  //  ** begintry
  "beginTryUnitTest" - {
    runUnitTest(
      UnitTest(
        description = s"beginTry",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
          } yield s,
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // ** trycatch
  // *** begintry endtry
  "beginEndTryUnitTest" - {
    runUnitTest(
      UnitTest(
        description = s"beginTry and endTry",
        interaction = s =>
          for {
            s <- Right(s.beginTry): ErrOr[LLState]
            s <- Right(s.endTry)
          } yield s,
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // *** begintry rollback
  "beginTryRollbackUnitTest" - {
    runUnitTest(
      UnitTest(
        description = s"beginTry and rollbackTry",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- rollbackTryForTesting(s)
          } yield s,
        transaction = mkRollbackTx(),
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // * create stuff
  // ** create
  "createUnitTest" - {
    val id = cid(1)
    runUnitTest(
      UnitTest(
        description = s"create",
        interaction = s =>
          for {
            s <- s.create(NodeId(0), id, None)
          } yield s,
        transaction = mkTx(mkCreate(id)),
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // ** double-create
  /// ** double-create (same cid)
  "doubleCreateSameCidUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then create same cid same key",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- s.create(NodeId(1), id, optkey)
            } yield s,
          transaction = mkTx(
            mkCreate(id),
            mkCreate(id),
          ),
          expected = Left(DuplicateContractId(id)),
        )
      },
    )
  }

  /// ** double-create (varying cid)
  "doubleCreateVaryingCidUnitTests" - {
    val id1 = cid(1)
    val id2 = cid(2)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then create new cid same key",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id1, optkey)
              s <- s.create(NodeId(1), id2, optkey)
            } yield s,
          transaction = mkTx(
            mkCreate(id1, optkey),
            mkCreate(id2, optkey),
          ),
          expected = Right(
            StateMachineResult.emptyWith(
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key.gkey -> Vector(id2, id1))
            )
          ),
        )
      },
    )
  }

  // *** double-create (same cid, different key)
  "createSameCidDifferentKeysUnitTest" - {
    val id = cid(1)
    val key1 = "foo"
    val key2 = "bar"

    runUnitTest(
      UnitTest(
        description = s"create(key=K1) then create same cid with key=K2",
        interaction = s =>
          for {
            s <- s.create(NodeId(0), id, key1.some)
            s <- s.create(NodeId(1), id, key2.some)
          } yield s,
        transaction = mkTx(
          mkCreate(id, key1.some),
          mkCreate(id, key2.some),
        ),
        expected = Left(DuplicateContractId(id)),
      )
    )
  }

  // ** create, trycatch
  // *** try create endtry
  "beginTryCreateEndTryUnitTest" - {
    val id = cid(1)
    runUnitTest(
      UnitTest(
        description = s"beginTry then create then endTry",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- s.create(NodeId(0), id, None)
            s <- Right(s.endTry)
          } yield s,
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // *** try create rollback
  "beginTryCreateRollbackUnitTest" - {
    val id = cid(1)
    runUnitTest(
      UnitTest(
        description = s"beginTry then create then rollback",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- s.create(NodeId(0), id, None)
            s <- rollbackTryForTesting(s)
          } yield s,
        transaction = mkRollbackTx(mkCreate(id)),
        expected = Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
          NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
          NextGenContractStateMachine.Mode.NoKey -> Right(StateMachineResult.empty),
        ),
      )
    )
  }

  // *** try create(key) rollback, then create same key
  "beginTryCreateKeyRollbackThenCreateSameKeyUnitTest" - {
    val id1 = cid(1)
    val id2 = cid(2)
    val key = "foo"

    runUnitTest(
      UnitTest(
        description = s"beginTry then create(key=K) then rollback, then create(key=K)",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- s.create(NodeId(0), id1, key.some)
            s <- rollbackTryForTesting(s)
            s <- s.create(NodeId(1), id2, key.some)
          } yield s,
        transaction = mkTx(
          mkRollbackTx(mkCreate(id1, key.some)),
          mkCreate(id2, key.some),
        ),
        expected = Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
          NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
          NextGenContractStateMachine.Mode.NoKey -> Right(
            StateMachineResult.emptyWith(
              localKeys = Map(key.gkey -> Vector(id2))
            )
          ),
        ),
      )
    )
  }

  // * archive stuff
  // ** archive
  "archiveUnitTest" - {
    val id = cid(1)
    runUnitTest(
      UnitTest(
        description = s"archive (without setting or creating id first)",
        interaction = s => {
          s.archive(tmplIdWithoutKey, id, NodeId(0)).isEmpty shouldBe true
          Right(s)
        },
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // ** create archive
  "createArchiveUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description = s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
            } yield s,
          transaction = mkTx(
            mkCreate(id, optkey),
            // our only access to archive is an consuming exercise, which will insert a queryById but its the best we
            // can get. The qyeryById will always be fully cached, so it doesn't affect expected end state
            mkExercise(id, consuming = true, optkey, byKey = false),
          ),
          expected = Right(
            StateMachineResult.emptyWith(
              consumed = Set[V.ContractId](id),
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key.gkey -> Vector(id)),
            )
          ),
        )
      },
    )
  }

  // ** double-create archive
  "createArchiveCreateUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive then create same",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- s.create(NodeId(2), id, optkey)
            } yield s,
          transaction = mkTx(
            mkCreate(id, optkey),
            // same note as test above, accessing archive via consuming exercise
            mkExercise(id, consuming = true, optkey, byKey = false),
            mkCreate(id, optkey),
          ),
          expected = Left(DuplicateContractId(id)),
        )
      },
    )
  }

  // ** double archive
  "doubleArchiveUnitTest" - {
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"create then archive then archive same cid",
        interaction = s =>
          for {
            s <- s.create(NodeId(0), id, None)
            s <- s.archive(tmplIdWithoutKey, id, NodeId(1)).get
            s <- s.archive(tmplIdWithoutKey, id, NodeId(2)).get
          } yield s,
        transaction = mkTx(
          mkCreate(id),
          // same note as test above, accessing archive via consuming exercise
          mkExercise(id, consuming = true, key = None, byKey = false),
          mkExercise(id, consuming = true, key = None, byKey = false),
        ),
        expected = Left(AlreadyConsumed(id, tmplIdWithoutKey, NodeId(1))),
      )
    )
  }

  // ** create, archive, trycatch
  // *** create try archive endtry
  "createBeginTryArchiveEndTryUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then begintry then archive then endtry",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- Right(s.endTry)
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              consumed = Set[V.ContractId](id),
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key.gkey -> Vector(id)),
            )
          ),
        )
      },
    )
  }

  // *** create try archive rollback
  "createBeginTryArchiveRollbackUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then begintry then archive then rollback",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- rollbackTryForTesting(s)
            } yield s,
          transaction = mkTx(
            mkCreate(id, optkey),
            mkRollbackTx(
              // same note as test above, accessing archive via consuming exercise
              mkExercise(id, consuming = true, optkey, byKey = false)
            ),
          ),
          expected = Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
            NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
            NextGenContractStateMachine.Mode.NoKey -> Right(
              StateMachineResult.emptyWith(
                localKeys =
                  if (optkey.isEmpty) Map.empty
                  else
                    Map(key.gkey -> Vector(id))
              )
            ),
          ),
        )
      },
    )
  }

  // *** double-create try archive endtry
  // this test is a bit nonsensical because the try/catch doesn't alter the fact that a recreation of a contract with
  // the same id is not allowed. As its already written, keep it. If this tests requires maitenence, remove it instead.
  "createBeginTryArchiveEndTryCreateUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then begintry then archive then endtry then" +
              s" create same",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- Right(s.endTry)
              s <- s.create(NodeId(2), id, optkey)
            } yield s,
          expected = Left(DuplicateContractId(id)),
        )
      },
    )
  }

  // *** double-create try archive rollback
  // this test is a bit nonsensical because the try/catch doesn't alter the fact that a recreation of a contract with
  // the same id is not allowed. As its already written, keep it. If this tests requires maitenence, remove it instead.
  "createBeginTryArchiveRollbackCreateUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then begintry then archive then rollback " +
              s"then create same",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- rollbackTryForTesting(s)
              s <- s.create(NodeId(2), id, optkey)
            } yield s,
          transaction = mkTx(
            mkCreate(id, optkey),
            mkRollbackTx(
              // same note as test above, accessing archive via consuming exercise
              mkExercise(id, consuming = true, optkey, byKey = false)
            ),
            mkCreate(id, optkey),
          ),
          expected = Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
            NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
            NextGenContractStateMachine.Mode.NoKey -> Left(DuplicateContractId(id)),
          ),
        )
      },
    )
  }

  // *** try create  archive endtry
  "beginTryCreateArchiveEndTryUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"begintry then create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive then endtry",
          interaction = s =>
            for {
              s <- Right(s.beginTry)
              s <- s.create(NodeId(0), id, optkey)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- Right(s.endTry)
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              consumed = Set[V.ContractId](id),
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key.gkey -> Vector(id)),
            )
          ),
        )
      },
    )
  }

  // *** try create archive rollback
  "beginTryCreateArchiveRollbackUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"begintry then create (${if (optkey.isEmpty) "with" else "without"} key) then archive then rollback",
          interaction = s =>
            for {
              s <- Right(s.beginTry)
              s <- s.create(NodeId(0), id, optkey)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- rollbackTryForTesting(s)
            } yield s,
          transaction = mkTx(
            mkRollbackTx(
              mkCreate(id, optkey),
              // same note as test above, accessing archive via consuming exercise
              mkExercise(id, consuming = true, optkey, byKey = false),
            )
          ),
          expected = Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
            NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
            NextGenContractStateMachine.Mode.NoKey -> Right(StateMachineResult.empty),
          ),
        )
      },
    )
  }

  // ** archive, rollback, archive
  "archiveRollbackArchiveUnitTest" - {
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"create then beginTry then archive then rollback then archive",
        interaction = s =>
          for {
            s <- s.create(NodeId(0), id, None)
            s <- Right(s.beginTry)
            s <- s.archive(tmplIdWithoutKey, id, NodeId(1)).get
            s <- rollbackTryForTesting(s)
            s <- s.archive(tmplIdWithoutKey, id, NodeId(2)).get
          } yield s,
        transaction = mkTx(
          mkCreate(id),
          mkRollbackTx(
            // same note as test above, accessing archive via consuming exercise
            mkExercise(id, consuming = true, key = None, byKey = false)
          ),
          mkExercise(id, consuming = true, key = None, byKey = false),
        ),
        expected = Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
          NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
          NextGenContractStateMachine.Mode.NoKey -> Right(
            StateMachineResult.emptyWith(
              consumed = Set[V.ContractId](id)
            )
          ),
        ),
      )
    )
  }

  // * querybyid stuff
  // ** querybyid
  "queryByIdUnitTests" - {
    val id = cid(1)
    val key = "foo"
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"queryById (continuation with key ${if (optkey.isEmpty) "unset" else "set"})",
          interaction = s =>
            for {
              s <- {
                val Left(NeedContract(resume)) = s.queryById(getTmplId(optkey), id): @unchecked
                resume(optkey)
              }
            } yield s,
          transaction = mkTx(mkFetch(id, key.some)),
          expected = Right(
            StateMachineResult.emptyWith(
              inputContractIds = Set[V.ContractId](id)
            )
          ),
        )
      },
    )
  }

  // ** queryById same contract twice (caching)
  "queryByIdTwiceCachingUnitTest" - {
    val id = cid(1)
    val key = "foo"

    runUnitTest(
      UnitTest(
        description = s"queryById then queryById same cid — cached, no NeedContract",
        interaction = s =>
          for {
            s <- {
              val Left(NeedContract(resume)) = s.queryById(tmplIdWithKey, id): @unchecked
              resume(key.some)
            }
            s <- s
              .queryById(tmplIdWithKey, id)
              .getOrElse(fail("unexpected NeedContract on second queryById")): ErrOr[LLState]
          } yield s,
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id)
          )
        ),
      )
    )
  }

  // ** create, querybyid
  // *** create then querybyid
  "createQueryByIdUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then queryById",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- s.queryById(getTmplId(optkey), id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState]
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key.gkey -> Vector(id))
            )
          ),
        )
      },
    )
  }

  // *** querybyid then create
  "queryByIdCreateUnitTests" - {
    val id = cid(1)
    val key = "foo"

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"queryById then create (with key ${if (optkey.isEmpty) "unset" else "set"})",
          interaction = s =>
            for {
              s <- {
                val Left(NeedContract(resume)) = s.queryById(getTmplId(optkey), id): @unchecked
                resume(optkey)
              }
              s <- s.create(NodeId(0), id, optkey)
            } yield s,
          transaction = mkTx(
            mkFetch(id, key.some, byKey = false),
            mkCreate(id, optkey),
          ),
          expected = Left(DuplicateContractId(id)),
        )
      },
    )
  }

  // ** create, archive, querybyid
  "createArchiveQueryByIdUnitTest" - {
    val id = cid(1)
    val key = "foo"

    runUnitTest(
      UnitTest(
        description = s"create with key then archive then queryById",
        interaction = s =>
          for {
            s <- s.create(NodeId(0), id, key.some)
            s <- s.archive(tmplIdWithKey, id, NodeId(1)).get
            s <- s.queryById(tmplIdWithKey, id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState]
          } yield s,
        transaction = mkTx(
          mkCreate(id, key.some),
          // same note as test above, accessing archive via consuming exercise
          mkExercise(id, consuming = true, key = key.some, byKey = false),
          mkFetch(id, key.some, byKey = false),
        ),
        expected = Left(AlreadyConsumed(id, tmplIdWithKey, NodeId(1))),
      )
    )
  }

  // ** querybyid, trycatch
  // *** try querybyid endtry
  "beginTryQueryByIdEndTryUnitTests" - {
    val id = cid(1)
    val key = "foo"
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"beginTry then queryById (continuation with key ${if (optkey.isEmpty) "unset"
              else "set"}) then endTry",
          interaction = s =>
            for {
              s <- Right(s.beginTry)
              s <- {
                val Left(NeedContract(resume)) = s.queryById(getTmplId(optkey), id): @unchecked;
                resume(optkey)
              }
              s <- Right(s.endTry)
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              inputContractIds = Set[V.ContractId](id)
            )
          ),
        )
      },
    )
  }

  // *** try querybyid rollback
  "beginTryQueryByIdRollbackUnitTests" - {
    val id = cid(1)
    val key = "foo"
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"beginTry then queryById (continuation with key ${if (optkey.isEmpty) "unset"
              else "set"}) then rollbackTry",
          interaction = s =>
            for {
              s <- Right(s.beginTry)
              s <- {
                val Left(NeedContract(resume)) = s.queryById(getTmplId(optkey), id): @unchecked;
                resume(optkey)
              }
              s <- rollbackTryForTesting(s)
            } yield s,
          transaction = mkRollbackTx(mkFetch(id, key.some, byKey = false)),
          expected = Right(
            StateMachineResult.emptyWith(
              inputContractIds = Set[V.ContractId](id)
            )
          ),
        )
      },
    )
  }

  // *** try querybyid rollback (reads survive rollback)
  "queryByIdTryRollbackQueryByIdUnitTest" - {
    val id = cid(1)
    val key = "foo"

    runUnitTest(
      UnitTest(
        description =
          s"beginTry then queryById then rollback then queryById — reads survive rollback",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- {
              val Left(NeedContract(resume)) = s.queryById(tmplIdWithKey, id): @unchecked;
              resume(key.some)
            }
            s <- rollbackTryForTesting(s)
            s <- s
              .queryById(tmplIdWithKey, id)
              .getOrElse(fail("unexpected NeedContract after rollback")): ErrOr[LLState]
          } yield s,
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id)
          )
        ),
      )
    )
  }

  // ** querybyid, archive
  // *** querybyid then archive
  "queryByIdArchiveUnitTests" - {
    val id = cid(1)
    val key = "foo"
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"queryById (continuation with key ${if (optkey.isEmpty) "unset" else "set"}) then archive",
          interaction = s =>
            for {
              s <- {
                val Left(NeedContract(resume)) = s.queryById(getTmplId(optkey), id): @unchecked;
                resume(optkey)
              }
              s <- s.archive(getTmplId(optkey), id, NodeId(0)).get
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              consumed = Set[V.ContractId](id),
              inputContractIds = Set[V.ContractId](id),
            )
          ),
        )
      },
    )
  }

  // *** querybyid then archive then querybyid
  "queryByIdArchiveQueryByIdUnitTest" - {
    val id = cid(1)
    val key = "foo"

    runUnitTest(
      UnitTest(
        description = s"queryById then archive then queryById same cid",
        interaction = s =>
          for {
            s <- {
              val Left(NeedContract(resume)) = s.queryById(tmplIdWithKey, id): @unchecked;
              resume(key.some)
            }
            s <- s.archive(tmplIdWithKey, id, NodeId(0)).get
            s <- s.queryById(tmplIdWithKey, id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState]
          } yield s,
        transaction = mkTx(
          mkExercise(id, consuming = true, key = key.some, byKey = false),
          mkFetch(id, key.some, byKey = false),
        ),
        expected = Left(AlreadyConsumed(id, tmplIdWithKey, NodeId(0))),
      )
    )
  }

  // ** create, archive, querybyid
  "createArchiveQueryByIdUnitTests" - {
    val id = cid(1)
    val key = "foo"
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive then queryById with same key",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- s.queryById(getTmplId(optkey), id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState]
            } yield s,
          transaction = mkTx(
            mkCreate(id, optkey),
            // same note as test above, accessing archive via consuming exercise
            mkExercise(id, consuming = true, optkey, byKey = false),
            mkFetch(id, optkey, byKey = false),
          ),
          expected = Left(AlreadyConsumed(id, getTmplId(optkey), NodeId(1))),
        )
      },
    )
  }

  // ** create, archive, querybyid, trycatch
  // *** create, archive, querybyid, endtry
  "createBeginTryArchiveEndTryQueryByIdUnitTests" - {
    val id = cid(1)
    val key = "foo"
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then beginTry then archive then endTry " +
              s"queryById with same key",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- Right(s.endTry)
              s <- s.queryById(getTmplId(optkey), id).getOrElse(fail("unexpected NeedContract")): ErrOr[State]
            } yield s,
          expected = Left(AlreadyConsumed(id, getTmplId(optkey), NodeId(1))),
        )
      },
    )
  }

  // *** create, archive, querybyid, rollback
  "createBeginTryArchiveRollbackQueryByIdUnitTests" - {
    val id = cid(1)
    val key = "foo"
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {

        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then beginTry then archive then rollbackTry" +
              s" queryById with same key",
          interaction = s =>
            for {
              s <- s.create(NodeId(0), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- rollbackTryForTesting(s)
              s <- s.queryById(getTmplId(optkey), id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState]
            } yield s,
          transaction = mkTx(
            mkCreate(id, optkey),
            mkRollbackTx(
              // same note as test above, accessing archive via consuming exercise
              mkExercise(id, consuming = true, optkey, byKey = false)
            ),
            mkFetch(id, optkey, byKey = false),
          ),
          expected = Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
            NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
            NextGenContractStateMachine.Mode.NoKey -> Right(
              StateMachineResult.emptyWith(
                localKeys =
                  if (optkey.isEmpty) Map.empty
                  else
                    Map(key.gkey -> Vector(id))
              )
            ),
          ),
        )
      },
    )
  }

  // *** try create, archive, rollback, querybyid
  "beginTryCreateArchiveRollbackQueryByIdUnitTests" - {
    val id = cid(1)
    val key = "foo"
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"beginTry then create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive then rollbackTry" +
              s" queryById with same key",
          interaction = s =>
            for {
              s <- Right(s.beginTry)
              s <- s.create(NodeId(0), id, optkey)
              s <- s.archive(getTmplId(optkey), id, NodeId(1)).get
              s <- rollbackTryForTesting(s)
              s <- s.queryById(getTmplId(optkey), id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState]
            } yield s,
          transaction = mkTx(
            mkRollbackTx(
              mkCreate(id, optkey),
              // same note as test above, accessing archive via consuming exercise
              mkExercise(id, consuming = true, optkey, byKey = false),
            ),
            mkFetch(id, optkey, byKey = false),
          ),
          expected = Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
            NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
            NextGenContractStateMachine.Mode.NoKey -> Right(StateMachineResult.empty),
          ),
        )
      },
    )
  }

  // ** querybykey
  // *** simple querybykey
  "simpleQuerybykeyUnitTest" - {
    val id = cid(1)
    val key = "foo"

    runUnitTest(
      UnitTest(
        description = s"just queryByKey with needskey call",
        interaction = s =>
          for {
            s <- {
              // explicit call to queryNByKey to showcase usual interaction (next cases will use
              // queryNByKeyAndReplyToNeedsKey, wherever applicable)
              val Left(nk) = s.queryNByKey(key, 2): @unchecked
              nk.resume(Seq(id).view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe Vector(id)
                  } yield s
                case Left(nk) => fail(s"unexpected NeedKeys: $nk")
              }
            }
          } yield s,
        transaction = mkTx(mkQueryByKey(Vector(id), key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** querybykey (varying n)
  "querybykeyUnitTest" - {
    val key = "foo"
    val ids = Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(cid)

    runUnitTestQueryToN(
      maxN = 10,
      n =>
        UnitTest(
          description = s"querybykey",
          interaction = s =>
            queryNByKeyAndReplyToNeedsKey(
              key = key,
              n = n + 1,
              replyToNeedsKeyWith = ids.take(n),
              state = s,
            ),
          transaction = mkTx(mkQueryByKey(ids.take(n), key, exhaustive = true)),
          expected = Right(
            StateMachineResult.emptyWith(
              inputContractIds = ids.take(n).toSet,
              globalKeyInputs = Map(key.gkey -> KeyMapping(queue = ids.take(n), exhaustive = true)),
            )
          ),
        ),
    )
  }

  // *** negative querynbykey
  "querybykeyNegativeUnitTest" - {
    val key = "foo"

    runUnitTest(
      UnitTest(
        description = s"querybykey with negative n",
        interaction = s => {
          an[IllegalArgumentException] should be thrownBy s.queryNByKey(key, -1)
          Right(s)
        },
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // *** queryNByKey with n=0
  "queryNByKeyZeroUnitTest" - {
    val key = "foo"

    runUnitTest(
      UnitTest(
        description = s"queryNByKey with n=0",
        interaction = s => {
          an[IllegalArgumentException] should be thrownBy s.queryNByKey(key, 0)
          Right(s)
        },
        expected = Right(StateMachineResult.empty),
        // framework does not support catching thrown exceptions from transactions
        // TODO[#30913]: extract to separate test
//        transaction =
//          Some(mkTx(mkQueryByKey(Vector.empty[V.ContractId], key, exhaustive = false))),
      )
    )
  }

  // *** queryNByKey empty exhaustive search
  "queryNByKeyEmptyExhaustiveUnitTest" - {
    val key = "foo"

    runUnitTest(
      UnitTest(
        description = s"queryNByKey(n=3) returning [] + Finished",
        interaction = s =>
          queryNByKeyAndReplyToNeedsKey(
            key = key,
            n = 3,
            replyToNeedsKeyWith = Seq.empty,
            state = s,
          ),
        transaction = mkTx(mkQueryByKey(Vector.empty, key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = Vector.empty, exhaustive = true))
          )
        ),
      )
    )
  }

  // ** queryNByKey continuation
  // *** queryNByKey with InProgress (multi-step)
  "queryNByKeyMultiStepContinuationUnitTest" - {
    val key = "foo"
    val ids = Vector(1, 2, 3, 4, 5).map(cid)
    val firstBatch = ids.take(3)
    val secondBatch = ids.drop(3)

    runUnitTest(
      UnitTest(
        description = s"queryNByKey(n=6), 3 + InProgress, then 2 + Finished",
        interaction = s => {
          val Left(nk) = s.queryNByKey(key, 6): @unchecked
          nk.n shouldBe 6
          nk.resume(firstBatch.view, NeedKeyProgression.InProgress(TestToken)) match {
            case Left(nk2) =>
              nk2.n shouldBe 3
              nk2.resume(secondBatch.view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe ids
                    _ = mp.exhaustive shouldBe true
                  } yield s
                case Left(nk3) => fail(s"unexpected third NeedKeys: $nk3")
              }
            case Right(_) => fail("expected NeedKeys continuation after InProgress, got Right")
          }
        },
        // visitQueryByKey does only support calling needskeys once per node
        transaction = mkTx(mkQueryByKey(ids, key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = ids.toSet,
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = ids, exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** queryNByKey continuation returns more results than requested
  "queryNByKeyContinuationExtraResultsUnitTest" - {
    val key = "foo"
    val ids = Vector(1, 2, 3, 4, 5).map(cid)
    val firstBatch = ids.take(1)
    val secondBatch = ids.drop(1)
    val firstThree = ids.take(3)

    ignoreUnitTest(
      UnitTest(
        description =
          s"queryNByKey(n=3), 1 + InProgress, then 4 + Finished — extra results accepted",
        interaction = s => {
          val Left(nk) = s.queryNByKey(key, 3): @unchecked
          nk.n shouldBe 3
          nk.resume(firstBatch.view, NeedKeyProgression.InProgress(TestToken)) match {
            case Left(nk2) =>
              nk2.n shouldBe 2
              nk2.resume(secondBatch.view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe firstThree
                    _ = mp.exhaustive shouldBe false
                  } yield s
                case Left(nk3) => fail(s"unexpected third NeedKeys: $nk3")
              }
            case Right(_) => fail("expected NeedKeys continuation after InProgress, got Right")
          }
        },
        // visitQueryByKey does only support calling needskeys once per node
        transaction = mkTx(mkQueryByKey(firstThree, key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = firstThree.toSet,
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = firstThree, exhaustive = false)),
          )
        ),
      )
    )
  }

  // *** queryNByKey with duplicates across batches (dedup)
  "queryNByKeyDuplicateInSingleResponseUnitTest" - {
    val key = "foo"
    val id1 = cid(1)
    val id2 = cid(2)

    runUnitTest(
      UnitTest(
        description = s"queryNByKey(n=3) with duplicates across batches — dedup",
        interaction = s => {
          val Left(nk) = s.queryNByKey(key, 3): @unchecked
          nk.n shouldBe 3
          nk.resume(Seq(id1, id1).view, NeedKeyProgression.InProgress(TestToken)) match {
            case Left(nk2) =>
              nk2.n shouldBe 2
              nk2.resume(Seq(id1, id2).view, NeedKeyProgression.InProgress(TestToken)) match {
                case Left(nk3) =>
                  nk3.n shouldBe 1
                  nk3.resume(Seq.empty[V.ContractId].view, NeedKeyProgression.Finished) match {
                    case Right(errOrResult) =>
                      for {
                        result <- errOrResult
                        (mp, s) = result
                        _ = mp.queue shouldBe Vector(id1, id2)
                        _ = mp.exhaustive shouldBe true
                      } yield s
                    case Left(nk4) => fail(s"unexpected fourth NeedKeys: $nk4")
                  }
                case Right(_) => fail("expected NeedKeys after batch 2, got Right")
              }
            case Right(_) => fail("expected NeedKeys after batch 1, got Right")
          }
        },
        // visitQueryByKey errors on duplicate contracts so we can only test supplying it with the deduped list, HH only
        // test below
        transaction = mkTx(mkQueryByKey(Vector(id1, id2), key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id1, id2),
            globalKeyInputs =
              Map(key.gkey -> KeyMapping(queue = Vector(id1, id2), exhaustive = true)),
          )
        ),
      )
    )
  }

  "queryNByKeyDuplicateInSingleResponseHHUnitTest" - {
    val key = "foo"
    val id1 = cid(1)
    val id2 = cid(2)

    runUnitTest(
      UnitTest(
        description = s"queryNByKey(n=3) with duplicates across batches — dedup",
        transaction = mkTx(mkQueryByKey(Vector(id1, id1, id2), key, exhaustive = false)),
        expected = Left(InconsistentContractKey(key)),
      )
    )
  }

  // *** queryNByKey with duplicate spanning two batches
  "queryNByKeyDuplicateAcrossBatchesUnitTest" - {
    val key = "foo"
    val id1 = cid(1)
    val id2 = cid(2)

    runUnitTest(
      UnitTest(
        description =
          s"queryNByKey(n=3), [c1]+InProgress then [c1,c2]+InProgress then []+Finished — cross-batch dedup",
        interaction = s => {
          val Left(nk) = s.queryNByKey(key, 3): @unchecked
          nk.n shouldBe 3
          nk.resume(Seq(id1).view, NeedKeyProgression.InProgress(TestToken)) match {
            case Left(nk2) =>
              nk2.n shouldBe 2
              nk2.resume(Seq(id1, id2).view, NeedKeyProgression.InProgress(TestToken)) match {
                case Left(nk3) =>
                  nk3.n shouldBe 1
                  nk3.resume(Seq.empty[V.ContractId].view, NeedKeyProgression.Finished) match {
                    case Right(errOrResult) =>
                      for {
                        result <- errOrResult
                        (mp, s) = result
                        _ = mp.queue shouldBe Vector(id1, id2)
                        _ = mp.exhaustive shouldBe true
                      } yield s
                    case Left(nk4) => fail(s"unexpected fourth NeedKeys: $nk4")
                  }
                case Right(_) => fail("expected NeedKeys after batch 2, got Right")
              }
            case Right(_) => fail("expected NeedKeys after batch 1, got Right")
          }
        },
        // visitQueryByKey errors on duplicate contracts so we can only test supplying it with the deduped list
        transaction = mkTx(mkQueryByKey(Vector(id1, id2), key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id1, id2),
            globalKeyInputs =
              Map(key.gkey -> KeyMapping(queue = Vector(id1, id2), exhaustive = true)),
          )
        ),
      )
    )
  }

  // ** queryNByKey caching
  // *** queryNByKey same key twice
  "queryNByKeySameKeyTwiceCachingUnitTest" - {
    val key = "foo"
    val ids = Vector(1, 2, 3).map(cid)

    runUnitTest(
      UnitTest(
        description = s"queryNByKey(n=4) then queryNByKey(n=2) same key — cached",
        interaction = s =>
          for {
            s <- queryNByKeyAndReplyToNeedsKey(
              key = key,
              n = 4,
              replyToNeedsKeyWith = ids,
              state = s,
            )
            result <- s
              .queryNByKey(key, 2)
              .getOrElse(fail("unexpected NeedKeys on second queryNByKey")): ErrOr[
              (KeyMapping, LLState)
            ]
            (mp, s) = result
            _ = mp.queue shouldBe ids.take(2)
          } yield s,
        // visitQueryByKey does only support calling needskeys with final result
        transaction = mkTx(mkQueryByKey(ids, key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = ids.toSet,
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = ids, exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** queryNByKey inside try, rollback, then same key (reads survive rollback)
  "queryNByKeyTryRollbackQueryNByKeyUnitTest" - {
    val key = "foo"
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description =
          s"beginTry then queryNByKey then rollback then queryNByKey — reads survive rollback",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- queryNByKeyAndReplyToNeedsKey(
              key = key,
              n = 2,
              replyToNeedsKeyWith = Seq(id),
              state = s,
            )
            s <- rollbackTryForTesting(s)
            result <- s
              .queryNByKey(key, 1)
              .getOrElse(fail("unexpected NeedKeys after rollback")): ErrOr[
              (KeyMapping, LLState)
            ]
            (mp, s) = result
            _ = mp.queue shouldBe Vector(id)
          } yield s,
        // we cannot observe the needskeys or cached return, so we ommit the second query and observe that the state
        // returns as expected either way -- ensuring the read survived the rollback`
        transaction = mkRollbackTx(mkQueryByKey(Vector(id), key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** querynbykey twice but inconsistent order (HH-only)
  "querynbykeyTwiceSameKeyInconsistantOrderUnitTest" - {
    val key = "foo"
    val id1 = cid(1)
    val id2 = cid(2)

    runUnitTest(
      UnitTest(
        description =
          s"QueryByKey([cid1, cid2], exhaustive = true); QueryByKey([cid2, cid1], exhaustive = true)",
        transaction = mkTx(
          mkQueryByKey(Vector(id1, id2), key, exhaustive = true),
          mkQueryByKey(Vector(id2, id1), key, exhaustive = true),
        ),
        expected = Left(InconsistentContractKey(key)),
      )
    )
  }

  // *** querynbykey twice but varying (but consistent) exhaustiveness (HH-only)
  "querynbykeyTwiceSameKeyVaryingOrderUnitTest" - {
    val key = "foo"
    val id1 = cid(1)
    val id2 = cid(2)

    runUnitTest(
      UnitTest(
        description =
          s"QueryByKey([cid1, cid2], exhaustive = false); QueryByKey([cid1, cid2], exhaustive = true) (first query was for 2, second for > 2)",
        transaction = mkTx(
          mkQueryByKey(Vector(id1, id2), key, exhaustive = false),
          mkQueryByKey(Vector(id1, id2), key, exhaustive = true),
        ),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id1, id2),
            globalKeyInputs =
              Map(key.gkey -> KeyMapping(queue = Vector(id1, id2), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** querynbykey twice but inconsistent exhaustiveness (HH-only)
  "querynbykeyTwiceSameKeyVaryingExhaustiveUnitTest" - {
    val key = "foo"
    val id1 = cid(1)
    val id2 = cid(2)

    runUnitTest(
      UnitTest(
        description =
          s"QueryByKey([cid1, cid2], exhaustive = true); QueryByKey([cid1, cid2], exhaustive = false) (inconsistent)",
        transaction = mkTx(
          mkQueryByKey(Vector(id1, id2), key, exhaustive = true),
          mkQueryByKey(Vector(id1, id2), key, exhaustive = false),
        ),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id1, id2),
            globalKeyInputs =
              Map(key.gkey -> KeyMapping(queue = Vector(id1, id2), exhaustive = true)),
          )
        ),
      )
    )
  }

  // ** create, querybykey
  // *** create then querybykey
  "createQuerybykeyUnitTest" - {
    val key = "foo"
    val ids = Vector(1, 2, 3, 4).map(cid)

    runUnitTestQueryToN(
      maxN = 10,
      n =>
        UnitTest(
          description = s"create then querybykey",
          interaction = s =>
            for {
              s <- ids.foldLeftM[ErrOr, LLState](s) { (s, id) =>
                s.create(NodeId(0), id, key.some)
              }
              s <- s.queryNByKey(key, n) match {
                case Right(errOrResult) if n <= ids.size =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe ids.reverse.take(n).take(ids.size)
                  } yield s.asInstanceOf[State]
                case Left(nk) if n > ids.size =>
                  nk.n shouldBe (n - ids.size)
                  val Right(errOrResult) =
                    nk.resume(Seq.empty.view, NeedKeyProgression.Finished): @unchecked
                  for {
                    result <- errOrResult
                    (_, s) = result
                  } yield s
                case Right(_) => fail(s"expected NeedKeys for n=$n but got Right")
                case Left(nk) => fail(s"unexpected NeedKeys for n=$n: $nk")
              }
            } yield s,
          transaction = mkTx(
            (ids.map(mkCreate(_, key.some)) :+ mkQueryByKey(
              ids.reverse.take(n),
              key,
              exhaustive = n > ids.size,
            )).map(nodeToTx)*
          ),
          expected = Right(
            StateMachineResult.emptyWith(
              localKeys = Map(key.gkey -> ids.reverse),
              globalKeyInputs =
                if (n > ids.size)
                  Map(key.gkey -> KeyMapping(queue = Vector.empty, exhaustive = true))
                else Map.empty,
            )
          ),
        ),
    )
  }

  // *** querybykey then create
  "querybykeyCreateUnitTest" - {
    val key = "foo"
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"querybykey then create",
        interaction = s =>
          for {
            s <- {
              val Left(nk) = s.queryNByKey(key, 1): @unchecked
              nk.resume(Seq(id).view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe Vector(id)
                  } yield s
                case Left(nk) => fail(s"unexpected NeedKeys: $nk")
              }
            }
            s <- s.create(NodeId(0), id, key.some)
          } yield s,
        transaction = mkTx(
          mkQueryByKey(Vector(id), key, exhaustive = true),
          mkCreate(id, key.some),
        ),
        expected = Left(DuplicateContractId(id)),
      )
    )
  }

  // *** create then queryNByKey, ledger returns local cid
  "createThenQueryNByKeyLedgerReturnsLocalCidUnitTest" - {
    val key = "foo"
    val id1 = cid(1)

    runUnitTest(
      UnitTest(
        description = s"create(key=K), queryNByKey(K, 2), ledger returns same cid",
        interaction = s =>
          for {
            s <- s.create(NodeId(0), id1, key.some)
            s <- queryNByKeyAndReplyToNeedsKey(
              key = key,
              n = 2,
              replyToNeedsKeyWith = Seq(id1),
              state = s,
            )
          } yield s,
        expected = Left(DuplicateContractId(id1)),
        // we cannot test this with a transaction
      )
    )
  }

  // ** querybykey, archive
  // *** create then archive then querybykey
  "createArchiveQueryByKeyUnitTests" - {
    val key = "foo"
    val tmplId = tmplIdWithKey
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"create with key then archive then queryBykey",
        interaction = s =>
          for {
            s <- s.create(NodeId(0), id, key.some)
            s <- s.archive(tmplId, id, NodeId(1)).get
            s <- {
              val Left(nk) = s.queryNByKey(key, 1): @unchecked
              nk.resume(Seq(id).view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe Vector(id)
                  } yield s
                case Left(nk) => fail(s"unexpected NeedKeys: $nk")
              }
            }
          } yield s,
        expected = Left(DuplicateContractId(id)),
      )
    )
  }

  "createArchiveQueryByKeyHHUnitTests" - {
    val key = "foo"
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"create with key then archive then queryBykey",
        transaction = mkTx(
          mkCreate(id, key.some),
          mkExercise(id, key = key.some, consuming = true),
          mkQueryByKey(Vector(id), key, exhaustive = true),
        ),
        // for some reason, this gives InconsistentContractKey (but the important bit is that it gives a Left, the
        // reason is less important
        expected = Left(InconsistentContractKey(key)),
      )
    )
  }

  // *** querybykey then archive
  "queryByKeyCreateArchiveUnitTests" - {
    val key = "foo"
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"queryBykey then archive",
        interaction = s =>
          for {
            s <- {
              val Left(nk) = s.queryNByKey(key, 1): @unchecked
              nk.resume(Seq(id).view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe Vector(id)
                  } yield s
                case Left(nk) => fail(s"unexpected NeedKeys: $nk")
              }
            }
            s <- s.archive(tmplIdWithKey, id, NodeId(0)).get
          } yield s,
        transaction = mkTx(
          mkQueryByKey(Vector(id), key, exhaustive = true),
          mkExercise(id, key = key.some, consuming = true),
        ),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = Vector(id), exhaustive = true)),
            consumed = Set(id),
          )
        ),
      )
    )
  }

  //  ** archive, querybykey
  "createQueryByKeyArchiveQueryByKeyRollbackUnitTest" - {
    val key = "foo"
    val ids = Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(cid)
    val perm = Vector(10, 8, 7, 3, 6, 1, 9, 4, 2, 5).map(cid)
    def freshNodeId = {
      var i = 0
      () => {i += 1; NodeId(i)}
    }

    (0 to 10).foreach { nrToArchive =>
      val toArchive = perm.take(nrToArchive)
      val toArchiveSet = toArchive.toSet
      Seq(1, 3, 5, 10).foreach { n =>
        val nrArchivedInQuery = ids.reverse.take(n).count(toArchiveSet)
        val n2 = n - nrArchivedInQuery
        if (n2 > 0) {
          s"query with n=$n" - {
            runUnitTest(
              UnitTest(
                description =
                  s"create 10 then queryBykey (n = $n) then archiving $nrToArchive some then queryByKey" +
                    s" (n = $n2)",
                interaction = s =>
                  for {
                    s <- ids.foldLeft[ErrOr[LLState]](Right(s)) { (acc, id) =>
                      acc.flatMap(_.create(freshNodeId(), id, key.some))
                    }
                    result <- s
                      .queryNByKey(key, n)
                      .getOrElse(fail("unexpected NeedContract")): ErrOr[
                      (KeyMapping, LLState)
                    ]
                    (km1, s) = result
                    s <- toArchive.foldLeft[ErrOr[LLState]](Right(s)) { (acc, id) =>
                      acc.flatMap(_.archive(tmplIdWithKey, id, freshNodeId()).get)
                    }
                    result <- s
                      .queryNByKey(key, n2)
                      .getOrElse(fail("unexpected NeedContract")): ErrOr[
                      (KeyMapping, LLState)
                    ]
                    (km2, s) = result
                    _ = km1.queue.filterNot(toArchiveSet) shouldBe km2.queue
                  } yield s,
                transaction = mkTx(
                  (ids.map(mkCreate(_, key.some))
                    :+ mkQueryByKey(ids.reverse.take(n), key, exhaustive = false)
                    :++ toArchive.map(mkExercise(_, key = key.some, consuming = true))
                    :+ mkQueryByKey(
                      ids.reverse.take(n).filterNot(toArchiveSet),
                      key,
                      exhaustive = false,
                    )).map(nodeToTx)*
                ),
                expected = Right(
                  StateMachineResult.emptyWith(
                    localKeys = Map(key.gkey -> ids.reverse),
                    consumed = perm.take(nrToArchive).toSet,
                  )
                ),
              )
            )
          }
        }
      }
    }
  }

  // ** querybykey, trycatch
  // *** try querybykey endtry
  "beginTryQueryByKeyEndTryUnitTest" - {
    val key = "foo"
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"beginTry then queryBykey then endTry",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- {
              val Left(nk) = s.queryNByKey(key, 1): @unchecked
              nk.resume(Seq(id).view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe Vector(id)
                  } yield s
                case Left(nk) => fail(s"unexpected NeedKeys: $nk")
              }
            }
            s <- Right(s.endTry)
          } yield s,
        transaction = mkTx(mkQueryByKey(Vector(id), key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** try querybykey rollback
  "beginTryQueryByKeyRollbackUnitTest" - {
    val key = "foo"
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"beginTry then queryBykey then rollbackTry",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- {
              val Left(nk) = s.queryNByKey(key, 1): @unchecked
              nk.resume(Seq(id).view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe Vector(id)
                  } yield s
                case Left(nk) => fail(s"unexpected NeedKeys: $nk")
              }
            }
            s <- rollbackTryForTesting(s)
          } yield s,
        transaction = mkRollbackTx(mkQueryByKey(Vector(id), key, exhaustive = true)),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // ** querybyid, querybykey
  // *** queryById then queryByKey
  "queryByKeyQueryNByKeyUnittest" - {
    val key = "foo"
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"querybyid then queryByKey with needskey call",
        interaction = s =>
          for {
            s <- {
              val Left(NeedContract(resume)) = s.queryById(tmplIdWithKey, id): @unchecked;
              resume(key.some)
            }
            s <- {
              val Left(nk) = s.queryNByKey(key, 1): @unchecked
              nk.resume(Seq(id).view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe Vector(id)
                  } yield s
                case Left(nk) => fail(s"unexpected NeedKeys: $nk")
              }
            }
          } yield s,
        transaction = mkTx(
          mkFetch(id, key = key.some, byKey = false),
          mkQueryByKey(Vector(id), key, exhaustive = true),
        ),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** queryByKey then queryById
  "queryNByKeyQueryByKeyUnittest" - {
    val key = "foo"
    val id = cid(1)

    runUnitTest(
      UnitTest(
        description = s"queryNByKey then queryById",
        interaction = s =>
          for {
            s <- {
              val Left(nk) = s.queryNByKey(key, 1): @unchecked
              nk.resume(Seq(id).view, NeedKeyProgression.Finished) match {
                case Right(errOrResult) =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe Vector(id)
                  } yield s
                case Left(nk) => fail(s"unexpected NeedKeys: $nk")
              }
            }
            s <- s.queryById(tmplIdWithKey, id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState]
          } yield s,
        transaction = mkTx(
          mkQueryByKey(Vector(id), key, exhaustive = true),
          mkFetch(id, key = key.some, byKey = false),
        ),
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key.gkey -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // * special scenarios
  // ** queryNByKey
  // *** inconsistent querybyid/queryNByKey
  // **** positive case: queryById reports contract+key EXISTS that queryNByKey DID NOT find
  "inconsistentQueryByIdBetweenQueryNByKeyUnittest" - {
    "positive case" - {
      val key = "foo"
      val id1 = cid(1)
      val id2 = cid(2)

      runUnitTest(
        UnitTest(
          description =
            s"inconsistent case: queryNByKey(${key}, 2) finding [cid1] (exhaustive) then queryById finds " +
              s"cid2 with key=${key}",
          interaction = s =>
            for {
              s <- queryNByKeyAndReplyToNeedsKey(
                key = key,
                n = 2,
                replyToNeedsKeyWith = Seq(id1),
                state = s,
              )
              s <- {
                val Left(NeedContract(resume)) = s.queryById(tmplIdWithKey, id2): @unchecked;
                resume(key.some)
              }
            } yield s,
          transaction = mkTx(
            mkQueryByKey(Vector(id1), key, exhaustive = true),
            mkFetch(id2, key = key.some, byKey = false),
          ),
          expected = Left(InconsistentContractKey(key)),
        )
      )
    }

    // **** negative case: queryNByKey reports contract+key EXISTS that queryById DID NOT find
    "negative case" - {
      val key = "foo"
      val id1 = cid(1)
      val id2 = cid(2)

      runUnitTest(
        UnitTest(
          description =
            s"inconsistent case: queryById(1) finds a contract with key ${key}, then queryNByKey(${key}, 2) finds [cid2] " +
              s"(exshaustive) (cid1 missing) " +
              s"finds cid2 with key=${key}",
          interaction = s =>
            for {
              s <- {
                val Left(NeedContract(resume)) = s.queryById(tmplIdWithKey, id1): @unchecked;
                resume(key.some)
              }
              s <- queryNByKeyAndReplyToNeedsKey(
                key = key,
                n = 2,
                replyToNeedsKeyWith = Seq(id2),
                state = s,
              )
            } yield s,
          transaction = mkTx(
            mkFetch(id1, key = key.some, byKey = false),
            mkQueryByKey(Vector(id2), key, exhaustive = true),
          ),
          expected = Left(InconsistentContractKey(key)),
        )
      )
    }
  }

  /** Collect all contract IDs referenced by the nodes in a transaction. */
  private def allContractIds[Tx](tx: HasTxNodes[Tx]): Set[V.ContractId] =
    tx.localContractIds ++ tx.inputContracts

  "contractOrder" - {

    "is complete w.r.t. all contracts in the transaction" - {

      "empty transaction" in {
        val tx = mkTx()
        walkTransactionOnHHState(tx).map(_.contractOrder) shouldBe Right(Nil)
      }

      "transaction with only creates" in {
        val tx = mkTx(
          mkCreate(cid(1)),
          mkCreate(cid(2)),
          mkCreate(cid(3)),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder
          allContractIds(tx).foreach(c => order should contain(c))
          order.toSet shouldBe allContractIds(tx)
        }
      }

      "transaction with fetches (input contracts without keys)" in {
        val tx = mkTx(
          mkFetch(cid(1)),
          mkFetch(cid(2)),
          mkFetch(cid(3)),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder
          allContractIds(tx).foreach(c => order should contain(c))
          order.toSet shouldBe allContractIds(tx)
        }
      }

      "transaction with creates, fetches, and queryByKey" in {
        val key1 = "key1"
        val key2 = "key2"
        val tx = mkTx(
          mkCreate(cid(1), key = Some(key1)),
          mkFetch(cid(2)),
          mkQueryByKey(Vector(cid(3)), key2, exhaustive = false),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder
          allContractIds(tx).foreach(c => order should contain(c))
          order.toSet shouldBe allContractIds(tx)
        }
      }

      "transaction with input contracts with and without keys" in {
        val key1 = "key1"
        val tx = mkTx(
          mkFetch(cid(1)),
          mkQueryByKey(Vector(cid(2), cid(3)), key1, exhaustive = false),
          mkFetch(cid(4)),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder
          val expected = Set(cid(1), cid(2), cid(3), cid(4))
          order.toSet shouldBe expected
          // no duplicates
          order.size shouldBe order.toSet.size
        }
      }
    }

    "is consistent with QueryByKey result ordering" - {

      "single queryByKey node preserves result order" in {
        val key1 = "key1"
        val tx = mkTx(
          mkQueryByKey(Vector(cid(3), cid(1), cid(2)), key1, exhaustive = false),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder.zipWithIndex.toMap
          // The relative order of cid(3), cid(1), cid(2) must be preserved
          val idx3 = order(cid(3))
          val idx1 = order(cid(1))
          val idx2 = order(cid(2))
          idx3 should be < idx1
          idx1 should be < idx2
        }
      }

      "multiple queryByKey nodes with different keys preserve result order within each key" in {
        val key1 = "key1"
        val key2 = "key2"
        val tx = mkTx(
          mkQueryByKey(Vector(cid(2), cid(1)), key1, exhaustive = false),
          mkQueryByKey(Vector(cid(4), cid(3)), key2, exhaustive = false),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder.zipWithIndex.toMap

          // Within key1: cid(2) before cid(1)
          order(cid(2)) should be < order(cid(1))

          // Within key2: cid(4) before cid(3)
          order(cid(4)) should be < order(cid(3))
        }
      }

      "queryByKey contracts appear after input contracts without key" in {
        val key1 = "key1"
        val tx = mkTx(
          mkFetch(cid(1)),
          mkQueryByKey(Vector(cid(2)), key1, exhaustive = false),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder.zipWithIndex.toMap
          // cid(1) has no key, cid(2) has a key -> cid(1) should come first
          order(cid(1)) should be < order(cid(2))
        }
      }

      "input contracts without key appear first and are sorted by contract ID" in {
        val key1 = "key1"
        // Fetch keyless contracts in non-sorted order
        val tx = mkTx(
          mkFetch(cid(3)),
          mkFetch(cid(1)),
          mkFetch(cid(5)),
          mkQueryByKey(Vector(cid(4)), key1, exhaustive = false),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder
          // Keyless inputs should all come before keyed input
          order.indexOf(cid(3)) should be < order.indexOf(cid(4))
          order.indexOf(cid(1)) should be < order.indexOf(cid(4))
          order.indexOf(cid(5)) should be < order.indexOf(cid(4))
          // Keyless inputs should be sorted by contract ID: cid(1) < cid(3) < cid(5)
          order.indexOf(cid(1)) should be < order.indexOf(cid(3))
          order.indexOf(cid(3)) should be < order.indexOf(cid(5))
        }
      }

      "local contracts appear before input contracts" in {
        val key1 = "key1"
        val tx = mkTx(
          mkFetch(cid(1)),
          mkCreate(cid(2)),
          mkQueryByKey(Vector(cid(3)), key1, exhaustive = false),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder.zipWithIndex.toMap
          // Local contract cid(2) should appear before input contracts cid(1) and cid(3)
          order(cid(2)) should be < order(cid(1))
          order(cid(2)) should be < order(cid(3))
        }
      }

      "local contracts are ordered most recent first" in {
        val tx = mkTx(
          mkCreate(cid(1)),
          mkCreate(cid(2)),
          mkCreate(cid(3)),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder.zipWithIndex.toMap
          // Most recent first: cid(3) before cid(2) before cid(1)
          order(cid(3)) should be < order(cid(2))
          order(cid(2)) should be < order(cid(1))
        }
      }

      "fetchById with key places contract in onlyQueriedById within its key group" in {
        val key1 = "key1"
        // cid(1) fetched by id (with key), cid(2) queried by key
        val tx = mkTx(
          mkFetch(cid(1), key = Some(key1), byKey = false),
          mkQueryByKey(Vector(cid(2)), key1, exhaustive = false),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder.zipWithIndex.toMap
          // Both cid(1) and cid(2) have key1.
          // In the key group: queriedByKey (cid(2)) comes first, then onlyQueriedById (cid(1))
          order(cid(2)) should be < order(cid(1))
        }
      }

      "exhaustive queryByKey with empty result still produces complete order" in {
        val key1 = "key1"
        val tx = mkTx(
          mkFetch(cid(1)),
          mkQueryByKey(Vector.empty, key1, exhaustive = true),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder
          order should contain(cid(1))
          order.size shouldBe 1
        }
      }

      "no duplicates when contract appears in both fetch and queryByKey" in {
        val key1 = "key1"
        val tx = mkTx(
          mkFetch(cid(1), key = Some(key1), byKey = false),
          mkQueryByKey(Vector(cid(1)), key1, exhaustive = false),
        )
        inside(walkTransactionOnHHState(tx)) { case Right(s) =>
          val order = s.contractOrder
          order.size shouldBe order.toSet.size
          order should contain(cid(1))
        }
      }
    }
  }

  case class UnitTest(
      description: String,
      interaction: Option[LLState => ErrOr[LLState]],
      expected: Map[NextGenContractStateMachine.Mode, OptStateMachineResult],
      // when both an interaction and transaction are supplied, evaluating the set transactino should lead to the same
      // expected result. Note that there is no 1-to-1 mapping between interactions and transactions.
      transaction: Option[HasTxNodes[?]] = None,
  )

  object UnitTest {
    def apply(
        description: String,
        interaction: LLState => Either[TransactionError, LLState],
        expected: OptStateMachineResult,
        transaction: HasTxNodes[?],
    ): UnitTest =
      UnitTest(description, Some(interaction), allModesMap(expected), Some(transaction))

    def apply(
        description: String,
        interaction: LLState => Either[TransactionError, LLState],
        expected: OptStateMachineResult,
    ): UnitTest =
      UnitTest(description, Some(interaction), allModesMap(expected))

    def apply(
        description: String,
        interaction: LLState => Either[TransactionError, LLState],
        expected: Map[NextGenContractStateMachine.Mode, OptStateMachineResult],
    ): UnitTest =
      UnitTest(description, Some(interaction), expected)

    def apply(
        description: String,
        interaction: LLState => Either[TransactionError, LLState],
        expected: Map[NextGenContractStateMachine.Mode, OptStateMachineResult],
        transaction: HasTxNodes[?],
    ): UnitTest =
      UnitTest(description, Some(interaction), expected, Some(transaction))

    def apply(
        description: String,
        expected: OptStateMachineResult,
        transaction: HasTxNodes[?],
    ): UnitTest =
      UnitTest(description, None, allModesMap(expected), Some(transaction))

    def allModesMap(expected: OptStateMachineResult) =
      Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
        NextGenContractStateMachine.Mode.NUCK -> expected,
        NextGenContractStateMachine.Mode.NoKey -> expected,
      )
  }

}

object NextGenContractStateMachineSpec {

  private case object TestToken extends NeedKeyProgression.Token

  class TxBuilder extends NodeIdTransactionBuilder with TestNodeBuilder

}
