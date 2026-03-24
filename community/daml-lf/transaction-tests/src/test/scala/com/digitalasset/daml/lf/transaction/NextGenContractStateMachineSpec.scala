// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.ContractStateMachineSpec.*
import com.digitalasset.daml.lf.transaction.NextGenContractStateMachine.{
  LLState,
  State,
  StateMachineResult,
}
import com.digitalasset.daml.lf.transaction.TransactionError.{
  AlreadyConsumed,
  DuplicateContractId,
  DuplicateContractKey,
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
import cats.syntax.foldable.*
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
          globalKey("k0") -> Vector(),
          globalKey("k1") -> Vector(cid(1)),
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
          globalKey("k0") -> Vector(),
          globalKey("k1") -> Vector(),
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
  val templateId: Ref.TypeConId = "Template:Id"
  val choiceId: Ref.ChoiceName = "Choice"
  val pkgName: Ref.PackageName = Ref.PackageName.assertFromString("package-name")
  val txVersion: SerializationVersion = SerializationVersion.maxVersion
  val unit: V = V.ValueUnit

  def contractKeyInputs[Tx](
      tx: HasTxNodes[Tx]
  ): Either[TransactionError, Map[GlobalKey, Vector[V.ContractId]]] =
    tx.foldInExecutionOrder[Either[TransactionError, NextGenContractStateMachine.LLState[NodeId]]](
      Right(NextGenContractStateMachine.empty[NodeId]())
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

  private def toOptKeyWithMaintainers(
      templateId: Ref.TypeConId,
      key: String,
  ): Option[GlobalKeyWithMaintainers] =
    if (key.isEmpty) None
    else Some(toKeyWithMaintainers(templateId, key))

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
        templateId,
        pkgName,
        V.ValueInt64(keyId.toLong),
        crypto.Hash.hashPrivateKey(s"key-$keyId"),
      )

    val start = Right(NextGenContractStateMachine.empty[Int](authorizeRollBack = false))
    val cid1 = toContractId(1)
    // val cid2 = toContractId(2)
    val key1 = toGlobalKey(1)

    "reject when a contract is return by a fetch-by-id and but not by subsequent queryByKey" in {
      val r = for {
        state <- start
        state <- state.visitFetchById(cid1, mbKey = Some(key1))
        state <- state.visitQueryByKey(key1, result = Vector(), exhaustive = true)
      } yield state
      r shouldBe a[Left[?, ?]]
    }

    "reject when a contract is not return by a queryByKey but by a subsequent fetch-by-id" in {
      val r = for {
        state <- start
        state <- state.visitQueryByKey(key1, result = Vector(), exhaustive = true)
        state <- state.visitFetchById(cid1, mbKey = Some(key1))
      } yield state
      r shouldBe a[Left[?, ?]]
    }

    "reject when a contract archive-by-id is then queryByKey" in {
      val r = for {
        state <- start
        state <- state.visitExercise(1, cid1, mbKey = Some(key1), byKey = false, consuming = true)
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
  def rollbackTryForTesting[A](s: LLState[A]): ErrOr[LLState[A]] =
    s.rollbackTry.left.map(_ => EffectfulRollback(Set.empty))

  def ignoreUnitTest(unitTest: UnitTest): Unit =
    unitTest.description - {
      unitTest.expected.foreach { case (mode, _) =>
        s"mode $mode" ignore {}
      }
    }

  def runUnitTestWithoutAndWithKey(
      key: GlobalKey,
      mkUnitTest: Option[GlobalKey] => UnitTest,
  ): Unit = {
    "with key unset" - {
      runUnitTest(mkUnitTest(None))
    }
    "with key set" - {
      runUnitTest(mkUnitTest(Some(key)))
    }
  }

  def runUnitTest(unitTest: UnitTest): Unit =
    unitTest.description - {
      unitTest.expected.foreach { case (mode, expectedResult) =>
        s"mode $mode" in {
          val fresh = NextGenContractStateMachine.empty[Unit](mode)
          val result = unitTest.interaction.apply(fresh).map(_.toStateMachineResult)

          (result, expectedResult) match {
            case (Left(err1), Left(err2)) => err1 shouldBe err2
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
                state.consumed shouldBe res.consumed
              }
            case _ => fail(s"$result was not equal to $expectedResult")
          }
        }
      }
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
      state: LLState[Unit],
  ): ErrOr[LLState[Unit]] = {
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
            s <- Right(s.beginTry): ErrOr[LLState[Unit]]
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
            s <- s.create((), id, None)
          } yield s,
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // ** double-create
  /// ** double-create (same cid)
  "doubleCreateSameCidUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then create same cid same key",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- s.create((), id, optkey)
            } yield s,
          expected = Left(DuplicateContractId(id)),
        )
      },
    )
  }

  /// ** double-create (varying cid)
  "doubleCreateVaryingCidUnitTests" - {
    val id1 = cid(1)
    val id2 = cid(2)
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then create new cid same key",
          interaction = s =>
            for {
              s <- s.create((), id1, optkey)
              s <- s.create((), id2, optkey)
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key -> Vector(id2, id1))
            )
          ),
        )
      },
    )
  }

  // *** double-create (same cid, different key)
  "createSameCidDifferentKeysUnitTest" - {
    val id = cid(1)
    val key1 = gkey("foo")
    val key2 = gkey("bar")

    runUnitTest(
      UnitTest(
        description = s"create(key=K1) then create same cid with key=K2",
        interaction = s =>
          for {
            s <- s.create((), id, Some(key1))
            s <- s.create((), id, Some(key2))
          } yield s,
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
            s <- s.create((), id, None)
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
            s <- s.create((), id, None)
            s <- rollbackTryForTesting(s)
          } yield s,
        Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
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
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"beginTry then create(key=K) then rollback, then create(key=K)",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- s.create((), id1, Some(key))
            s <- rollbackTryForTesting(s)
            s <- s.create((), id2, Some(key))
          } yield s,
        Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
          NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
          NextGenContractStateMachine.Mode.NoKey -> Right(
            StateMachineResult.emptyWith(
              localKeys = Map(key -> Vector(id2))
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
          s.archive(id, ()).isEmpty shouldBe true
          Right(s)
        },
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // ** create archive
  "createArchiveUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description = s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- s.archive(id, ()).get
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              consumed = Set[V.ContractId](id),
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key -> Vector(id)),
            )
          ),
        )
      },
    )
  }

  // ** double-create archive
  "createArchiveCreateUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive then create same",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- s.archive(id, ()).get
              s <- s.create((), id, optkey)
            } yield s,
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
            s <- s.create((), id, None)
            s <- s.archive(id, ()).get
            s <- s.archive(id, ()).get
          } yield s,
        expected = Left(AlreadyConsumed(id, ())),
      )
    )
  }

  // ** create, archive, trycatch
  // *** create try archive endtry
  "createBeginTryArchiveEndTryUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then begintry then archive then endtry",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(id, ()).get
              s <- Right(s.endTry)
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              consumed = Set[V.ContractId](id),
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key -> Vector(id)),
            )
          ),
        )
      },
    )
  }

  // *** create try archive rollback
  "createBeginTryArchiveRollbackUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then begintry then archive then rollback",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(id, ()).get
              s <- rollbackTryForTesting(s)
            } yield s,
          Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
            NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
            NextGenContractStateMachine.Mode.NoKey -> Right(
              StateMachineResult.emptyWith(
                localKeys =
                  if (optkey.isEmpty) Map.empty
                  else
                    Map(key -> Vector(id))
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
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then begintry then archive then endtry then" +
              s" create same",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(id, ()).get
              s <- Right(s.endTry)
              s <- s.create((), id, optkey)
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
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then begintry then archive then rollback " +
              s"then create same",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(id, ()).get
              s <- rollbackTryForTesting(s)
              s <- s.create((), id, optkey)
            } yield s,
          Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
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
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"begintry then create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive then endtry",
          interaction = s =>
            for {
              s <- Right(s.beginTry)
              s <- s.create((), id, optkey)
              s <- s.archive(id, ()).get
              s <- Right(s.endTry)
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              consumed = Set[V.ContractId](id),
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key -> Vector(id)),
            )
          ),
        )
      },
    )
  }

  // *** try create archive rollback
  "beginTryCreateArchiveRollbackUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"begintry then create (${if (optkey.isEmpty) "with" else "without"} key) then archive then rollback",
          interaction = s =>
            for {
              s <- Right(s.beginTry)
              s <- s.create((), id, optkey)
              s <- s.archive(id, ()).get
              s <- rollbackTryForTesting(s)
            } yield s,
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
            s <- s.create((), id, None)
            s <- Right(s.beginTry)
            s <- s.archive(id, ()).get
            s <- rollbackTryForTesting(s)
            s <- s.archive(id, ()).get
          } yield s,
        Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
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
    val key = gkey("foo")
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"queryById (continuation with key ${if (optkey.isEmpty) "unset" else "set"})",
          interaction = s =>
            for {
              s <- {
                val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
                resume(optkey)
              }
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

  // ** queryById same contract twice (caching)
  "queryByIdTwiceCachingUnitTest" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"queryById then queryById same cid — cached, no NeedContract",
        interaction = s =>
          for {
            s <- {
              val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
              resume(Some(key))
            }
            s <- s
              .queryById(id)
              .getOrElse(fail("unexpected NeedContract on second queryById")): ErrOr[LLState[Unit]]
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
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then queryById",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- s.queryById(id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState[Unit]]
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              localKeys =
                if (optkey.isEmpty) Map.empty
                else
                  Map(key -> Vector(id))
            )
          ),
        )
      },
    )
  }

  // *** querybyid then create
  "queryByIdCreateUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"queryById then create (with key ${if (optkey.isEmpty) "unset" else "set"})",
          interaction = s =>
            for {
              s <- {
                val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
                resume(optkey)
              }
              s <- s.create((), id, optkey)
            } yield s,
          expected = Left(DuplicateContractId(id)),
        )
      },
    )
  }

  // ** create, archive, querybyid
  "createArchiveQueryByIdUnitTest" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"create with key then archive then queryById",
        interaction = s =>
          for {
            s <- s.create((), id, Some(key))
            s <- s.archive(id, ()).get
            s <- s.queryById(id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState[Unit]]
          } yield s,
        expected = Left(AlreadyConsumed(id, ())),
      )
    )
  }

  // ** querybyid, trycatch
  // *** try querybyid endtry
  "beginTryQueryByIdEndTryUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")
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
                val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
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
    val key = gkey("foo")
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
                val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
                resume(optkey)
              }
              s <- rollbackTryForTesting(s)
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

  // *** try querybyid rollback (reads survive rollback)
  "queryByIdTryRollbackQueryByIdUnitTest" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description =
          s"beginTry then queryById then rollback then queryById — reads survive rollback",
        interaction = s =>
          for {
            s <- Right(s.beginTry)
            s <- {
              val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
              resume(Some(key))
            }
            s <- rollbackTryForTesting(s)
            s <- s
              .queryById(id)
              .getOrElse(fail("unexpected NeedContract after rollback")): ErrOr[LLState[Unit]]
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
    val key = gkey("foo")
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"queryById (continuation with key ${if (optkey.isEmpty) "unset" else "set"}) then archive",
          interaction = s =>
            for {
              s <- {
                val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
                resume(optkey)
              }
              s <- s.archive(id, ()).get
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
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"queryById then archive then queryById same cid",
        interaction = s =>
          for {
            s <- {
              val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
              resume(Some(key))
            }
            s <- s.archive(id, ()).get
            s <- s.queryById(id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState[Unit]]
          } yield s,
        expected = Left(AlreadyConsumed(id, ())),
      )
    )
  }

  // ** create, archive, querybyid
  "createArchiveQueryByIdUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then archive then queryById with same key",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- s.archive(id, ()).get
              s <- s.queryById(id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState[Unit]]
            } yield s,
          expected = Left(AlreadyConsumed(id, ())),
        )
      },
    )
  }

  // ** create, archive, querybyid, trycatch
  // *** create, archive, querybyid, endtry
  "createBeginTryArchiveEndTryQueryByIdUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {
        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then beginTry then archive then endTry " +
              s"queryById with same key",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(id, ()).get
              s <- Right(s.endTry)
              s <- s.queryById(id).getOrElse(fail("unexpected NeedContract")): ErrOr[State[Unit]]
            } yield s,
          expected = Left(AlreadyConsumed(id, ())),
        )
      },
    )
  }

  // *** create, archive, querybyid, rollback
  "createBeginTryArchiveRollbackQueryByIdUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")
    runUnitTestWithoutAndWithKey(
      key,
      optkey => {

        UnitTest(
          description =
            s"create (with key ${if (optkey.isEmpty) "unset" else "set"}) then beginTry then archive then rollbackTry" +
              s" queryById with same key",
          interaction = s =>
            for {
              s <- s.create((), id, optkey)
              s <- Right(s.beginTry)
              s <- s.archive(id, ()).get
              s <- rollbackTryForTesting(s)
              s <- s.queryById(id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState[Unit]]
            } yield s,
          Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
            NextGenContractStateMachine.Mode.NUCK -> Left(EffectfulRollback(Set.empty)),
            NextGenContractStateMachine.Mode.NoKey -> Right(
              StateMachineResult.emptyWith(
                localKeys =
                  if (optkey.isEmpty) Map.empty
                  else
                    Map(key -> Vector(id))
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
    val key = gkey("foo")
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
              s <- s.create((), id, optkey)
              s <- s.archive(id, ()).get
              s <- rollbackTryForTesting(s)
              s <- s.queryById(id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState[Unit]]
            } yield s,
          Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
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
    val key = gkey("foo")

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
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** querybykey (varying n)
  "querybykeyUnitTest" - {
    val key = gkey("foo")
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
          expected = Right(
            StateMachineResult.emptyWith(
              inputContractIds = ids.take(n).toSet,
              globalKeyInputs = Map(key -> KeyMapping(queue = ids.take(n), exhaustive = true)),
            )
          ),
        ),
    )
  }

  // *** negative querynbykey
  "querybykeyNegativeUnitTest" - {
    val key = gkey("foo")

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
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"queryNByKey with n=0",
        interaction = s => {
          an[IllegalArgumentException] should be thrownBy s.queryNByKey(key, 0)
          Right(s)
        },
        expected = Right(StateMachineResult.empty),
      )
    )
  }

  // *** queryNByKey empty exhaustive search
  "queryNByKeyEmptyExhaustiveUnitTest" - {
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"queryNByKey(n=3) returning [] + Finished",
        interaction =
          s => queryNByKeyAndReplyToNeedsKey(key = key, n = 3, replyToNeedsKeyWith = Seq.empty, state = s),
        expected = Right(
          StateMachineResult.emptyWith(
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector.empty, exhaustive = true))
          )
        ),
      )
    )
  }

  // ** queryNByKey continuation
  // *** queryNByKey with InProgress (multi-step)
  "queryNByKeyMultiStepContinuationUnitTest" - {
    val key = gkey("foo")
    val ids = Vector(1, 2, 3, 4, 5).map(cid)
    val firstBatch = ids.take(3)
    val secondBatch = ids.drop(3)

    case object TestToken extends NeedKeyContinuationToken

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
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = ids.toSet,
            globalKeyInputs = Map(key -> KeyMapping(queue = ids, exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** queryNByKey continuation returns more results than requested
  "queryNByKeyContinuationExtraResultsUnitTest" - {
    val key = gkey("foo")
    val ids = Vector(1, 2, 3, 4, 5).map(cid)
    val firstBatch = ids.take(1)
    val secondBatch = ids.drop(1)
    val firstThree = ids.take(3)

    case object TestToken extends NeedKeyContinuationToken

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
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = firstThree.toSet,
            globalKeyInputs = Map(key -> KeyMapping(queue = firstThree, exhaustive = false)),
          )
        ),
      )
    )
  }

  // *** queryNByKey with duplicates across batches (dedup)
  "queryNByKeyDuplicateInSingleResponseUnitTest" - {
    val key = gkey("foo")
    val id1 = cid(1)
    val id2 = cid(2)

    case object TestToken extends NeedKeyContinuationToken

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
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id1, id2),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id1, id2), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** queryNByKey with duplicate spanning two batches
  "queryNByKeyDuplicateAcrossBatchesUnitTest" - {
    val key = gkey("foo")
    val id1 = cid(1)
    val id2 = cid(2)

    case object TestToken extends NeedKeyContinuationToken

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
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id1, id2),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id1, id2), exhaustive = true)),
          )
        ),
      )
    )
  }

  // ** queryNByKey caching
  // *** queryNByKey same key twice
  "queryNByKeySameKeyTwiceCachingUnitTest" - {
    val key = gkey("foo")
    val ids = Vector(1, 2, 3).map(cid)

    runUnitTest(
      UnitTest(
        description = s"queryNByKey(n=4) then queryNByKey(n=2) same key — cached",
        interaction = s =>
          for {
            s <- queryNByKeyAndReplyToNeedsKey(key = key, n = 4, replyToNeedsKeyWith = ids, state = s)
            result <- s
              .queryNByKey(key, 2)
              .getOrElse(fail("unexpected NeedKeys on second queryNByKey")): ErrOr[
              (KeyMapping, LLState[Unit])
            ]
            (mp, s) = result
            _ = mp.queue shouldBe ids.take(2)
          } yield s,
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = ids.toSet,
            globalKeyInputs = Map(key -> KeyMapping(queue = ids, exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** queryNByKey inside try, rollback, then same key (reads survive rollback)
  "queryNByKeyTryRollbackQueryNByKeyUnitTest" - {
    val id = cid(1)
    val key = gkey("foo")

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
              (KeyMapping, LLState[Unit])
            ]
            (mp, s) = result
            _ = mp.queue shouldBe Vector(id)
          } yield s,
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // ** create, querybykey
  // *** create then querybykey
  "createQuerybykeyUnitTest" - {
    val ids = Vector(1, 2, 3, 4).map(cid)
    val key = gkey("foo")

    runUnitTestQueryToN(
      maxN = 10,
      n =>
        UnitTest(
          description = s"create then querybykey",
          interaction = s =>
            for {
              s <- ids.foldLeftM[ErrOr, LLState[Unit]](s) { (s, id) =>
                s.create((), id, Some(key))
              }
              s <- s.queryNByKey(key, n) match {
                case Right(errOrResult) if n <= ids.size =>
                  for {
                    result <- errOrResult
                    (mp, s) = result
                    _ = mp.queue shouldBe ids.reverse.take(n).take(ids.size)
                  } yield s.asInstanceOf[State[Unit]]
                case Left(nk) if n > ids.size =>
                  nk.n shouldBe (n - ids.size)
                  Right(s)
                case Right(_) => fail(s"expected NeedKeys for n=$n but got Right")
                case Left(nk) => fail(s"unexpected NeedKeys for n=$n: $nk")
              }
            } yield s,
          expected = Right(
            StateMachineResult.emptyWith(
              localKeys = Map(key -> ids.reverse)
            )
          ),
        ),
    )
  }

  // *** querybykey then create
  "querybykeyCreateUnitTest" - {
    val id = cid(1)
    val key = gkey("foo")

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
            s <- s.create((), id, Some(key))
          } yield s,
        expected = Left(DuplicateContractId(id)),
      )
    )
  }

  // *** create then queryNByKey, ledger returns local cid
  "createThenQueryNByKeyLedgerReturnsLocalCidUnitTest" - {
    val id1 = cid(1)
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"create(key=K), queryNByKey(K, 2), ledger returns same cid",
        interaction = s =>
          for {
            s <- s.create((), id1, Some(key))
            s <- queryNByKeyAndReplyToNeedsKey(
              key = key,
              n = 2,
              replyToNeedsKeyWith = Seq(id1),
              state = s,
            )
          } yield s,
        expected = Left(DuplicateContractId(id1)),
      )
    )
  }

  // ** querybykey, archive
  // *** create then archive then querybykey
  "createArchiveQueryByKeyUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"create with key then archive then queryBykey",
        interaction = s =>
          for {
            s <- s.create((), id, Some(key))
            s <- s.archive(id, ()).get
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

  // *** querybykey then archive
  "queryByKeyCreateArchiveUnitTests" - {
    val id = cid(1)
    val key = gkey("foo")

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
            s <- s.archive(id, ()).get
          } yield s,
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id), exhaustive = true)),
            consumed = Set(id),
          )
        ),
      )
    )
  }

  //  ** archive, querybykey
  "createQueryByKeyArchiveQueryByKeyRollbackUnitTest" - {
    val ids = Vector(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(cid)
    val perm = Vector(10, 8, 7, 3, 6, 1, 9, 4, 2, 5).map(cid)
    val key = gkey("foo")

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
                    s <- ids.foldLeft[ErrOr[LLState[Unit]]](Right(s)) { (acc, id) =>
                      acc.flatMap(_.create((), id, Some(key)))
                    }
                    result <- s
                      .queryNByKey(key, n)
                      .getOrElse(fail("unexpected NeedContract")): ErrOr[
                      (KeyMapping, LLState[Unit])
                    ]
                    (km1, s) = result
                    s <- toArchive.foldLeft[ErrOr[LLState[Unit]]](Right(s)) { (acc, id) =>
                      acc.flatMap(_.archive(id, ()).get)
                    }
                    result <- s
                      .queryNByKey(key, n2)
                      .getOrElse(fail("unexpected NeedContract")): ErrOr[
                      (KeyMapping, LLState[Unit])
                    ]
                    (km2, s) = result
                    _ = km1.queue.filterNot(toArchiveSet) shouldBe km2.queue
                  } yield s,
                expected = Right(
                  StateMachineResult.emptyWith(
                    localKeys = Map(key -> ids.reverse),
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
    val id = cid(1)
    val key = gkey("foo")

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
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** try querybykey rollback
  "beginTryQueryByKeyRollbackUnitTest" - {
    val id = cid(1)
    val key = gkey("foo")

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
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // ** querybyid, querybykey
  // *** queryById then queryByKey
  "queryByKeyQueryNByKeyUnittest" - {
    val id = cid(1)
    val key = gkey("foo")

    runUnitTest(
      UnitTest(
        description = s"querybyid then queryByKey with needskey call",
        interaction = s =>
          for {
            s <- {
              val Left(NeedContract(resume)) = s.queryById(id): @unchecked;
              resume(Some(key))
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
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // *** queryByKey then queryById
  "queryNByKeyQueryByKeyUnittest" - {
    val id = cid(1)
    val key = gkey("foo")

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
            s <- s.queryById(id).getOrElse(fail("unexpected NeedContract")): ErrOr[LLState[Unit]]
          } yield s,
        expected = Right(
          StateMachineResult.emptyWith(
            inputContractIds = Set[V.ContractId](id),
            globalKeyInputs = Map(key -> KeyMapping(queue = Vector(id), exhaustive = true)),
          )
        ),
      )
    )
  }

  // * special scenarios
  // ** queryNByKey
  // *** inconsistent querybyid/queryNByKey
  // **** positive case: queryById reports contract+key EXISTS that queryNByKey DID NOT find
  // TODO[#30398]: implmement when its ready!
  "inconsistentQueryByIdBetweenQueryNByKeyUnittest" - {
    "positive case" - {
      val id1 = cid(1)
      val id2 = cid(2)
      val key = gkey("foo")

      runUnitTest(
        UnitTest(
          description =
            s"inconsistent case: queryNByKey($key, 2) finding [cid1] (exhaustive) then queryById finds " +
              s"cid2 with key=$key",
          interaction = s =>
            for {
              s <- queryNByKeyAndReplyToNeedsKey(
                key = key,
                n = 2,
                replyToNeedsKeyWith = Seq(id1),
                state = s,
              )
              s <- {
                val Left(NeedContract(resume)) = s.queryById(id2): @unchecked;
                resume(Some(key))
              }
            } yield s,
          expected = Left(InconsistentContractKey(key)),
        )
      )
    }

    // **** negative case: queryNByKey reports contract+key EXISTS that queryById DID NOT find
    // TODO[#30398]: implmement when its ready!
    "negative case" - {
      val id1 = cid(1)
      val id2 = cid(2)
      val key = gkey("foo")

      runUnitTest(
        UnitTest(
          description =
            s"inconsistent case: queryById(1) finds a contract with key $key, then queryNByKey($key, 2) finds [cid2] " +
              s"(exshaustive) (cid1 missing) " +
              s"finds cid2 with key=$key",
          interaction = s =>
            for {
              s <- {
                val Left(NeedContract(resume)) = s.queryById(id1): @unchecked;
                resume(Some(key))
              }
              s <- queryNByKeyAndReplyToNeedsKey(
                key = key,
                n = 2,
                replyToNeedsKeyWith = Seq(id2),
                state = s,
              )
            } yield s,
          expected = Left(InconsistentContractKey(key)),
        )
      )
    }
  }

  case class UnitTest(
      description: String,
      interaction: LLState[Unit] => ErrOr[LLState[Unit]],
      expected: Map[NextGenContractStateMachine.Mode, OptStateMachineResult],
  )

  object UnitTest {
    def apply(
        description: String,
        interaction: LLState[Unit] => Either[TransactionError, LLState[Unit]],
        expected: OptStateMachineResult,
    ): UnitTest = UnitTest(description, interaction, allModesMap(expected))

    def allModesMap(expected: OptStateMachineResult) =
      Map[NextGenContractStateMachine.Mode, OptStateMachineResult](
        NextGenContractStateMachine.Mode.NUCK -> expected,
        NextGenContractStateMachine.Mode.NoKey -> expected,
      )
  }

}
