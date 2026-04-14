// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.crypto.SValueHash
import com.digitalasset.daml.lf.data.Ref.{ChoiceName, PackageId, PackageName, Party, TypeConId}
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.language.Ast.*
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.ledger.{Authorize, FailedAuthorization}
import com.digitalasset.daml.lf.speedy.SError.*
import com.digitalasset.daml.lf.speedy.SExpr.*
import com.digitalasset.daml.lf.speedy.SValue.*
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  NextGenContractStateMachine as ContractStateMachine,
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ValueParty, ValueRecord}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success, Try}
import com.digitalasset.canton.logging.SuppressingLogging

class EvaluationOrderWithoutKeyTest_V2Dev
    extends EvaluationOrderTest(LanguageVersion.v2_dev, withKey = true)
class EvaluationOrderWithoutKeyTest_V23
    extends EvaluationOrderTest(LanguageVersion.v2_3, withKey = true)
class EvaluationOrderWithoutKeyTest_V22
    extends EvaluationOrderTest(LanguageVersion.v2_2, withKey = false)

// Used by buildLog to accept a mix of individual Strings and Seq[String] (e.g. from replicate).
sealed trait LogEntry
object LogEntry {
  import scala.language.implicitConversions
  final case class Single(msg: String) extends LogEntry
  final case class Multiple(msgs: Seq[String]) extends LogEntry
  implicit def fromString(s: String): LogEntry = Single(s)
  implicit def fromSeq(ss: Seq[String]): LogEntry = Multiple(ss)
}

// Dummy checker that just logs calls
class AuthorizationCheckerLogger(logger: RecordingMachineLogger) extends AuthorizationChecker {

  override private[lf] def authorizeCreate(optLocation: Option[Ref.Location], templateId: TypeConId, signatories: Set[Party], maintainers: Option[Set[Party]])(auth: Authorize): List[FailedAuthorization] = {
    logger.llTrace("authorizes create")
    List.empty
  }

  override private[lf] def authorizeFetch(optLocation: Option[Ref.Location], templateId: TypeConId, stakeholders: Set[Party])(auth: Authorize): List[FailedAuthorization] = {
    logger.llTrace("authorizes fetch")
    List.empty
  }

  override private[lf] def authorizeLookupByKey(optLocation: Option[Ref.Location], templateId: TypeConId, maintainers: Set[Party])(auth: Authorize): List[FailedAuthorization] = {
    logger.llTrace("authorizes lookup-by-key")
    List.empty
  }

  override private[lf] def authorizeExercise(optLocation: Option[Ref.Location], templateId: TypeConId, choiceId: ChoiceName, actingParties: Set[Party], choiceAuthorizers: Option[Set[Party]])(auth: Authorize): List[FailedAuthorization] = {
    logger.llTrace("authorizes exercise")
    List.empty
  }
}

abstract class EvaluationOrderTest(languageVersion: LanguageVersion, withKey: Boolean)
    extends AnyFreeSpec
    with Matchers
    with Inside
    with SuppressingLogging {

  private val testPkg = new TestPkg(withKey, languageVersion)
  import testPkg._

  private[this] val cId: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))

  private[this] val cId2: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test2"))

  private[this] val cId3: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test3"))

  private[this] val cId4: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test4"))

  private[this] val cId5: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test5"))

  private[this] val emptyNestedValue = Value.ValueRecord(None, ImmArray.empty)

  private[this] val keySValue = SRecord(
    TKey,
    ImmArray("maintainers", "optCid", "nested").map(Ref.Name.assertFromString),
    ArraySeq(
      SList(FrontStack(SParty(alice))),
      SOptional(None),
      SRecord(Nested, ImmArray(Ref.Name.assertFromString("f")), ArraySeq(SOptional(None))),
    ),
  )

  private[this] val keyValue = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueList(FrontStack(Value.ValueParty(alice))),
      None -> Value.ValueNone,
      None -> emptyNestedValue,
    ),
  )

  private[this] val normalizedKeyValue = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueList(FrontStack(Value.ValueParty(alice))),
      None -> Value.ValueNone,
      None -> Value.ValueRecord(None, ImmArray()),
    ),
  )

  private val testTxVersion: SerializationVersion = serializationVersion

  private[this] def buildContract(
      observer: Party,
      contractId: Value.ContractId,
      template: Ref.Identifier = T,
  ): FatContractInstance =
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      testTxVersion,
      packageName = pkg.pkgName,
      template = template,
      arg = Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(alice),
          None -> Value.ValueParty(observer),
          None -> Value.ValueTrue,
          None -> keyValue,
          None -> emptyNestedValue,
        ),
      ),
      signatories = List(alice),
      observers = List(observer),
      contractKeyWithMaintainers = Option.when(withKey)(
        GlobalKeyWithMaintainers(
          GlobalKey
            .assertBuild(
              templateId = template,
              packageName = pkg.pkgName,
              key = normalizedKeyValue,
              keyHash =
                SValueHash.assertHashContractKey(pkg.pkgName, template.qualifiedName, keySValue),
            ),
          Set(alice),
        )
      ),
      contractId = contractId,
    )

  private[this] val iface_contract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    testTxVersion,
    packageName = pkg.pkgName,
    template = Human,
    arg = Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueParty(alice),
        None -> Value.ValueParty(bob),
        None -> Value.ValueParty(alice),
        None -> Value.ValueTrue,
        None -> keyValue,
        None -> emptyNestedValue,
      ),
    ),
    signatories = List(alice),
    observers = List(bob),
    contractKeyWithMaintainers = Option.when(withKey)(
      GlobalKeyWithMaintainers(
        GlobalKey.assertBuild(
          templateId = Human,
          packageName = pkg.pkgName,
          key = normalizedKeyValue,
          keyHash = SValueHash.assertHashContractKey(pkg.pkgName, Human.qualifiedName, keySValue),
        ),
        Set(alice),
      )
    ),
    contractId = cId,
  )

  private[this] val getContract = { val c = buildContract(bob, cId); Map(c.contractId -> c) }
  private[this] def contractsToMap(contracts: Seq[FatContractInstance]) =
    contracts.map(c => c.contractId -> c).toMap
  private[this] val contracts =
    Seq(cId, cId2, cId3, cId4, cId5).map(buildContract(bob, _))
  private[this] def getContracts(n: Int) = contractsToMap(contracts.take(n))
  private[this] val getIfaceContract = Map(iface_contract.contractId -> iface_contract)

  private[this] val getKeys = Map(
    GlobalKey.assertBuild(
      templateId = T,
      packageName = pkg.pkgName,
      key = keyValue,
      keyHash = SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
    ) -> Vector(cId)
  )

  private[this] val cIds = Vector(cId, cId2, cId3, cId4, cId5)

  private[this] def getKeysWithNContracts(n: Int) = Map(
    GlobalKey.assertBuild(
      templateId = T,
      packageName = pkg.pkgName,
      key = keyValue,
      keyHash = SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
    ) -> cIds.take(n)
  )

  private[this] val dummyContract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    testTxVersion,
    packageName = pkg.pkgName,
    template = Dummy,
    arg = ValueRecord(None, ImmArray(None -> ValueParty(alice))),
    signatories = List(alice),
    contractId = cId,
  )
  private[this] val getWronglyTypedContract = Map(dummyContract.contractId -> dummyContract)

  private[this] val seed = crypto.Hash.hashPrivateKey("seed")

  private def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: ArraySeq[SValue],
      parties: Set[Party],
      readAs: Set[Party] = Set.empty,
      packageResolution: Map[PackageName, PackageId] = packageNameMap,
      getContract: PartialFunction[Value.ContractId, FatContractInstance] = PartialFunction.empty,
      getKeys: PartialFunction[GlobalKey, Vector[FatContractInstance]] = PartialFunction.empty,
  ) = {
    val se = pkgs.compiler.unsafeCompile(e)
    val recordingLogger = new RecordingMachineLogger(MachineLogger())
    val machine = Speedy.Machine
      .fromUpdateSExpr(
        compiledPackages = pkgs,
        transactionSeed = seed,
        updateSE = if (args.isEmpty) se else SEApp(se, args),
        committers = parties,
        logger = recordingLogger,
        readAs = readAs,
        authorizationChecker= new AuthorizationCheckerLogger(recordingLogger),
        mode =
          if (withKey) ContractStateMachine.Mode.NUCK
          else ContractStateMachine.Mode.NoKey,
        packageResolution = packageResolution,
      )
    val res = Try(
      SpeedyTestLib.run(
        machine,
        getContract = recordingLogger.tracePartialFunction("queries contract", getContract),
        getKeys = recordingLogger.tracePartialFunction("queries key", getKeys),
      )
    )
    val msgs = recordingLogger.recordedMessages.dropWhile(_ != "starts test")
    (res, msgs)
  }

  private val msgsToIgnore: Set[String] =
    if (withKey) Set.empty else Set("key", "maintainers")

  /** Repeat a sequence of log messages `n` times. For use in [[buildLog]]. */
  def replicate(n: Int, msgs: String*): Seq[String] = Seq.fill(n)(msgs).flatten

  /** Conditionally include log messages. For use in [[buildLog]]. */
  def when(cond: Boolean, msgs: String*): Seq[String] = if (cond) msgs else Seq.empty

  import LogEntry.*

  def buildLog(entries: LogEntry*): Seq[String] =
    entries
      .flatMap {
        case Single(s) => Seq(s)
        case Multiple(ss) => ss
      }
      .filterNot(msgsToIgnore.contains)

  // We cover all errors for each node in the order they are defined
  // in com.digitalasset.daml.lf.interpretation.Error.
  // We don’t check for exceptions/aborts during evaluation of an expression instead
  // assume that those always stop at the point of the corresponding
  // trace statement.
  // The important cases to test are ones that result in either a different transaction
  // or a transaction that is rejected vs one that is accepted. Cases where the transaction
  // is rejected in both cases “only” change the error message which is relatively harmless.
  // Specifically this means that we need to test ordering of catchable errors
  // relative to other catchable errors and other non-catchable errors but we don’t
  // need to check ordering of non-catchable errors relative to other non-catchable errors.

  "evaluation order" - {

    "native foldl match LF implementation" in {

      val (_, refMsgs) =
        evalUpdateApp(pkgs, e"""Test:testFold (M:foldl @Text @Text)""", ArraySeq.empty, Set.empty)

      val (_, msgs) =
        evalUpdateApp(pkgs, e"""Test:testFold (FOLDL @Text @Text)""", ArraySeq.empty, Set.empty)

      refMsgs shouldBe List("starts test", "0", "1", "01", "2", "012", "3", "ends test")
      msgs shouldBe refMsgs
    }

    "native foldr match LF implementation" in {

      val (_, refMsgs) =
        evalUpdateApp(pkgs, e"""Test:testFold (M:foldr @Text @Text)""", ArraySeq.empty, Set.empty)

      val (_, msgs) =
        evalUpdateApp(pkgs, e"""Test:testFold (FOLDR @Text @Text)""", ArraySeq.empty, Set.empty)

      refMsgs shouldBe List("starts test", "3", "0", "2", "30", "1", "230", "ends test")
      msgs shouldBe refMsgs
    }

    "create" - {

      // TEST_EVIDENCE: Integrity: Evaluation order of successful create
      "success" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
             Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) { case Success(Right(_)) =>
          msgs shouldBe buildLog(
            "starts test",
            "precondition",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
            "authorizes create",
            "ends test",
          )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create with failed precondition
      "failed precondition" in {
        // Note that for LF >= 1.14 we don’t hit this as the compiler
        // generates code that throws an exception instead of returning False.
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            Test:create M:T { signatory = sig, observer = obs, precondition = False, key = M:toKey sig, nested = M:buildNested 0 }
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(Left(SErrorDamlException(IE.TemplatePreconditionViolated(T, _, _)))) =>
            msgs shouldBe buildLog("starts test", "precondition")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create with duplicate contract key
      if (withKey) "duplicate contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            let c: M:T = M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
            in ubind x : ContractId M:T <- create @M:T c
              in Test:create c
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) { case Success(Right(_)) =>
          msgs shouldBe buildLog(
            "starts test",
            "precondition",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
            "authorizes create",
            "ends test",
          )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create with empty contract key maintainers
      if (withKey) "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:keyNoMaintainers, nested = M:buildNested 0 }
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.CreateEmptyContractKeyMaintainers(T, _, _)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create with contract ID in contract key
      if (withKey) "contract ID in contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) (cid : ContractId Unit) ->
            Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:toKeyWithCid sig cid, nested = M:buildNested 0 }
       """,
          ArraySeq(
            SParty(alice),
            SParty(bob),
            SContractId(Value.ContractId.V1.assertFromString("00" * 32 + "0000")),
          ),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe buildLog(
            "starts test",
            "precondition",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
          )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create with create argument exceeding max nesting
      "create argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 100 }
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
          let key: M:TKey = M:TKey { maintainers = Cons @Party [sig] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 100 }
          in Test:create M:T { signatory = sig, observer = obs, precondition = True, key = key, nested = M:buildNested 0 }
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
        }
      }
    }

    "create by interface" - {

      // TEST_EVIDENCE: Integrity: Evaluation order of successful create_interface
      "success" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
             Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) { case Success(Right(_)) =>
          msgs shouldBe buildLog(
            "starts test",
            "precondition",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
            "authorizes create",
            "ends test",
          )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create_interface with failed precondition
      "failed precondition" in {
        // Note that for LF >= 1.14 we don’t hit this as the compiler
        // generates code that throws an exception instead of returning False.
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
             Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = False, key = M:toKey sig, nested = M:buildNested 0}
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.TemplatePreconditionViolated(Human, _, _)))
              ) =>
            msgs shouldBe buildLog("starts test", "precondition")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create_interface with duplicate contract key
      if (withKey) "duplicate contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            let c: M:Human = M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
            in ubind x : ContractId M:Human <- create @M:Human c
              in Test:create_interface c
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) { case Success(Right(_)) =>
          msgs shouldBe buildLog(
            "starts test",
            "precondition",
            "contract signatories",
            "contract observers",
            "key",
            "maintainers",
            "authorizes create",
            "ends test",
          )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create_interface with empty contract key maintainers
      if (withKey) "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:keyNoMaintainers, nested = M:buildNested 0}
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.CreateEmptyContractKeyMaintainers(Human, _, _)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create_interface with create argument exceeding max nesting
      "create argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 100 }
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create_interface with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
          let key: M:TKey = M:TKey { maintainers = Cons @Party [sig] (Nil @Party), optCid = None @(ContractId Unit), nested = M:buildNested 100 }
          in Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = key, nested = M:buildNested 0 }
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
        }
      }
    }

    "exercise" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
              "authorizes exercise",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of a wrongly typed non-cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe buildLog("starts test", "queries contract")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of a non-cached global contract with inconsistent key
        if (withKey) "inconsistent key" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(maintainer: Party) (exercisingParty: Party) (cId: ContractId M:T) ->
           ubind x : Option (ContractId M:T) <- lookup_by_key @M:T (M:toKey maintainer)
           in Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
           """,
            ArraySeq(SParty(alice), SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKeys = PartialFunction.empty,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.InconsistentContractKey(_)))) =>
            msgs shouldBe buildLog(
              "starts test",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
            )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) ->
           ubind x: M:T <- fetch_template @M:T cId in
           Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
           """,
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
              "authorizes exercise",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T)  ->
         ubind x: Unit <- exercise @M:T Archive cId () in
           Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
         """,
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) ->
           ubind x: M:Dummy <- fetch_template @M:Dummy cId in
           Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
           """,
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness (pre upgrading)
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) ->
           ubind x: M:Dummy <- exercise @M:Dummy Archive cId () in
           Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
           """,
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (exercisingParty : Party) ->
         ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } in
         Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
         """,
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
              "authorizes exercise",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (exercisingParty : Party) ->
         ubind
           cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } ;
           x: Unit <- exercise @M:T Archive cId ()
         in
           Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
         """,
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of an wrongly typed local contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (exercisingParty : Party) ->
         ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig }
         in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
         in
           Test:exercise_by_id exercisingParty cId1 (M:Either:Left @Int64 @Int64 0)
         """,
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (exercisingParty : Party) ->
         ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig }
         in ubind x: Unit <- exercise @M:Dummy Archive cId1 ()
         in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
         in
           Test:exercise_by_id exercisingParty cId1 (M:Either:Left @Int64 @Int64 0)
         """,
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise of an unknown contract
      "unknown contract" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
          ArraySeq(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = PartialFunction.empty,
        )
        inside(res) { case Failure(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe buildLog("starts test", "queries contract")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise with argument exceeding max nesting
      "argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 100)""",
          ArraySeq(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = getContract,
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
            )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise with output exceeding max nesting
      "output exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Right @Int64 @Int64 100)""",
          ArraySeq(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = getContract,
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
              "authorizes exercise",
              "choice body",
            )
        }
      }
    }

    if (withKey) "exercise_by_key" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_by_key of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "maintainers",
              "queries key",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
              "authorizes exercise",
              "choice body",
              "ends test",
            )
          }
        }

        // This case may happen only if there is a bug in the ledger.
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getWronglyTypedContract,
            getKeys = mapKeys(getKeys, getWronglyTypedContract),
          )
          inside(res) { case Success(Left(SErrorCrash(_, _))) =>
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_by_key of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) (cId: ContractId M:T) ->
           ubind x: M:T <- fetch_template @M:T cId in
           Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
           """,
            ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "maintainers",
              "queries key",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
              "authorizes exercise",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (exercisingParty : Party) (cId: ContractId M:T)  ->
         ubind x: Unit <- exercise @M:T Archive cId () in
           Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
         """,
            ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(gkey)))) =>
            gkey.templateId shouldBe T
            msgs shouldBe buildLog("starts test", "maintainers", "queries key")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) (sig: Party) ->
           ubind x: M:Dummy <- fetch_template @M:Dummy cId in
           Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
           """,
            ArraySeq(SParty(alice), SContractId(cId), SParty(alice)),
            Set(alice),
            getContract = getWronglyTypedContract,
            getKeys = mapKeys(getKeys, getWronglyTypedContract),
          )
          inside(res) { case Success(Left(SErrorCrash(_, _))) =>
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_by_key of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (exercisingParty : Party) ->
         ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } in
         Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
         """,
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "maintainers",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
              "authorizes exercise",
              "choice body",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (exercisingParty : Party) ->
         ubind
           cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } ;
           x: Unit <- exercise @M:T Archive cId ()
         in
           Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
         """,
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(gKey)))) =>
            gKey.templateId shouldBe T
            msgs shouldBe buildLog("starts test", "maintainers", "queries key")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of an unknown contract
      "unknown contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Some @Party sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
          ArraySeq(SParty(alice), SParty(alice)),
          Set(alice),
          getContract = PartialFunction.empty,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
          key.templateId shouldBe T
          msgs shouldBe buildLog("starts test", "maintainers", "queries key")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key with argument exceeding max nesting
      "argument exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Some @Party sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 100)""",
          ArraySeq(SParty(alice), SParty(alice)),
          Set(alice),
          getContract = getContract,
          getKeys = mapKeys(getKeys, getContract),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "maintainers",
              "queries key",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
            )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key with result exceeding max nesting
      "result exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Right @Int64 @Int64 100)""",
          ArraySeq(SParty(alice), SParty(alice)),
          Set(alice),
          getContract = getContract,
          getKeys = mapKeys(getKeys, getContract),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog(
              "starts test",
              "maintainers",
              "queries key",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "template choice controllers",
              "template choice observers",
              "template choice authorizers",
              "authorizes exercise",
              "choice body",
            )
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_vy_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty: Party) -> Test:exercise_by_key exercisingParty Test:noParty Test:noCid 0 (M:Either:Right @Int64 @Int64 100)""",
          ArraySeq(SParty(alice)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.FetchEmptyContractKeyMaintainers(T, _, _)))
              ) =>
            msgs shouldBe buildLog("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key with contract ID in contract key
      "contract ID in contract key " in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(exercisingParty: Party) (sig: Party) (cId: ContractId M:T) ->
             Test:exercise_by_key exercisingParty (Test:someParty sig) (Test:someCid cId) 0 (M:Either:Right @Int64 @Int64 100)""",
          ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe buildLog("starts test", "maintainers")
        }
      }
    }

    List("exercise_interface", "exercise_interface_with_guard").foreach { testCase =>
      val msgsToIgnore_ =
        testCase match {
          case "exercise_interface" => msgsToIgnore + "interface guard"
          case _ => msgsToIgnore
        }

      def buildLog(msgs: String*) = msgs.filterNot(msgsToIgnore_)

      testCase - {

        "a non-cached global contract" - {

          // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise by interface of a non-cached global contract
          "success" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty: Party) (cId: ContractId M:Human) -> Test:$testCase exercisingParty cId""",
              ArraySeq(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getIfaceContract,
            )
            inside(res) { case Success(Right(_)) =>
              msgs shouldBe buildLog(
                "starts test",
                "queries contract",
                "precondition",
                "contract signatories",
                "contract observers",
                "key",
                "maintainers",
                "interface guard",
                "interface choice controllers",
                "interface choice observers",
                "authorizes exercise",
                "choice body",
                "ends test",
              )
            }
          }
          // TEST_EVIDENCE: Integrity: exercise_interface with a contract instance that does not implement t  he interface fails.
          "contract doesn't implement interface" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human) -> Test:$testCase exercisingParty cId""",
              ArraySeq(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getWronglyTypedContract,
              getKeys = mapKeys(getKeys, getWronglyTypedContract),
            )
            inside(res) {
              case Success(
                    Left(SErrorDamlException(IE.ContractDoesNotImplementInterface(_, _, _)))
                  ) =>
                msgs shouldBe buildLog("starts test", "queries contract")
            }
          }
        }
        "a cached global contract" - {

          // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_interface of a cached global contract
          "success" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId Human) ->
           ubind x: M:Human <- fetch_template @M:Human cId in
           Test:$testCase exercisingParty cId
           """,
              ArraySeq(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getIfaceContract,
            )
            inside(res) { case Success(Right(_)) =>
              msgs shouldBe buildLog(
                "starts test",
                "interface guard",
                "interface choice controllers",
                "interface choice observers",
                "authorizes exercise",
                "choice body",
                "ends test",
              )
            }
          }

          // TEST_EVIDENCE: Integrity: Evaluation order of exercise by interface of an inactive global contract
          "inactive contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human)  ->
         ubind x: Unit <- exercise @M:Human Archive cId () in
           Test:$testCase exercisingParty cId
         """,
              ArraySeq(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getIfaceContract,
            )
            inside(res) {
              case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Human, _)))) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Integrity: Evaluation order of exercise by interface of a cached global contract that does not implement the interface.
          "wrongly typed contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human) ->
           ubind x: M:Dummy <- fetch_template @M:Dummy cId in
           Test:$testCase exercisingParty cId
           """,
              ArraySeq(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getWronglyTypedContract,
            )
            inside(res) {
              case Success(
                    Left(
                      SErrorDamlException(
                        IE.ContractDoesNotImplementInterface(Person, _, Dummy)
                      )
                    )
                  ) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Integrity: This checks that type checking is done after checking activeness.
          "wrongly typed inactive contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:T) ->
           ubind x: M:Dummy <- exercise @M:Dummy Archive cId () in
           Test:$testCase exercisingParty cId
           """,
              ArraySeq(SParty(alice), SContractId(cId)),
              Set(alice),
              getContract = getWronglyTypedContract,
            )
            inside(res) {
              case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
                msgs shouldBe buildLog("starts test")
            }
          }
        }

        "a local contract" - {

          // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_interface of a local contract
          "success" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) ->
         ubind cId: ContractId M:Human <- create @M:Human M:Human {person = exercisingParty, obs = exercisingParty, ctrl = exercisingParty, precond = True, key = M:toKey exercisingParty, nested = M:buildNested 0} in
         Test:$testCase exercisingParty cId
         """,
              ArraySeq(SParty(alice)),
              Set(alice),
            )
            inside(res) { case Success(Right(_)) =>
              msgs shouldBe buildLog(
                "starts test",
                "interface guard",
                "interface choice controllers",
                "interface choice observers",
                "authorizes exercise",
                "choice body",
                "ends test",
              )
            }
          }

          // TEST_EVIDENCE: Integrity: Evaluation order of exercise_interface of an inactive local contract
          "inactive contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) ->
         ubind cId: ContractId M:Human <- create @M:Human M:Human {person = exercisingParty, obs = exercisingParty, ctrl = exercisingParty, precond = True, key = M:toKey exercisingParty, nested = M:buildNested 0} in
         ubind x: Unit <- exercise @M:Human Archive cId ()
         in
         Test:$testCase exercisingParty cId
         """,
              ArraySeq(SParty(alice)),
              Set(alice),
            )
            inside(res) {
              case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Human, _)))) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Integrity: Evaluation order of exercise_interface of an local contract not implementing the interface
          "wrongly typed contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) ->
         ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = exercisingParty }
         in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
         in
           Test:$testCase exercisingParty cId1
         """,
              ArraySeq(SParty(alice)),
              Set(alice),
            )
            inside(res) {
              case Success(
                    Left(
                      SErrorDamlException(
                        IE.ContractDoesNotImplementInterface(Person, _, Dummy)
                      )
                    )
                  ) =>
                msgs shouldBe buildLog("starts test")
            }
          }

          // TEST_EVIDENCE: Integrity: This checks that type checking in exercise_interface is done after checking activeness.
          "wrongly typed inactive contract" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) ->
         ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = exercisingParty}
         in ubind x: Unit <- exercise @M:Dummy Archive cId1 ()
         in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
         in
           Test:$testCase exercisingParty cId1
         """,
              ArraySeq(SParty(alice)),
              Set(alice),
            )
            inside(res) {
              case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
                msgs shouldBe buildLog("starts test")
            }
          }
        }
      }
    }

    "fetch" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_by_id""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "authorizes fetch",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of a wrongly typed non-cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_by_id""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe buildLog("starts test", "queries contract")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of a non-cached global contract with inconsistent key
        if (withKey) "inconsistent key" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(maintainer: Party) (fetchingParty: Party) (cId: ContractId M:T) ->
           ubind x : Option (ContractId M:T) <- lookup_by_key @M:T (M:toKey maintainer)
           in Test:fetch_by_id fetchingParty cId
           """,
            ArraySeq(SParty(alice), SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKeys = PartialFunction.empty,
          )

          inside(res) { case Success(Left(SErrorDamlException(IE.InconsistentContractKey(_)))) =>
            msgs shouldBe buildLog(
              "starts test",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
            )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) ->
           ubind x: M:T <- fetch_template @M:T cId in
           Test:fetch_by_id fetchingParty cId
           """,
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T)  ->
         ubind x: Unit <- exercise @M:T Archive cId ()
         in Test:fetch_by_id fetchingParty cId""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) ->
           ubind x: M:Dummy <- fetch_template @M:Dummy cId
           in Test:fetch_by_id fetchingParty cId""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) ->
           ubind x: M:Dummy <- exercise @M:Dummy Archive cId ()
           in Test:fetch_by_id fetchingParty cId""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (fetchingParty: Party) ->
         ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
         in Test:fetch_by_id fetchingParty cId""",
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (fetchingParty: Party) ->
         ubind
           cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 } ;
           x: Unit <- exercise @M:T Archive cId ()
         in Test:fetch_by_id fetchingParty cId""",
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractNotActive(_, T, _)))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of an wrongly typed local contract
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) ->
         ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig }
         in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
         in Test:fetch_by_id fetchingParty cId2""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.WronglyTypedContract(_, T, Dummy)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) ->
         ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig }
         in ubind x: Unit <- exercise @M:Dummy Archive cId1 ()
         in let cId2: ContractId M:T = COERCE_CONTRACT_ID @M:Dummy @M:T cId1
         in Test:fetch_by_id fetchingParty cId2""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch of an unknown contract
      "unknown contract" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty: Party) (cId: ContractId M:T) -> Test:fetch_by_id fetchingParty cId""",
          ArraySeq(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = PartialFunction.empty,
        )
        inside(res) { case Failure(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe buildLog("starts test", "queries contract")
        }
      }
    }

    if (withKey) "fetch_by_key" - {
      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_by_key of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "maintainers",
              "queries key",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "authorizes fetch",
              "ends test",
            )
          }
        }

        // This case may happen only if there is a bug in the ledger.
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getWronglyTypedContract,
            getKeys = mapKeys(getKeys, getWronglyTypedContract),
          )
          inside(res) { case Success(Left(SErrorCrash(_, _))) =>
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_by_key of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) (cId: ContractId M:T) ->
             ubind x: M:T <- fetch_template @M:T cId
             in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "maintainers", "queries key", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of an inactive global contract
        "inactive contract" in {

          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) (fetchingParty: Party) (sig: Party) ->
         ubind x: Unit <- exercise @M:T Archive cId ()
         in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SContractId(cId), SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
            key.templateId shouldBe T
            msgs shouldBe buildLog("starts test", "maintainers", "queries key")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_by_key of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (fetchingParty: Party)  ->
         ubind
           cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
         in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "maintainers", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (fetchingParty: Party) ->
         ubind
           cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 };
           x: Unit <- exercise @M:T Archive cId ()
         in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
            key.templateId shouldBe T
            msgs shouldBe buildLog("starts test", "maintainers", "queries key")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of an unknown contract key
      "unknown contract key" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Some @Party sig) (None @(ContractId Unit)) 0""",
          ArraySeq(SParty(alice), SParty(alice)),
          Set(alice),
          getContract = getContract,
          getKeys = PartialFunction.empty,
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractKeyNotFound(key)))) =>
          key.templateId shouldBe T
          msgs shouldBe buildLog("starts test", "maintainers", "queries key")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty: Party) -> Test:fetch_by_key fetchingParty Test:noParty Test:noCid 0""",
          ArraySeq(SParty(alice)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.FetchEmptyContractKeyMaintainers(T, _, _)))
              ) =>
            msgs shouldBe buildLog("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key with contract ID in contract key
      "contract ID in contract key " in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty: Party) (sig: Party) (cId: ContractId M:T) ->
             Test:fetch_by_key fetchingParty (Test:someParty sig) (Test:someCid cId) 0""",
          ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe buildLog("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (fetchingParty: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 100""",
          ArraySeq(SParty(alice), SParty(alice)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog("starts test", "maintainers")
        }
      }
    }

    "fetch_interface" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_interface of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_interface""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getIfaceContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "authorizes fetch",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of a non-cached global contract that doesn't implement interface.
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""Test:fetch_interface""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(IE.ContractDoesNotImplementInterface(Person, _, Dummy))
                  )
                ) =>
              msgs shouldBe buildLog("starts test", "queries contract")
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_interface of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Person) ->
           ubind x: M:Person <- fetch_interface @M:Person cId in
           Test:fetch_interface fetchingParty cId
           """,
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getIfaceContract,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Person)  ->
         ubind x: Unit <- exercise @M:Human Archive cId ()
         in Test:fetch_interface fetchingParty cId""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getIfaceContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Human, _)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of a cached global contract not implementing the interface.
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Person) ->
           ubind x: M:Dummy <- fetch_template @M:Dummy cId
           in Test:fetch_interface fetchingParty cId""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(IE.ContractDoesNotImplementInterface(Person, _, Dummy))
                  )
                ) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:Human) ->
           ubind x: M:Dummy <- exercise @M:Dummy Archive cId ()
           in Test:fetch_interface fetchingParty cId""",
            ArraySeq(SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_interface of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (fetchingParty: Party) ->
         ubind cId: ContractId M:Human <- create @M:Human M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
         in Test:fetch_interface fetchingParty cId""",
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (fetchingParty: Party) ->
         ubind cId: ContractId M:Human <- create @M:Human M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
         in ubind x: Unit <- exercise @M:Human Archive cId ()
         in Test:fetch_interface fetchingParty cId""",
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Human, _)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of an local contract not implementing the interface
        "wrongly typed contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) ->
         ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig }
         in let cId2: ContractId M:Human = COERCE_CONTRACT_ID @M:Dummy @M:Human cId1
         in Test:fetch_interface fetchingParty cId2""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(IE.ContractDoesNotImplementInterface(Person, _, Dummy))
                  )
                ) =>
              msgs shouldBe buildLog("starts test")
          }
        }
        // TEST_EVIDENCE: Integrity: This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (fetchingParty: Party) ->
         ubind cId1: ContractId M:Dummy <- create @M:Dummy M:Dummy { signatory = sig }
         in ubind x: Unit <- exercise @M:Dummy Archive cId1 ()
         in let cId2: ContractId M:Human = COERCE_CONTRACT_ID @M:Dummy @M:Human cId1
         in Test:fetch_interface fetchingParty cId2""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
          )
          inside(res) {
            case Success(Left(SErrorDamlException(IE.ContractNotActive(_, Dummy, _)))) =>
              msgs shouldBe buildLog("starts test")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of an unknown contract
      "unknown contract" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(fetchingParty: Party) (cId: ContractId M:Person) -> Test:fetch_interface fetchingParty cId""",
          ArraySeq(SParty(alice), SContractId(cId)),
          Set(alice),
          getContract = PartialFunction.empty,
        )
        inside(res) { case Failure(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe buildLog("starts test", "queries contract")
        }
      }

    }

    if (withKey) "query_n_by_key" - {
      val queryContractMsgs = Seq(
        "queries contract",
        "precondition",
        "contract signatories",
        "contract observers",
        "key",
        "maintainers",
      )

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful query_n_by_key of a non-cached global contract
        "success" - {
          for (n <- Seq(1, 2, 5)) {
            s"n=$n" in {
              val (res, msgs) = evalUpdateApp(
                pkgs,
                e"""\(lookingParty:Party) (sig: Party) ->
                   Test:query_n_by_key $n lookingParty (Test:someParty sig) Test:noCid 0""",
                ArraySeq(SParty(alice), SParty(alice)),
                Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
              )
              inside(res) { case Success(Right(_)) =>
                msgs shouldBe buildLog(
                  "starts test",
                  "maintainers",
                  "authorizes lookup-by-key",
                  "queries key",
                  replicate(n, queryContractMsgs*),
                  "ends test",
                )
              }
            }
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key of a non-cached global contract with authorization failure
        "authorization failure -- TExcept" in {
          val getContract = {
            val c = buildContract(bob, cId, template = TExcept);
            Map(c.contractId -> c)
          }
          val getKeys = Map(
            GlobalKey.assertBuild(
              templateId = TExcept,
              packageName = pkg.pkgName,
              key = keyValue,
              keyHash =
                SValueHash.assertHashContractKey(pkg.pkgName, TExcept.qualifiedName, keySValue),
            ) -> Vector(cId)
          )

          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) ->
               Test:query_n_by_key_except 5 lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(charlie), SParty(alice)),
            Set(alice, charlie),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailureStatus(
                        errorId,
                        _,
                        _,
                        _,
                      )
                    )
                  )
                ) if errorId.startsWith("UNHANDLED_EXCEPTION") =>
              msgs shouldBe buildLog(
                "starts test"
              )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful query_n_by_key of a cached global contract
        "success" - {
          for (n <- Seq(1, 2, 5)) {
            s"n=$n" in {
              val (res, msgs) = evalUpdateApp(
                pkgs,
                e"""\(lookingParty:Party) (sig: Party) (cId: ContractId M:T) ->
                   ubind x: M:T <- fetch_template @M:T cId
                   in Test:query_n_by_key $n lookingParty (Test:someParty sig) Test:noCid 0""",
                ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
                Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
              )
              inside(res) { case Success(Right(_)) =>
                msgs shouldBe buildLog(
                  "starts test",
                  "maintainers",
                  "authorizes lookup-by-key",
                  "queries key",
                  replicate(n - 1, queryContractMsgs*),
                  "ends test",
                )
              }
            }
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key of an inactive global contract
        "inactive contract" - {
          for (n <- Seq(1, 2, 5)) {
            s"n=$n" in {
              val (res, msgs) = evalUpdateApp(
                pkgs,
                e"""\(cId: ContractId M:T) (lookingParty: Party) (sig: Party) ->
                    ubind x: Unit <- exercise @M:T Archive cId ()
                    in Test:query_n_by_key $n lookingParty (Test:someParty sig) Test:noCid 0""",
                ArraySeq(SContractId(cId), SParty(alice), SParty(alice)),
                Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
              )
              inside(res) { case Success(Right(_)) =>
                msgs shouldBe buildLog(
                  "starts test",
                  "maintainers",
                  "authorizes lookup-by-key",
                  "queries key",
                  replicate(n - 1, queryContractMsgs*),
                  "ends test",
                )
              }
            }
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful query_n_by_key of a local contract
        "success" - {
          for (n <- Seq(1, 2, 5)) {
            s"n=$n" in {
              val (res, msgs) = evalUpdateApp(
                pkgs,
                e"""\(sig : Party) (obs : Party) (lookingParty: Party)  ->
                ubind
                  cId: ContractId M:T <-
                    create @M:T M:T
                      { signatory = sig,
                        observer = obs,
                        precondition = True,
                        key = M:toKey sig,
                        nested = M:buildNested 0
                      }
                in Test:query_n_by_key $n lookingParty (Test:someParty sig) Test:noCid 0""",
                ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
                Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
              )
              inside(res) { case Success(Right(_)) =>
                msgs shouldBe buildLog(
                  "starts test",
                  "maintainers",
                  "authorizes lookup-by-key",
                  when(
                    n > 1,
                    "queries key",
                  ), // n > 1 ==> we are going to needsKey so query key first
                  replicate(n - 1, queryContractMsgs*),
                  "ends test",
                )
              }
            }
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key of an inactive local contract
        // this testcase is less relevant as NUCK than UCK test, as UCK test it asserted that archiving it does not
        // trigger a needsKeys but in the world of NUCK we needKey as if the archive wasn't there since the only thing
        // we learn is that _this specific contract with this key_ doesn't exist anymore, but we should query either way
        // for the remaining.
        "inactive contract" - {
          for (n <- Seq(1, 2, 5)) {
            s"n=$n" in {
              val (res, msgs) = evalUpdateApp(
                pkgs,
                e"""\(sig : Party) (obs : Party) (lookingParty: Party) ->
                    ubind cId: ContractId M:T <- create @M:T M:T
                      { signatory = sig,
                        observer = obs,
                        precondition = True,
                        key = M:toKey sig,
                        nested = M:buildNested 0
                      };
                    x: Unit <- exercise @M:T Archive cId ()
                    in Test:query_n_by_key $n lookingParty (Test:someParty sig) Test:noCid 0""",
                ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
                Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
              )
              inside(res) { case Success(Right(_)) =>
                msgs shouldBe buildLog(
                  "starts test",
                  "maintainers",
                  "authorizes lookup-by-key",
                  "queries key",
                  replicate(n, queryContractMsgs*),
                  "ends test",
                )
              }
            }
          }
        }
      }

      "an undefined key" - {
        // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key of an unknown contract key
        "successful" - {
          for (n <- Seq(1, 2, 5)) {
            s"n=$n" in {
              val (res, msgs) = evalUpdateApp(
                pkgs,
                e"""\(lookingParty:Party) (sig: Party) ->
                   Test:query_n_by_key $n lookingParty (Some @Party sig) None @(ContractId Unit) 0""",
                ArraySeq(SParty(alice), SParty(alice)),
                Set(alice),
                getContract = getContracts(n),
                getKeys = PartialFunction.empty,
              )
              inside(res) { case Success(Right(_)) =>
                msgs shouldBe buildLog("starts test", "maintainers", "authorizes lookup-by-key", "queries key", "ends test")
              }
            }
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key with empty contract key maintainers
      "empty contract key maintainers" - {
        for (n <- Seq(1, 2, 5)) {
          s"n=$n" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(lookingParty: Party) -> Test:query_n_by_key $n lookingParty Test:noParty Test:noCid 0""",
              ArraySeq(SParty(alice)),
              Set(alice),
            )
            inside(res) {
              case Success(
                    Left(SErrorDamlException(IE.FetchEmptyContractKeyMaintainers(T, _, _)))
                  ) =>
                msgs shouldBe buildLog("starts test", "maintainers")
            }
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key with contract ID in contract key
      "contract ID in contract key " - {
        for (n <- Seq(1, 2, 5)) {
          s"n=$n" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(lookingParty: Party) (sig: Party) (cId: ContractId M:T) ->
                 Test:query_n_by_key $n lookingParty (Test:someParty sig) (Test:someCid cId) 0""",
              ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
              Set(alice),
            )
            inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
              msgs shouldBe buildLog("starts test", "maintainers")
            }
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key with contract key exceeding max nesting
      "key exceeds max nesting" - {
        for (n <- Seq(1, 2, 5)) {
          s"n=$n" in {
            val (res, msgs) = evalUpdateApp(
              pkgs,
              e"""\(sig : Party) (lookingParty: Party) ->
                 Test:query_n_by_key $n lookingParty (Test:someParty sig) Test:noCid 100""",
              ArraySeq(SParty(alice), SParty(alice)),
              Set(alice),
            )
            inside(res) {
              case Success(
                    Left(SErrorDamlException(IE.ValueNesting(_)))
                  ) =>
                msgs shouldBe buildLog("starts test", "maintainers")
            }
          }
        }
      }
    }

    if (withKey) "lookup_by_key" - {
      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful lookup_by_key of a non-cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) -> Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog(
              "starts test",
              "maintainers",
              "authorizes lookup-by-key",
              "queries key",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "ends test",
            )
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful lookup_by_key of a cached global contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) (cId: ContractId M:T) ->
             ubind x: M:T <- fetch_template @M:T cId
             in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "maintainers", "authorizes lookup-by-key", "queries key", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key of an inactive global contract
        "inactive contract" in {

          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(cId: ContractId M:T) (lookingParty: Party) (sig: Party) ->
         ubind x: Unit <- exercise @M:T Archive cId ()
         in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SContractId(cId), SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "maintainers", "authorizes lookup-by-key", "queries key", "ends test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful lookup_by_key of a local contract
        "success" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (lookingParty: Party)  ->
         ubind
           cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
         in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "maintainers", "authorizes lookup-by-key", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(sig : Party) (obs : Party) (lookingParty: Party) ->
         ubind
           cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 };
           x: Unit <- exercise @M:T Archive cId ()
         in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(bob), SParty(alice)),
            Set(alice),
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "maintainers", "authorizes lookup-by-key", "queries key", "ends test")
          }
        }
      }

      "an undefined key" - {
        // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key of an unknown contract key
        "successful" in {
          val (res, msgs) = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) -> Test:lookup_by_key lookingParty (Some @Party sig) None @(ContractId Unit) 0""",
            ArraySeq(SParty(alice), SParty(alice)),
            Set(alice),
            getContract = getContract,
            getKeys = PartialFunction.empty,
          )
          inside(res) { case Success(Right(_)) =>
            msgs shouldBe buildLog("starts test", "maintainers", "authorizes lookup-by-key", "queries key", "ends test")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(lookingParty: Party) -> Test:lookup_by_key lookingParty Test:noParty Test:noCid 0""",
          ArraySeq(SParty(alice)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.FetchEmptyContractKeyMaintainers(T, _, _)))
              ) =>
            msgs shouldBe buildLog("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key with contract ID in contract key
      "contract ID in contract key " in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(lookingParty: Party) (sig: Party) (cId: ContractId M:T) ->
             Test:lookup_by_key lookingParty (Test:someParty sig) (Test:someCid cId) 0""",
          ArraySeq(SParty(alice), SParty(alice), SContractId(cId)),
          Set(alice),
        )
        inside(res) { case Success(Left(SErrorDamlException(IE.ContractIdInContractKey(_)))) =>
          msgs shouldBe buildLog("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (lookingParty: Party) -> Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 100""",
          ArraySeq(SParty(alice), SParty(alice)),
          Set(alice),
        )
        inside(res) {
          case Success(
                Left(SErrorDamlException(IE.ValueNesting(_)))
              ) =>
            msgs shouldBe buildLog("starts test", "maintainers")
        }
      }
    }
  }

  def mapKeys[K, V, R](getKeys: Map[K, Vector[V]], getContract: V => R): Map[K, Vector[R]] =
    getKeys.view.mapValues(_.map(getContract)).toMap
}
