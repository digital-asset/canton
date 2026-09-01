// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.crypto.SValueHash
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.ledger.{Authorize, FailedAuthorization}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
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

// Test classes using UpdateMachine
class UpdateEvaluationOrderWithoutKeyTest_V2Dev
    extends UpdateEvaluationOrderTest(LanguageVersion.v2_dev, withKey = true)
    with CmdFlowRunnerWithUpdateMachine
class UpdateEvaluationOrderWithoutKeyTest_V23
    extends UpdateEvaluationOrderTest(LanguageVersion.v2_3, withKey = true)
    with CmdFlowRunnerWithUpdateMachine
class UpdateEvaluationOrderWithoutKeyTest_V22
    extends UpdateEvaluationOrderTest(LanguageVersion.v2_2, withKey = false)
    with CmdFlowRunnerWithUpdateMachine

// Test classes using TransactionConductor
class UpdateEvaluationOrderWithTransactionConductorTest_V2Dev
    extends UpdateEvaluationOrderTest(LanguageVersion.v2_dev, withKey = true)
    with CmdFlowRunnerWithTransactionConductor
class UpdateEvaluationOrderWithTransactionConductorTest_V23
    extends UpdateEvaluationOrderTest(LanguageVersion.v2_3, withKey = true)
    with CmdFlowRunnerWithTransactionConductor
class UpdateEvaluationOrderWithTransactionConductorTest_V22
    extends UpdateEvaluationOrderTest(LanguageVersion.v2_2, withKey = false)
    with CmdFlowRunnerWithTransactionConductor

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

  override private[lf] def authorizeCreate(
      optLocation: Option[Ref.Location],
      templateId: Ref.TypeConId,
      signatories: Set[Ref.Party],
      maintainers: Option[Set[Ref.Party]],
  )(auth: Authorize): List[FailedAuthorization] = {
    logger.llTrace("authorizes create")
    List.empty
  }

  override private[lf] def authorizeFetch(
      optLocation: Option[Ref.Location],
      templateId: Ref.TypeConId,
      stakeholders: Set[Ref.Party],
  )(auth: Authorize): List[FailedAuthorization] = {
    logger.llTrace("authorizes fetch")
    List.empty
  }

  override private[lf] def authorizeLookupByKey(
      optLocation: Option[Ref.Location],
      templateId: Ref.TypeConId,
      maintainers: Set[Ref.Party],
  )(auth: Authorize): List[FailedAuthorization] = {
    logger.llTrace("authorizes lookup-by-key")
    List.empty
  }

  override private[lf] def authorizeExercise(
      optLocation: Option[Ref.Location],
      templateId: Ref.TypeConId,
      choiceId: Ref.ChoiceName,
      actingParties: Set[Ref.Party],
      choiceAuthorizers: Option[Set[Ref.Party]],
  )(auth: Authorize): List[FailedAuthorization] = {
    logger.llTrace("authorizes exercise")
    List.empty
  }
}

abstract class UpdateEvaluationOrderTest(languageVersion: LanguageVersion, withKey: Boolean)
    extends AnyFreeSpec
    with CmdFlowRunner
    with Matchers
    with Inside
    with SuppressingLogging {

  private val testPkg = new TestPkg(withKey, languageVersion, cmdMode = cmdMode)
  import TestPkg.*, testPkg.*

  private val testTxVersion: SerializationVersion = serializationVersion

  private[this] def buildContract(
      observer: Ref.Party,
      contractId: Value.ContractId,
      template: Ref.Identifier = T,
  ): FatContractInstance =
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      testTxVersion,
      packageName = pkg.pkgName,
      template = template,
      arg = defaultPayload.update("observer", SValue.SParty(observer)).toNormalizedValue,
      contractKeyWithMaintainers = Option.when(withKey)(
        GlobalKeyWithMaintainers(
          GlobalKey(
            templateId = template,
            packageName = pkg.pkgName,
            key = normalizedKeyValue,
            hash = SValueHash.assertHashContractKey(pkg.pkgName, template.qualifiedName, keySValue),
          ),
          Set(alice),
        )
      ),
      contractId = contractId,
      signatories = List(alice),
      observers = List(observer),
    )

  private[this] lazy val getContract = { val c = buildContract(bob, cId); Map(c.contractId -> c) }
  private[this] def contractsToMap(contracts: Seq[FatContractInstance]) =
    contracts.map(c => c.contractId -> c).toMap
  private[this] lazy val contracts =
    Seq(cId, cId2, cId3, cId4, cId5).map(buildContract(bob, _))
  private[this] def getContracts(n: Int) = contractsToMap(contracts.take(n))

  private[this] lazy val getKeys = Map(
    GlobalKey(
      templateId = T,
      packageName = pkg.pkgName,
      key = keyValue,
      hash = SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
    ) -> Vector(cId)
  )

  private[this] lazy val cIds = Vector(cId, cId2, cId3, cId4, cId5)

  private[this] def getKeysWithNContracts(n: Int) = Map(
    GlobalKey(
      templateId = T,
      packageName = pkg.pkgName,
      key = keyValue,
      hash = SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
    ) -> cIds.take(n)
  )

  private[this] lazy val dummyContract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    testTxVersion,
    packageName = pkg.pkgName,
    template = Dummy,
    arg = ValueRecord(None, ImmArray(None -> ValueParty(alice))),
    signatories = List(alice),
    contractId = cId,
  )
  private[this] lazy val getWronglyTypedContract = Map(dummyContract.contractId -> dummyContract)

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

    def create(tmplId: Ref.Identifier, payload: SValue.SRecord): CmdFlow[SValue] =
      CmdFlow.submit(Command.Create(tmplId, payload))

    def createAndArchive(
        templateId: Ref.Identifier,
        payload: SValue.SRecord,
    ): CmdFlow[SValue] =
      for {
        cid <- create(templateId, payload)
        _ <- CmdFlow.submit(
          Command.ExerciseTemplate(templateId, asSCid(cid), n"Archive", SValue.SUnit)
        )
      } yield cid

    val createT = create(T, defaultPayload)
    val createAndArchiveT = createAndArchive(T, defaultPayload)
    val createDummy =
      create(
        Dummy,
        SValue.SRecord(
          Dummy,
          ImmArray(Ref.Name.assertFromString("f")),
          ArraySeq(SValue.SParty(alice)),
        ),
      )
    val createAndArchiveDummy =
      createAndArchive(
        Dummy,
        SValue.SRecord(
          Dummy,
          ImmArray(Ref.Name.assertFromString("f")),
          ArraySeq(SValue.SParty(alice)),
        ),
      )
    def archive(cid: Value.ContractId, tmplId: Ref.TypeConId = T) = CmdFlow.submit(
      Command.ExerciseTemplate(tmplId, SValue.SContractId(cid), n"Archive", SValue.SUnit)
    )

    "create" - {

      // TEST_EVIDENCE: Integrity: Evaluation order of successful create
      "success" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ => CmdFlow.submit(Command.Create(T, defaultPayload)),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) { case Right(_) =>
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
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.Create(T, defaultPayload.update("precondition", SValue.SBool(false)))
            ),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) {
          case Left(SError.InterpretationError(IE.TemplatePreconditionViolated(T, _, _))) =>
            msgs shouldBe buildLog("starts test", "precondition")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of create with empty contract key maintainers
      if (withKey) "empty contract key maintainers" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command
                .Create(T, defaultPayload.update("maintainers", SValue.SList(FrontStack.empty)))
            ),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) {
          case Left(SError.InterpretationError(IE.CreateEmptyContractKeyMaintainers(T, _, _))) =>
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
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.Create(
                T,
                defaultPayload.update("keyCidOpt", SValue.SOptional(Some(SValue.SContractId(cId)))),
              )
            ),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ContractIdInContractKey(_))) =>
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
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(Command.Create(T, defaultPayload.update("input", SNat.fromInt(100)))),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ValueNesting(_))) =>
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
      if (withKey) "key exceeds max nesting" in {

        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.Create(T, defaultPayload.update("keySize", SValue.SInt64(100L)))
            ),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ValueNesting(_))) =>
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

      val defaultCmd = Command.ExerciseTemplate(
        T,
        SValue.SContractId(cId),
        n"Choice",
        SEither.Left(SNat.fromInt(0)),
      )

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise of a non-cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ => CmdFlow.submit(defaultCmd),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ => CmdFlow.submit(defaultCmd),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Left(SError.InterpretationError(IE.WronglyTypedContract(_, T, Dummy))) =>
              msgs shouldBe buildLog("starts test", "queries contract")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of a non-cached global contract with inconsistent key
        if (withKey) "inconsistent key" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.QueryNByKey(T, SValue.SInt64(1L), keySValue)),
            test = _ => CmdFlow.submit(defaultCmd),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
            getKeys = PartialFunction.empty,
          )

          inside(res) { case Left(SError.InterpretationError(IE.InconsistentContractKey(_))) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            test = _ => CmdFlow.submit(defaultCmd),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId),
            test = _ => CmdFlow.submit(defaultCmd),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, T, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(Dummy, SValue.SContractId(cId))),
            test = _ => CmdFlow.submit(defaultCmd),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Left(SError.InterpretationError(IE.WronglyTypedContract(_, T, Dummy))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness (pre upgrading)
        "wrongly typed inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId, tmplId = Dummy),
            test = _ => CmdFlow.submit(defaultCmd),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getWronglyTypedContract,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, Dummy, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise of a local contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createT,
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseTemplate(T, asSCid(cid), n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveT,
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseTemplate(T, asSCid(cid), n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, T, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of an wrongly typed local contract
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createDummy,
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseTemplate(T, asSCid(cid), n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) {
            case Left(SError.InterpretationError(IE.WronglyTypedContract(_, T, Dummy))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveDummy,
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseTemplate(T, asSCid(cid), n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            Set(alice),
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, Dummy, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise of an unknown contract
      "unknown contract" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseTemplate(
                T,
                SValue.SContractId(cId),
                n"Choice",
                SEither.Left(SNat.fromInt(0)),
              )
            ),
          parties = Set(alice),
          getContract = PartialFunction.empty,
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe buildLog("starts test", "queries contract")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise with argument exceeding max nesting
      "argument exceeds max nesting" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseTemplate(
                T,
                SValue.SContractId(cId),
                n"Choice",
                SEither.Left(SNat.fromInt(100)),
              )
            ),
          parties = Set(alice),
          getContract = getContract,
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ValueNesting(_))) =>
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
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseTemplate(
                T,
                SValue.SContractId(cId),
                n"Choice",
                SEither.Right(SValue.SInt64(100L)),
              )
            ),
          parties = Set(alice),
          getContract = getContract,
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ValueNesting(_))) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            getContract = getWronglyTypedContract,
            getKeys = mapKeys(getKeys, getWronglyTypedContract),
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.Crash(_, _)) =>
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_by_key of a cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId),
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractKeyNotFound(gkey))) =>
            gkey.templateId shouldBe T
            msgs shouldBe buildLog("starts test", "maintainers", "queries key")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(Dummy, SValue.SContractId(cId))),
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            getContract = getWronglyTypedContract,
            getKeys = mapKeys(getKeys, getWronglyTypedContract),
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.Crash(_, _)) =>
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_by_key of a local contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createT,
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveT,
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractKeyNotFound(gKey))) =>
            gKey.templateId shouldBe T
            msgs shouldBe buildLog("starts test", "maintainers", "queries key")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of an unknown contract
      "unknown contract key" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
            ),
          parties = Set(alice),
          getContract = PartialFunction.empty,
          getKeys = PartialFunction.empty,
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ContractKeyNotFound(key))) =>
          key.templateId shouldBe T
          msgs shouldBe buildLog("starts test", "maintainers", "queries key")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key with argument exceeding max nesting
      "argument exceeds max nesting" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(100)))
            ),
          parties = Set(alice),
          getContract = getContract,
          getKeys = mapKeys(getKeys, getContract),
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ValueNesting(_))) =>
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
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Right(SValue.SInt64(100)))
            ),
          parties = Set(alice),
          getContract = getContract,
          getKeys = mapKeys(getKeys, getContract),
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ValueNesting(_))) =>
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

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val emptyKeyValue = SValue.SRecord(
          TKey,
          ImmArray("maintainers", "optCid", "nat").map(Ref.Name.assertFromString),
          ArraySeq(
            SValue.SList(FrontStack.empty),
            SValue.SOptional(None),
            SNat.fromInt(0),
          ),
        )
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseByKey(T, emptyKeyValue, n"Choice", SEither.Right(SNat.fromInt(0)))
            ),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) {
          case Left(SError.InterpretationError(IE.FetchEmptyContractKeyMaintainers(T, _, _))) =>
            msgs shouldBe buildLog("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key with contract ID in contract key
      "contract ID in contract key" in {
        val keyWithCid = SValue.SRecord(
          TKey,
          ImmArray("maintainers", "optCid", "nat").map(Ref.Name.assertFromString),
          ArraySeq(
            SValue.SList(FrontStack(SValue.SParty(alice))),
            SValue.SOptional(Some(SValue.SContractId(cId))),
            SNat.fromInt(0),
          ),
        )
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseByKey(T, keyWithCid, n"Choice", SEither.Right(SNat.fromInt(0)))
            ),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ContractIdInContractKey(_))) =>
          msgs shouldBe buildLog("starts test", "maintainers")
        }
      }
    }
    "exercise_interface" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise by interface of a non-cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseInterface(
                  I,
                  SValue.SContractId(cId),
                  n"Choice",
                  SEither.Left(SNat.fromInt(0)),
                )
              ),
            parties = Set(alice),
            getContract = getContract,
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog(
              "starts test",
              "queries contract",
              "precondition",
              "contract signatories",
              "contract observers",
              "key",
              "maintainers",
              "interface choice controllers",
              "interface choice observers",
              "authorizes exercise",
              "choice body",
              "ends test",
            )
          }
        }
        // TEST_EVIDENCE: Integrity: exercise_interface with a contract instance that does not implement the interface fails.
        "contract doesn't implement interface" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseInterface(
                  I,
                  SValue.SContractId(cId),
                  n"Choice",
                  SEither.Left(SNat.fromInt(0)),
                )
              ),
            parties = Set(alice),
            getContract = getWronglyTypedContract,
            packageResolution = packageNameMap,
          )
          inside(res) {
            case Left(SError.InterpretationError(IE.ContractDoesNotImplementInterface(_, _, _))) =>
              msgs shouldBe buildLog("starts test", "queries contract")
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_interface of a cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseInterface(
                  I,
                  SValue.SContractId(cId),
                  n"Choice",
                  SEither.Left(SNat.fromInt(0)),
                )
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog(
              "starts test",
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId),
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseInterface(
                  I,
                  SValue.SContractId(cId),
                  n"Choice",
                  SEither.Left(SNat.fromInt(0)),
                )
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, T, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise by interface of a cached global contract that does not implement the interface.
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(Dummy, SValue.SContractId(cId))),
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseInterface(
                  I,
                  SValue.SContractId(cId),
                  n"Choice",
                  SEither.Left(SNat.fromInt(0)),
                )
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Left(
                  SError.InterpretationError(IE.ContractDoesNotImplementInterface(I, _, Dummy))
                ) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId, tmplId = Dummy),
            test = _ =>
              CmdFlow.submit(
                Command.ExerciseInterface(
                  I,
                  SValue.SContractId(cId),
                  n"Choice",
                  SEither.Left(SNat.fromInt(0)),
                )
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getWronglyTypedContract,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, Dummy, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful exercise_interface of a local contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createT,
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseInterface(I, asSCid(cid), n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog(
              "starts test",
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveT,
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseInterface(I, asSCid(cid), n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, T, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise_interface of an local contract not implementing the interface
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createDummy,
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseInterface(I, asSCid(cid), n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
          )
          inside(res) {
            case Left(
                  SError.InterpretationError(
                    IE.ContractDoesNotImplementInterface(I, _, Dummy)
                  )
                ) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: This checks that type checking in exercise_interface is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveDummy,
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseInterface(I, asSCid(cid), n"Choice", SEither.Left(SNat.fromInt(0)))
              ),
            parties = Set(alice),
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, Dummy, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }
      }
    }

    "fetch" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch of a non-cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            parties = Set(alice),
            getContract = getWronglyTypedContract,
            packageResolution = packageNameMap,
          )
          inside(res) {
            case Left(SError.InterpretationError(IE.WronglyTypedContract(_, T, Dummy))) =>
              msgs shouldBe buildLog("starts test", "queries contract")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of a non-cached global contract with inconsistent key
        if (withKey) "inconsistent key" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.QueryNByKey(T, SValue.SInt64(1L), keySValue)),
            test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            parties = Set(alice),
            getContract = getContract,
            getKeys = PartialFunction.empty,
            packageResolution = packageNameMap,
          )

          inside(res) { case Left(SError.InterpretationError(IE.InconsistentContractKey(_))) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            parties = Set(alice),
            getContract = getContract,
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog("starts test", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId),
            test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            parties = Set(alice),
            getContract = getContract,
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, T, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of a wrongly typed cached global contract
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(Dummy, SValue.SContractId(cId))),
            test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            parties = Set(alice),
            getContract = getWronglyTypedContract,
            packageResolution = packageNameMap,
          )
          inside(res) {
            case Left(SError.InterpretationError(IE.WronglyTypedContract(_, T, Dummy))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId, tmplId = Dummy),
            test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            parties = Set(alice),
            getContract = getWronglyTypedContract,
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, Dummy, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch of a local contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createT,
            test = cid => CmdFlow.submit(Command.FetchTemplate(T, asSCid(cid))),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog("starts test", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveT,
            test = cid => CmdFlow.submit(Command.FetchTemplate(T, asSCid(cid))),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, T, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of a wrongly typed local contract
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createDummy,
            test = cid => CmdFlow.submit(Command.FetchTemplate(T, asSCid(cid))),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) {
            case Left(SError.InterpretationError(IE.WronglyTypedContract(_, T, Dummy))) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveDummy,
            test = cid => CmdFlow.submit(Command.FetchTemplate(T, asSCid(cid))),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, Dummy, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch of an unknown contract
      "unknown contract" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
          parties = Set(alice),
          getContract = PartialFunction.empty,
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SpeedyTestLib.UnknownContract(`cId`)) =>
          msgs shouldBe buildLog("starts test", "queries contract")
        }
      }
    }

    if (withKey) "fetch_by_key" - {
      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_by_key of a non-cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ => CmdFlow.submit(Command.FetchByKey(T, keySValue)),
            parties = Set(alice),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ => CmdFlow.submit(Command.FetchByKey(T, keySValue)),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getWronglyTypedContract,
            getKeys = mapKeys(getKeys, getWronglyTypedContract),
          )
          inside(res) { case Left(SError.Crash(_, _)) =>
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_by_key of a cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            test = _ => CmdFlow.submit(Command.FetchByKey(T, keySValue)),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog(
              "starts test",
              "maintainers",
              "queries key",
              "authorizes fetch",
              "ends test",
            )
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId),
            test = _ => CmdFlow.submit(Command.FetchByKey(T, keySValue)),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractKeyNotFound(key))) =>
            key.templateId shouldBe T
            msgs shouldBe buildLog("starts test", "maintainers", "queries key")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_by_key of a local contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createT,
            test = _ => CmdFlow.submit(Command.FetchByKey(T, keySValue)),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog("starts test", "maintainers", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveT,
            test = _ => CmdFlow.submit(Command.FetchByKey(T, keySValue)),
            parties = Set(alice),
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractKeyNotFound(key))) =>
            key.templateId shouldBe T
            msgs shouldBe buildLog("starts test", "maintainers", "queries key")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of an unknown contract key
      "unknown contract key" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ => CmdFlow.submit(Command.FetchByKey(T, keySValue)),
          parties = Set(alice),
          packageResolution = packageNameMap,
          getContract = PartialFunction.empty,
          getKeys = PartialFunction.empty,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ContractKeyNotFound(key))) =>
          key.templateId shouldBe T
          msgs shouldBe buildLog("starts test", "maintainers", "queries key")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key with empty contract key maintainers
      "empty contract key maintainers" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.FetchByKey(T, keySValue.update("maintainers", SValue.SList(FrontStack.empty)))
            ),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) {
          case Left(SError.InterpretationError(IE.FetchEmptyContractKeyMaintainers(T, _, _))) =>
            msgs shouldBe buildLog("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key with contract ID in contract key
      "contract ID in contract key" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.FetchByKey(
                T,
                keySValue.update("optCid", SValue.SOptional(Some(SValue.SContractId(cId)))),
              )
            ),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ContractIdInContractKey(_))) =>
          msgs shouldBe buildLog("starts test", "maintainers")
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key with contract key exceeding max nesting
      "key exceeds max nesting" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test =
            _ => CmdFlow.submit(Command.FetchByKey(T, keySValue.update("nat", SNat.fromInt(100)))),
          parties = Set(alice),
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SError.InterpretationError(IE.ValueNesting(_))) =>
          msgs shouldBe buildLog("starts test", "maintainers")
        }
      }
    }

    "fetch_interface" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_interface of a non-cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ => CmdFlow.submit(Command.FetchInterface(I, SValue.SContractId(cId))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Right(_) =>
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
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            test = _ => CmdFlow.submit(Command.FetchInterface(I, SValue.SContractId(cId))),
            parties = Set(alice),
            getContract = getWronglyTypedContract,
            packageResolution = packageNameMap,
          )
          inside(res) {
            case Left(
                  SError.InterpretationError(IE.ContractDoesNotImplementInterface(I, _, Dummy))
                ) =>
              msgs shouldBe buildLog("starts test", "queries contract")
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_interface of a cached global contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
            test = _ => CmdFlow.submit(Command.FetchInterface(I, SValue.SContractId(cId))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog("starts test", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of an inactive global contract
        "inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId),
            test = _ => CmdFlow.submit(Command.FetchInterface(I, SValue.SContractId(cId))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getContract,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, T, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of a cached global contract not implementing the interface.
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = CmdFlow.submit(Command.FetchTemplate(Dummy, SValue.SContractId(cId))),
            test = _ => CmdFlow.submit(Command.FetchInterface(I, SValue.SContractId(cId))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getWronglyTypedContract,
          )
          inside(res) {
            case Left(
                  SError.InterpretationError(IE.ContractDoesNotImplementInterface(I, _, Dummy))
                ) =>
              msgs shouldBe buildLog("starts test")
          }
        }

        // This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = archive(cId, tmplId = Dummy),
            test = _ => CmdFlow.submit(Command.FetchInterface(I, SValue.SContractId(cId))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = getWronglyTypedContract,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, Dummy, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful fetch_interface of a local contract
        "success" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createT,
            test = cid => CmdFlow.submit(Command.FetchInterface(I, asSCid(cid))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = PartialFunction.empty,
          )
          inside(res) { case Right(_) =>
            msgs shouldBe buildLog("starts test", "authorizes fetch", "ends test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of an inactive local contract
        "inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveT,
            test = cid => CmdFlow.submit(Command.FetchInterface(I, asSCid(cid))),
            parties = Set(alice),
            getContract = PartialFunction.empty,
            packageResolution = packageNameMap,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, T, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of an local contract not implementing the interface
        "wrongly typed contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createDummy,
            test = cid => CmdFlow.submit(Command.FetchInterface(I, asSCid(cid))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = PartialFunction.empty,
          )
          inside(res) {
            case Left(
                  SError.InterpretationError(IE.ContractDoesNotImplementInterface(I, _, Dummy))
                ) =>
              msgs shouldBe buildLog("starts test")
          }
        }
        // TEST_EVIDENCE: Integrity: This checks that type checking is done after checking activeness.
        "wrongly typed inactive contract" in {
          val (res, msgs) = runCmdFlow(
            pkgs = pkgs,
            setup = createAndArchiveDummy,
            test = cid => CmdFlow.submit(Command.FetchInterface(I, asSCid(cid))),
            parties = Set(alice),
            packageResolution = packageNameMap,
            getContract = PartialFunction.empty,
          )
          inside(res) { case Left(SError.InterpretationError(IE.ContractNotActive(_, Dummy, _))) =>
            msgs shouldBe buildLog("starts test")
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of fetch_interface of an unknown contract
      "unknown contract" in {
        val (res, msgs) = runCmdFlow(
          pkgs = pkgs,
          test = _ => CmdFlow.submit(Command.FetchInterface(I, SValue.SContractId(cId))),
          parties = Set(alice),
          getContract = PartialFunction.empty,
          packageResolution = packageNameMap,
        )
        inside(res) { case Left(SpeedyTestLib.UnknownContract(`cId`)) =>
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
              val (res, msgs) = runCmdFlow(
                pkgs = pkgs,
                test =
                  _ => CmdFlow.submit(Command.QueryNByKey(T, SValue.SInt64(n.toLong), keySValue)),
                parties = Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
                packageResolution = packageNameMap,
              )
              inside(res) { case Right(_) =>
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

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of successful query_n_by_key of a cached global contract
        "success" - {
          for (n <- Seq(1, 2, 5)) {
            s"n=$n" in {
              val (res, msgs) = runCmdFlow(
                pkgs = pkgs,
                setup = CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
                test =
                  _ => CmdFlow.submit(Command.QueryNByKey(T, SValue.SInt64(n.toLong), keySValue)),
                parties = Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
                packageResolution = packageNameMap,
              )
              inside(res) { case Right(_) =>
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
              val (res, msgs) = runCmdFlow(
                pkgs = pkgs,
                setup = archive(cId),
                test =
                  _ => CmdFlow.submit(Command.QueryNByKey(T, SValue.SInt64(n.toLong), keySValue)),
                parties = Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
                packageResolution = packageNameMap,
              )
              inside(res) { case Right(_) =>
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
              val (res, msgs) = runCmdFlow(
                pkgs = pkgs,
                setup = createT,
                test =
                  _ => CmdFlow.submit(Command.QueryNByKey(T, SValue.SInt64(n.toLong), keySValue)),
                parties = Set(alice),
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
                packageResolution = packageNameMap,
              )
              inside(res) { case Right(_) =>
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
              val (res, msgs) = runCmdFlow(
                pkgs = pkgs,
                setup = createAndArchiveT,
                test =
                  _ => CmdFlow.submit(Command.QueryNByKey(T, SValue.SInt64(n.toLong), keySValue)),
                parties = Set(alice),
                packageResolution = packageNameMap,
                getContract = getContracts(n),
                getKeys = mapKeys(getKeysWithNContracts(n), getContracts(n)),
              )
              inside(res) { case Right(_) =>
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
              val (res, msgs) = runCmdFlow(
                pkgs = pkgs,
                test = _ =>
                  CmdFlow.submit(
                    Command.QueryNByKey(
                      T,
                      SValue.SInt64(n.toLong),
                      keySValue
                        .update("maintainers", SValue.SList(FrontStack(SValue.SParty(charlie)))),
                    )
                  ),
                parties = Set(alice),
                getContract = PartialFunction.empty,
                getKeys = PartialFunction.empty,
                packageResolution = packageNameMap,
              )
              inside(res) { case Right(_) =>
                msgs shouldBe buildLog(
                  "starts test",
                  "maintainers",
                  "authorizes lookup-by-key",
                  "queries key",
                  "ends test",
                )
              }
            }
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key with empty contract key maintainers
      "empty contract key maintainers" - {
        for (n <- Seq(1, 2, 5)) {
          s"n=$n" in {
            val (res, msgs) = runCmdFlow(
              pkgs = pkgs,
              test = _ =>
                CmdFlow.submit(
                  Command.QueryNByKey(
                    T,
                    SValue.SInt64(n.toLong),
                    keySValue.update("maintainers", SValue.SList(FrontStack.empty)),
                  )
                ),
              parties = Set(alice),
              packageResolution = packageNameMap,
            )
            inside(res) {
              case Left(SError.InterpretationError(IE.FetchEmptyContractKeyMaintainers(T, _, _))) =>
                msgs shouldBe buildLog("starts test", "maintainers")
            }
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key with contract ID in contract key
      "contract ID in contract key" - {
        for (n <- Seq(1, 2, 5)) {
          s"n=$n" in {
            val (res, msgs) = runCmdFlow(
              pkgs = pkgs,
              test = _ =>
                CmdFlow.submit(
                  Command.QueryNByKey(
                    T,
                    SValue.SInt64(n.toLong),
                    keySValue.update("optCid", SValue.SOptional(Some(SValue.SContractId(cId)))),
                  )
                ),
              parties = Set(alice),
              packageResolution = packageNameMap,
            )
            inside(res) { case Left(SError.InterpretationError(IE.ContractIdInContractKey(_))) =>
              msgs shouldBe buildLog("starts test", "maintainers")
            }
          }
        }
      }

      // TEST_EVIDENCE: Integrity: Evaluation order of query_n_by_key with contract key exceeding max nesting
      "key exceeds max nesting" - {
        for (n <- Seq(1, 2, 5)) {
          s"n=$n" in {
            val (res, msgs) = runCmdFlow(
              pkgs = pkgs,
              test = _ =>
                CmdFlow.submit(
                  Command.QueryNByKey(
                    T,
                    SValue.SInt64(n.toLong),
                    keySValue.update("nat", SNat.fromInt(100)),
                  )
                ),
              parties = Set(alice),
              packageResolution = packageNameMap,
            )
            inside(res) { case Left(SError.InterpretationError(IE.ValueNesting(_))) =>
              msgs shouldBe buildLog("starts test", "maintainers")
            }
          }
        }
      }
    }
  }

  def mapKeys[K, V, R](getKeys: Map[K, Vector[V]], getContract: V => R): Map[K, Vector[R]] =
    getKeys.view.mapValues(_.map(getContract)).toMap
}
