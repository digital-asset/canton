// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.crypto.SValueHash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.ledger.FailedAuthorization
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class UpdMachineAuthorizationTest_V2Dev
    extends AuthorizationTest(LanguageVersion.v2_dev, withKey = true)
    with CmdFlowRunnerWithUpdateMachine
class UpdMachineAuthorizationTest_V23
    extends AuthorizationTest(LanguageVersion.v2_3, withKey = true)
    with CmdFlowRunnerWithUpdateMachine

class ConductorAuthorizationTest_V2Dev
    extends AuthorizationTest(LanguageVersion.v2_dev, withKey = true)
    with CmdFlowRunnerWithTransactionConductor
class ConductorAuthorizationTest_V23
    extends AuthorizationTest(LanguageVersion.v2_3, withKey = true)
    with CmdFlowRunnerWithTransactionConductor

/** Tests for authorization failure.
  *
  * These tests were extracted from EvaluationOrderTest where they were removed when the
  * AuthorizationCheckerLogger (a dummy that always succeeds) was introduced. Here we use the
  * DefaultAuthorizationChecker (the real authorization checker) so that actual
  * ledger.FailedAuthorization errors are produced.
  *
  * NOTE: Many values and helpers are duplicated from EvaluationOrderTest because the originals are
  * private[this] and cannot be accessed from a separate file. Do NOT modify EvaluationOrderTest to
  * change visibility.
  */
abstract class AuthorizationTest(languageVersion: LanguageVersion, withKey: Boolean)
    extends AnyFreeSpec
    with CmdFlowRunner
    with Matchers
    with Inside
    with SuppressingLogging {

  private val testPkg = new TestPkg(withKey, languageVersion, cmdMode = cmdMode)
  import TestPkg.*, testPkg.*

  private val testTxVersion: SerializationVersion = serializationVersion

  // NOTE: duplicated from EvaluationOrderTest (private[this] there)
  private[this] def buildContract(
      observer: Ref.Party,
      contractId: Value.ContractId,
  ): FatContractInstance =
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      testTxVersion,
      packageName = pkg.pkgName,
      template = T,
      arg = payload(alice, observer).toNormalizedValue,
      signatories = List(alice),
      observers = List(observer),
      contractKeyWithMaintainers = Option.when(withKey)(
        GlobalKeyWithMaintainers(
          GlobalKey(
            templateId = T,
            packageName = pkg.pkgName,
            key = normalizedKeyValue,
            hash = SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
          ),
          Set(alice),
        )
      ),
      contractId = contractId,
    )

  private[this] val getContract = { val c = buildContract(bob, cId); Map(c.contractId -> c) }

  private[this] val getKeys = Map(
    GlobalKey(
      templateId = T,
      packageName = pkg.pkgName,
      key = keyValue,
      hash = SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
    ) -> Vector(cId)
  )

  private[this] val cIds = Vector(cId, cId2, cId3, cId4, cId5)

  private[this] def getKeysWithNContracts(n: Int) = Map(
    GlobalKey(
      templateId = T,
      packageName = pkg.pkgName,
      key = keyValue,
      hash = SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
    ) -> cIds.take(n)
  )

  private def mapKeys[K, V, R](getKeys: Map[K, Vector[V]], getContract: V => R): Map[K, Vector[R]] =
    getKeys.view.mapValues(_.map(getContract)).toMap

  private[this] def contractsToMap(contracts: Seq[FatContractInstance]) =
    contracts.map(c => c.contractId -> c).toMap
  private[this] val contracts =
    Seq(cId, cId2, cId3, cId4, cId5).map(buildContract(bob, _))
  private[this] def getContracts(n: Int) = contractsToMap(contracts.take(n))

  // ---------------------------------------------------------------------------
  // Authorization failure tests
  // ---------------------------------------------------------------------------

  private def failedAuthorization(result: Either[Throwable, SValue]): FailedAuthorization =
    inside(result) { case Left(SError.SErrorDamlException(IE.FailedAuthorization(_, failure))) =>
      failure
    }

  private val realAuthorizationChecker: RecordingMachineLogger => AuthorizationChecker =
    _ => DefaultAuthorizationChecker

  "authorization failures" - {
    "create" in {
      val (result, _) = runCmdFlow(
        pkgs = pkgs,
        test = _ => CmdFlow.submit(Command.Create(T, payload(alice, bob))),
        parties = Set(bob),
        packageResolution = packageNameMap,
        authorizationChecker = realAuthorizationChecker,
      )
      inside(failedAuthorization(result)) {
        case FailedAuthorization.CreateMissingAuthorization(`T`, _, authorizing, required) =>
          authorizing shouldBe Set(bob)
          required shouldBe Set(alice)
      }
    }

    "exercise" in {
      val (result, _) = runCmdFlow(
        pkgs = pkgs,
        test = _ =>
          CmdFlow.submit(
            Command
              .ExerciseTemplate(
                T,
                SValue.SContractId(cId),
                n"Choice",
                SEither.Left(SNat.fromInt(0)),
              )
          ),
        parties = Set(charlie),
        packageResolution = packageNameMap,
        getContract = getContract,
        authorizationChecker = realAuthorizationChecker,
      )
      inside(failedAuthorization(result)) {
        case FailedAuthorization.ExerciseMissingAuthorization(
              `T`,
              "Choice",
              _,
              authorizing,
              required,
            ) =>
          authorizing shouldBe Set(charlie)
          required shouldBe Set(alice)
      }
    }

    "exercise interface" in {
      val (result, _) = runCmdFlow(
        pkgs = pkgs,
        test = _ =>
          CmdFlow.submit(
            Command
              .ExerciseInterface(
                I,
                SValue.SContractId(cId),
                n"Choice",
                SEither.Left(SNat.fromInt(0)),
              )
          ),
        parties = Set(charlie),
        readAs = Set(alice),
        packageResolution = packageNameMap,
        getContract = getContract,
        authorizationChecker = realAuthorizationChecker,
      )
      inside(failedAuthorization(result)) {
        case FailedAuthorization.ExerciseMissingAuthorization(
              `T`,
              "Choice",
              _,
              authorizing,
              required,
            ) =>
          authorizing shouldBe Set(charlie)
          required shouldBe Set(alice)
      }
    }

    "fetch" in {
      val (result, _) = runCmdFlow(
        pkgs = pkgs,
        test = _ => CmdFlow.submit(Command.FetchTemplate(T, SValue.SContractId(cId))),
        parties = Set(charlie),
        packageResolution = packageNameMap,
        getContract = getContract,
        authorizationChecker = realAuthorizationChecker,
      )
      inside(failedAuthorization(result)) {
        case FailedAuthorization.FetchMissingAuthorization(`T`, _, stakeholders, authorizing) =>
          stakeholders shouldBe Set(alice, bob)
          authorizing shouldBe Set(charlie)
      }
    }

    if (withKey) "key operations" - {
      "exercise by key" in {
        val (result, _) = runCmdFlow(
          pkgs = pkgs,
          test = _ =>
            CmdFlow.submit(
              Command.ExerciseByKey(T, keySValue, n"Choice", SEither.Left(SNat.fromInt(0)))
            ),
          parties = Set(charlie),
          packageResolution = packageNameMap,
          getContract = getContract,
          getKeys = mapKeys(getKeys, getContract),
          authorizationChecker = realAuthorizationChecker,
        )
        inside(failedAuthorization(result)) {
          case FailedAuthorization
                .ExerciseMissingAuthorization(`T`, "Choice", _, authorizing, required) =>
            authorizing shouldBe Set(charlie)
            required shouldBe Set(alice)
        }
      }

      "fetch by key" in {
        val (result, _) = runCmdFlow(
          pkgs = pkgs,
          test = _ => CmdFlow.submit(Command.FetchByKey(T, keySValue)),
          parties = Set(charlie),
          packageResolution = packageNameMap,
          getContract = getContract,
          getKeys = mapKeys(getKeys, getContract),
          authorizationChecker = realAuthorizationChecker,
        )
        inside(failedAuthorization(result)) {
          case FailedAuthorization.FetchMissingAuthorization(`T`, _, stakeholders, authorizing) =>
            stakeholders shouldBe Set(alice, bob)
            authorizing shouldBe Set(charlie)
        }
      }

      "query n by key" in {
        val (result, _) = runCmdFlow(
          pkgs = pkgs,
          test = _ => CmdFlow.submit(Command.QueryNByKey(T, SValue.SInt64(1L), keySValue)),
          parties = Set(charlie),
          packageResolution = packageNameMap,
          getContract = getContracts(1),
          getKeys = mapKeys(getKeysWithNContracts(1), getContracts(1)),
          authorizationChecker = realAuthorizationChecker,
        )
        inside(failedAuthorization(result)) {
          case FailedAuthorization
                .LookupByKeyMissingAuthorization(`T`, _, maintainers, authorizing) =>
            maintainers shouldBe Set(alice)
            authorizing shouldBe Set(charlie)
        }
      }
    }
  }

}
