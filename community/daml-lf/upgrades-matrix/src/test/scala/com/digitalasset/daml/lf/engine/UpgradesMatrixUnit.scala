// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.engine.Error as EE
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.speedy.ValueTranslator
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{CreationTime, *}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.*
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{Assertion, ParallelTestExecution}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

// Split the Upgrade unit tests over four suites, which seems to be the sweet
// spot (~95s instead of ~185s runtime)
class UpgradesMatrixUnit0 extends UpgradesMatrixUnit(UpgradesMatrixCasesV2MaxStable, 4, 0)
class UpgradesMatrixUnit1 extends UpgradesMatrixUnit(UpgradesMatrixCasesV2MaxStable, 4, 1)
class UpgradesMatrixUnit2 extends UpgradesMatrixUnit(UpgradesMatrixCasesV2MaxStable, 4, 2)
class UpgradesMatrixUnit3 extends UpgradesMatrixUnit(UpgradesMatrixCasesV2MaxStable, 4, 3)

/** A test suite to run the UpgradesMatrix matrix directly in the engine
  *
  * This runs a lot more quickly (~35s on a single suite) than UpgradesMatrixIT (~5000s) because it
  * does not need to spin up Canton, so we can use this for sanity checking before running
  * UpgradesMatrixIT.
  */
class UpgradesMatrixUnit(upgradesMatrixCases: UpgradesMatrixCases, n: Int, k: Int)
    extends AsyncWordSpec
    with ParallelTestExecution
    with Matchers
    with UpgradesMatrix[Error, (SubmittedTransaction, Transaction.Metadata), Unit]
    with SuppressingLogging {
  override val cases = upgradesMatrixCases
  defineTestCases()

  override def nk = Some((n, k))
  override def testHelperToDeclaration(
      testHelper: UpgradesMatrixCases#TestHelper,
      assertion: ExecutionContext => Future[Assertion],
  ): Unit =
    testHelper.title in assertion(executionContext)

  def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))

  override def setup(testHelper: UpgradesMatrixCases#TestHelper)(implicit
      ec: ExecutionContext
  ): Future[UpgradesMatrixCases.SetupData[Unit]] =
    Future.successful(
      UpgradesMatrixCases.SetupData(
        alice = Party.assertFromString("Alice"),
        bob = Party.assertFromString("Bob"),
        clientLocalContractId = toContractId("client-local"),
        clientGlobalContractId = toContractId("client-global"),
        globalContractId = toContractId("1"),
        extraGlobalContractId = toContractId("2"),
        additionalSetup = (),
      )
    )

  def normalize(value: Value, typ: Ast.Type): Value =
    new ValueTranslator(
      cases.compiledPackages.pkgInterface,
      forbidLocalContractIds = true,
      forbidTrailingNones = false,
    )
      .translateValue(typ, value) match {
      case Left(err) => throw new RuntimeException(s"Normalization failed: $err")
      case Right(sValue) => sValue.toNormalizedValue
    }

  def newEngine() = new Engine(cases.engineConfig, loggerFactory)

  override def execute(
      setupData: UpgradesMatrixCases.SetupData[Unit],
      testHelper: UpgradesMatrixCases#TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: UpgradesMatrixCases.ContractOrigin,
      creationPackageStatus: UpgradesMatrixCases.CreationPackageStatus,
  )(implicit
      ec: ExecutionContext
  ): Future[Either[Error, (SubmittedTransaction, Transaction.Metadata)]] =
    // We can drop the implicit ec here, since it will be the same as the one
    // provided by AsyncWordSpec
    executeWithoutEC(setupData, testHelper, apiCommands, contractOrigin, creationPackageStatus)

  def executeWithoutEC(
      setupData: UpgradesMatrixCases.SetupData[Unit],
      testHelper: UpgradesMatrixCases#TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: UpgradesMatrixCases.ContractOrigin,
      creationPackageStatus: UpgradesMatrixCases.CreationPackageStatus,
  ): Future[Either[Error, (SubmittedTransaction, Transaction.Metadata)]] = Future {
    val clientLocalContract: FatContractInstance =
      TransactionBuilder.fatContractInstanceWithDummyDefaults(
        version = cases.serializationVersion,
        packageName = cases.clientLocalPkg.pkgName,
        template = testHelper.clientLocalTplId,
        arg = testHelper.clientContractArg(setupData.alice, setupData.bob),
        signatories = immutable.Set(setupData.alice),
      )

    val clientGlobalContract: FatContractInstance =
      TransactionBuilder.fatContractInstanceWithDummyDefaults(
        version = cases.serializationVersion,
        packageName = cases.clientGlobalPkg.pkgName,
        template = testHelper.clientGlobalTplId,
        arg = testHelper.clientContractArg(setupData.alice, setupData.bob),
        signatories = immutable.Set(setupData.alice),
      )

    val globalContract: FatContractInstance = FatContractInstanceImpl(
      version = cases.serializationVersion,
      contractId = setupData.globalContractId,
      packageName = cases.templateDefsPkgName,
      templateId = testHelper.v1TplId,
      createArg = normalize(
        testHelper.globalContractArgV1(setupData.alice, setupData.bob),
        Ast.TTyCon(testHelper.v1TplId),
      ),
      signatories = immutable.TreeSet(setupData.alice),
      stakeholders = immutable.TreeSet(setupData.alice),
      contractKeyWithMaintainers = testHelper.globalContractV1KeyWithMaintainers(setupData),
      createdAt = CreationTime.CreatedAt(Time.Timestamp.Epoch),
      authenticationData = Bytes.assertFromString("00"),
    )

    val extraGlobalContract: FatContractInstance = FatContractInstanceImpl(
      version = cases.serializationVersion,
      contractId = setupData.extraGlobalContractId,
      packageName = cases.templateDefsPkgName,
      templateId = testHelper.v1TplId,
      createArg = normalize(
        testHelper.globalContractArgV1(setupData.alice, setupData.bob),
        Ast.TTyCon(testHelper.v1TplId),
      ),
      signatories = immutable.TreeSet(setupData.alice),
      stakeholders = immutable.TreeSet(setupData.alice),
      contractKeyWithMaintainers = testHelper.globalContractV1KeyWithMaintainers(setupData),
      createdAt = CreationTime.CreatedAt(Time.Timestamp.Epoch),
      authenticationData = Bytes.assertFromString("00"),
    )

    val participant = ParticipantId.assertFromString("participant")
    val submissionSeed = crypto.Hash.hashPrivateKey("command")
    val submitters = Set(setupData.alice)
    val readAs = Set.empty[Party]

    val lookupContractById = (contractOrigin, testHelper.operation) match {
      case (_, UpgradesMatrixCases.LookupNByKey) =>
        Map(
          setupData.clientLocalContractId -> clientLocalContract,
          setupData.clientGlobalContractId -> clientGlobalContract,
          setupData.globalContractId -> globalContract,
          setupData.extraGlobalContractId -> extraGlobalContract,
        )
      case (UpgradesMatrixCases.Global | UpgradesMatrixCases.Disclosed, _) =>
        Map(
          setupData.clientLocalContractId -> clientLocalContract,
          setupData.clientGlobalContractId -> clientGlobalContract,
          setupData.globalContractId -> globalContract,
        )
      case _ =>
        Map(
          setupData.clientLocalContractId -> clientLocalContract,
          setupData.clientGlobalContractId -> clientGlobalContract,
        )
    }
    val lookupContractByKey = (contractOrigin, testHelper.operation) match {
      case (_, UpgradesMatrixCases.LookupNByKey) =>
        (
            (gkey: GlobalKey) =>
              testHelper
                .globalContractV1KeyWithMaintainers(setupData)
                .flatMap(helperKey =>
                  Option.when(helperKey.globalKey == gkey)(
                    Vector(extraGlobalContract, globalContract)
                  )
                )
        ).unlift
      case (UpgradesMatrixCases.Global | UpgradesMatrixCases.Disclosed, _) =>
        (
            (gkey: GlobalKey) =>
              testHelper
                .globalContractV1KeyWithMaintainers(setupData)
                .flatMap(helperKey =>
                  Option.when(helperKey.globalKey == gkey)(Vector(globalContract))
                )
        ).unlift
      case _ => PartialFunction.empty
    }

    def hash(fci: FatContractInstance): crypto.Hash =
      newEngine()
        .hashCreateNode(fci.toCreateNode, identity, crypto.Hash.HashingMethod.TypedNormalForm)
        .consume(pkgs = cases.allPackages)
        .fold(e => throw new IllegalArgumentException(s"hashing $fci failed: $e"), identity)

    val hashes = Map(
      setupData.clientLocalContractId -> hash(clientLocalContract),
      setupData.clientGlobalContractId -> hash(clientGlobalContract),
      setupData.globalContractId -> hash(globalContract),
      setupData.extraGlobalContractId -> hash(extraGlobalContract),
    )

    newEngine()
      .submit(
        packageMap = cases.packageMap,
        packagePreference = Set(
          cases.commonDefsPkgId,
          cases.templateDefsV2PkgId,
          cases.clientLocalPkgId,
          cases.clientGlobalPkgId,
        ),
        submitters = submitters,
        readAs = readAs,
        cmds = ApiCommands(apiCommands, Time.Timestamp.Epoch, "test"),
        participantId = participant,
        submissionSeed = submissionSeed,
        contractIdVersion = cases.contractIdVersion,
        contractStateMode = cases.contractStateMode,
        prefetchKeys = Seq.empty,
      )
      .consume(
        pcs = lookupContractById,
        pkgs = creationPackageStatus match {
          case UpgradesMatrixCases.CreationPackageVetted => cases.allPackages
          case UpgradesMatrixCases.CreationPackageUnvetted => cases.allNonCreationPackages
        },
        keys = lookupContractByKey,
        idValidator = (cid, hash) =>
          hashes.get(cid) match {
            case Some(expectedHash) => hash == expectedHash
            case None => false
          },
      )
  }

  override def assertResultMatchesExpectedOutcome(
      result: Either[Error, (SubmittedTransaction, Transaction.Metadata)],
      expectedOutcome: UpgradesMatrixCases.ExpectedOutcome,
  )(implicit ec: ExecutionContext): Assertion =
    expectedOutcome match {
      case UpgradesMatrixCases.ExpectSuccess =>
        inside(result) { case Right((VersionedTransaction(_, nodes, rootNodes), _)) =>
          rootNodes.toSeq.map(nodes(_)).collect { case e: Node.Exercise => e } match {
            case Seq(e) =>
              e.exerciseResult match {
                case Some(ValueText(t)) =>
                  // TODO(#32310) We have choices that are expected to succeed and
                  // need to have an exception-free failure when they fail.
                  // Currently, they return text, and we match for the phrase in
                  // UpgradesMatrixCases.unexpectedErrorMessage. Later, we should
                  // return Either and match on the variant instead.
                  t should not include UpgradesMatrixCases.unexpectedErrorMessage
                case Some(_) =>
                  succeed // non-text type in result
                case None =>
                  fail("Expected success, got an exercise with no result value")
              }
            case Seq() =>
              fail("Expected success, got no exercise node")
            case _ =>
              fail("Expected success, got more than one exercise node")
          }
        }
      case UpgradesMatrixCases.ExpectUpgradeError =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.Upgrade]
        }
      case UpgradesMatrixCases.ExpectAuthenticationError =>
        inside(result) {
          case Left(EE.Interpretation(EE.Interpretation.DamlException(IE.Upgrade(error)), _)) =>
            error shouldBe a[IE.Upgrade.AuthenticationFailed]
        }
      case UpgradesMatrixCases.ExpectRuntimeTypeMismatchError =>
        inside(result) {
          case Left(EE.Interpretation(EE.Interpretation.DamlException(IE.Upgrade(error)), _)) =>
            error shouldBe a[IE.Upgrade.TranslationFailed]
        }
      case UpgradesMatrixCases.ExpectPreprocessingError =>
        inside(result) { case Left(error) =>
          error shouldBe a[EE.Preprocessing]
        }
      case UpgradesMatrixCases.ExpectPreconditionViolated =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.TemplatePreconditionViolated]
        }
      case UpgradesMatrixCases.ExpectUnhandledException(expectedMsg) =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          inside(error) { case e: IE.FailureStatus =>
            e.errorMessage should include(expectedMsg)
          }
        }
      case UpgradesMatrixCases.ExpectInternalInterpretationError =>
        inside(result) { case Left(EE.Interpretation(error, _)) =>
          error shouldBe a[EE.Interpretation.Internal]
        }
    }
}
