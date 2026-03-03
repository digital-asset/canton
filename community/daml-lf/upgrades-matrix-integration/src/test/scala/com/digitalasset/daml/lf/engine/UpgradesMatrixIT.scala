// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.admin.package_management_service.VettedPackagesChange.{
  Operation,
  Unvet,
  Vet,
}
import com.daml.ledger.api.v2.admin.package_management_service.{
  UpdateVettedPackagesRequest,
  VettedPackagesChange,
  VettedPackagesRef,
}
import com.daml.ledger.api.v2.admin.user_management_service as user
import com.daml.ledger.api.v2.command_service.SubmitAndWaitForTransactionResponse
import com.daml.ledger.api.v2.commands.{
  Command,
  Commands,
  CreateAndExerciseCommand,
  CreateCommand,
  DisclosedContract,
  ExerciseByKeyCommand,
  ExerciseCommand,
}
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
}
import com.daml.ledger.api.v2.value as api
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.integration.ConfigTransforms
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.ledger.api.util.LfEngineToApi.{
  lfValueToApiRecord,
  lfValueToApiValue,
  toApiIdentifier,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.daml.lf.archive.DamlLf.*
import com.digitalasset.daml.lf.archive.{ArchiveParser, Dar, DarWriter}
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.engine.{
  UpgradesMatrix,
  UpgradesMatrixCases,
  UpgradesMatrixCasesV2Dev,
  UpgradesMatrixCasesV2MaxStable,
}
import com.digitalasset.daml.lf.value.Value.*
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.google.rpc.status.Status as RpcStatus
import io.grpc.{Status, StatusRuntimeException}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

// Split the tests across eight suites with eight Canton runners, which brings
// down the runtime from ~4000s on a single suite to ~1400s
class UpgradesMatrixIntegration0
    extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2MaxStable, 3, 0)
class UpgradesMatrixIntegration1
    extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2MaxStable, 3, 1)
class UpgradesMatrixIntegration2
    extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2MaxStable, 3, 2)

class UpgradesMatrixIntegration3 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 0)
class UpgradesMatrixIntegration4 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 1)
class UpgradesMatrixIntegration5 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 2)
class UpgradesMatrixIntegration6 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 3)
class UpgradesMatrixIntegration7 extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, 5, 4)

/** A test suite to run the UpgradesMatrix matrix on Canton.
  *
  * This takes a while (~5000s when running with a single suite), so we have a different test
  * [[UpgradesMatrixUnit]] to catch simple engine issues early which takes only ~40s.
  */
abstract class UpgradesMatrixIntegration(
    override val cases: UpgradesMatrixCases,
    modN: Int,
    modK: Int,
) extends CantonFixture
    with UpgradesMatrix[
      RpcStatus,
      SubmitAndWaitForTransactionResponse,
      LedgerClient,
    ] {
  defineTestCases()

  def encodeDar(
      mainDalfName: String,
      mainDalf: Archive,
      deps: List[(String, Archive)],
  ): ByteString = {
    val os = ByteString.newOutput()
    DarWriter.encode(
      "0.0.0", // TODO(#30144) Use something sane/configurable here, used to be DamlVersion from Daml repo
      Dar(
        (mainDalfName, Bytes.fromByteString(mainDalf.toByteString)),
        deps.map { case (name, dalf) => (name, Bytes.fromByteString(dalf.toByteString)) },
      ),
      os,
    )
    os.toByteString
  }

  override def nk = Some((modN, modK))
  override def createTestCase(
      name: String,
      assertion: ExecutionContext => Future[Assertion],
  ): Unit =
    // TODO(#30144): Should re-enable Upgrade Matrix integration tests
    name ignore { env =>
      Await.result(
        assertion(env.executionContext),
        30.seconds,
      )
    }

  // Compiled dars
  val primDATypes = cases.stablePackages.allPackages.find(_.moduleName.dottedName == "DA.Types").get
  val primDATypesDalfName = s"${primDATypes.name}-${primDATypes.packageId}.dalf"
  val primDATypesDalf = ArchiveParser.assertFromBytes(primDATypes.bytes)

  val commonDefsDar = encodeDar(cases.commonDefsDalfName, cases.commonDefsDalf, List())
  val templateDefsV1Dar = encodeDar(
    cases.templateDefsV1DalfName,
    cases.templateDefsV1Dalf,
    List((cases.commonDefsDalfName, cases.commonDefsDalf)),
  )
  val templateDefsV2Dar = encodeDar(
    cases.templateDefsV2DalfName,
    cases.templateDefsV2Dalf,
    List((cases.commonDefsDalfName, cases.commonDefsDalf)),
  )
  val clientLocalDar = encodeDar(
    cases.clientLocalDalfName,
    cases.clientLocalDalf,
    List(
      (cases.templateDefsV1DalfName, cases.templateDefsV1Dalf),
      (cases.templateDefsV2DalfName, cases.templateDefsV2Dalf),
      (cases.commonDefsDalfName, cases.commonDefsDalf),
      (primDATypesDalfName, primDATypesDalf),
    ),
  )
  val clientGlobalDar = encodeDar(
    cases.clientGlobalDalfName,
    cases.clientGlobalDalf,
    List(
      (cases.templateDefsV2DalfName, cases.templateDefsV2Dalf),
      (cases.commonDefsDalfName, cases.commonDefsDalf),
      (primDATypesDalfName, primDATypesDalf),
    ),
  )

  private var adminLedgerClient: LedgerClient = null

  override def environmentDefinition = super.environmentDefinition
    .addConfigTransforms(
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.parameters.alphaVersionSupport).replace(true)
      },
      ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.parameters.disableUpgradeValidation).replace(true)
      },
    )

  override protected def beforeAll(): scala.Unit = {
    super.beforeAll()
    implicit val esf: ExecutionSequencerFactory = provideEnvironment.executionSequencerFactory
    implicit val ec: ExecutionContext = provideEnvironment.executionContext
    adminLedgerClient = Await.result(
      for {
        (user, context) <- createUserByAdmin(
          "UpgradesMatrixIT",
          rights = Vector(
            user.Right(user.Right.Kind.ParticipantAdmin(user.Right.ParticipantAdmin()))
          ),
        )

        ledgerClientConfiguration =
          LedgerClientConfiguration(
            userId = user.id,
            commandClient = CommandClientConfiguration.default,
            token = () => context.token,
          )

        client <- LedgerClient.apply(channel, ledgerClientConfiguration, loggerFactory)
        _ <- Future.traverse(
          List(commonDefsDar, templateDefsV1Dar, templateDefsV2Dar, clientLocalDar, clientGlobalDar)
        )(dar => client.packageManagementClient.uploadDarFile(dar))
      } yield client,
      30.seconds,
    )
  }

  private def createContract(
      ledgerClient: LedgerClient,
      party: Party,
      tplId: Identifier,
      arg: ValueRecord,
  )(implicit ec: ExecutionContext): Future[ContractId] =
    for {
      apiRecord <- lfValueToApiRecord(true, arg).fold(
        err => Future.failed(new RuntimeException(err)),
        res => Future(res),
      )
      resp <- ledgerClient.commandService.submitAndWaitForTransaction(
        Commands.defaultInstance.copy(
          commandId = UUID.randomUUID.toString,
          actAs = Seq(party),
          commands = Seq(
            Command.defaultInstance.withCreate(
              CreateCommand.defaultInstance.copy(
                templateId = Some(toApiIdentifierUpgrades(tplId.toRef, false)),
                createArguments = Some(apiRecord),
              )
            )
          ),
        )
      )
      contractId <- resp.fold(
        err => Future.failed(new RuntimeException(s"Couldn't create contract: ${err.message}")),
        res =>
          res.getTransaction.events.headOption match {
            case Some(e) => Future(ContractId.assertFromString(e.getCreated.contractId))
            case _ =>
              Future.failed(
                new RuntimeException("Could not get a CreatedEvent out of the response")
              )
          },
      )
    } yield contractId

  private val globalRandom = new scala.util.Random(0)

  private def randomHex(rand: scala.util.Random, length: Int) =
    s"%0${length}x".format(rand.nextLong()).takeRight(length)

  private def allocateParty(name: String)(implicit ec: ExecutionContext): Future[Party] =
    adminLedgerClient.partyManagementClient
      .allocateParty(Some(name + "-" + randomHex(globalRandom, 8)))
      .map(_.party)

  override def setup(
      testHelper: UpgradesMatrixCases#TestHelper
  )(implicit ec: ExecutionContext): Future[UpgradesMatrixCases.SetupData[LedgerClient]] = {
    implicit val esf: ExecutionSequencerFactory = provideEnvironment.executionSequencerFactory
    for {
      alice <- allocateParty("Alice")
      bob <- allocateParty("Bob")

      (user, context) <- createUserByAdmin(
        "UpgradesMatrixTestSetup-" + randomHex(globalRandom, 8),
        rights = Vector(
          user.Right(user.Right.Kind.ParticipantAdmin(user.Right.ParticipantAdmin())),
          user.Right(user.Right.Kind.CanActAs(user.Right.CanActAs(alice))),
          user.Right(user.Right.Kind.CanActAs(user.Right.CanActAs(bob))),
        ),
      )

      ledgerClientConfiguration =
        LedgerClientConfiguration(
          userId = user.id,
          commandClient = CommandClientConfiguration.default,
          token = () => context.token,
        )

      ledgerClient <- LedgerClient.apply(channel, ledgerClientConfiguration, loggerFactory)

      clientLocalContractId <- createContract(
        ledgerClient,
        alice,
        testHelper.clientLocalTplId,
        testHelper.clientContractArg(alice, bob),
      )
      clientGlobalContractId <- createContract(
        ledgerClient,
        alice,
        testHelper.clientGlobalTplId,
        testHelper.clientContractArg(alice, bob),
      )
      globalContractId <- createContract(
        ledgerClient,
        alice,
        testHelper.v1TplId,
        testHelper.globalContractArg(alice, bob),
      )
    } yield UpgradesMatrixCases.SetupData(
      alice = alice,
      bob = bob,
      clientLocalContractId = clientLocalContractId,
      clientGlobalContractId = clientGlobalContractId,
      globalContractId = globalContractId,
      additionalSetup = ledgerClient,
    )
  }

  private def withUnvettedPackages[A](
      packages: List[(PackageName, PackageVersion)]
  )(action: => Future[A])(implicit ec: ExecutionContext): Future[A] =
    if (packages.isEmpty)
      action
    else
      for {
        _ <- adminLedgerClient.packageManagementClient.updateVettedPackages(
          UpdateVettedPackagesRequest.of(
            Seq(
              VettedPackagesChange.of(
                Operation.Unvet(
                  Unvet(packages.map(pkg => VettedPackagesRef("", pkg._1, pkg._2.toString)))
                )
              )
            ),
            dryRun = false,
            synchronizerId = "",
            expectedTopologySerial = None,
            updateVettedPackagesForceFlags = Seq.empty,
          )
        )
        // _ <- scriptClient.waitUntilUnvettingVisible(packages, scriptClient.getParticipantUid)
        result <- action
        _ <- adminLedgerClient.packageManagementClient.updateVettedPackages(
          UpdateVettedPackagesRequest.of(
            Seq(
              VettedPackagesChange.of(
                Operation.Vet(
                  Vet(
                    packages.map(pkg => VettedPackagesRef("", pkg._1, pkg._2.toString)),
                    newValidFromInclusive = None,
                    newValidUntilExclusive = None,
                  )
                )
              )
            ),
            dryRun = false,
            synchronizerId = "",
            expectedTopologySerial = None,
            updateVettedPackagesForceFlags = Seq.empty,
          )
        )
        // _ <- scriptClient.waitUntilVettingVisible(packages, scriptClient.getParticipantUid)
      } yield result

  private val creationPackages = cases.allCreationPackages.view.values
    .map(pkg => (pkg.pkgName, pkg.pkgVersion))
    .toList

  private def packageIdToUpgradeName(
      explicitPackageId: Boolean,
      pkgId: PackageId,
  ): Option[PackageName] =
    cases.compiledPackages.pkgInterface
      .lookupPackage(pkgId)
      .toOption
      .filter(pkgSig => pkgSig.supportsUpgrades(pkgId) && !explicitPackageId)
      .map(_.metadata.name)

  // Omits the package id on an identifier if contract upgrades are enabled unless explicitPackageId is true
  private def toApiIdentifierUpgrades(
      identifier: TypeConRef,
      explicitPackageId: Boolean,
  ): api.Identifier =
    identifier.pkg match {
      case PackageRef.Name(name) =>
        if (explicitPackageId)
          throw new IllegalArgumentException(
            "Cannot set explicitPackageId = true on an ApiCommand that uses a PackageName"
          )
        else
          api.Identifier(
            "#" + name,
            identifier.qualifiedName.module.toString(),
            identifier.qualifiedName.name.toString(),
          )
      case PackageRef.Id(pkgId) =>
        val converted = toApiIdentifier(identifier.assertToTypeConId)
        packageIdToUpgradeName(explicitPackageId, pkgId)
          .fold(converted)(name => converted.copy(packageId = "#" + name.toString))
    }

  private def toCommand(apiCmd: ApiCommand): Either[String, Command] = {
    val explicitPackageId = apiCmd.typeRef.pkg match {
      case Ref.PackageRef.Id(_) => true
      case Ref.PackageRef.Name(_) => false
    }
    apiCmd match {
      case ApiCommand.Create(tmplRef, argument) =>
        for {
          arg <- lfValueToApiRecord(true, argument)
        } yield Command.defaultInstance.withCreate(
          CreateCommand(
            Some(toApiIdentifierUpgrades(tmplRef, explicitPackageId)),
            Some(arg),
          )
        )
      case ApiCommand.Exercise(typeRef, contractId, choice, argument) =>
        for {
          arg <- lfValueToApiValue(true, argument)
        } yield Command.defaultInstance.withExercise(
          ExerciseCommand(
            Some(toApiIdentifierUpgrades(typeRef, explicitPackageId)),
            contractId.coid,
            choice,
            Some(arg),
          )
        )
      case ApiCommand.ExerciseByKey(tmplRef, k, choice, argument) =>
        for {
          key <- lfValueToApiValue(true, k)
          argument <- lfValueToApiValue(true, argument)
        } yield Command.defaultInstance.withExerciseByKey(
          ExerciseByKeyCommand(
            Some(toApiIdentifierUpgrades(tmplRef, explicitPackageId)),
            Some(key),
            choice,
            Some(argument),
          )
        )
      case ApiCommand.CreateAndExercise(tmplRef, template, choice, argument) =>
        for {
          template <- lfValueToApiRecord(true, template)
          argument <- lfValueToApiValue(true, argument)
        } yield Command.defaultInstance.withCreateAndExercise(
          CreateAndExerciseCommand(
            Some(toApiIdentifierUpgrades(tmplRef, explicitPackageId)),
            Some(template),
            choice,
            Some(argument),
          )
        )
    }
  }

  override def execute(
      setupData: UpgradesMatrixCases.SetupData[LedgerClient],
      testHelper: UpgradesMatrixCases#TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: UpgradesMatrixCases.ContractOrigin,
      creationPackageStatus: UpgradesMatrixCases.CreationPackageStatus,
  )(implicit ec: ExecutionContext): Future[Either[
    RpcStatus,
    SubmitAndWaitForTransactionResponse,
  ]] = {
    implicit val actorSystem: org.apache.pekko.actor.ActorSystem = provideEnvironment.actorSystem
    val ledgerClient = setupData.additionalSetup
    for {
      disclosures <- contractOrigin match {
        case UpgradesMatrixCases.Disclosed =>
          for {
            offset <- ledgerClient.stateService.getLedgerEndOffset()
            filters = Filters(
              Seq(
                CumulativeFilter(
                  IdentifierFilter.TemplateFilter(
                    TemplateFilter(
                      Some(toApiIdentifier(testHelper.v1TplId)),
                      includeCreatedEventBlob = true,
                    )
                  )
                )
              )
            )
            activeContracts <- ledgerClient.stateService
              .getActiveContracts(
                eventFormat = EventFormat(
                  filtersByParty = Map(setupData.alice -> filters),
                  filtersForAnyParty = None,
                  verbose = false,
                ),
                validAtOffset = offset,
              )
            result <- activeContracts
              .flatMap(_.createdEvent)
              .find(_.contractId == setupData.globalContractId.toString) match {
              case None => Future.failed(new RuntimeException("Couldn't fetch disclosure?"))
              case Some(activeContract) =>
                Future.successful(
                  Seq(
                    DisclosedContract.defaultInstance.copy(
                      templateId = activeContract.templateId,
                      contractId = activeContract.contractId,
                      createdEventBlob = activeContract.createdEventBlob,
                    )
                  )
                )
            }
          } yield result
        case _ => Future.successful(Seq())
      }
      commands <- Future.traverse(apiCommands.toSeq) { apiCommand =>
        toCommand(apiCommand).fold(
          err =>
            Future.failed(new RuntimeException(s"Couldn't convert API commands to commands: $err")),
          res => Future.successful(res),
        )
      }
      result <- withUnvettedPackages(creationPackageStatus match {
        case UpgradesMatrixCases.CreationPackageVetted => List.empty
        case UpgradesMatrixCases.CreationPackageUnvetted => creationPackages
      }) {
        ledgerClient.commandService.submitAndWaitForTransaction(
          Commands.defaultInstance.copy(
            commandId = UUID.randomUUID.toString,
            actAs = Seq(setupData.alice),
            disclosedContracts = disclosures,
            packageIdSelectionPreference =
              List(cases.commonDefsPkgId, cases.templateDefsV2PkgId, cases.clientLocalPkgId),
            commands = commands,
          )
        )
      }
    } yield result
  }

  override def assertResultMatchesExpectedOutcome(
      result: Either[
        RpcStatus,
        SubmitAndWaitForTransactionResponse,
      ],
      expectedOutcome: UpgradesMatrixCases.ExpectedOutcome,
  )(implicit ec: ExecutionContext): Assertion = {
    def expectStatusHasErrorCode(status: RpcStatus, expectedCode: String): Assertion = {
      val details = ErrorDetails.from(status.details.map(Any.toJavaProto))
      val oErrorInfoDetail = details.collectFirst { case eid: ErrorDetails.ErrorInfoDetail => eid }
      val errorCode = oErrorInfoDetail.fold("UNKNOWN")(_.errorCodeId)
      errorCode shouldBe expectedCode
    }

    expectedOutcome match {
      case UpgradesMatrixCases.ExpectSuccess =>
        result shouldBe a[Right[?, ?]]
      case UpgradesMatrixCases.ExpectUpgradeError =>
        inside(result) { case Left(statusError) =>
          expectStatusHasErrorCode(statusError, "INTERPRETATION_UPGRADE_ERROR_VALIDATION_FAILED")
        }
      case UpgradesMatrixCases.ExpectAuthenticationError =>
        inside(result) { case Left(statusError) =>
          expectStatusHasErrorCode(
            statusError,
            "INTERPRETATION_UPGRADE_ERROR_AUTHENTICATION_FAILED",
          )
        }
      case UpgradesMatrixCases.ExpectRuntimeTypeMismatchError =>
        inside(result) { case Left(statusError) =>
          expectStatusHasErrorCode(statusError, "INTERPRETATION_UPGRADE_ERROR_TRANSLATION_FAILED")
        }
      case UpgradesMatrixCases.ExpectPreprocessingError =>
        inside(result) { case Left(statusError) =>
          statusError shouldBe a[StatusRuntimeException]
          val status = statusError.asInstanceOf[StatusRuntimeException].getStatus
          status.getCode shouldEqual Status.INVALID_ARGUMENT.getCode
          status.getDescription should startWith("COMMAND_PREPROCESSING_FAILED")
          expectStatusHasErrorCode(statusError, "UNKNOWN")
        }
      case UpgradesMatrixCases.ExpectPreconditionViolated =>
        inside(result) { case Left(statusError) =>
          expectStatusHasErrorCode(statusError, "TEMPLATE_PRECONDITION_VIOLATED")
        }
      case UpgradesMatrixCases.ExpectUnhandledException =>
        inside(result) { case Left(statusError) =>
          expectStatusHasErrorCode(statusError, "DAML_FAILURE")
        }
      case UpgradesMatrixCases.ExpectInternalInterpretationError =>
        inside(result) { case Left(statusError) =>
          statusError shouldBe a[StatusRuntimeException]
          val status = statusError.asInstanceOf[StatusRuntimeException].getStatus
          status.getCode shouldEqual Status.INVALID_ARGUMENT.getCode
          status.getDescription should startWith("DAML_INTERPRETATION_ERROR")
          expectStatusHasErrorCode(statusError, "UNKNOWN")
        }
    }
  }
}
