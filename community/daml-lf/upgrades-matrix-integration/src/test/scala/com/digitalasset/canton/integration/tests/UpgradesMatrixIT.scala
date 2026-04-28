// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

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
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
}
import com.daml.ledger.api.v2.value as api
import com.daml.ledger.javaapi.data.{
  Event as JavaEvent,
  ExercisedEvent as JavaExercisedEvent,
  Transaction as JavaTransaction,
}
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
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
import com.digitalasset.canton.version.ProtocolVersion
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
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.Duration
import java.util.UUID
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

// Split the tests across eight suites with eight Canton runners, which brings
// down the runtime from ~4000s on a single suite to ~1400s
class UpgradesMatrixIntegration0 extends UpgradesMatrixIntegrationV2MaxStable(7, 0)
class UpgradesMatrixIntegration1 extends UpgradesMatrixIntegrationV2MaxStable(7, 1)
class UpgradesMatrixIntegration2 extends UpgradesMatrixIntegrationV2MaxStable(7, 2)
class UpgradesMatrixIntegration3 extends UpgradesMatrixIntegrationV2MaxStable(7, 3)
class UpgradesMatrixIntegration4 extends UpgradesMatrixIntegrationV2MaxStable(7, 4)
class UpgradesMatrixIntegration5 extends UpgradesMatrixIntegrationV2MaxStable(7, 5)
class UpgradesMatrixIntegration6 extends UpgradesMatrixIntegrationV2MaxStable(7, 6)

class UpgradesMatrixIntegration7 extends UpgradesMatrixIntegrationV2Dev(1, 0)

abstract class UpgradesMatrixIntegrationV2MaxStable(n: Int, k: Int)
    extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2MaxStable, n, k)

// Only need this shard for running LookupByKey
abstract class UpgradesMatrixIntegrationV2Dev(n: Int, k: Int)
    extends UpgradesMatrixIntegration(UpgradesMatrixCasesV2Dev, n, k) {
  override def testFilter(
      testHelper: UpgradesMatrixCases#TestHelper
  ): Boolean =
    super.testFilter(testHelper) && testHelper.operation == UpgradesMatrixCases.LookupByKey

  override def baseEnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Config.withNetworkBootstrap { implicit env =>
      import env.*
      new NetworkBootstrapper(
        NetworkTopologyDescription(
          daName,
          synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
          synchronizerThreshold = PositiveInt.one,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          overrideStaticSynchronizerParameters = Some(
            EnvironmentDefinition.defaultStaticSynchronizerParameters.copy(
              protocolVersion = ProtocolVersion.dev
            )
          ),
        )
      )
    }

}

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

  def testFilter(
      testHelper: UpgradesMatrixCases#TestHelper
  ): Boolean =
    (
      testHelper.testCase,
      testHelper.operation,
      testHelper.catchBehavior,
      testHelper.entryPoint,
      testHelper.contractOrigin,
    ) match {
      case _ =>
        true
    }

  override def testHelperToDeclaration(
      testHelper: UpgradesMatrixCases#TestHelper,
      assertion: ExecutionContext => Future[Assertion],
  ): Unit =
    if (testFilter(testHelper)) {
      testHelper.title in { env =>
        Await.result(
          assertion(env.executionContext),
          30.seconds,
        )
      }
    } else {
      testHelper.title ignore { env =>
        Await.result(
          assertion(env.executionContext),
          30.seconds,
        )
      }
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
      ConfigTransforms.updateSequencerConfig("sequencer1") {
        _.focus(_.parameters.alphaVersionSupport).replace(true)
      },
      ConfigTransforms.updateMediatorConfig("mediator1") {
        _.focus(_.parameters.alphaVersionSupport).replace(true)
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
          expiresIn = Some(
            Duration.ofMinutes(120)
          ), // Set a high timeout - we can rely on the test timeout in CI instead
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
                templateId = Some(toApiIdentifier(tplId)),
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
      extraGlobalContractId <- createContract(
        ledgerClient,
        alice,
        testHelper.v1TplId,
        testHelper.globalContractArgV1(alice, bob),
      )
      globalContractId <- createContract(
        ledgerClient,
        alice,
        testHelper.v1TplId,
        testHelper.globalContractArgV1(alice, bob),
      )
    } yield UpgradesMatrixCases.SetupData(
      alice = alice,
      bob = bob,
      clientLocalContractId = clientLocalContractId,
      clientGlobalContractId = clientGlobalContractId,
      globalContractId = globalContractId,
      extraGlobalContractId = extraGlobalContractId,
      additionalSetup = ledgerClient,
    )
  }

  private def withUnvettedPackages[A](
      ledgerClient: LedgerClient,
      packages: List[(PackageName, PackageVersion)],
  )(action: => Future[A])(implicit ec: ExecutionContext): Future[A] =
    if (packages.isEmpty)
      action
    else
      for {
        _ <- ledgerClient.packageManagementClient.updateVettedPackages(
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
        result <- action
        _ <- ledgerClient.packageManagementClient.updateVettedPackages(
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
                      Some(toApiIdentifierUpgrades(testHelper.v1TplId.toRef, false)),
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
              .find(_.contractId == setupData.globalContractId.coid) match {
              case None =>
                Future.failed(
                  new RuntimeException(
                    s"Couldn't fetch disclosure?\nTarget: ${setupData.globalContractId.coid}\nActiveContracts: ${activeContracts.flatMap(_.createdEvent).map(_.contractId).mkString(", ")}"
                  )
                )
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
      result <- withUnvettedPackages(
        ledgerClient,
        creationPackageStatus match {
          case UpgradesMatrixCases.CreationPackageVetted => List.empty
          case UpgradesMatrixCases.CreationPackageUnvetted => creationPackages
        },
      ) {
        ledgerClient.commandService.submitAndWaitForTransaction(
          Commands.defaultInstance.copy(
            commandId = UUID.randomUUID.toString,
            actAs = Seq(setupData.alice),
            disclosedContracts = disclosures,
            packageIdSelectionPreference =
              List(cases.commonDefsPkgId, cases.templateDefsV2PkgId, cases.clientLocalPkgId),
            commands = commands,
          ),
          transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
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
        inside(result) { case Right(response) =>
          val tx = JavaTransaction.fromProto(Transaction.toJavaProto(response.getTransaction))
          val eventsById: Map[Integer, JavaEvent] = tx.getEventsById().asScala.toMap
          val exercisedEvents: Seq[JavaExercisedEvent] =
            tx.getRootNodeIds.asScala.toSeq.map(eventsById(_)).collect {
              case e: JavaExercisedEvent => e
            }
          exercisedEvents match {
            case Seq(e) =>
              Option(e.getExerciseResult).map(_.asText.toScala) match {
                case Some(Some(t)) =>
                  // TODO(#32310) We have choices that are expected to succeed and
                  // need to have an exception-free failure when they fail.
                  // Currently, they return text, and we match for the phrase in
                  // UpgradesMatrixCases.unexpectedErrorMessage. Later, we should
                  // return Either and match on the variant instead.
                  t.getValue should not include UpgradesMatrixCases.unexpectedErrorMessage
                case Some(None) => succeed // non-text type in result
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
          statusError.code shouldEqual Status.INVALID_ARGUMENT.getCode.value
          statusError.message should startWith("COMMAND_PREPROCESSING_FAILED")
          expectStatusHasErrorCode(statusError, "COMMAND_PREPROCESSING_FAILED")
        }
      case UpgradesMatrixCases.ExpectPreconditionViolated =>
        inside(result) { case Left(statusError) =>
          expectStatusHasErrorCode(statusError, "TEMPLATE_PRECONDITION_VIOLATED")
        }
      case UpgradesMatrixCases.ExpectUnhandledException(expectedMsg) =>
        inside(result) { case Left(statusError) =>
          expectStatusHasErrorCode(statusError, "DAML_FAILURE")
          statusError.message should include(expectedMsg)
        }
      case UpgradesMatrixCases.ExpectInternalInterpretationError =>
        inside(result) { case Left(statusError) =>
          statusError.code shouldEqual Status.INVALID_ARGUMENT.getCode.value
          statusError.message should startWith("DAML_INTERPRETATION_ERROR")
          expectStatusHasErrorCode(statusError, "UNKNOWN")
        }
    }
  }
}
