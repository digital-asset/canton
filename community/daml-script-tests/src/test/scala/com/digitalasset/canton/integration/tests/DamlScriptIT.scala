// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetup,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.NextGenContractStateMachine as ContractStateMachine
import io.circe.*
import io.circe.parser.*
import monocle.macros.syntax.lens.*
import org.apache.commons.io.FileUtils
import org.scalatest.{Args, Assertion, BeforeAndAfterAllConfigMap, ConfigMap, Status}

import java.nio.file.*
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

abstract class DamlScriptIT
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BeforeAndAfterAllConfigMap {
  self: EnvironmentSetup =>

  // Resolve the conflict between BeforeAndAfterAll and BeforeAndAfterAllConfigMap
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected: Boolean = true

  import DamlScriptIT.*

  protected val debug = false

  protected def projectName: String

  protected var damlProjectDir: Path = _
  var env: Seq[(String, String)] = _

  private val darFileUploaded: AtomicBoolean = new AtomicBoolean(false)

  // As non-isolated daml-scripts are ran as a single batch, we use this var to cache the result of the first `dpm script --all` run
  // This allows non-isolated to then be treated as test cases on a per script ID basis
  private val actualNonIsolatedTestData
      : AtomicReference[Option[Map[String, Either[String, Json]]]] = new AtomicReference(None)

  // TODO (#30398): replace the following with the results of a call to `dpm script --list`
  private val scriptIdsToTest: List[String] = List(
    "ActionTest:testFilterA",
    "AuthEvalOrder:t1_create_success",
    "AuthEvalOrder:t2_create_badlyAuthorized",
    "AuthEvalOrder:t3_createViaExerice_success",
    "AuthEvalOrder:t4_createViaExerice_badlyAuthorized",
    "AuthFailure:t1_CreateMissingAuthorization",
    "AuthFailure:t2_MaintainersNotSubsetOfSignatories",
    "AuthFailure:t3_FetchMissingAuthorization",
    "AuthFailure:t4_LookupByKeyMissingAuthorization",
    "AuthFailure:t5_ExerciseMissingAuthorization",
    "AuthorizedDivulgence:test_authorizedFetch",
    "AuthorizedDivulgence:test_divulgeChoiceTargetContractId",
    "AuthorizedDivulgence:test_noDivulgenceForFetch",
    "AuthorizedDivulgence:test_noDivulgenceOfCreateArguments",
    "BasicTests:test_createAndFetch",
    "BasicTests:test_doubleLetTest",
    "BasicTests:test_exponentiation",
    "BasicTests:test_failedAuths",
    "BasicTests:test_getTimeTest",
    "BasicTests:test_letTest",
    "BasicTests:test_listMatchTest",
    "BasicTests:test_mustFails",
    "BasicTests:test_payoutTest",
    "BasicTests:test_screateAndExercise",
    "BasicTests:test_screateAndExerciseComposit",
    "BasicTests:test_sgetTimeTest",
    "BasicTests:test_testXyzTest",
    "BasicTests:test_typeWithParameters",
    "ChoiceShadowing:test1",
    "CoerceContractId:test",
    "Conjunction:main",
    "ConjunctionChoices:demo",
    "ConsumedContractKey:testFetchFromConsumingChoice",
    "ConsumedContractKey:testFetchKeyFromConsumingChoice",
    "ConsumedContractKey:testLookupKeyFromConsumingChoice",
    "ConsumingTests:main",
    "ContractIdInContractKeySkipCheck:createCmdCrashes",
    "ContractIdInContractKeySkipCheck:createCrashes",
    "ContractIdInContractKeySkipCheck:exerciseCmdCrashes",
    "ContractIdInContractKeySkipCheck:exerciseCrashes",
    "ContractIdInContractKeySkipCheck:fetchCrashes",
    "ContractIdInContractKeySkipCheck:lookupCrashes",
    "ContractIdInContractKeySkipCheck:queryCrashes",
    "ContractKeyNotEffective:fetchByKeyMustFail",
    "ContractKeyNotVisible:aScript",
    "ContractKeyNotVisible:blindLookup",
    "ContractKeyNotVisible:divulgeeLookup",
    "ContractKeyNotVisible:localFetch",
    "ContractKeyNotVisible:localLookup",
    "ContractKeys:test",
    "CreateAndExercise:main",
    "DamlScriptTrySubmit:authorizationError",
    "DamlScriptTrySubmit:contractKeyNotFound",
    "DamlScriptTrySubmit:contractNotActive",
    "DamlScriptTrySubmit:createEmptyContractKeyMaintainers",
    // TODO(#30398): move to a 2.dev test suite
    // "DamlScriptTrySubmit:devError",
    "DamlScriptTrySubmit:duplicateContractKey",
    "DamlScriptTrySubmit:failureStatusError",
    "DamlScriptTrySubmit:fetchEmptyContractKeyMaintainers",
    "DamlScriptTrySubmit:truncatedError",
    "DamlScriptTrySubmit:wronglyTypedContract",
    "EmptyContractKeyMaintainers:createCmdNoMaintainer",
    "EmptyContractKeyMaintainers:createNoMaintainer",
    "EmptyContractKeyMaintainers:fetchNoMaintainer",
    "EmptyContractKeyMaintainers:lookupNoMaintainer",
    "EmptyContractKeyMaintainers:queryNoMaintainer",
    "EqContractId:main",
    "ExceptionAndContractKey:testCreate",
    "ExceptionAndContractKey:testLookup",
    "ExceptionSemantics:divulgence",
    "ExceptionSemantics:duplicateKey",
    "ExceptionSemantics:handledArithmeticError",
    "ExceptionSemantics:handledUserException",
    "ExceptionSemantics:rollbackArchive",
    "ExceptionSemantics:rollbackConsumingExercise",
    "ExceptionSemantics:rollbackCreate",
    "ExceptionSemantics:tryContext",
    "ExceptionSemantics:uncaughtArithmeticError",
    "ExceptionSemantics:uncaughtUserException",
    "ExceptionSemantics:unhandledArithmeticError",
    "ExceptionSemantics:unhandledUserException",
    "FailedFetch:fetchNonStakeholder",
    "FetchByKey:failLedger",
    "FetchByKey:failSpeedy",
    "FetchByKey:mustFail",
    "Interface:main",
    "InterfaceArchive:main",
    "Iou12:main",
    "KeyNotVisibleStakeholders:blindFetch",
    "KeyNotVisibleStakeholders:blindLookup",
    "KeyNotVisibleStakeholders:divulgeeFetch",
    "KeyNotVisibleStakeholders:divulgeeLookup",
    "LargeTransaction:largeListAsAChoiceArgTest",
    "LargeTransaction:largeTransactionWithManyContractsTest",
    "LargeTransaction:largeTransactionWithOneContractTest",
    "LargeTransaction:listSizeTest",
    "LargeTransaction:rangeOfIntsToListContainerTest",
    "LargeTransaction:rangeOfIntsToListTest",
    "LargeTransaction:rangeTest",
    "LedgerTestException:test",
    "LFContractKeys:lookupTest",
    "LfInterfaces:run",
    "LfStableContractKeys:run",
    "LfStableContractKeyThroughExercises:run",
    "MoreChoiceObserverDivulgence:test",
    "Self:main",
    "Self2:main",
    "TransientFailure:testBio",
  )

  private var scalatestFilteredScriptIdsToIgnore: List[String] = List.empty

  final override def beforeAll(configMap: ConfigMap): Unit = {
    super.beforeAll(configMap)
    def getEnv(name: String, default: String) =
      configMap.getOptional[String](name) match {
        case Some(value) =>
          // on CI we should get the value from the configMap
          value
        case None =>
          logger.warn(s"Using default value for $name: $default.")
          default
      }
    env = Seq(
      "DAML_VERSION" -> getEnv("damlVersion", BuildInfo.damlLibrariesVersion),
      "DPM_REGISTRY" -> getEnv("dpmRegistry", "europe-docker.pkg.dev/da-images/public-unstable"),
    )

    Option(getClass.getResource(s"/daml/$projectName")) match {
      case Some(in) =>
        damlProjectDir = Files.createTempDirectory(s"test_${getClass.getSimpleName}_")
        if (debug) println(s"Saving daml project to $damlProjectDir")
        FileUtils.copyDirectory(Paths.get(in.toURI).toFile, damlProjectDir.toFile)
        // compile the project
        val _ = run(cmd = List("dpm", "build", "--output", projectName + ".dar"))
      case None =>
        throw new java.lang.Error("could not find daml project in resources: " + projectName)
    }
  }

  final override def afterAll(configMap: ConfigMap): Unit = {
    super.afterAll(configMap)
    if (!debug)
      FileUtils.deleteDirectory(damlProjectDir.toFile)
  }

  final def scriptError(
      cmd: List[String],
      stdout: String,
      stderr: String,
      cause: String,
  ): Nothing = {
    Console.err.println(
      s"""running command failed:
         |  command: ${cmd.mkString(" ")}
         |  cause: $cause
         |  cwd: $damlProjectDir (switch DamlScriptId.debug to true to keep this temporary directory)
         |  DAML_VERSION=${sys.env.getOrElse("DAML_VERSION", "<not set>")}
         |  DPM_REGISTRY=${sys.env.getOrElse("DPM_REGISTRY", "<not set>")}
         |  stdout: $stdout
         |  stderr: $stderr
         |""".stripMargin
    )
    throw new java.lang.Error(s"command failed: $cause")
  }

  private def run(
      cmd: List[String],
      ignoreExitCode: Boolean = false,
  ): (String, String) = {
    val stderr = new StringBuilder
    val stdout = new StringBuilder
    val logger =
      sys.process.ProcessLogger(stdout.append(_).append("\n"), stderr.append(_).append("\n"))

    val exitCode =
      try
        sys.process.Process(
          cmd,
          cwd = Some(damlProjectDir.toFile),
          env*
        ) ! logger
      catch {
        case scala.util.control.NonFatal(cause) =>
          scriptError(cmd, stdout.result(), stderr.result(), cause.getMessage)
      }
    if (ignoreExitCode || exitCode == 0)
      (stdout.result(), stderr.result())
    else
      scriptError(cmd, stdout.result(), stderr.result(), s"exitCode = $exitCode")
  }

  def runDamlScriptTests(
      host: String,
      port: Port,
      skippedTests: List[String],
      testScriptId: Option[String] = None,
  ): Map[String, Either[String, Json]] = {
    val outputFile = Files.createTempFile(
      damlProjectDir,
      s"$projectName-${testScriptId.getOrElse("non")}-isolated",
      ".json",
    )
    val cmd = List(
      List("dpm", "script"),
      List("--dar", projectName + ".dar"),
      testScriptId.fold(List("--all"))(scriptId => List("--script-name", scriptId)),
      testScriptId.fold(skippedTests.flatMap(List("--skip-script-name", _)))(_ => List.empty),
      List("--ledger-host", host),
      List("--ledger-port", port.unwrap.toString),
      List("--static-time"),
      List("--max-inbound-message-size", Int.MaxValue.toString),
      List("--upload-dar", s"${!darFileUploaded.get()}"),
      List("--json-test-summary", outputFile.toString),
    ).flatten
    val (stdout, stderr) = run(cmd, ignoreExitCode = true)
    val resultOrErr = for {
      output <- scala.util.Try(Files.readString(outputFile)).toEither
      json <- parse(output)
      result <- json.as[Map[String, Either[String, Json]]]
    } yield result
    resultOrErr match {
      case Right(value) =>
        darFileUploaded.set(true)
        value
      case Left(err) =>
        scriptError(
          cmd,
          stdout,
          stderr,
          s"failed to parse script output: ${err.getMessage}",
        )
    }
  }

  protected def langVersion: LanguageVersion

  protected def enableLfDev: Boolean =
    langVersion == com.digitalasset.daml.lf.language.LanguageVersion.devLfVersion

  // Skip test if protocol version is smaller
  protected lazy val minimumProtocolVersion: ProtocolVersion = ProtocolVersion.minimum

  private lazy val lowerCommandTrackerDuration: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_ {
      _.focus(_.ledgerApi.commandService.defaultTrackingTimeout)
        .replace(config.NonNegativeFiniteDuration.ofSeconds(15))
    }

  private lazy val useTestingTimeService: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService)

  private lazy val maybeEnableLfDev: Seq[ConfigTransform] =
    if (enableLfDev)
      ConfigTransforms.enableAlphaVersionSupport
    else Nil

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(lowerCommandTrackerDuration)
      .addConfigTransforms(useTestingTimeService)
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .addConfigTransforms(maybeEnableLfDev*)
      .addConfigTransforms(withContractStateMode(ContractStateMachine.Mode.default)*)
      .withSetup { env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

  protected def expectedResults: Map[String, ExpectedResult] = Map.empty

  def checkForSuccess(
      scriptId: String,
      expectedValueOpt: Option[Json],
      actualResultOpt: Option[Either[String, Json]],
  ): Option[String] =
    actualResultOpt match {
      case Some(Right(actualValue)) =>
        expectedValueOpt match {
          case Some(expectedValue) if expectedValue != actualValue =>
            Some(
              s"script $scriptId was expected to succeed with value $expectedValue but it succeeded with a different value: $actualValue"
            )
          case _ =>
            None
        }
      case Some(Left(err)) =>
        Some(
          s"script $scriptId was expected to succeed but it fails with message: $err}"
        )
      case None =>
        Some(s"script $scriptId was expected to succeed but it was not found in the results")
    }

  def checkForFailure(
      scriptId: String,
      errorMsgPattern: String,
      actualResultOpt: Option[Either[String, Json]],
  ): Option[String] =
    actualResultOpt match {
      case Some(Left(actualErrorMsg)) =>
        if (errorMsgPattern.r.findFirstIn(actualErrorMsg).isEmpty)
          Some(
            s"script $scriptId was expected to fail with message $errorMsgPattern but it failed with a different message: $actualErrorMsg"
          )
        else
          None
      case Some(Right(_)) =>
        Some(
          s"script $scriptId was expected to fail with message $errorMsgPattern but it succeeded"
        )
      case None =>
        Some(s"script $scriptId was expected to fail but it was not found in the results")
    }

  def assertDamlScriptTestResult(
      scriptId: String,
      expectedResult: Option[ExpectedResult],
      actualResult: Option[Either[String, Json]],
  ): Assertion =
    expectedResult match {
      case None =>
        fail(s"script $scriptId had no test expectations")
      case Some(ExpectedResult.Ignored) =>
        actualResult match {
          case None =>
            // Not able to distinguish between a non-existent and actual tests that are to be ignored
            if (debug) println(s"script $scriptId was ignored")
            succeed
          case Some(_) =>
            fail(s"script $scriptId was expected to be skipped but it was executed")
        }
      case Some(ExpectedResult.Success(expectedValueOpt, _*)) =>
        checkForSuccess(scriptId, expectedValueOpt, actualResult) match {
          case Some(error) if error.endsWith("was not found in the results") =>
            if (debug) println(error)
            fail(error)
          case Some(error) =>
            if (debug) println(s"script $scriptId failed, but was expected to succeed - $error")
            fail(error)
          case None =>
            if (debug) println(s"script $scriptId succeeded")
            succeed
        }
      case Some(ExpectedResult.Failure(errorMsgPattern, _*)) =>
        checkForFailure(scriptId, errorMsgPattern, actualResult) match {
          case Some(error) if error.endsWith("was not found in the results") =>
            if (debug) println(error)
            fail(error)
          case Some(error) =>
            if (debug) println(s"script $scriptId succeeded, but was expected to fail - $error")
            fail(error)
          case None =>
            if (debug) println(s"script $scriptId failed")
            succeed
        }
      case Some(ExpectedResult.Broken(expected)) =>
        val assessment = expected match {
          case Left(ExpectedResult.Failure(errorMsgPattern, _*)) =>
            checkForFailure(scriptId, errorMsgPattern, actualResult)
          case Right(ExpectedResult.Success(expectedValueOpt, _*)) =>
            checkForSuccess(scriptId, expectedValueOpt, actualResult)
        }
        assessment match {
          case Some(error) if error.endsWith("was not found in the results") =>
            if (debug) println(error)
            fail(error)
          case Some(_) =>
            if (debug) println(s"script $scriptId is broken")
            succeed
          case None =>
            fail(s"script $scriptId was expected to be broken but it succeeded")
        }
    }

  // For the current test suite capture the test names that are to run so that `dpm script --all` may respect command line test filtering
  override protected def runTests(testName: Option[String], args: Args): Status = {
    val testSuiteName = getClass.getName

    args.filter.dynaTags.testTags.get(testSuiteName).foreach { testSuiteTags =>
      // scalatest runner test filtering has been specified, so we save the test cases that are to be ignored
      scalatestFilteredScriptIdsToIgnore =
        args.filter.apply(scriptIdsToTest.toSet, testSuiteTags, testSuiteName).collect {
          case (scriptId, true) => scriptId
        }
    }

    super.runTests(testName, args)
  }

  "All project script IDs match expected result map script IDs" onlyRunWithOrGreaterThan minimumProtocolVersion in {
    _ =>
      expectedResults.keySet shouldEqual scriptIdsToTest.toSet
  }

  scriptIdsToTest.foreach { testScriptId =>
    testScriptId onlyRunWithOrGreaterThan minimumProtocolVersion in { env =>
      import env.participant1

      val ignoredTests = expectedResults.collect {
        case (id, ExpectedResult.Ignored) => id
        case (id, ExpectedResult.Broken(_)) => id
      }.toList

      if (ignoredTests.contains(testScriptId)) {
        if (debug) println(s"script $testScriptId was ignored")
        succeed
      } else {
        val isolatedTests = expectedResults.collect {
          case (id, ExpectedResult.Success(_, logAssertions*)) if logAssertions.nonEmpty => id
          case (id, ExpectedResult.Failure(_, logAssertions*)) if logAssertions.nonEmpty => id
        }.toList
        val testsToIgnore = ignoredTests ++ isolatedTests
        val expectedResult = expectedResults.get(testScriptId)

        if (isolatedTests.contains(testScriptId)) {
          // Isolated test case - run these with no result caching
          val actualResult = loggerFactory
            .assertLogs(
              within = {
                runDamlScriptTests(
                  host = participant1.config.ledgerApi.address,
                  port = participant1.config.ledgerApi.port,
                  skippedTests = testsToIgnore,
                  testScriptId = Some(testScriptId),
                )
              },
              assertions = expectedResults
                .get(testScriptId)
                .fold[Seq[LogEntry => Assertion]](Seq.empty)(_.logAssertions) *,
            )
            .get(testScriptId)

          assertDamlScriptTestResult(testScriptId, expectedResult, actualResult)
        } else {
          // Non-isolated test cases - run all of these scripts once and cache all results
          val actualResults = actualNonIsolatedTestData.get().getOrElse {
            runDamlScriptTests(
              host = participant1.config.ledgerApi.address,
              port = participant1.config.ledgerApi.port,
              skippedTests = testsToIgnore ++ scalatestFilteredScriptIdsToIgnore,
            )
          }
          actualNonIsolatedTestData.set(Some(actualResults))

          assertDamlScriptTestResult(testScriptId, expectedResult, actualResults.get(testScriptId))
        }
      }
    }
  }
}

// TODO(#16458) we eventually need:
//     - a PV34, LF 2.2 test suite with mode NoKey
//     - a PV35, LF 2.3 test suite with mode NUCK
//     - a PVDev test suite with mode NUCK, because guarded exercises are only available in LF dev
//   For now we have :
//     - a PV35, LF 2.3 test suite with mode NoKey
//     - a PV35, LF 2.3 test suite with mode NUCK
//     - the only LF 2.dev test case (guarded exercises) is disabled
abstract class DamlScriptPV35IT(contractStateMode: ContractStateMachine.Mode) extends DamlScriptIT {

  import DamlScriptIT.withContractStateMode
  import DamlScriptIT.ExpectedResult.*

  override lazy val langVersion = LanguageVersion.v2_3
  override lazy val projectName = "ScriptLF23Tests"
  override lazy val minimumProtocolVersion = ProtocolVersion.v35

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransforms(withContractStateMode(contractStateMode)*)

  override def expectedResults = super.expectedResults ++ List(
    "ActionTest:testFilterA" -> Success(),
    "AuthEvalOrder:t1_create_success" -> Failure("t1 finished with no authorization failure"),
    "AuthEvalOrder:t2_create_badlyAuthorized" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "AuthEvalOrder:t3_createViaExerice_success" -> Failure(
      "t3 finished with no authorization failure"
    ),
    "AuthEvalOrder:t4_createViaExerice_badlyAuthorized" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "AuthFailure:t1_CreateMissingAuthorization" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "AuthFailure:t2_MaintainersNotSubsetOfSignatories" -> Failure(
      "has maintainers .* which are not a subset of the signatories"
    ),
    "AuthFailure:t3_FetchMissingAuthorization" -> Failure(
      "requires one of the stakeholders .* of the fetched contract to be an authorizer"
    ),
    "AuthFailure:t5_ExerciseMissingAuthorization" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "AuthorizedDivulgence:test_authorizedFetch" -> Success(),
    "AuthorizedDivulgence:test_divulgeChoiceTargetContractId" -> Success(),
    "AuthorizedDivulgence:test_noDivulgenceForFetch" -> Success(),
    "AuthorizedDivulgence:test_noDivulgenceOfCreateArguments" -> Success(),
    "BasicTests:test_createAndFetch" -> Success(),
    "BasicTests:test_doubleLetTest" -> Success(),
    "BasicTests:test_exponentiation" -> Success(),
    "BasicTests:test_failedAuths" -> Success(),
    "BasicTests:test_getTimeTest" -> Success(),
    "BasicTests:test_letTest" -> Success(),
    "BasicTests:test_listMatchTest" -> Success(),
    "BasicTests:test_mustFails" -> Success(),
    "BasicTests:test_payoutTest" -> Success(),
    "BasicTests:test_screateAndExercise" -> Success(),
    "BasicTests:test_screateAndExerciseComposit" -> Success(),
    "BasicTests:test_sgetTimeTest" -> Success(),
    "BasicTests:test_testXyzTest" -> Success(),
    "BasicTests:test_typeWithParameters" -> Success(),
    "ChoiceShadowing:test1" -> Success(),
    "CoerceContractId:test" -> Success(),
    "Conjunction:main" -> Success(),
    "ConjunctionChoices:demo" -> Success(),
    "ConsumingTests:main" -> Success(),
    "CreateAndExercise:main" -> Success(),
    "DamlScriptTrySubmit:authorizationError" -> Success(),
    "DamlScriptTrySubmit:failureStatusError" -> Success(),
    "EqContractId:main" -> Success(),
    "ExceptionSemantics:handledArithmeticError" -> Success(),
    "ExceptionSemantics:handledUserException" -> Success(),
    "ExceptionSemantics:uncaughtArithmeticError" -> Success(),
    "ExceptionSemantics:uncaughtUserException" -> Success(),
    "ExceptionSemantics:unhandledArithmeticError" -> Failure(
      "UNHANDLED_EXCEPTION/DA.Exception.ArithmeticError:ArithmeticError"
    ),
    "ExceptionSemantics:unhandledUserException" -> Failure(
      "UNHANDLED_EXCEPTION/ExceptionSemantics:E"
    ),
    "FailedFetch:fetchNonStakeholder" -> Failure("CONTRACT_NOT_FOUND"),
    "Interface:main" -> Success(),
    "InterfaceArchive:main" -> Success(),
    "Iou12:main" -> Success(),
    "LargeTransaction:largeListAsAChoiceArgTest" -> Success(),
    "LargeTransaction:largeTransactionWithManyContractsTest" -> Success(),
    "LargeTransaction:largeTransactionWithOneContractTest" -> Success(),
    "LargeTransaction:listSizeTest" -> Success(),
    "LargeTransaction:rangeOfIntsToListContainerTest" -> Success(),
    "LargeTransaction:rangeOfIntsToListTest" -> Success(),
    "LargeTransaction:rangeTest" -> Success(),
    "LedgerTestException:test" -> Failure("ohno"),
    "LfInterfaces:run" -> Success(),
    "MoreChoiceObserverDivulgence:test" -> Success(),
    "Self2:main" -> Success(),
    "Self:main" -> Success(),
    "TransientFailure:testBio" -> Failure("FAILED_PRECONDITION"),
  )
}

class DamlScriptPV35NUCKIT extends DamlScriptPV35IT(ContractStateMachine.Mode.NUCK) {
  import DamlScriptIT.contractIDsNotSupported
  import DamlScriptIT.ExpectedResult.*

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override val expectedResults = super.expectedResults ++ List(
    "AuthFailure:t4_LookupByKeyMissingAuthorization" -> Failure(
      "requires authorizers .* for lookup by key"
    ),
    "DamlScriptTrySubmit:contractKeyNotFound" -> Success(),
    "DamlScriptTrySubmit:contractNotActive" -> Failure("contractNotActive no additional info"),
    // TODO(#30398): move to a 2.dev test suite
    // "DamlScriptTrySubmit:devError" -> Success(),
    "DamlScriptTrySubmit:truncatedError" -> Failure("EXPECTED_TRUNCATED_ERROR"),
    "DamlScriptTrySubmit:wronglyTypedContract" -> Success(),
    "ExceptionSemantics:divulgence" -> Success(),
    "ExceptionSemantics:duplicateKey" -> Ignored,
    // ignored because it rolls back an effect
    "ExceptionSemantics:rollbackArchive" -> Ignored,
    // ignored because it rolls back an effect
    "ExceptionSemantics:rollbackConsumingExercise" -> Ignored,
    // ignored because it rolls back an effect
    "ExceptionSemantics:rollbackCreate" -> Ignored,
    "ExceptionSemantics:tryContext" -> Failure("Contract could not be found"),
    "FetchByKey:failLedger" -> Failure("couldn't find key"),
    "FetchByKey:failSpeedy" -> Failure("couldn't find key"),
    "FetchByKey:mustFail" -> Success(),
    "KeyNotVisibleStakeholders:blindFetch" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "KeyNotVisibleStakeholders:blindLookup" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "KeyNotVisibleStakeholders:divulgeeFetch" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "KeyNotVisibleStakeholders:divulgeeLookup" -> Failure(
      "requires authorizers .* but only .* were given"
    ),
    "ConsumedContractKey:testFetchFromConsumingChoice" -> Failure(
      "Update failed due to fetch of an inactive contract"
    ),
    "ConsumedContractKey:testFetchKeyFromConsumingChoice" -> Failure(
      "dependency error: couldn't find key"
    ),
    "ConsumedContractKey:testLookupKeyFromConsumingChoice" -> Success(),
    "ContractIdInContractKeySkipCheck:createCmdCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:createCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:exerciseCmdCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:exerciseCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:fetchCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:lookupCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:queryCrashes" ->
      // should have failed but it succeeds (tracked by https://github.com/digital-asset/daml/issues/17554)
      Broken(contractIDsNotSupported),
    "ContractKeyNotEffective:fetchByKeyMustFail" -> Failure(
      "Setting time backwards is not allowed"
    ),
    "ContractKeyNotVisible:aScript" -> Failure("Couldn't see contract with key .*"),
    "ContractKeyNotVisible:blindLookup" -> Broken(
      Failure("expected unassigned key, which already exists")
    ),
    "ContractKeyNotVisible:divulgeeLookup" -> Broken(
      Failure("expected unassigned key, which already exists")
    ),
    "ContractKeyNotVisible:localFetch" -> Success(),
    // TODO(#30398): fails with a MALFORMED_REQUEST, probably due to discrepancy between the CSM used in the engine
    //     and the one used in view decomposition.
    "ContractKeyNotVisible:localLookup" -> Ignored,
    "ContractKeys:test" -> Success(),
    "DamlScriptTrySubmit:createEmptyContractKeyMaintainers" -> Success(),
    "DamlScriptTrySubmit:duplicateContractKey" -> Failure("incorrectly succeeded"),
    "DamlScriptTrySubmit:fetchEmptyContractKeyMaintainers" -> Success(),
    "EmptyContractKeyMaintainers:createCmdNoMaintainer" -> Failure(
      "Update failed due to a contract key with an empty set of maintainers"
    ),
    "EmptyContractKeyMaintainers:createNoMaintainer" -> Failure(
      "Update failed due to a contract key with an empty set of maintainers"
    ),
    "EmptyContractKeyMaintainers:fetchNoMaintainer" -> Failure(
      "Update failed due to a contract key with an empty set of maintainers"
    ),
    "EmptyContractKeyMaintainers:lookupNoMaintainer" -> Failure(
      "Update failed due to a contract key with an empty set of maintainers"
    ),
    "EmptyContractKeyMaintainers:queryNoMaintainer" -> Failure("Couldn't see contract with key"),
    "ExceptionAndContractKey:testCreate" -> Success(),
    "ExceptionAndContractKey:testLookup" -> Success(),
    "LFContractKeys:lookupTest" -> Ignored,
    "LfStableContractKeyThroughExercises:run" -> Ignored,
    "LfStableContractKeys:run" -> Ignored,
  )
}

class DamlScriptPV35NoContractKeyIT extends DamlScriptPV35IT(ContractStateMachine.Mode.NoKey) {
  import DamlScriptIT.{assertionFailed, contractIDsNotSupported, commandSubmissionFailure}
  import DamlScriptIT.ExpectedResult.*

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override val expectedResults = super.expectedResults ++ List(
    // TODO(#30398): ignoring key-related test cases as you shouldn't be able to run a dar that uses a keys against PV34
    //     anyway, so all these tests will eventually fail with a compilation error or not exist.
    "AuthFailure:t4_LookupByKeyMissingAuthorization" -> Ignored,
    "ConsumedContractKey:testFetchFromConsumingChoice" -> Ignored,
    "ConsumedContractKey:testFetchKeyFromConsumingChoice" -> Ignored,
    "ConsumedContractKey:testLookupKeyFromConsumingChoice" -> Ignored,
    "ContractKeyNotEffective:fetchByKeyMustFail" -> Ignored,
    "ContractKeyNotVisible:aScript" -> Ignored,
    "KeyNotVisibleStakeholders:blindLookup" -> Ignored,
    "ContractKeyNotVisible:blindLookup" -> Ignored,
    "ContractKeyNotVisible:divulgeeLookup" -> Ignored,
    "ContractKeyNotVisible:localFetch" -> Ignored,
    "ContractKeyNotVisible:localLookup" -> Ignored,
    "ContractKeys:test" -> Ignored,
    "DamlScriptTrySubmit:contractKeyNotFound" -> Ignored,
    "DamlScriptTrySubmit:contractNotActive" -> Ignored,
    // TODO(#30398): move to a 2.dev test suite
    // "DamlScriptTrySubmit:devError" -> Ignored,
    "DamlScriptTrySubmit:truncatedError" -> Ignored,
    "DamlScriptTrySubmit:wronglyTypedContract" -> Ignored,
    "ExceptionSemantics:divulgence" -> Ignored,
    "ExceptionSemantics:duplicateKey" -> Ignored,
    "ExceptionSemantics:rollbackArchive" -> Ignored,
    "ExceptionSemantics:rollbackConsumingExercise" -> Ignored,
    "ExceptionSemantics:rollbackCreate" -> Ignored,
    "ExceptionSemantics:tryContext" -> Ignored,
    "FetchByKey:failLedger" -> Ignored,
    "FetchByKey:failSpeedy" -> Ignored,
    "FetchByKey:mustFail" -> Ignored,
    "KeyNotVisibleStakeholders:blindFetch" -> Ignored,
    "KeyNotVisibleStakeholders:divulgeeFetch" -> Ignored,
    "KeyNotVisibleStakeholders:divulgeeLookup" -> Ignored,
    "DamlScriptTrySubmit:duplicateContractKey" -> Ignored,
    "ExceptionAndContractKey:testCreate" -> Ignored,
    "ExceptionAndContractKey:testLookup" -> Ignored,
    "LFContractKeys:lookupTest" -> Ignored,
    "LfStableContractKeyThroughExercises:run" -> Ignored,
    "LfStableContractKeys:run" -> Ignored,
    "ContractIdInContractKeySkipCheck:createCmdCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:createCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:exerciseCmdCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:exerciseCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:fetchCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:lookupCrashes" -> contractIDsNotSupported,
    "ContractIdInContractKeySkipCheck:queryCrashes" -> Failure("Command QueryContractKey failed"),
    "DamlScriptTrySubmit:createEmptyContractKeyMaintainers" -> Success(),
    "DamlScriptTrySubmit:fetchEmptyContractKeyMaintainers" -> Success(),
    "EmptyContractKeyMaintainers:createCmdNoMaintainer" -> commandSubmissionFailure,
    "EmptyContractKeyMaintainers:createNoMaintainer" -> commandSubmissionFailure,
    "EmptyContractKeyMaintainers:fetchNoMaintainer" -> commandSubmissionFailure,
    "EmptyContractKeyMaintainers:lookupNoMaintainer" -> commandSubmissionFailure,
    "EmptyContractKeyMaintainers:queryNoMaintainer" -> assertionFailed,
  )
}

object DamlScriptIT {

  sealed abstract class ExpectedResult extends Product with Serializable {
    def logAssertions: Seq[LogEntry => Assertion] = Seq.empty
  }

  object ExpectedResult {
    final case class Success(
        value: Option[Json],
        override val logAssertions: (LogEntry => Assertion)*
    ) extends ExpectedResult {
      def withLogAssertions(additionalLogAssertions: (LogEntry => Assertion)*): Success =
        Success(value, logAssertions ++ additionalLogAssertions: _*)
    }

    object Success {
      def apply(logAssertions: (LogEntry => Assertion)*): Success =
        new Success(None, logAssertions*)

      def apply(value: Json, logAssertions: (LogEntry => Assertion)*): Success =
        new Success(Some(value), logAssertions*)
    }

    final case class Failure(
        errorMsgPattern: String,
        override val logAssertions: (LogEntry => Assertion)*
    ) extends ExpectedResult {
      def withLogAssertions(additionalLogAssertions: (LogEntry => Assertion)*): Failure =
        Failure(errorMsgPattern, logAssertions ++ additionalLogAssertions: _*)
    }

    final case class Broken(result: Either[Failure, Success]) extends ExpectedResult

    object Broken {
      def apply(success: Success): Broken = Broken(Right(success))
      def apply(failure: Failure): Broken = Broken(Left(failure))
    }

    final case object Ignored extends ExpectedResult
  }

  val contractIDsNotSupported: ExpectedResult.Failure = ExpectedResult.Failure(
    "Contract IDs are not supported"
  )

  val contractKeyNotFound: ExpectedResult.Failure = ExpectedResult.Failure(
    "contractKeyNotFound"
  )

  val internalErrorOccurred: ExpectedResult.Failure = ExpectedResult.Failure(
    "INTERNAL: An error occurred"
  )

  val commandSubmissionFailure: ExpectedResult.Failure = ExpectedResult.Failure(
    "Command Submit failed: INVALID_ARGUMENT"
  )

  val assertionFailed: ExpectedResult.Failure = ExpectedResult.Failure(
    "User failure: UNHANDLED_EXCEPTION/DA.Exception.AssertionFailed:AssertionFailed"
  )

  def withContractStateMode(contractStateMode: ContractStateMachine.Mode): Seq[ConfigTransform] =
    Seq(
      ConfigTransforms.enableNonStandardConfig,
      ConfigTransforms.updateAllParticipantConfigs_(
        _.focus(_.parameters.engine.contractStateMode).replace(contractStateMode)
      ),
    )

  private implicit val decodeResult: Decoder[Either[String, Json]] = Decoder.instance { c =>
    c.downField("error").as[String].map(Left(_)) orElse c.downField("result").as[Json].map(Right(_))
  }
}
