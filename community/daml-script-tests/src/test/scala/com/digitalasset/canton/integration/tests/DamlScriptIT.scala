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
import io.circe.*
import io.circe.parser.*
import monocle.macros.syntax.lens.*
import org.apache.commons.io.FileUtils
import org.scalatest.{Args, Assertion, BeforeAndAfterAll, Status}

import java.nio.file.*
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

abstract class DamlScriptIT(langVersion: LanguageVersion)
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BeforeAndAfterAll {
  self: EnvironmentSetup =>

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  // Resolve the conflict between BeforeAndAfterAll and BeforeAndAfterAllConfigMap
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected: Boolean = true

  import DamlScriptIT.*

  protected val debug = false

  protected def projectName: String

  protected var damlProjectDir: Path = _

  private var env: Seq[(String, String)] = _

  private val darFileUploaded: AtomicBoolean = new AtomicBoolean(false)

  // As non-isolated daml-scripts are ran as a single batch, we use this var to cache the result of the first `dpm script --all` run
  // This allows non-isolated to then be treated as test cases on a per script ID basis
  private val actualNonIsolatedTestData
      : AtomicReference[Option[Map[String, Either[String, Json]]]] = new AtomicReference(None)

  protected def scriptIdsToTest: List[String]

  private var scalatestFilteredScriptIdsToIgnore: List[String] = List.empty

  final override def afterAll(): Unit = {
    super.afterAll()
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

  /** As a side effect, creates a temporary copy of the source daml project which is used for dpm
    * based builds.
    *
    * @return
    *   path to daml project containing our source files
    */
  private def setDamlProjectDir(): Path =
    Option(getClass.getResource(s"/daml/$projectName")) match {
      case Some(in) =>
        damlProjectDir = Files.createTempDirectory(s"test_${getClass.getSimpleName}_")
        if (debug) println(s"Saving daml project to $damlProjectDir")
        Paths.get(in.toURI)
      case None =>
        throw new java.lang.Error("could not find daml project in resources: " + projectName)
    }

  private def setEnv(): Unit = {
    def getEnv(name: String, default: String): String =
      sys.props.get(name) match {
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
  }

  private def buildDamlScriptProject(
      projectName: String,
      projectDir: Path,
      buildDir: Path,
  ): Unit = {
    FileUtils.copyDirectory(projectDir.toFile, buildDir.toFile)
    // compile the project
    val _ = run(cmd = List("dpm", "build", "--output", projectName + ".dar"))
  }

  def listDamlScriptIds(projectName: String): List[String] = {
    setEnv()
    val currentDir = setDamlProjectDir()
    buildDamlScriptProject(projectName, currentDir, damlProjectDir)

    val outputFile = Files.createTempFile(
      damlProjectDir,
      s"$projectName-script-ids",
      ".json",
    )
    val cmd = List(
      List("dpm", "script"),
      List("--dar", projectName + ".dar"),
      List("--list-scripts-json", outputFile.toString),
    ).flatten
    val (stdout, stderr) = run(cmd, ignoreExitCode = true)
    val resultOrErr = for {
      output <- scala.util.Try(Files.readString(outputFile)).toEither
      json <- parse(output)
      result <- json.as[List[String]]
    } yield result
    resultOrErr match {
      case Right(value) =>
        value
      case Left(err) =>
        scriptError(
          cmd,
          stdout,
          stderr,
          s"failed to parse list script Id output: ${err.getMessage}",
        )
    }
  }

  protected def enableLfDev: Boolean =
    langVersion == com.digitalasset.daml.lf.language.LanguageVersion.devLfVersion

  // Only test for this protocol version
  protected def protocolVersionForTesting: ProtocolVersion

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
            fail(s"script $scriptId was expected to be broken but the test passed")
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

  "All project script IDs match expected result map script IDs" onlyRunWith protocolVersionForTesting in {
    _ =>
      expectedResults.keySet shouldEqual scriptIdsToTest.toSet
  }

  def testGivenScriptId(testScriptId: String)(env: FixtureParam): Assertion = {
    import env.participant1

    val ignoredTests = expectedResults.collect { case (id, ExpectedResult.Ignored) =>
      id
    }.toList

    if (ignoredTests.contains(testScriptId)) {
      if (debug) println(s"script $testScriptId was ignored")
      succeed
    } else {
      val isolatedTests = expectedResults.collect {
        case (id, ExpectedResult.Success(_, logAssertions*)) if logAssertions.nonEmpty => id
        case (id, ExpectedResult.Failure(_, logAssertions*)) if logAssertions.nonEmpty => id
        case (id, result @ ExpectedResult.Broken(_)) if result.logAssertions.nonEmpty => id
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

class DamlScriptPV34LF22IT extends DamlScriptIT(LanguageVersion.v2_2) {

  import DamlScriptIT.ExpectedResult.*

  override lazy val projectName = "ScriptLF22Tests"
  override lazy val protocolVersionForTesting = ProtocolVersion.v34

  override protected val scriptIdsToTest: List[String] = listDamlScriptIds(projectName)

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
    "DamlScriptTrySubmit:wronglyTypedContract" -> Success(),
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
    "ExceptionSemantics:tryContext" -> Failure("Contract could not be found"),
    "ExceptionSemantics:rollbackArchive" -> Success(),
    "ExceptionSemantics:rollbackConsumingExercise" -> Success(),
    "ExceptionSemantics:rollbackCreate" -> Success(),
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

  // scriptIdsToTest needs to be defined in order for individual tests to be defined
  scriptIdsToTest.foreach { testScriptId =>
    testScriptId onlyRunWith protocolVersionForTesting in testGivenScriptId(testScriptId)
  }
}

abstract class DamlScriptPV35IT(langVersion: LanguageVersion) extends DamlScriptIT(langVersion) {

  import DamlScriptIT.ExpectedResult.*

  override lazy val protocolVersionForTesting = ProtocolVersion.v35

  override protected val scriptIdsToTest: List[String] = listDamlScriptIds(projectName)

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
    "AuthFailureWithKeys:t2_MaintainersNotSubsetOfSignatories" -> Failure(
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
    "DamlScriptTrySubmitWithKeys:authorizationError" -> Success(),
    "DamlScriptTrySubmit:failureStatusError" -> Success(),
    "DamlScriptTrySubmit:wronglyTypedContract" -> Success(),
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

  // scriptIdsToTest needs to be defined in order for individual tests to be defined
  scriptIdsToTest.foreach { testScriptId =>
    testScriptId onlyRunWith protocolVersionForTesting in testGivenScriptId(testScriptId)
  }
}

class DamlScriptPV35LF23IT extends DamlScriptPV35IT(LanguageVersion.v2_3) {
  import DamlScriptIT.contractIDsNotSupported
  import DamlScriptIT.ExpectedResult.*

  override lazy val projectName = "ScriptLF23Tests"

  override val expectedResults = super.expectedResults ++ List(
    "AuthFailureWithKeys:t4_LookupByKeyMissingAuthorization" -> Failure(
      "requires authorizers .* for lookup by key"
    ),
    "DamlScriptTrySubmitWithKeys:contractKeyNotFound" -> Success(),
    "DamlScriptTrySubmitWithKeys:contractNotActive" -> Failure(
      "contractNotActive no additional info"
    ),
    "DamlScriptTrySubmitWithKeys:truncatedError" -> Failure("EXPECTED_TRUNCATED_ERROR"),
    "DamlScriptTrySubmitWithKeys:wronglyTypedContract" -> Success(),
    "ExceptionSemanticsWithKeys:divulgence" -> Success(),
    "ExceptionSemanticsWithKeys:duplicateKey" -> Ignored,
    "ExceptionSemanticsWithKeys:tryContext" -> Failure("Contract could not be found"),
    "ExceptionSemanticsWithKeys:rollbackArchive" -> Failure("INVALID_ARGUMENT"),
    "ExceptionSemanticsWithKeys:rollbackConsumingExercise" -> Failure("INVALID_ARGUMENT"),
    "ExceptionSemanticsWithKeys:rollbackCreate" -> Failure("INVALID_ARGUMENT"),
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
    "ContractIdInContractKeySkipCheck:queryCrashes" -> contractIDsNotSupported,
    "ContractKeyNotEffective:fetchByKeyMustFail" -> Failure(
      "Setting time backwards is not allowed"
    ),
    "ContractKeyNotVisible:aScript" -> Failure("Couldn't see contract with key .*"),
    "ContractKeyNotVisible:blindLookup" -> Success(),
    "ContractKeyNotVisible:divulgeeLookup" -> Success(),
    "ContractKeyNotVisible:localFetch" -> Success(),
    "ContractKeyNotVisible:localLookup" -> Success(),
    "ContractKeys:test" -> Success(),
    "DamlScriptTrySubmitWithKeys:createEmptyContractKeyMaintainers" -> Success(),
    "DamlScriptTrySubmitWithKeys:duplicateContractKey" -> Failure("incorrectly succeeded"),
    "DamlScriptTrySubmitWithKeys:fetchEmptyContractKeyMaintainers" -> Success(),
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
    "NUCKTests:useAllOperations" -> Success(),
    "NUCKTests:createMultiple" -> Success(),
    "NUCKTests:exerciseByMultiple" -> Success(),
    "NUCKTests:lookupNByKeyMultiple" -> Success(),
    "NUCKTests:queryNByKeyMultiple" -> Success(),
    "NUCKTests:queryNByKeyUnauthorized" -> Success(),
    "NUCKTests:exerciseByKeyWithInvisibleButDisclosedKey" -> Success(),
  )
}

class DamlScriptPVDevLFDevIT extends DamlScriptIT(LanguageVersion.v2_dev) {
  import DamlScriptIT.contractIDsNotSupported
  import DamlScriptIT.ExpectedResult.*

  override lazy val projectName = "ScriptDevTests"
  override lazy val protocolVersionForTesting = ProtocolVersion.dev

  override protected val scriptIdsToTest: List[String] = listDamlScriptIds(projectName)

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
    "AuthFailureWithKeys:t2_MaintainersNotSubsetOfSignatories" -> Failure(
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
    "DamlScriptTrySubmitWithKeys:authorizationError" -> Success(),
    "DamlScriptTrySubmit:failureStatusError" -> Success(),
    "DamlScriptTrySubmit:wronglyTypedContract" -> Success(),
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
    "AuthFailureWithKeys:t4_LookupByKeyMissingAuthorization" -> Failure(
      "requires authorizers .* for lookup by key"
    ),
    "DamlScriptTrySubmitWithKeys:contractKeyNotFound" -> Success(),
    "DamlScriptTrySubmitWithKeys:contractNotActive" -> Failure(
      "contractNotActive no additional info"
    ),
    "DamlScriptTrySubmitWithKeys:devError" -> Success(),
    "DamlScriptTrySubmitWithKeys:truncatedError" -> Failure("EXPECTED_TRUNCATED_ERROR"),
    "DamlScriptTrySubmitWithKeys:wronglyTypedContract" -> Success(),
    "ExceptionSemanticsWithKeys:divulgence" -> Success(),
    "ExceptionSemanticsWithKeys:duplicateKey" -> Ignored,
    "ExceptionSemanticsWithKeys:tryContext" -> Failure("Contract could not be found"),
    "ExceptionSemanticsWithKeys:rollbackArchive" -> Failure("INVALID_ARGUMENT"),
    "ExceptionSemanticsWithKeys:rollbackConsumingExercise" -> Failure("INVALID_ARGUMENT"),
    "ExceptionSemanticsWithKeys:rollbackCreate" -> Failure("INVALID_ARGUMENT"),
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
    "ContractIdInContractKeySkipCheck:queryCrashes" -> contractIDsNotSupported,
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
    "ContractKeyNotVisible:localLookup" -> Success(),
    "ContractKeys:test" -> Success(),
    "DamlScriptTrySubmitWithKeys:createEmptyContractKeyMaintainers" -> Success(),
    "DamlScriptTrySubmitWithKeys:duplicateContractKey" -> Failure("incorrectly succeeded"),
    "DamlScriptTrySubmitWithKeys:fetchEmptyContractKeyMaintainers" -> Success(),
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
    "NUCKTests:useAllOperations" -> Success(),
    "NUCKTests:createMultiple" -> Success(),
    "NUCKTests:exerciseByMultiple" -> Success(),
    "NUCKTests:lookupNByKeyMultiple" -> Success(),
    "NUCKTests:queryNByKeyMultiple" -> Success(),
    "NUCKTests:queryNByKeyUnauthorized" -> Success(),
    "NUCKTests:exerciseByKeyWithInvisibleButDisclosedKey" -> Success(),
  )

  // scriptIdsToTest needs to be defined in order for individual tests to be defined
  scriptIdsToTest.foreach { testScriptId =>
    testScriptId onlyRunWith protocolVersionForTesting in testGivenScriptId(testScriptId)
  }
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
      require(errorMsgPattern.nonEmpty)

      def withLogAssertions(additionalLogAssertions: (LogEntry => Assertion)*): Failure =
        Failure(errorMsgPattern, logAssertions ++ additionalLogAssertions: _*)
    }

    final case class Broken(result: Either[Failure, Success]) extends ExpectedResult {
      override val logAssertions: Seq[LogEntry => Assertion] =
        result.fold(_.logAssertions, _.logAssertions)
    }

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

  private implicit val decodeResult: Decoder[Either[String, Json]] = Decoder.instance { c =>
    c.downField("error").as[String].map(Left(_)) orElse c.downField("result").as[Json].map(Right(_))
  }
}
