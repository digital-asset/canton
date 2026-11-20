// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.testsuite.DamlScriptTestSuite
import com.digitalasset.canton.integration.{
  CantonEnvironmentSetup,
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.version.{InterpretationConfig, ProtocolVersion}
import com.digitalasset.daml.lf.language.{LanguageFeaturesGenerated, LanguageVersion}
import io.circe.*
import io.circe.parser.*
import monocle.macros.syntax.lens.*
import org.apache.commons.io.FileUtils
import org.scalatest.{Args, Assertion, BeforeAndAfterAll, Status}

import java.nio.file.*
import java.util.concurrent.atomic.AtomicReference

object LanguageFeatures extends LanguageFeaturesGenerated

/** Given a non-empty list of `DamlScriptTestSuite` instances, provide a testing framework for
  * running each of these test suite instances against a Canton instance. Daml script test suites
  * declare the LF version and protocol version they should be enabled for.
  *
  * During test debugging, Scala test tag filtering can be used to filter which Daml script test IDs
  * will be ran. In addition, the `debug` flag can be set if Daml project build environments need to
  * be preserved.
  */
abstract class DamlScriptTestSuiteFramework(
    firstTestSuite: DamlScriptTestSuite,
    additionalTestSuites: DamlScriptTestSuite*
) extends CommunityIntegrationTest
    with SharedEnvironment
    with BeforeAndAfterAll {
  self: CantonEnvironmentSetup =>

  import DamlScriptTestSuiteFramework.*

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val testSuites = (firstTestSuite +: additionalTestSuites).map(testSuite =>
    ConfiguredDamlScriptTestSuite(
      testSuite,
      testName = getClass.getSimpleName,
      protocolVersion = testedProtocolVersion,
    )
  )

  // Resolve the conflict between BeforeAndAfterAll and BeforeAndAfterAllConfigMap
  override val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected: Boolean = true

  private val debug = false

  private val darFileUploaded: AtomicReference[Set[DamlScriptTestSuite]] = new AtomicReference(
    Set.empty
  )

  // As non-isolated daml-scripts are ran as a single batch, we use this var to cache the result of the first `dpm script --all` run
  // This allows non-isolated to then be treated as test cases on a per script ID basis
  private val actualNonIsolatedTestData: AtomicReference[Map[String, Either[String, Json]]] =
    new AtomicReference(Map.empty)

  private var scalatestFilteredScriptIdsToIgnore: List[String] = List.empty

  final override def beforeAll(): Unit =
    // Only setup the Canton test environment if we have something to test!
    if (testSuites.exists(testSuite => testSuite.scriptIds.nonEmpty)) {
      super.beforeAll()
    }

  final override def afterAll(): Unit = {
    super.afterAll()
    if (!debug) {
      testSuites.foreach { testSuite =>
        FileUtils.deleteDirectory(testSuite.damlProjectBuildDir.toFile)
      }
    }
  }

  private def runDamlScriptTests(
      host: String,
      port: Port,
      skippedTests: List[String],
      testScriptId: Option[String] = None,
  )(implicit testSuite: ConfiguredDamlScriptTestSuite): Map[String, Either[String, Json]] = {
    val outputFile = testSuite.testResultFilename(testScriptId)
    val cmd = List(
      List("dpm", "script"),
      List("--dar", testSuite.projectName + ".dar"),
      testScriptId.fold(List("--all"))(scriptId => List("--script-name", scriptId)),
      testScriptId.fold(skippedTests.flatMap(List("--skip-script-name", _)))(_ => List.empty),
      List("--ledger-host", host),
      List("--ledger-port", port.unwrap.toString),
      List("--static-time"),
      List("--max-inbound-message-size", Int.MaxValue.toString),
      List("--upload-dar", s"${!darFileUploaded.get().contains(testSuite)}"),
      List("--json-test-summary", outputFile.toString),
    ).flatten
    val (stdout, stderr) =
      runProcess(cmd, testSuite, ignoreExitCode = true)
    val resultOrErr = for {
      output <- scala.util.Try(Files.readString(outputFile)).toEither
      json <- parse(output)
      result <- json.as[Map[String, Either[String, Json]]]
    } yield result
    resultOrErr match {
      case Right(value) =>
        darFileUploaded.updateAndGet(_ + testSuite)
        value
      case Left(err) =>
        scriptError(
          cmd,
          testSuite,
          stdout,
          stderr,
          s"failed to parse script output: ${err.getMessage}",
        )
    }
  }

  private lazy val lowerCommandTrackerDuration: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_ {
      _.focus(_.ledgerApi.commandService.defaultTrackingTimeout)
        .replace(config.NonNegativeFiniteDuration.ofSeconds(15))
    }

  private lazy val useTestingTimeService: ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService)

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(lowerCommandTrackerDuration)
      .addConfigTransforms(useTestingTimeService)
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

  private def checkForSuccess(
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

  private def checkForFailure(
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

  private def assertDamlScriptTestResult(
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
      scalatestFilteredScriptIdsToIgnore = args.filter
        .apply(testSuites.flatMap(_.scriptIds).toSet, testSuiteTags, testSuiteName)
        .collect { case (scriptId, true) =>
          scriptId
        }
    }

    super.runTests(testName, args)
  }

  private def testGivenScriptId(
      testScriptId: String
  )(env: FixtureParam)(implicit testSuite: ConfiguredDamlScriptTestSuite): Assertion = {
    import env.participant1

    val ignoredTests = testSuite.expectedResults.collect { case (id, ExpectedResult.Ignored) =>
      id
    }.toList

    if (ignoredTests.contains(testScriptId)) {
      if (debug) println(s"script $testScriptId was ignored")
      succeed
    } else {
      val isolatedTests = testSuite.expectedResults.collect {
        case (id, ExpectedResult.Success(_, logAssertions*)) if logAssertions.nonEmpty => id
        case (id, ExpectedResult.Failure(_, logAssertions*)) if logAssertions.nonEmpty => id
        case (id, result @ ExpectedResult.Broken(_)) if result.logAssertions.nonEmpty => id
      }.toList
      val testsToIgnore = ignoredTests ++ isolatedTests
      val expectedResult = testSuite.expectedResults.get(testScriptId)

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
            assertions = testSuite.expectedResults
              .get(testScriptId)
              .fold[Seq[LogEntry => Assertion]](Seq.empty)(_.logAssertions) *,
          )
          .get(testScriptId)

        assertDamlScriptTestResult(testScriptId, expectedResult, actualResult)
      } else {
        // Non-isolated test cases - run all of these scripts once and cache all results
        val actualResults = actualNonIsolatedTestData.updateAndGet { testData =>
          testData ++ testData
            .get(testScriptId)
            .fold {
              runDamlScriptTests(
                host = participant1.config.ledgerApi.address,
                port = participant1.config.ledgerApi.port,
                skippedTests = testsToIgnore ++ scalatestFilteredScriptIdsToIgnore,
              )
            }(result => Map(testScriptId -> result))
        }

        assertDamlScriptTestResult(testScriptId, expectedResult, actualResults.get(testScriptId))
      }
    }
  }

  /** This function contains the actual tests that will be run.
    */
  protected final def declareTests(): Unit = {
    val testsRunningLocally = !sys.env.contains("CI")

    testSuites.foreach { implicit testSuite =>
      implicit val languageVersion: Option[LanguageVersion] = testSuite.testedLanguageVersion

      (languageVersion, testSuite.scriptIds) match {
        case (Some(lf), testScriptIds) if testScriptIds.isEmpty =>
          if (testsRunningLocally) {
            info(
              s"${testSuite.testName}: no Daml script tests to run for protocol version ${testSuite.protocolVersion} and latest enabled language version $lf for Daml project ${testSuite.projectName}"
            )
          }

        case (Some(lf), testScriptIds) =>
          if (testsRunningLocally) {
            info(
              s"${testSuite.testName}: for protocol version ${testSuite.protocolVersion}, latest enabled language version is $lf for Daml project ${testSuite.projectName}"
            )
          }
          s"${testSuite.projectName}" should {
            "All project script IDs match expected result map script IDs" in { _ =>
              testSuite.expectedResults.keySet shouldEqual testScriptIds.toSet
            }

            testScriptIds.foreach { testScriptId =>
              testScriptId in testGivenScriptId(testScriptId)
            }
          }

        case (None, _) =>
          if (testsRunningLocally) {
            info(
              s"${testSuite.testName}: no Daml script tests ran as there is no enabled language version for protocol version ${testSuite.protocolVersion} for Daml project ${testSuite.projectName}"
            )
          }
      }
    }
  }
}

object DamlScriptTestSuiteFramework {

  /** Internal case class that binds a `DamlScriptTestSuite` instance to the runtime known test
    * class and protocol version. This allows a number of helper/utility methods to be defined.
    */
  private final case class ConfiguredDamlScriptTestSuite(
      testSuite: DamlScriptTestSuite,
      testName: String,
      protocolVersion: ProtocolVersion,
  ) extends DamlScriptTestSuite {
    override val projectName: String = testSuite.projectName

    override val expectedResults: Map[String, DamlScriptTestSuiteFramework.ExpectedResult] =
      testSuite.expectedResults

    override def isEnabledFor(
        protocolVersion: ProtocolVersion,
        languageVersion: LanguageVersion,
    ): Boolean = testSuite.isEnabledFor(protocolVersion, languageVersion)

    val darFilename: String = projectName + ".dar"

    val damlProjectDir: Path =
      Option(getClass.getResource(s"/daml/$projectName")) match {
        case Some(in) =>
          Paths.get(in.toURI)
        case None =>
          throw new java.lang.Error(
            "could not find daml project in resources: " + projectName
          )
      }

    val testedLanguageVersion: Option[LanguageVersion] =
      InterpretationConfig
        .forProtocolVersion(protocolVersion)
        .allowedLanguageVersions
        .filter(lf => isEnabledFor(protocolVersion, lf))
        .maxOption

    val damlProjectBuildDir: Path =
      Option(getClass.getResource(s"/daml/$projectName")) match {
        case Some(_) =>
          val buildDir = Files.createTempDirectory(s"test_${testName}_${projectName}_")
          // copy daml project directory to a temporary build directory
          FileUtils.copyDirectory(damlProjectDir.toFile, buildDir.toFile)
          buildDir
        case None =>
          throw new java.lang.Error(
            "could not find daml project in resources: " + projectName
          )
      }

    val scriptIds: List[String] = {
      def buildDamlScriptProject(): Unit = {
        // compile the project
        val _ = runProcess(
          cmd = List("dpm", "build", "--output", darFilename),
          testSuite = this,
        )
      }

      testedLanguageVersion match {
        case None =>
          List.empty
        case Some(_) =>
          buildDamlScriptProject()

          val outputFile = Files.createTempFile(
            damlProjectBuildDir,
            s"$projectName-script-ids",
            ".json",
          )
          val cmd = List(
            List("dpm", "script"),
            List("--dar", darFilename),
            List("--list-scripts-json", outputFile.toString),
          ).flatten
          val (stdout, stderr) = runProcess(cmd, this, ignoreExitCode = true)
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
                this,
                stdout,
                stderr,
                s"failed to parse list script Id output: ${err.getMessage}",
              )
          }
      }
    }

    private val damlProjectScriptResultPathMap: AtomicReference[Map[Option[String], Path]] =
      new AtomicReference(Map.empty)
    def testResultFilename(scriptId: Option[String]): Path =
      damlProjectScriptResultPathMap.updateAndGet { pathMap =>
        pathMap.get(scriptId) match {
          case None =>
            val resultFile = Files.createTempFile(
              damlProjectBuildDir,
              s"$projectName-${scriptId.getOrElse("non")}-isolated",
              ".json",
            )

            pathMap + (scriptId -> resultFile)

          case Some(_) =>
            pathMap
        }
      }(scriptId)
  }

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

  private def getEnvValue(name: String, default: String): String =
    sys.props.get(name) match {
      case Some(value) =>
        // on CI we should get the value from the configMap
        value
      case None =>
        Console.err.println(s"WARN: Using default value for $name: $default.")
        default
    }

  @throws[java.lang.Error]
  private def scriptError(
      cmd: List[String],
      testSuite: ConfiguredDamlScriptTestSuite,
      stdout: String,
      stderr: String,
      cause: String,
  ): Nothing = {
    Console.err.println(
      s"""running command failed:
         |  command: ${cmd.mkString(" ")}
         |  cause: $cause
         |  test suite: ${testSuite.projectName}
         |  cwd: ${testSuite.damlProjectBuildDir} (switch DamlScriptId.debug to true to keep this temporary directory)
         |  DAML_VERSION=${getEnvValue("damlVersion", "<not set>")}
         |  DPM_REGISTRY=${getEnvValue("dpmRegistry", "<not set>")}
         |  stdout: $stdout
         |  stderr: $stderr
         |""".stripMargin
    )
    throw new java.lang.Error(s"command failed: $cause")
  }

  private def getEnvVarMap(languageVersion: Option[LanguageVersion]): Seq[(String, String)] =
    Seq(
      "DAML_VERSION" -> getEnvValue("damlVersion", BuildInfo.damlLibrariesVersion),
      "DPM_REGISTRY" -> getEnvValue(
        "dpmRegistry",
        "europe-docker.pkg.dev/da-images/public-unstable",
      ),
    ) ++ languageVersion.fold(Seq.empty[(String, String)])(lf => Seq("DAML_LF_VERSION" -> s"$lf"))

  def runProcess(
      cmd: List[String],
      testSuite: ConfiguredDamlScriptTestSuite,
      ignoreExitCode: Boolean = false,
  ): (String, String) = {
    val stderr = new StringBuilder
    val stdout = new StringBuilder
    val processLogger =
      sys.process.ProcessLogger(stdout.append(_).append("\n"), stderr.append(_).append("\n"))
    val exitCode =
      try
        sys.process.Process(
          cmd,
          cwd = Some(testSuite.damlProjectBuildDir.toFile),
          getEnvVarMap(testSuite.testedLanguageVersion)*
        ) ! processLogger
      catch {
        case scala.util.control.NonFatal(cause) =>
          scriptError(cmd, testSuite, stdout.result(), stderr.result(), cause.getMessage)
      }
    if (ignoreExitCode || exitCode == 0)
      (stdout.result(), stderr.result())
    else
      scriptError(cmd, testSuite, stdout.result(), stderr.result(), s"exitCode = $exitCode")
  }
}
