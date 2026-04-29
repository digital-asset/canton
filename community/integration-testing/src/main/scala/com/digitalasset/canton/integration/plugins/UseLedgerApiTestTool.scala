// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.daml.ledger.api.testtool.CliParser
import com.daml.ledger.api.testtool.runner.{AvailableTests, Config, ConfiguredTests, TestRunner}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{
  CantonConfig,
  ClientConfig,
  NonNegativeFiniteDuration as NonNegativeFiniteDurationConfig,
}
import com.digitalasset.canton.console.{LocalParticipantReference, RemoteParticipantReference}
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.{
  EnvVarTestOverrides,
  LAPITTVersion,
  LedgerTestTool,
}
import com.digitalasset.canton.integration.util.ExternalCommandExecutor
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentSetupPlugin,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.platform.apiserver.SeedService.Seeding
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.daml.lf.language.LanguageVersion
import monocle.macros.syntax.lens.*
import org.scalatest.Assertions
import org.scalatest.concurrent.ScalaFutures.*
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.blocking
import scala.util.{Failure, Success, Try}

/** Plugin to provide the LedgerApiTestTool to a
  * [[com.digitalasset.canton.integration.BaseIntegrationTest]] instance for
  *   - invoking ledger api test tool in an external java process
  *   - configuring canton with config settings required for conformance tests
  */
class UseLedgerApiTestTool(
    protected val loggerFactory: NamedLoggerFactory,
    connectedSynchronizersCount: Int,
    lfVersion: LanguageVersion = LanguageVersion.v2_2,
    // If set, unique benchmark name for uploading benchmark results to datadog.
    benchmarkReportFileO: Option[String] = None,
    version: LAPITTVersion = LAPITTVersion.Latest,
    javaOpts: String = "-Xmx500m",
    defaultExtraArguments: Map[String, String] = Map("--timeout-scale-factor" -> "4"),
) extends EnvironmentSetupPlugin
    with NoTracing
    with EnvVarTestOverrides {

  protected val ledgerApiTestToolPatience: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(500, Seconds)))

  private def defaultExtraArgumentsSeq: Seq[String] = defaultExtraArguments.flatMap { case (k, v) =>
    Seq(k, v)
  }.toSeq

  require(
    benchmarkReportFileO.forall(_.startsWith("benchmark_")),
    s"Benchmark report file must start with 'benchmark_', otherwise it won't be reported to DataDog. Found: $benchmarkReportFileO",
  )

  private var testTool: LedgerTestTool = _

  private val tempDir = File.newTemporaryDirectory()

  private val commandExecutor = new ExternalCommandExecutor(loggerFactory)

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    // First ensure we are able to find and invoke java as that is needed to invoke the test tool.
    commandExecutor.exec(cmd = "java --version", errorHint = "Is 'java' not on the path?")

    def tryDownload(testToolRelease: LAPITTRelease): LedgerTestTool.Assembly = {
      val otherLfVersions =
        if (LanguageVersion.stableLfVersions.contains(lfVersion))
          LanguageVersion.stableLfVersions.takeWhile(_ < lfVersion)
        else List.empty

      // find and download test tool with the higher stable LF version
      otherLfVersions
        .foldRight(getOrDownloadTestTool(testToolRelease, lfVersion)) { (otherLfVersion, res) =>
          res.recoverWith {
            case error if isMissingArtifact(error) =>
              logger.info(s"unable to load TestTool $lfVersion trying substitute")
              getOrDownloadTestTool(testToolRelease, otherLfVersion).orElse(Failure(error))
          }
        }
        .fold(throw _, identity)
    }

    testTool = version match {
      case LAPITTVersion.Local => LedgerTestTool.Local(AvailableTests(lfVersion))
      case LAPITTVersion.Latest => tryDownload(UseLedgerApiTestTool.latestRelease(logger))
      case LAPITTVersion.Explicit(release) => tryDownload(release)
    }

    // ensure we use production seeding setting in ledger api conformance and performance tests
    (ConfigTransforms.updateContractIdSeeding(Seeding.Weak) andThen
      // static time tests require this
      (_.focus(_.monitoring.logging.delayLoggingThreshold)
        .replace(NonNegativeFiniteDurationConfig.ofSeconds(1000))))(config)
  }

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
    // clear dars in temp dir
    tempDir.clear()

  def runSuites(
      suites: String, // comma-separated list of suites
      exclude: Seq[String],
      concurrency: Int,
      kv: (String, String)*
  )(implicit env: TestConsoleEnvironment): Unit = {
    val excludeParameter = NonEmpty.from(exclude) match {
      case Some(suitesNE) => Seq("--exclude", suitesNE.mkString(","))
      case None => Nil
    }

    val additionalParameters = (defaultExtraArguments ++ kv).flatMap { case (k, v) => Seq(k, v) }

    runTestsInternal(
      concurrentTestRuns = concurrency,
      connectedSynchronizersCount = connectedSynchronizersCount,
      testInclusions = suites.split(",").toSeq,
      extraArgs = additionalParameters.toSeq ++ excludeParameter,
      testParticipants = testParticipants(useJson = false),
      useJson = false,
    )
  }

  def runSuitesSerially(
      suites: String, // comma-separated list of suites
      exclude: Seq[String],
      kv: (String, String)*
  )(implicit env: TestConsoleEnvironment): Unit =
    runSuites(suites = suites, exclude = exclude, concurrency = 1, kv*)

  def runShardedSuites(
      shard: Int,
      numShards: Int,
      exclude: Seq[String],
      concurrentTestRuns: Int = 4,
      useJson: Boolean,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    val allTests: Seq[String] = testTool match {
      case LedgerTestTool.Assembly(assemblyJar) =>
        execTestTool(assemblyJar, Array("--list-all"))
          .split("\n")
          .toSeq
          .filter(_.contains(":"))
          .map(_.trim)
      case LedgerTestTool.Local(tests) => ConfiguredTests(tests, Config.default).allTestNames
    }
    val filteredTests = allTests
      .filter(line => exclude.forall(not => !line.contains(not)))
      .zipWithIndex
      .collect { case (test, idx) if idx % numShards == shard => test }

    runTestsInternal(
      concurrentTestRuns = concurrentTestRuns,
      connectedSynchronizersCount = connectedSynchronizersCount,
      testInclusions = filteredTests,
      extraArgs = defaultExtraArgumentsSeq,
      testParticipants = testParticipants(useJson),
      useJson = useJson,
    )
  }

  @SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
  private def getOrDownloadTestTool(
      release: LAPITTRelease,
      lfVersion: LanguageVersion,
  ): Try[LedgerTestTool.Assembly] = {
    val testToolName: String = s"ledger-api-test-tool-$lfVersion"
    val filename = s"$testToolName-${release.version}.jar"
    val destination =
      File(System.getProperty("user.home")) / ".cache" / testToolName / filename
    // Check if test tool resides in destination. If not, download test tool.
    blocking(this.synchronized {
      if (!destination.exists) {
        logger.info(s"Downloading $filename from S3.")
        destination.parent.createDirectoryIfNotExists(createParents = true)
        LAPITTResolver.download(release, lfVersion, destination, logger)
      } else Success(())
    }).map(_ => LedgerTestTool.Assembly(destination))
  }

  private def isMissingArtifact(error: Throwable): Boolean =
    Option(error.getMessage).exists { msg =>
      val lower = msg.toLowerCase
      msg.contains("404") || lower.contains("not found")
    }

  private def runTestsInternal(
      concurrentTestRuns: Int,
      connectedSynchronizersCount: Int,
      testInclusions: Seq[String],
      extraArgs: Seq[String],
      testParticipants: Seq[String],
      useJson: Boolean,
  ): Unit = {
    val testInclusionsAfterEnvArgConsideration = envArgTestsInclusion
      .map { selectedTests =>
        val filtered = testInclusions.filter(selectedTests.testCaseEnabled)
        if (filtered.isEmpty) {
          // Fine to use the scalatest cancel here as this method is expected to be invoked from
          // from a ScalaTest case.
          org.scalatest.Assertions.cancel(
            s"After applying the restriction from the env var $LapittRunOnlyEnvVarName no tests remain to be run. " +
              s"Original test selection: $testInclusions. Restriction applied: $selectedTests."
          )
        }

        filtered
      }
      .getOrElse(testInclusions)
    val jsonOpt = if (useJson) Seq("--json-api-mode") else Seq.empty
    val args = Array(
      "--concurrent-test-runs",
      concurrentTestRuns.toString,
      "--connected-synchronizers",
      connectedSynchronizersCount.toString,
      "-v",
      "--include",
      testInclusionsAfterEnvArgConsideration.mkString(","),
    ) ++ jsonOpt ++ extraArgs ++ testParticipants

    testTool match {
      case LedgerTestTool.Assembly(assemblyJar) => execTestTool(assemblyJar, args)
      case LedgerTestTool.Local(tests) =>
        val config = CliParser.parse(args).getOrElse(sys.error("Invalid config"))
        val runner = new TestRunner(tests, config)
        val failures = runner
          .runInProcess(logger.underlying)
          .futureValue(config = ledgerApiTestToolPatience, pos = implicitly)
          .map(test => test.result.left.map(failure => s"${test.name} failed: $failure"))
          .collect { case Left(failure) => failure }
        if (failures.nonEmpty)
          Assertions.fail(
            s"Some Ledger API tests have failed: ${failures.mkString("\n\t", "\n\t", "")}"
          )
    }
  }

  private def execTestTool(assemblyJar: File, args: Array[String]): String =
    commandExecutor.exec(
      cmd = s"java $javaOpts -jar ${assemblyJar.toString} ${args.mkString(" ")}",
      errorHint = s"Failures in aforementioned test suite.",
    )

  private def endpointAsString(config: ClientConfig) = s"${config.address}:${config.port.toString}"

  private def testParticipants(
      useJson: Boolean
  )(implicit env: TestConsoleEnvironment): Seq[String] =
    env.participants.all
      .map { p =>
        val ledgerApiEndpoint = p match {
          case remote: RemoteParticipantReference if useJson =>
            remote.config.ledgerJsonApi
              .map(_.endpointAsString)
              .getOrElse(throw new IllegalArgumentException(s"invalid remote reference: $remote"))
          case local: LocalParticipantReference if useJson =>
            local.config.httpLedgerApi.clientConfig
              .map(_.endpointAsString)
              .getOrElse(throw new IllegalArgumentException(s"invalid local reference: $local"))
          case _ => endpointAsString(p.config.clientLedgerApi)
        }
        val adminApiEndpoint = endpointAsString(p.config.clientAdminApi)
        s"$ledgerApiEndpoint;$adminApiEndpoint"
      }

}

object UseLedgerApiTestTool {
  sealed trait TestInclusions extends Product with Serializable {
    def testCaseEnabled(testCaseName: String): Boolean
  }

  object TestInclusions {
    case object AllIncluded extends TestInclusions {
      def testCaseEnabled(testCaseName: String): Boolean = true
    }

    /** @param includedSuites
      *   Full suites to include
      * @param includedTestCases
      *   Specific test cases to include. Adding individual test cases here is redundant if their
      *   suite is already included in [[includedSuites]].
      */
    final case class SelectedTests(
        includedSuites: Set[String],
        includedTestCases: Set[String] = Set.empty,
    ) extends TestInclusions {
      def testCaseEnabled(testCaseName: String): Boolean =
        testCaseName.split(":").map(_.trim).toSeq match {
          case Seq(suite, _) =>
            includedSuites.contains(suite) || includedTestCases.contains(testCaseName)
          case Seq(suite) => includedSuites.contains(suite)
          case _other =>
            throw new IllegalArgumentException(
              s"Invalid test case name: $testCaseName. Expected format: SuiteName:TestCaseName"
            )
        }
    }
  }

  trait EnvVarTestOverrides {
    this: NamedLogging =>

    protected val LapittRunOnlyEnvVarName = "LAPI_CONFORMANCE_TEST_RUN_ONLY"

    /** Set the environment variable `LAPI_CONFORMANCE_TEST_RUN_ONLY` to a comma-separated list of
      * test suite names or test case names to restrict the tests being run as part of a specific
      * conformance test suite target.
      *
      * e.g. LAPI_CONFORMANCE_TEST_RUN_ONLY=CommandServiceIT sbt "testOnly
      * *JsonApiConformanceIntegrationShardedTest_Shard_0"
      */
    protected lazy val envTestFilterO: Option[Seq[String]] =
      sys.env.get(LapittRunOnlyEnvVarName).map(_.split(",").view.map(_.trim).toSeq)

    // Implementors of this trait should use this value to filter the tests being run
    // with the restriction provided via the env var.
    protected lazy val envArgTestsInclusion: Option[TestInclusions.SelectedTests] = envTestFilterO
      .flatMap { envTestFilter =>
        val selectedTestsO = envTestFilter
          .map(_.split(":").toSeq match {
            case Seq(suite, test) =>
              TestInclusions
                .SelectedTests(includedSuites = Set.empty, includedTestCases = Set(s"$suite:$test"))
            case Seq(suite) => TestInclusions.SelectedTests(includedSuites = Set(suite))
            case _ => throw new IllegalArgumentException(s"Invalid test filter: $envTestFilter")
          })
          .reduceOption((s1, s2) =>
            TestInclusions.SelectedTests(
              includedSuites = s1.includedSuites ++ s2.includedSuites,
              includedTestCases = s1.includedTestCases ++ s2.includedTestCases,
            )
          )

        selectedTestsO.foreach(selectedTests =>
          logger.debug(
            s"$LapittRunOnlyEnvVarName set to $envTestFilterO. Filtering current test selection in ${getClass.getSimpleName} using restriction $selectedTests."
          )(TraceContext.empty)
        )

        selectedTestsO
      }
  }

  sealed trait LAPITTVersion

  object LAPITTVersion {
    // Run the latest released version of the LAPITT.
    case object Latest extends LAPITTVersion

    // Run the specified version of the LAPITT.
    final case class Explicit(version: LAPITTRelease) extends LAPITTVersion

    // Run the LAPITT from the classpath.
    // Requires running `sbt ledger-test-tool/assembly` first
    case object Local extends LAPITTVersion
  }

  private sealed trait LedgerTestTool

  private object LedgerTestTool {
    final case class Assembly(assemblyJar: File) extends LedgerTestTool
    final case class Local(tests: AvailableTests) extends LedgerTestTool
  }

  // finds all major.minor.patch releases
  def findAllCoreVersions(toolReleases: Seq[LAPITTRelease]): Seq[String] =
    toolReleases.flatMap(_.baseVersion).distinct.sorted

  // finds version of the release given and sorts them by the date produced
  def findMatchingVersions(
      toolReleases: Seq[LAPITTRelease],
      baseVersion: String,
  ): Seq[LAPITTRelease] =
    toolReleases
      .filter(_.baseVersion.contains(baseVersion))
      .sortBy(_.releaseDate)

  def latestRelease(logger: TracedLogger)(implicit tc: TraceContext): LAPITTRelease = {
    val toolReleases: Seq[LAPITTRelease] = LAPITTResolver.listAllReleases()
    val latestCoreVersion = findAllCoreVersions(toolReleases).lastOption.getOrElse(
      throw new RuntimeException(
        s"No releases found among the following versions: ${toolReleases.map(_.version)}"
      )
    )

    val matchingVersions = findMatchingVersions(toolReleases, latestCoreVersion)
    val matchingVersion = matchingVersions.lastOption.getOrElse(
      throw new RuntimeException(s"No matching version found for release $latestCoreVersion")
    )
    logger.debug(s"found ${matchingVersion.version} as latest version of $latestCoreVersion")

    matchingVersion
  }

  def latestReleases(logger: TracedLogger)(implicit tc: TraceContext): Seq[LAPITTRelease] = {
    val toolVersions = LAPITTResolver.listAllReleases()

    val coreVersions = findAllCoreVersions(toolVersions)
    val latestReleases = coreVersions.flatMap(findMatchingVersions(toolVersions, _).lastOption)
    logger.debug(s"found $latestReleases as latest versions for each release")

    latestReleases
  }
}
