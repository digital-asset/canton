// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import better.files.File
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config.{
  CantonConfig,
  ClientConfig,
  NonNegativeFiniteDuration as NonNegativeFiniteDurationConfig,
}
import com.digitalasset.canton.console.{LocalParticipantReference, RemoteParticipantReference}
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.{
  EnvVarTestOverrides,
  LAPITTVersion,
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
import monocle.macros.syntax.lens.*

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
    lfVersion: UseLedgerApiTestTool.LfVersion = UseLedgerApiTestTool.LfVersion.Stable,
    // If set, unique benchmark name for uploading benchmark results to datadog.
    benchmarkReportFileO: Option[String] = None,
    version: LAPITTVersion = LAPITTVersion.Latest,
    javaOpts: String = "-Xmx500m",
    defaultExtraArguments: Map[String, String] = Map("--timeout-scale-factor" -> "4"),
) extends EnvironmentSetupPlugin
    with NoTracing
    with EnvVarTestOverrides {

  private def defaultExtraArgumentsSeq: Seq[String] = defaultExtraArguments.flatMap { case (k, v) =>
    Seq(k, v)
  }.toSeq

  require(
    benchmarkReportFileO.forall(_.startsWith("benchmark_")),
    s"Benchmark report file must start with 'benchmark_', otherwise it won't be reported to DataDog. Found: $benchmarkReportFileO",
  )

  private var testTool: File = _

  private val tempDir = File.newTemporaryDirectory()

  private val commandExecutor = new ExternalCommandExecutor(loggerFactory)

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig = {
    // First ensure we are able to find and invoke java as that is needed to invoke the test tool.
    commandExecutor.exec(cmd = "java --version", errorHint = "Is 'java' not on the path?")

    def tryDownload(testToolRelease: LAPITTRelease): File =
      getOrDownloadTestTool(testToolRelease, lfVersion)
        .recoverWith {
          case error if isMissingArtifact(error) =>
            // TODO (i31441) substitute is a temporary solution, fix publishing of testtools
            // TestTools 2.1 are missing in 3.4 line hence substitution with 2.2
            // We might want to republish test tools for 2.1
            logger.info(s"unable to load TestTool $lfVersion trying substitute")
            getOrDownloadTestTool(testToolRelease, UseLedgerApiTestTool.LfVersion.V22).orElse(
              Failure(error)
            )
        }
        .fold(throw _, identity)

    testTool = version match {
      case LAPITTVersion.LocalJar =>
        val repoDir = File(System.getProperty("user.dir"))
        val projectDir =
          repoDir / s"community/ledger-test-tool/tool/lf-v${lfVersion.testToolSuffix.tail}"
        projectDir / s"target/scala-2.13/ledger-api-test-tool${lfVersion.testToolSuffix}-${BuildInfo.version}.jar"
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
  )(implicit env: TestConsoleEnvironment): String = {
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
  )(implicit env: TestConsoleEnvironment): String =
    runSuites(suites = suites, exclude = exclude, concurrency = 1, kv*)

  def runShardedSuites(
      shard: Int,
      numShards: Int,
      exclude: Seq[String],
      concurrentTestRuns: Int = 4,
      useJson: Boolean,
  )(implicit
      env: TestConsoleEnvironment
  ): String = {
    val allTests = execTestTool("--list-all").split("\n")
    val listing = allTests
      .filter(line => exclude.forall(not => !line.contains(not)))
      .filter(_.contains(":"))
      .map(_.trim)
      .zipWithIndex
      .filter { case (_, idx) =>
        idx % numShards == shard
      }
      .map(_._1)

    runTestsInternal(
      concurrentTestRuns = concurrentTestRuns,
      connectedSynchronizersCount = connectedSynchronizersCount,
      testInclusions = listing.toSeq,
      extraArgs = defaultExtraArgumentsSeq,
      testParticipants = testParticipants(useJson),
      useJson = useJson,
    )
  }

  @SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
  private def getOrDownloadTestTool(
      release: LAPITTRelease,
      lfVersion: UseLedgerApiTestTool.LfVersion,
  ): Try[File] = {
    val testToolName: String = s"ledger-api-test-tool${lfVersion.testToolSuffix}"
    val filename = s"$testToolName-${release.version}.jar"
    val destination =
      File(System.getProperty("user.home")) / ".cache" / testToolName / filename
    // Check if test tool resides in destination. If not, download test tool.
    blocking(this.synchronized {
      if (!destination.exists) {
        logger.info(s"Downloading $filename from ${release.repositoryName}.")
        destination.parent.createDirectoryIfNotExists(createParents = true)
        release.download(lfVersion, destination, logger).map(_ => destination)
      } else Success(destination)
    })
  }

  private def isMissingArtifact(error: Throwable): Boolean =
    Option(error.getMessage)
      .exists { msg =>
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
  ): String = {
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
    execTestTool(
      Seq(
        "--concurrent-test-runs",
        concurrentTestRuns.toString,
        "--connected-synchronizers",
        connectedSynchronizersCount.toString,
        "-v",
        "--include",
        testInclusionsAfterEnvArgConsideration.mkString(","),
      ) ++ jsonOpt ++ extraArgs ++ testParticipants: _*
    )
  }

  private def execTestTool(option: String*): String =
    commandExecutor.exec(
      cmd = s"java $javaOpts -jar ${testTool.toString} ${option.mkString(" ")}",
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

  sealed trait LfVersion {
    def testToolSuffix: String
  }

  object LfVersion {
    case object Stable extends LfVersion {
      override def testToolSuffix: String = "-2.1"
    }

    case object V22 extends LfVersion {
      override def testToolSuffix: String = "-2.2"
    }

    case object V23 extends LfVersion {
      override def testToolSuffix: String = "-2.3"
    }

    case object Dev extends LfVersion {
      override def testToolSuffix: String = "-2.dev"
    }
  }

  sealed trait LAPITTVersion

  object LAPITTVersion {
    // The lapitt runs only for the latest version of latest release.
    case object Latest extends LAPITTVersion

    // The lapitt runs only for the specified version.
    final case class Explicit(version: LAPITTRelease) extends LAPITTVersion

    // The lapitt runs with the local lapitt jar.
    // Requires running `sbt ledger-test-tool-<lfVersion>/assembly` first
    case object LocalJar extends LAPITTVersion
  }

  private val allResolvers: Seq[LAPITTResolver] = Seq(LAPITTArtifactoryResolver, LAPITTS3Resolver)

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

  def latestRelease(logger: TracedLogger, resolvers: Seq[LAPITTResolver] = allResolvers)(implicit
      tc: TraceContext
  ): LAPITTRelease = {
    val toolReleases: Seq[LAPITTRelease] = resolvers.flatMap(_.listAllReleases())
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

  def latestReleases(logger: TracedLogger, resolvers: Seq[LAPITTResolver] = allResolvers)(implicit
      tc: TraceContext
  ): Seq[LAPITTRelease] = {
    val toolVersions = resolvers.flatMap(_.listAllReleases())

    val coreVersions = findAllCoreVersions(toolVersions)
    val latestReleases = coreVersions.flatMap(findMatchingVersions(toolVersions, _).lastOption)
    logger.debug(s"found $latestReleases as latest versions for each release")

    latestReleases
  }
}
