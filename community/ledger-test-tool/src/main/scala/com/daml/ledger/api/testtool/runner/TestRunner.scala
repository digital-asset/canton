// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import com.daml.ledger.api.testtool.TestDar
import com.daml.ledger.api.testtool.infrastructure.*
import com.daml.ledger.api.testtool.runner.TestRunner.*
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.tls.TlsClientConfig
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import io.grpc.Channel
import io.grpc.netty.shaded.io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.concurrent.Executors
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object TestRunner {

  private type ResourceOwner[T] = com.daml.resources.AbstractResourceOwner[ExecutionContext, T]
  private type Resource[T] = com.daml.resources.Resource[ExecutionContext, T]
  private val Resource = new com.daml.resources.ResourceFactories[ExecutionContext]

  private val logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  // The suffix that will be appended to all party and command identifiers to ensure
  // they are unique across test runs (but still somewhat stable within a single test run)
  // This implementation could fail based on the limitations of System.nanoTime, that you
  // can read on here: https://docs.oracle.com/javase/8/docs/api/java/lang/System.html#nanoTime--
  // Still, the only way in which this can fail is if two test runs target the same ledger
  // with the identifier suffix being computed to the same value, which at the very least
  // requires this to happen on what is resolved by the JVM as the very same millisecond.
  // This is very unlikely to fail and allows to easily "date" parties on a ledger used
  // for testing and compare data related to subsequent runs without any reference
  private val identifierSuffix = f"${System.nanoTime}%x"

  private val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION ON MAIN THREAD, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private def exitCode(summaries: Vector[LedgerTestSummary], expectFailure: Boolean): Int =
    if (summaries.exists(_.result.isLeft) == expectFailure) 0 else 1

  private def extractResources(resources: Seq[TestDar]): Unit = {
    val pwd = Paths.get(".").toAbsolutePath
    println(s"Extracting all Daml resources necessary to run the tests into $pwd.")
    for (resource <- resources) {
      val is = getClass.getClassLoader.getResourceAsStream(resource.path)
      if (is == null) sys.error(s"Could not find $resource in classpath")
      val targetFile = new File(new File(resource.path).getName)
      Files.copy(is, targetFile.toPath, StandardCopyOption.REPLACE_EXISTING)
      println(s"Extracted $resource to $targetFile")
    }
  }

  private def matches(prefixes: Iterable[String])(test: LedgerTestCase): Boolean =
    prefixes.exists(test.name.startsWith)
}

final class TestRunner(availableTests: AvailableTests, config: Config) {
  private val configuredTests = new ConfiguredTests(availableTests, config)

  private implicit val resourceManagementExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  def runAndExit(): Unit = {
    val (result, excludedTests) = runInternal(System.out.println)
    result.onComplete {
      case Success(summaries) =>
        sys.exit(exitCode(summaries, config.mustFail))
      case Failure(exception: Errors.FrameworkException) =>
        logger.error(exception.getMessage)
        logger.debug(exception.getMessage, exception)
        sys.exit(1)
      case Failure(exception) =>
        logger.error(exception.getMessage, exception)
        sys.exit(1)

    }
  }

  def runInProcess(logger: Logger): Future[Vector[LedgerTestSummary]] = {
    val reportLogBuilder = new mutable.StringBuilder()
    reportLogBuilder.addOne('\n')
    val addReportLineToLogEntry: String => Unit = reportLogBuilder.addAll(_).addOne('\n')

    val res = runInternal(reporterPrintln = addReportLineToLogEntry)
    // Log the full report at info level
    logger.info(reportLogBuilder.result())
    res._1
  }

  private def runInternal(
      reporterPrintln: String => Unit
  ): (Future[Vector[LedgerTestSummary]], Vector[LedgerTestCase]) = {

    if (configuredTests.missingTests.nonEmpty) {
      println("The following exclusion or inclusion does not match any test:")
      configuredTests.missingTests.foreach { testName =>
        println(s"  - $testName")
      }
      sys.exit(64)
    }

    if (config.listTestSuites) {
      printAvailableTestSuites()
      sys.exit(0)
    }

    if (config.listTests) {
      printAvailableTests()
      sys.exit(0)
    }

    if (config.extract) {
      extractResources(availableTests.darsToUpload)
      sys.exit(0)
    }

    if (config.participantsEndpoints.isEmpty) {
      logger.error("No participant to test, exiting.")
      sys.exit(1)
    }

    Thread
      .currentThread()
      .setUncaughtExceptionHandler { (_, exception) =>
        logger.error(uncaughtExceptionErrorMessage, exception)
        sys.exit(1)
      }

    val includedTests =
      if (config.included.isEmpty) configuredTests.defaultCases
      else configuredTests.allCases.filter(matches(config.included))

    val addedTests = configuredTests.allCases.filter(matches(config.additional))

    val (excludedTests, testsToRun) =
      (includedTests ++ addedTests).partition(matches(config.excluded))

    val runner = newLedgerCasesRunner(testsToRun)
    val testsF = runner.asFuture
      .flatMap(
        _.runTests(
          Threading.newExecutionContext(
            "TestRunner",
            com.typesafe.scalalogging.Logger(logger),
            new ExecutorServiceMetrics(NoOpMetricsFactory),
          )
        )
          .transformWith {
            case scala.util.Success(summaries) => runner.release().map(_ => summaries)
            case scala.util.Failure(error) => runner.release().flatMap(_ => Future.failed(error))
          }
      )
      .map { summaries =>
        val excludedTestSummaries =
          excludedTests.map { ledgerTestCase =>
            LedgerTestSummary(
              suite = ledgerTestCase.suite.name,
              name = ledgerTestCase.name,
              description = ledgerTestCase.description,
              result = Right(Result.Excluded("excluded test")),
            )
          }
        new Reporter.ColorizedPrintStreamReporter(
          reporterPrintln,
          config.verbose,
          config.reportOnFailuresOnly,
        ).report(
          summaries,
          excludedTestSummaries,
          Seq(
            "identifierSuffix" -> identifierSuffix,
            "concurrentTestRuns" -> config.concurrentTestRuns.toString,
            "timeoutScaleFactor" -> config.timeoutScaleFactor.toString,
          ),
        )
        summaries
      }

    (testsF, excludedTests)
  }

  private def printAvailableTests(): Unit = {
    println("Listing all tests. Run with --list to only see test suites.")
    println("All tests are run by default.")
    println()
    configuredTests.allTestNames.foreach(println(_))
  }

  private def printAvailableTestSuites(): Unit = {
    println("Listing test suites. Run with --list-all to see individual tests.")
    println("All tests are run by default.")
    println()
    configuredTests.allSuiteNames.foreach(println(_))
  }

  private def newLedgerCasesRunner(
      cases: Vector[LedgerTestCase]
  ): Resource[LedgerTestCasesRunner] =
    if (config.jsonApiMode) {
      initializeParticipantChannels(
        participantEndpoints = config.participantsAdminEndpoints,
        tlsConfig = config.tlsConfig,
      ).map(adminChannels =>
        new LedgerTestCasesRunner(
          testCases = cases,
          participantChannels = Left(config.participantsEndpoints),
          participantAdminChannels = adminChannels,
          maxConnectionAttempts = config.maxConnectionAttempts,
          skipDarNamesPattern = config.skipDarNamesPattern,
          partyAllocation = config.partyAllocation,
          shuffleParticipants = config.shuffleParticipants,
          timeoutScaleFactor = config.timeoutScaleFactor,
          concurrentTestRuns = config.concurrentTestRuns,
          identifierSuffix = identifierSuffix,
          allDars = availableTests.darsToUpload,
          connectedSynchronizers = config.connectedSynchronizers,
        )
      )
    } else {
      for {
        lapiChannels <- initializeParticipantChannels(
          participantEndpoints = config.participantsEndpoints,
          tlsConfig = config.tlsConfig,
        )
        adminChannels <- initializeParticipantChannels(
          participantEndpoints = config.participantsAdminEndpoints,
          tlsConfig = config.tlsConfig,
        )
      } yield {
        new LedgerTestCasesRunner(
          testCases = cases,
          participantChannels = Right(lapiChannels),
          participantAdminChannels = adminChannels,
          maxConnectionAttempts = config.maxConnectionAttempts,
          skipDarNamesPattern = config.skipDarNamesPattern,
          partyAllocation = config.partyAllocation,
          shuffleParticipants = config.shuffleParticipants,
          timeoutScaleFactor = config.timeoutScaleFactor,
          concurrentTestRuns = config.concurrentTestRuns,
          identifierSuffix = identifierSuffix,
          allDars = availableTests.darsToUpload,
          connectedSynchronizers = config.connectedSynchronizers,
        )
      }
    }

  private def initializeParticipantChannel(
      host: String,
      port: Int,
      tlsConfig: Option[TlsClientConfig],
  ): ResourceOwner[Channel] = {
    logger.info(s"Setting up managed channel to participant at $host:$port...")
    val channelBuilder = NettyChannelBuilder.forAddress(host, port).usePlaintext()
    for (ssl <- tlsConfig; sslContext = ClientChannelBuilder.sslContext(ssl)) {
      logger.info("Setting up managed channel with transport security.")
      channelBuilder
        .useTransportSecurity()
        .sslContext(sslContext)
        .negotiationType(NegotiationType.TLS)
    }
    channelBuilder.maxInboundMessageSize(10000000)
    ResourceOwner.forChannel(channelBuilder, shutdownTimeout = 5.seconds)
  }

  private def initializeParticipantChannels(
      participantEndpoints: Vector[(String, Int)],
      tlsConfig: Option[TlsClientConfig],
  ): Resource[Vector[ChannelEndpoint]] =
    Resource.sequence(participantEndpoints.map { case (host, port) =>
      initializeParticipantChannel(host, port, tlsConfig)
        .acquire()
        .map(channel => ChannelEndpoint.forRemote(channel = channel, hostname = host, port = port))
    })
}
