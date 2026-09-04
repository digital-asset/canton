// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.testtool.TestDar
import com.daml.ledger.api.testtool.infrastructure.ChannelEndpoint.JsonApiEndpoint
import com.daml.ledger.api.testtool.infrastructure.LedgerTestCasesRunner.*
import com.daml.ledger.api.testtool.infrastructure.PartyAllocationConfiguration.ClosedWorldWaitingForAllParticipants
import com.daml.ledger.api.testtool.infrastructure.participant.{
  ParticipantSession,
  ParticipantTestContext,
}
import com.digitalasset.canton.util.MonadUtil
import io.grpc.ClientInterceptor
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.slf4j.LoggerFactory

import java.util.concurrent.{ExecutionException, TimeoutException}
import java.util.{Timer, TimerTask}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.util.control.NonFatal
import scala.util.matching.Regex

object LedgerTestCasesRunner {
  private[testtool] val DefaultTimeout = 30.seconds

  /** A failing `Eventually` can use all of its retries before reporting the useful error. The case
    * timeout needs enough headroom to avoid firing first and only reporting `TimedOut`.
    *
    * Allow for two loops because `waitForPartiesOnOtherParticipants` runs them back to back. Add
    * the headroom after scaling since the `Eventually` schedule itself is not scaled.
    */
  private[testtool] val EventuallyHeadroom = Eventually.DefaultMaxDeadline * 2

  private[testtool] def caseTimeout(
      timeoutScaleFactor: Double,
      testTimeoutScale: Double,
  ): Duration =
    DefaultTimeout * timeoutScaleFactor * testTimeoutScale + EventuallyHeadroom

  private val timer = new Timer("ledger-test-suite-runner-timer", true)

  private val logger = LoggerFactory.getLogger(classOf[LedgerTestCasesRunner])

  private[this] val uncaughtExceptionErrorMessage =
    "UNEXPECTED UNCAUGHT EXCEPTION, GATHER THE STACKTRACE AND OPEN A _DETAILED_ TICKET DESCRIBING THE ISSUE HERE: https://github.com/digital-asset/daml/issues/new"

  private final class UncaughtExceptionError(cause: Throwable)
      extends RuntimeException(uncaughtExceptionErrorMessage, cause)

}

final class LedgerTestCasesRunner(
    testCases: Vector[LedgerTestCase],
    participantChannels: Either[Vector[JsonApiEndpoint], Vector[ChannelEndpoint]],
    participantAdminChannels: Vector[ChannelEndpoint],
    skipDarNamesPattern: Option[Regex],
    maxConnectionAttempts: Int = 10,
    partyAllocation: PartyAllocationConfiguration = ClosedWorldWaitingForAllParticipants,
    shuffleParticipants: Boolean = false,
    timeoutScaleFactor: Double = 1.0,
    concurrentTestRuns: Int = 8,
    identifierSuffix: String = "test",
    commandInterceptors: Seq[ClientInterceptor] = Seq.empty,
    allDars: List[TestDar],
    connectedSynchronizers: Int,
) {
  private[this] val verifyRequirements: Try[Unit] =
    Try {
      require(
        maxConnectionAttempts > 0,
        "The number of connection attempts must be strictly positive",
      )
      require(timeoutScaleFactor > 0, "The timeout scale factor must be strictly positive")
      require(identifierSuffix.nonEmpty, "The identifier suffix cannot be an empty string")
    }

  def runTests(implicit executionContext: ExecutionContext): Future[Vector[LedgerTestSummary]] =
    verifyRequirements.fold(
      Future.failed,
      _ => prepareResourcesAndRun,
    )

  private def createTestContextAndStart(
      test: LedgerTestCase.Repetition,
      session: LedgerSession,
  )(implicit executionContext: ExecutionContext): Future[Duration] = {
    val execution = Promise[Duration]()
    val timeout = caseTimeout(timeoutScaleFactor, test.timeoutScale)

    val testName =
      test.repetition.fold[String](test.shortIdentifier)(r => s"${test.shortIdentifier}_${r._1}")
    val qualifiedTestName =
      test.repetition.fold(test.testCase.name)(r => s"${test.testCase.name}_${r._1}")
    val startedTest =
      session
        .createTestContext(testName, identifierSuffix)
        .flatMap { context =>
          val start = System.nanoTime()
          val result = test
            .allocatePartiesAndRun(context)
            .map(_ => Duration.fromNanos(System.nanoTime() - start))
          logger.info(
            s"Started '$qualifiedTestName' (${test.description})${test.repetition
                .fold("")(r => s" (${r._1}/${r._2})")} with a timeout of $timeout."
          )
          result
        }

    val testTimeout = new TimerTask {
      override def run(): Unit = {
        val message =
          s"Timeout of $timeout for '$qualifiedTestName' (${test.description}) hit."
        if (execution.tryFailure(new TimeoutException(message))) {
          logger.error(message)
        }
      }
    }
    timer.schedule(testTimeout, timeout.toMillis)
    startedTest.onComplete { _ =>
      testTimeout.cancel()
      logger.info(s"Finished '$qualifiedTestName' (${test.description}).")
    }
    execution.completeWith(startedTest).future
  }

  private def result(
      startedTest: Future[Duration]
  )(implicit executionContext: ExecutionContext): Future[Either[Result.Failure, Result.Success]] =
    startedTest
      .map[Either[Result.Failure, Result.Success]](duration => Right(Result.Succeeded(duration)))
      .recover[Either[Result.Failure, Result.Success]] {
        case Result.Retired =>
          Right(Result.Retired)
        case Result.Excluded(reason) =>
          Right(Result.Excluded(reason))
        case timeout: TimeoutException =>
          Left(Result.TimedOut(timeout.getMessage))
        case failure: AssertionError =>
          Left(Result.Failed(failure))
        case NonFatal(box: ExecutionException) =>
          box.getCause match {
            case failure: AssertionError =>
              Left(Result.Failed(failure))
            case exception =>
              Left(Result.FailedUnexpectedly(exception))
          }
        case NonFatal(exception) =>
          Left(Result.FailedUnexpectedly(exception))
      }

  private def summarize(
      suite: LedgerTestSuite,
      test: LedgerTestCase,
      result: Either[Result.Failure, Result.Success],
  ): LedgerTestSummary =
    LedgerTestSummary(suite.name, test.name, test.description, result)

  private def run(
      test: LedgerTestCase.Repetition,
      session: LedgerSession,
  )(implicit executionContext: ExecutionContext): Future[Either[Result.Failure, Result.Success]] =
    result(createTestContextAndStart(test, session))

  private def uploadDarsIfRequired(
      sessions: Vector[ParticipantSession]
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    val darsToUpload = skipDarNamesPattern
      .map { skipRegex =>
        val darsToUpload = allDars.filterNot(dar => skipRegex.matches(dar.path))
        logger.info(
          s"Uploading DARs excluding pattern ${skipRegex.pattern.toString}: ${darsToUpload
              .mkString("[", ",", "]")}"
        )
        darsToUpload
      }
      .getOrElse {
        logger.info(s"Uploading all available test DARs")
        allDars
      }

    MonadUtil
      .sequentialTraverse(sessions) { session =>
        logger.info(s"Uploading DAR files for session $session")
        for {
          context <- session.createInitContext(
            userId = "upload-dars",
            identifierSuffix = identifierSuffix,
            features = session.features,
          )
          // upload the dars sequentially to avoid conflicts
          _ <- MonadUtil.sequentialTraverse_(darsToUpload)(dar => uploadDar(context, dar))
        } yield ()
      }
      .map(_ => ())
  }

  private def uploadDar(
      context: ParticipantTestContext,
      dar: TestDar,
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    logger.info(s"""Uploading DAR $dar...""")
    context
      .uploadDarFileAndVetOnConnectedSynchronizers(dar.bytes)
      .map(_ => logger.info(s"""Uploaded DAR $dar."""))
      .recover { case NonFatal(exception) => throw new Errors.DarUploadException(dar, exception) }
  }

  private def createActorSystem: ActorSystem =
    ActorSystem(classOf[LedgerTestCasesRunner].getSimpleName)

  private def runTestCases(
      ledgerSession: LedgerSession,
      testCases: Vector[LedgerTestCase],
      concurrency: Int,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Vector[LedgerTestSummary]] = {
    val testCaseRepetitions = testCases.flatMap(_.repetitions)
    val testCount = testCaseRepetitions.size
    logger.info(s"Running $testCount tests with concurrency of $concurrency.")
    Source(testCaseRepetitions.zipWithIndex)
      .mapAsyncUnordered(concurrency) { case (test, index) =>
        run(test, ledgerSession).map(summarize(test.suite, test.testCase, _) -> index)
      }
      .runWith(Sink.seq)
      .map(_.toVector.sortBy(_._2).map(_._1))
  }

  private def run(
      participantChannels: Either[Vector[JsonApiEndpoint], Vector[ChannelEndpoint]],
      participantAdminChannels: Vector[ChannelEndpoint],
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Vector[LedgerTestSummary]] = {
    val sessions: Future[Vector[ParticipantSession]] = ParticipantSession.createSessions(
      partyAllocationConfig = partyAllocation,
      participantChannels = participantChannels,
      participantAdminChannels = participantAdminChannels,
      maxConnectionAttempts = maxConnectionAttempts,
      commandInterceptors = commandInterceptors,
      timeoutScaleFactor = timeoutScaleFactor,
      dars = allDars,
      connectedSynchronizers = connectedSynchronizers,
    )
    sessions
      .flatMap { (sessions: Vector[ParticipantSession]) =>
        // All the participants should support the same features (for testing at least)
        val ledgerFeatures =
          sessions.headOption.getOrElse(sys.error("No participant sessions")).features
        val (disabledTestCases, enabledTestCases) =
          testCases.partitionMap(testCase =>
            testCase
              .isEnabled(ledgerFeatures, sessions.size, connectedSynchronizers)
              .fold(disabledReason => Left(testCase -> disabledReason), _ => Right(testCase))
          )
        val excludedTestResults = disabledTestCases
          .map { case (testCase, disabledReason) =>
            LedgerTestSummary(
              testCase.suite.name,
              testCase.name,
              testCase.description,
              Right(Result.Excluded(disabledReason)),
            )
          }
        val (concurrentTestCases, sequentialTestCases) =
          enabledTestCases.partition(_.runConcurrently)
        val ledgerSession = LedgerSession(
          sessions,
          shuffleParticipants,
          connectedSynchronizers,
        )
        val testResults =
          for {
            _ <- uploadDarsIfRequired(sessions)
            sequentialTestResults <- runTestCases(
              ledgerSession,
              sequentialTestCases,
              concurrency = 1,
            )(materializer, executionContext)
            concurrentTestResults <- runTestCases(
              ledgerSession,
              concurrentTestCases,
              concurrentTestRuns,
            )(materializer, executionContext)
          } yield concurrentTestResults ++ sequentialTestResults ++ excludedTestResults

        testResults.recoverWith {
          case NonFatal(e: Errors.FrameworkException) => Future.failed(e)
          case NonFatal(other) =>
            Future.failed(new LedgerTestCasesRunner.UncaughtExceptionError(other))
        }
      }
  }

  private def prepareResourcesAndRun(implicit
      executionContext: ExecutionContext
  ): Future[Vector[LedgerTestSummary]] = {

    val materializerResources =
      ResourceOwner.forMaterializerDirectly(() => createActorSystem).acquire()

    // When running the tests, explicitly use the materializer's execution context
    // The tests will then be executed on it instead of the implicit one -- which
    // should only be used to manage resources' lifecycle
    val results =
      for {
        materializer <- materializerResources.asFuture
        results <- run(participantChannels, participantAdminChannels)(
          materializer,
          executionContext,
        )
      } yield results

    results.onComplete(_ => materializerResources.release())

    results
  }

}
