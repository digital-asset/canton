// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.tracking

import com.daml.error.{ContextualizedErrorLogger, ErrorsAssertions}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.api.v1.completion.Completion
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.digitalasset.canton.ledger.error.{
  CommonErrors,
  DamlContextualizedErrorLogger,
  LedgerApiErrors,
}
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.SubmissionTrackerImpl
import com.google.protobuf.empty.Empty
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException
import org.mockito.MockitoSugar
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, Inside, Succeeded}

import java.time.Duration
import java.util.Timer
import scala.concurrent.{ExecutionContext, Future}

class SubmissionTrackerSpec
    extends AnyFlatSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with Inside
    with ErrorsAssertions
    with IntegrationPatience
    with Eventually {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private implicit val errorLogger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forTesting(getClass)

  behavior of classOf[SubmissionTracker].getSimpleName

  it should "track a submission by correct SubmissionKey" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track new submission
      trackedSubmissionF = submissionTracker.track(commands, `1 day timeout`, submit)

      // Completion with mismatching submissionId
      completionWithMismatchingSubmissionId = completionOk.copy(submissionId = "wrongSubmissionId")
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completions =
          Seq(completionWithMismatchingSubmissionId)
        ) -> submitters
      )

      // Completion with mismatching commandId
      completionWithMismatchingCommandId = completionOk.copy(commandId = "wrongCommandId")
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completions =
          Seq(completionWithMismatchingCommandId)
        ) -> submitters
      )

      // Completion with mismatching applicationId
      completionWithMismatchingAppId = completionOk.copy(applicationId = "wrongAppId")
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completions = Seq(completionWithMismatchingAppId)) -> submitters
      )

      // Completion with mismatching actAs
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completions = Seq(completionOk)) -> (submitters + "another_party")
      )

      // Matching completion
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completions = Seq(completionOk)) -> submitters
      )

      trackedSubmission <- trackedSubmissionF
    } yield {
      trackedSubmission shouldBe CompletionResponse(None, completionOk)
    }
  }

  it should "fail on submission failure" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track new submission
      trackedSubmissionF = submissionTracker.track(commandsThatFail, `1 day timeout`, submit)

      failure <- trackedSubmissionF.failed
    } yield {
      failure shouldBe a[RuntimeException]
      failure.getMessage shouldBe failureInSubmit.getMessage
    }
  }

  it should "fail on completion failure" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track new submission
      trackedSubmissionF = submissionTracker.track(commands, `1 day timeout`, submit)

      // Complete the submission with a failed completion
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(
          completions = Seq(completionFailed),
          checkpoint = None,
        ) -> submitters
      )

      failure <- trackedSubmissionF.failed
    } yield inside(failure) { case sre: StatusRuntimeException =>
      assertError(
        sre,
        completionFailedGrpcCode,
        completionFailedMessage,
        Seq.empty,
        verifyEmptyStackTrace = false,
      )
      succeed
    }
  }

  it should "fail if timeout reached" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] =
      submissionTracker
        .track(commands, zeroTimeout, submit)
        .failed
        .map(inside(_) { case actualStatusRuntimeException: StatusRuntimeException =>
          assertError(
            actual = actualStatusRuntimeException,
            expected = CommonErrors.RequestTimeOut
              .Reject(
                "Timed out while awaiting for a completion corresponding to a command submission with command-id=cId_1 and submission-id=sId_1.",
                definiteAnswer = false,
              )
              .asGrpcError,
          )
          succeed
        })
  }

  it should "fail on duplicate submission" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit

      // Track new submission
      firstSubmissionF = submissionTracker.track(commands, `1 day timeout`, submit)

      // Track the same submission again
      actualException <- submissionTracker.track(commands, `1 day timeout`, submit).failed

      // Complete the first submission to ensure clean pending map at the end
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(completions = Seq(completionOk), checkpoint = None) -> submitters
      )
      _ <- firstSubmissionF
    } yield inside(actualException) { case actualStatusRuntimeException: StatusRuntimeException =>
      // Expect duplicate error
      assertError(
        actual = actualStatusRuntimeException,
        expected = LedgerApiErrors.ConsistencyErrors.DuplicateCommand
          .Reject(existingCommandSubmissionId = Some(submissionId))
          .asGrpcError,
      )
      succeed
    }
  }

  it should "fail on a submission with a command missing the submission id" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] =
      submissionTracker
        .track(
          commands = commands.copy(submissionId = ""),
          timeout = `1 day timeout`,
          submit = submit,
        )
        .failed
        .map(inside(_) { case actualStatusRuntimeException: StatusRuntimeException =>
          assertError(
            actual = actualStatusRuntimeException,
            expected = CommonErrors.ServiceInternalError
              .Generic("Missing submission id in submission tracker")
              .asGrpcError,
          )
          succeed
        })
  }

  it should "fail after exceeding the max-commands-in-flight" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit

      _ = submissionTracker.track(commands.copy(commandId = "c1"), `1 day timeout`, submit)
      _ = submissionTracker.track(commands.copy(commandId = "c2"), `1 day timeout`, submit)
      _ = submissionTracker.track(commands.copy(commandId = "c3"), `1 day timeout`, submit)
      // max-commands-in-flight = 3. Expect rejection
      submissionOverLimitF = submissionTracker.track(
        commands.copy(commandId = "c4"),
        `1 day timeout`,
        submit,
      )

      // Close the tracker to ensure clean pending map at the end
      _ = submissionTracker.close()
      failure <- submissionOverLimitF.failed
    } yield inside(failure) { case actualStatusRuntimeException: StatusRuntimeException =>
      assertError(
        actual = actualStatusRuntimeException,
        expected = LedgerApiErrors.ParticipantBackpressure
          .Rejection("Maximum number of commands in-flight reached")
          .asGrpcError,
      )
      succeed
    }
  }

  it should "fail if a command completion is missing its completion status" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track new submission
      trackedSubmissionF = submissionTracker.track(commands, `1 day timeout`, submit)

      // Complete the submission with completion response
      _ = submissionTracker.onCompletion(
        CompletionStreamResponse(
          completions = Seq(completionOk.copy(status = None)),
          checkpoint = None,
        ) -> submitters
      )

      failure <- trackedSubmissionF.failed
    } yield inside(failure) { case ex: StatusRuntimeException =>
      assertError(
        actual = ex,
        expected = CommonErrors.ServiceInternalError
          .Generic("Command completion is missing completion status")(
            DamlContextualizedErrorLogger.forTesting(getClass, Some(submissionId))
          )
          .asGrpcError,
      )
      succeed
    }
  }

  it should "cancel all trackers on close" in new SubmissionTrackerFixture {
    override def run: Future[Assertion] = for {
      _ <- Future.unit
      // Track some submissions
      submission1 = submissionTracker.track(commands, `1 day timeout`, submit)
      submission2 = submissionTracker.track(otherCommands, `1 day timeout`, submit)

      // Close the tracker
      _ = submissionTracker.close()

      failure1 <- submission1.failed
      failure2 <- submission2.failed
    } yield {
      inside(failure1) { case actualStatusRuntimeException: StatusRuntimeException =>
        assertError(
          actual = actualStatusRuntimeException,
          expected = CommonErrors.ServerIsShuttingDown.Reject().asGrpcError,
        )
      }
      inside(failure2) { case actualStatusRuntimeException: StatusRuntimeException =>
        assertError(
          actual = actualStatusRuntimeException,
          expected = CommonErrors.ServerIsShuttingDown.Reject().asGrpcError,
        )
      }
      succeed
    }
  }

  trait SubmissionTrackerFixture {
    private val timer = new Timer("test-timer")
    val timeoutSupport = new CancellableTimeoutSupportImpl(timer)
    val submissionTracker =
      new SubmissionTrackerImpl(timeoutSupport, maxCommandsInFlight = 3, Metrics.ForTesting)

    val zeroTimeout: Duration = Duration.ZERO
    val `1 day timeout`: Duration = Duration.ofDays(1L)

    val submissionId = "sId_1"
    val commandId = "cId_1"
    val applicationId = "apId_1"
    val actAs = Seq("p1", "p2")
    val party = "p3"
    val commands: Commands = Commands(
      submissionId = submissionId,
      commandId = commandId,
      applicationId = applicationId,
      actAs = actAs,
      party = party,
    )
    val otherCommands: Commands = commands.copy(commandId = "cId_2")
    val commandsThatFail: Commands = Commands(submissionId = "failing", commandId = "failing")
    val failureInSubmit = new RuntimeException("failure in submit")
    val submit: Map[SubmitRequest, Future[Empty]] =
      Map(
        SubmitRequest(Some(commandsThatFail)) -> Future.failed(failureInSubmit)
      )
        .withDefaultValue(Future.successful(com.google.protobuf.empty.Empty()))

    val submitters: Set[String] = (actAs :+ party).toSet

    val completionOk: Completion = Completion(
      submissionId = submissionId,
      commandId = commandId,
      status = Some(Status(code = io.grpc.Status.Code.OK.value())),
      applicationId = applicationId,
    )

    val completionFailedGrpcCode = io.grpc.Status.Code.NOT_FOUND
    val completionFailedMessage: String = "ledger rejection"
    val completionFailed: Completion = completionOk.copy(
      status = Some(
        Status(code = completionFailedGrpcCode.value(), message = completionFailedMessage)
      )
    )

    // TODO(#13019) Avoid the global execution context
    @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
    implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    def run: Future[Assertion]

    run.futureValue shouldBe Succeeded
    // We want to assert this for each test
    // Completion of futures might race with removal of the entries from the map
    eventually {
      submissionTracker.pending shouldBe empty
    }
    // Stop the timer
    timer.purge()
    timer.cancel()
  }
}
