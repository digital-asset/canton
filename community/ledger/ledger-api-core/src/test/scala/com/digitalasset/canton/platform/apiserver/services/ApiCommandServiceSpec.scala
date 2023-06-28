// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.RpcProtoExtractors
import com.daml.ledger.api.v1.command_completion_service.Checkpoint
import com.daml.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.daml.ledger.api.v1.command_service.{CommandServiceGrpc, SubmitAndWaitRequest}
import com.daml.ledger.api.v1.command_submission_service.SubmitRequest
import com.daml.ledger.api.v1.commands.{Command, Commands, CreateCommand}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v2.completion.Completion
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.tracing.DefaultOpenTelemetry
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.{ErrorLoggingContext, LoggingContextWithTrace}
import com.digitalasset.canton.platform.apiserver.services.ApiCommandServiceSpec.*
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker.SubmissionKey
import com.digitalasset.canton.platform.apiserver.services.tracking.{
  CompletionResponse,
  SubmissionTracker,
}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.empty.Empty
import com.google.rpc.Code
import com.google.rpc.status.Status as StatusProto
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.{Deadline, Status}
import io.opentelemetry.sdk.OpenTelemetrySdk
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

class ApiCommandServiceSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with BaseTest {
  private implicit val resourceContext: ResourceContext = ResourceContext(executionContext)
  private val telemetry = new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build())

  s"the command service" should {
    val trackerCompletionResponse = tracking.CompletionResponse(
      completion = Completion(
        commandId = "command ID",
        status = Some(OkStatus),
        updateId = "transaction ID",
      ),
      checkpoint =
        Some(Checkpoint(offset = Some(LedgerOffset(LedgerOffset.Value.Absolute("offset"))))),
    )
    val commands = someCommands()
    val expectedSubmissionKey = SubmissionKey(
      commandId = "command ID",
      submissionId = "",
      applicationId = "",
      parties = Set.empty,
    )
    val submissionTracker = mock[SubmissionTracker]
    val submit = mock[SubmitRequest => Future[Empty]]
    when(
      submissionTracker.track(eqTo(expectedSubmissionKey), any[Duration], any[() => Future[Any]])(
        any[ContextualizedErrorLogger],
        any[TraceContext],
      )
    ).thenReturn(Future.successful(trackerCompletionResponse))

    "submit a request, and wait for a response" in {
      openChannel(
        new ApiCommandService(
          UnimplementedTransactionServices,
          submissionTracker,
          submit,
          Duration.ofSeconds(1000L),
          telemetry,
          loggerFactory,
        )
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub.submitAndWaitForTransactionId(request).map { response =>
          verify(submissionTracker).track(
            eqTo(expectedSubmissionKey),
            eqTo(Duration.ofSeconds(1000L)),
            any[() => Future[Any]],
          )(any[ContextualizedErrorLogger], any[TraceContext])
          response.transactionId should be("transaction ID")
          response.completionOffset shouldBe "offset"
        }
      }
    }

    "pass the provided deadline to the tracker as a timeout" in {
      val now = Instant.parse("2021-09-01T12:00:00Z")
      val deadlineTicker = new Deadline.Ticker {
        override def nanoTime(): Long =
          now.getEpochSecond * TimeUnit.SECONDS.toNanos(1) + now.getNano
      }

      openChannel(
        new ApiCommandService(
          UnimplementedTransactionServices,
          submissionTracker,
          submit,
          Duration.ofSeconds(1L),
          telemetry,
          loggerFactory,
        ),
        deadlineTicker,
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub
          .withDeadline(Deadline.after(3600L, TimeUnit.SECONDS, deadlineTicker))
          .submitAndWaitForTransactionId(request)
          .map { response =>
            verify(submissionTracker).track(
              eqTo(expectedSubmissionKey),
              eqTo(Duration.ofSeconds(1000L)),
              any[() => Future[Any]],
            )(any[ContextualizedErrorLogger], any[TraceContext])
            response.transactionId should be("transaction ID")
            succeed
          }
      }
    }

    "time out if the tracker times out" in {
      when(
        submissionTracker.track(eqTo(expectedSubmissionKey), any[Duration], any[() => Future[Any]])(
          any[ContextualizedErrorLogger],
          any[TraceContext],
        )
      ).thenReturn(
        Future.fromTry(
          CompletionResponse.timeout("some-cmd-id", "some-submission-id")(
            ErrorLoggingContext(
              loggerFactory.getTracedLogger(getClass),
              LoggingContextWithTrace.ForTesting,
            )
          )
        )
      )

      val service = new ApiCommandService(
        UnimplementedTransactionServices,
        submissionTracker,
        submit,
        Duration.ofSeconds(1337L),
        telemetry,
        loggerFactory,
      )

      openChannel(
        service
      ).use { stub =>
        val request = SubmitAndWaitRequest.of(Some(commands))
        stub.submitAndWaitForTransactionId(request).failed.map {
          case RpcProtoExtractors.Exception(RpcProtoExtractors.Status(Code.DEADLINE_EXCEEDED)) =>
            succeed
          case unexpected => fail(s"Unexpected exception", unexpected)
        }
      }
    }

    "close the supplied tracker when closed" in {
      val submissionTracker = mock[SubmissionTracker]
      val service = new ApiCommandService(
        UnimplementedTransactionServices,
        submissionTracker,
        submit,
        Duration.ofSeconds(1337L),
        telemetry,
        loggerFactory,
      )

      verifyZeroInteractions(submissionTracker)

      service.close()
      verify(submissionTracker).close()
      succeed
    }
  }
}

object ApiCommandServiceSpec {
  private val UnimplementedTransactionServices = new ApiCommandService.TransactionServices(
    getTransactionById = _ => Future.failed(new RuntimeException("This should never be called.")),
    getFlatTransactionById = _ =>
      Future.failed(new RuntimeException("This should never be called.")),
  )

  private val OkStatus = StatusProto.of(Status.Code.OK.value, "", Seq.empty)

  private def someCommands() = Commands(
    ledgerId = "ledger ID",
    commandId = "command ID",
    commands = Seq(
      Command.of(Command.Command.Create(CreateCommand()))
    ),
  )

  // TODO(#13019) Avoid the global execution context
  @SuppressWarnings(Array("com.digitalasset.canton.GlobalExecutionContext"))
  private def openChannel(
      service: ApiCommandService,
      deadlineTicker: Deadline.Ticker = Deadline.getSystemTicker,
  ): ResourceOwner[CommandServiceGrpc.CommandServiceStub] =
    for {
      name <- ResourceOwner.forValue(() => UUID.randomUUID().toString)
      _ <- ResourceOwner.forServer(
        InProcessServerBuilder
          .forName(name)
          .deadlineTicker(deadlineTicker)
          .addService(() => CommandService.bindService(service, ExecutionContext.global)),
        shutdownTimeout = 10.seconds,
      )
      channel <- ResourceOwner.forChannel(
        InProcessChannelBuilder.forName(name),
        shutdownTimeout = 10.seconds,
      )
    } yield CommandServiceGrpc.stub(channel)
}
