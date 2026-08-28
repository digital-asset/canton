// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.client

import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.generator.ErrorCodeDocumentationGenerator
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.v1.TrafficServiceGrpc.TrafficService
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  PruneEventsRequest,
  PruneEventsResponse,
  TrafficServiceGrpc,
  UpdateAccountRequest,
  UpdateAccountResponse,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtoDeserializationError}
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future

class RichTrafficServiceClientTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private def grpcError(status: Status, requestDescription: String = "get-account"): GrpcError =
    GrpcError(requestDescription, "traffic-service", status.asRuntimeException())

  private class RecordingTrafficService extends TrafficService {
    val getAccountTraceContext = new AtomicReference[TraceContext]()

    override def getAccount(request: GetAccountRequest): Future[GetAccountResponse] = {
      getAccountTraceContext.set(TraceContextGrpc.fromGrpcContext)
      Future.successful(GetAccountResponse(request.accountId, 0L))
    }

    override def updateAccount(request: UpdateAccountRequest): Future[UpdateAccountResponse] =
      Future.successful(
        UpdateAccountResponse(Some(GetAccountResponse(request.accountId, 0L)))
      )

    override def pruneEvents(request: PruneEventsRequest): Future[PruneEventsResponse] =
      Future.successful(PruneEventsResponse(0))
  }

  private def withTrafficService(
      body: (RichTrafficServiceClient, RecordingTrafficService) => Unit
  ): Unit = {
    val service = new RecordingTrafficService
    val serverName = InProcessServerBuilder.generateName()
    val server = InProcessServerBuilder
      .forName(serverName)
      .intercept(TraceContextGrpc.serverInterceptor)
      .addService(TrafficServiceGrpc.bindService(service, parallelExecutionContext))
      .build()
      .start()
    val client = RichTrafficServiceClient.toInternalServer(
      serverName,
      timeouts,
      PositiveFiniteDuration.ofSeconds(10),
      loggerFactory,
    )

    try body(client, service)
    finally {
      client.close()
      server.shutdown().awaitTermination().discard
    }
  }

  "toInternalServer" should {
    "carry the caller's trace context to the traffic service" in withTrafficService {
      (client, service) =>
        val callerTraceContext = TraceContext.withNewTraceContext("caller")(identity)
        val callerTraceId = callerTraceContext.traceId.valueOrFail("caller trace id")

        client
          .getAccount(GetAccountRequest("alice"))(callerTraceContext, parallelExecutionContext)
          .futureValueUS
          .valueOrFail("get-account")
          .discard

        Option(service.getAccountTraceContext.get()).flatMap(_.traceId) shouldBe Some(callerTraceId)
    }
  }

  "retryUnlessClientGaveUp" should {

    "retry an ordinary UNAVAILABLE failure" in {
      RichTrafficServiceClient.retryUnlessClientGaveUp(
        grpcError(Status.UNAVAILABLE)
      ) shouldBe true
    }

    "retry an ordinary UNIMPLEMENTED failure" in {
      RichTrafficServiceClient.retryUnlessClientGaveUp(
        grpcError(Status.UNIMPLEMENTED)
      ) shouldBe true
    }

    "retry a UNAVAILABLE failure decoding a retryable TransientFailure" in {
      val error = GrpcError(
        "get-account",
        "traffic-service",
        TrafficEnforcementErrors.TransientFailure
          .Reject(new RuntimeException("db down"))
          .asGrpcError,
      )
      RichTrafficServiceClient.retryUnlessClientGaveUp(error) shouldBe true
    }

    "never retry a bare DEADLINE_EXCEEDED failure, the shared budget is already spent" in {
      RichTrafficServiceClient.retryUnlessClientGaveUp(
        grpcError(Status.DEADLINE_EXCEEDED)
      ) shouldBe false
    }

    "never retry a bare CANCELLED failure, the caller is gone" in {
      RichTrafficServiceClient.retryUnlessClientGaveUp(
        grpcError(Status.CANCELLED)
      ) shouldBe false
    }

    "never retry a DEADLINE_EXCEEDED failure (even if its decoded error is itself retryable)" in {
      val error = GrpcError(
        "get-account",
        "traffic-service",
        CommonErrors.RequestTimeOut.Reject("timed out", definiteAnswer = false).asGrpcError,
      )
      RichTrafficServiceClient.retryUnlessClientGaveUp(error) shouldBe false
    }

    "not retry a bare INVALID_ARGUMENT failure" in {
      RichTrafficServiceClient.retryUnlessClientGaveUp(
        grpcError(Status.INVALID_ARGUMENT)
      ) shouldBe false
    }

    "not retry an INTERNAL failure decoding a non-retryable FatalFailure" in {
      val error = loggerFactory.assertLogs(
        GrpcError(
          "get-account",
          "traffic-service",
          TrafficEnforcementErrors.FatalFailure.Reject(new RuntimeException("boom")).asGrpcError,
        ),
        _.errorMessage should include("The traffic account operation failed"),
      )
      RichTrafficServiceClient.retryUnlessClientGaveUp(error) shouldBe false
    }

    "not retry a TrafficUpdateOutOfBound rejection" in {
      val error = GrpcError(
        "update-account",
        "traffic-service",
        TrafficEnforcementErrors.TrafficUpdateOutOfBound
          .Reject(accountId = "alice", delta = "-100")
          .asGrpcError,
      )
      RichTrafficServiceClient.retryUnlessClientGaveUp(error) shouldBe false
    }
  }

  "normalizeTeaError" should {

    def decodedId(exception: StatusRuntimeException): String =
      DecodedCantonError
        .fromStatusRuntimeException(exception)
        .fold(err => fail(s"expected a decodable status, got $err"), _.code.id)

    "forward a TransientFailure untouched" in {
      val error = GrpcError(
        "get-account",
        "traffic-service",
        TrafficEnforcementErrors.TransientFailure
          .Reject(new RuntimeException("db down"))
          .asGrpcError,
      )
      decodedId(
        RichTrafficServiceClient.normalizeTeaError(error)
      ) shouldBe "TRAFFIC_TRANSIENT_FAILURE"
    }

    "wrap a bare UNAVAILABLE as SERVICE_NOT_RUNNING" in {
      val result =
        RichTrafficServiceClient.normalizeTeaError(grpcError(Status.UNAVAILABLE))
      decodedId(result) shouldBe "SERVICE_NOT_RUNNING"
    }

    "wrap a bare UNIMPLEMENTED as SERVICE_NOT_RUNNING" in {
      val result =
        RichTrafficServiceClient.normalizeTeaError(grpcError(Status.UNIMPLEMENTED))
      decodedId(result) shouldBe "SERVICE_NOT_RUNNING"
    }

    "wrap a bare DEADLINE_EXCEEDED as REQUEST_TIME_OUT with a fixed message" in {
      val result =
        RichTrafficServiceClient.normalizeTeaError(grpcError(Status.DEADLINE_EXCEEDED))
      decodedId(result) shouldBe "REQUEST_TIME_OUT"
      result.getStatus.getDescription should include("did not complete in time")
    }

    "wrap a bare PERMISSION_DENIED as a redacted INTERNAL, not exposing the details" in {
      val redactedDescription = "token rejected by the TEA"
      val status = Status.PERMISSION_DENIED.withDescription(redactedDescription)
      val result = loggerFactory.assertLogs(
        RichTrafficServiceClient.normalizeTeaError(grpcError(status)),
        _.errorMessage should include("Error in submitting request to traffic service"),
      )
      decodedId(result) shouldBe "NA"
      Option(result.getStatus.getDescription) should not contain redactedDescription
    }

    "forward a ProtoDeserializationFailure.Wrap untouched, the traffic service's own verdict on a malformed request" in {
      val error = GrpcError(
        "update-account",
        "traffic-service",
        ProtoDeserializationError.ProtoDeserializationFailure
          .Wrap(ProtoDeserializationError.OtherError("malformed account id"))
          .asGrpcError,
      )
      decodedId(
        RichTrafficServiceClient.normalizeTeaError(error)
      ) shouldBe "PROTO_DESERIALIZATION_FAILURE"
    }

    "forward a bare INVALID_ARGUMENT untouched, keeping its description" in {
      val description = "malformed account id"
      val status = Status.INVALID_ARGUMENT.withDescription(description)
      val result = RichTrafficServiceClient.normalizeTeaError(grpcError(status))
      result.getStatus.getDescription shouldBe description
    }

    "wrap a FatalFailure as a redacted INTERNAL, since redaction already erases its id" in {
      val result = loggerFactory.assertLogs(
        RichTrafficServiceClient.normalizeTeaError(
          GrpcError(
            "get-account",
            "traffic-service",
            TrafficEnforcementErrors.FatalFailure.Reject(new RuntimeException("boom")).asGrpcError,
          )
        ),
        _.errorMessage should include("The traffic account operation failed"),
        _.errorMessage should include("Error in submitting request to traffic service"),
      )
      decodedId(result) shouldBe "NA"
    }

    "forward a TrafficUpdateOutOfBound (TEA origin, FAILED_PRECONDITION) untouched" in {
      val error = GrpcError(
        "update-account",
        "traffic-service",
        TrafficEnforcementErrors.TrafficUpdateOutOfBound
          .Reject(accountId = "alice", delta = "-100")
          .asGrpcError,
      )
      decodedId(
        RichTrafficServiceClient.normalizeTeaError(error)
      ) shouldBe "TRAFFIC_UPDATE_OUT_OF_BOUND"
    }

    "wrap a bare FAILED_PRECONDITION as a redacted INTERNAL, not exposing the details" in {
      val redactedDescription = "some external precondition failure"
      val status = Status.FAILED_PRECONDITION.withDescription(redactedDescription)
      val result = loggerFactory.assertLogs(
        RichTrafficServiceClient.normalizeTeaError(grpcError(status, "update-account")),
        _.errorMessage should include("Error in submitting request to traffic service"),
      )
      decodedId(result) shouldBe "NA"
      Option(result.getStatus.getDescription) should not contain redactedDescription
    }
  }

  "TrafficEnforcementErrors.allErrorIds" should {
    "match every error code declared under com.digitalasset.canton.tea" in {
      val declaredIds =
        ErrorCodeDocumentationGenerator
          .getErrorCodeItems(Array("com.digitalasset.canton.tea"))
          .map(_.code)
          .toSet
      declaredIds shouldBe TrafficEnforcementErrors.allErrorIds
    }
  }
}
