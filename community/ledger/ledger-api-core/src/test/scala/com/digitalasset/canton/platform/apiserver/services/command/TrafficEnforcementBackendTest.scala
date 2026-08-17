// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.v1.{GetAccountRequest, GetAccountResponse}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPartyId,
  ProtoDeserializationError,
}
import io.grpc.{Status, StatusRuntimeException}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

class TrafficEnforcementBackendTest
    extends AsyncWordSpec
    with MockitoSugar
    with ArgumentMatchersSugar
    with HasExecutionContext
    with FailOnShutdown
    with BaseTest {

  private val adminParty = LfPartyId.assertFromString("participantAdmin")
  private val alice = LfPartyId.assertFromString("Alice")
  private val bob = LfPartyId.assertFromString("Bob")

  private def newBackend(
      enforceCostOnSubmissions: Boolean,
      rejectMultiPartySubmissions: Boolean,
      trafficServiceClient: RichTrafficServiceClient,
      allowSubmissionsOnDegradation: Boolean = false,
  ): TrafficEnforcementBackend =
    new TrafficEnforcementBackend(
      enforceCostOnSubmissions = enforceCostOnSubmissions,
      rejectMultiPartySubmissions = rejectMultiPartySubmissions,
      allowSubmissionsOnDegradation = allowSubmissionsOnDegradation,
      trafficServiceClient = trafficServiceClient,
      adminParty = adminParty,
      timeouts = ProcessingTimeout(),
      loggerFactory = loggerFactory,
    )

  "validateTraffic" should {

    "succeed when the singleton actAs party's account has sufficient balance" in {
      val client = mock[RichTrafficServiceClient]
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(
          EitherT.rightT[FutureUnlessShutdown, GrpcError](
            GetAccountResponse(accountId = alice, balance = 10L)
          )
        )
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend.validateTraffic(actAs = Seq(alice), trafficCost = 10L).value.map { result =>
        result shouldBe Right(())
      }
    }

    "reject when the singleton actAs party's account has insufficient balance" in {
      val client = mock[RichTrafficServiceClient]
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(
          EitherT.rightT[FutureUnlessShutdown, GrpcError](
            GetAccountResponse(accountId = alice, balance = 5L)
          )
        )
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend.validateTraffic(actAs = Seq(alice), trafficCost = 10L).value.map {
        case Left(err) =>
          err.code.id shouldBe "TRAFFIC_ACCOUNT_VALIDATION_FAILED"
          err.cause should include regex raw"Insufficient balance \(5\) for actual traffic cost \(10\) for account $alice"
        case Right(_) => fail("expected the submission to be rejected")
      }
    }

    "reject a non-singleton actAs submission when configured to, without looking up an account" in {
      val client = mock[RichTrafficServiceClient]
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend.validateTraffic(actAs = Seq(alice, bob), trafficCost = 10L).value.map {
        case Left(err) =>
          verifyZeroInteractions(client)
          err.code.id shouldBe "TRAFFIC_MULTI_PARTY_SUBMISSION_REJECTED"
        case Right(_) => fail("expected the submission to be rejected")
      }
    }

    "not look up an account at all when cost enforcement is disabled" in {
      val client = mock[RichTrafficServiceClient]
      val backend =
        newBackend(
          enforceCostOnSubmissions = false,
          rejectMultiPartySubmissions = false,
          trafficServiceClient = client,
        )

      backend.validateTraffic(actAs = Seq(alice), trafficCost = 10L).value.map { result =>
        verifyZeroInteractions(client)
        result shouldBe Right(())
      }
    }

    "propagate a failed account lookup as a failed future preserving the original error id" in {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      val client = mock[RichTrafficServiceClient]
      val grpcError = GrpcError(
        request = "get-account",
        serverName = "traffic-service",
        e = TrafficEnforcementErrors.TransientFailure
          .Reject(new RuntimeException("db down"))
          .asGrpcError,
      )
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend
        .validateTraffic(actAs = Seq(alice), trafficCost = 10L)
        .value
        .unwrap
        .failed
        .map {
          case sre: StatusRuntimeException =>
            DecodedCantonError
              .fromStatusRuntimeException(sre)
              .map(_.code.id) shouldBe Right("TRAFFIC_TRANSIENT_FAILURE")
          case other => fail(s"expected a StatusRuntimeException, got $other")
        }
    }

    "propagate a bare transport failure normalized to a participant-owned error" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError =
        GrpcError("get-account", "traffic-service", Status.UNAVAILABLE.asRuntimeException())
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      backend
        .validateTraffic(actAs = Seq(alice), trafficCost = 10L)
        .value
        .unwrap
        .failed
        .map {
          case sre: StatusRuntimeException =>
            DecodedCantonError
              .fromStatusRuntimeException(sre)
              .map(_.code.id) shouldBe Right("SERVICE_NOT_RUNNING")
          case other => fail(s"expected a StatusRuntimeException, got $other")
        }
    }

    "degrade an ordinary failed account lookup when configured to, allowing the submission to proceed" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError = GrpcError(
        request = "get-account",
        serverName = "traffic-service",
        e = TrafficEnforcementErrors.TransientFailure
          .Reject(new RuntimeException("db down"))
          .asGrpcError,
      )
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      loggerFactory.assertLogs(
        backend.validateTraffic(actAs = Seq(alice), trafficCost = 10L).value.map { result =>
          result shouldBe Right(())
        },
        _.warningMessage should include("degrading"),
      )
    }

    "degrade a bare DEADLINE_EXCEEDED account lookup failure, since it is not a client cancellation" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError =
        GrpcError("get-account", "traffic-service", Status.DEADLINE_EXCEEDED.asRuntimeException())
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      loggerFactory.assertLogs(
        backend.validateTraffic(actAs = Seq(alice), trafficCost = 10L).value.map { result =>
          result shouldBe Right(())
        },
        _.warningMessage should include("degrading"),
      )
    }

    "never degrade a bare CANCELLED account lookup failure, an explicit client cancellation" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError =
        GrpcError("get-account", "traffic-service", Status.CANCELLED.asRuntimeException())
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      backend
        .validateTraffic(actAs = Seq(alice), trafficCost = 10L)
        .value
        .unwrap
        .failed
        .map {
          case sre: StatusRuntimeException =>
            DecodedCantonError
              .fromStatusRuntimeException(sre)
              .map(_.code.id) shouldBe Right("REQUEST_TIME_OUT")
          case other => fail(s"expected a StatusRuntimeException, got $other")
        }
    }

    "never degrade a refusal even when degradation is allowed, the traffic service already answered" in {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      val client = mock[RichTrafficServiceClient]
      val grpcError = GrpcError(
        request = "get-account",
        serverName = "traffic-service",
        e = ProtoDeserializationError.ProtoDeserializationFailure
          .Wrap(ProtoDeserializationError.OtherError("malformed account id"))
          .asGrpcError,
      )
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      backend
        .validateTraffic(actAs = Seq(alice), trafficCost = 10L)
        .value
        .unwrap
        .failed
        .map {
          case sre: StatusRuntimeException =>
            DecodedCantonError
              .fromStatusRuntimeException(sre)
              .map(_.code.id) shouldBe Right("PROTO_DESERIALIZATION_FAILURE")
          case other => fail(s"expected a StatusRuntimeException, got $other")
        }
    }

    "never degrade a bare PERMISSION_DENIED account lookup failure, a deterministic refusal" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError =
        GrpcError("get-account", "traffic-service", Status.PERMISSION_DENIED.asRuntimeException())
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      loggerFactory.assertLogs(
        backend
          .validateTraffic(actAs = Seq(alice), trafficCost = 10L)
          .value
          .unwrap
          .failed
          .map(_ => succeed),
        _.errorMessage should include("Error in submitting request to traffic service"),
      )
    }
  }
}
