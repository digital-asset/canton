// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.base.error.RpcError
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveDouble}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.ledger.participant.state.SyncService.CostEstimateWithReassignments
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.platform.apiserver.services.metrics.{
  TrafficEnforcementInventory,
  TrafficEnforcementMetrics,
}
import com.digitalasset.canton.tea.v1.RegisterSubmissionRequest.ChargedAccount
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  RegisterSubmissionRequest,
  RegisterSubmissionResponse,
}
import com.digitalasset.canton.tea.{AccountId, TrafficEnforcementErrors}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{
  BaseTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPartyId,
  ProtoDeserializationError,
}
import com.digitalasset.daml.lf.data.Ref
import com.google.rpc.status.Status as RpcStatus
import io.grpc.Status
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AsyncWordSpec

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

  val parentName = MetricName("test")
  val noOpMetrics = new TrafficEnforcementMetrics(
    inventory = new TrafficEnforcementInventory(parentName)(new HistogramInventory()),
    metricsFactory = NoOpMetricsFactory,
  )(MetricsContext.Empty)

  private def newBackend(
      enforceCostOnSubmissions: Boolean,
      rejectMultiPartySubmissions: Boolean,
      trafficServiceClient: RichTrafficServiceClient,
      allowSubmissionsOnDegradation: Boolean = false,
      estimateMultiplier: Double = 1.0,
  ): TrafficEnforcementBackend =
    new TrafficEnforcementBackendImpl(
      enforceCostOnSubmissions = enforceCostOnSubmissions,
      rejectMultiPartySubmissions = rejectMultiPartySubmissions,
      allowSubmissionsOnDegradation = allowSubmissionsOnDegradation,
      estimateMultiplier = PositiveDouble.tryCreate(estimateMultiplier),
      trafficServiceClient = trafficServiceClient,
      adminParty = adminParty,
      metrics = noOpMetrics,
      timeouts = ProcessingTimeout(),
      loggerFactory = loggerFactory,
    )

  private def checkEstimatedCost(
      backend: TrafficEnforcementBackend,
      actAs: Seq[LfPartyId],
      trafficCost: Long,
  ) =
    backend.checkEstimatedCost(
      SubmissionAccounting.forSubmitter(None, actAs.toSet, adminParty),
      CostEstimateWithReassignments(NonNegativeLong.tryCreate(trafficCost), Seq.empty),
    )

  "checkEstimatedCost" should {

    "succeed when the singleton actAs party's account has sufficient balance" in {
      val client = mock[RichTrafficServiceClient]
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
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

      checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map { result =>
        result shouldBe Right(())
      }
    }

    "reject when the singleton actAs party's account has insufficient balance" in {
      val client = mock[RichTrafficServiceClient]
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
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

      checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map {
        case Left(err) =>
          err.code.id shouldBe "TRAFFIC_ACCOUNT_VALIDATION_FAILED"
          err.cause should include regex raw"Insufficient balance \(5\) for estimated traffic cost \(10\) for account $alice"
          err.context should contain allOf (
            "account_id" -> alice,
            "balance" -> "5",
            "traffic_cost" -> "10",
          )
          // Check that the values actually survive the round-trip
          val decoded = DecodedCantonError
            .fromStatusRuntimeException(err.asGrpcError)
            .fold(decodeErr => fail(s"expected a decodable status, got $decodeErr"), identity)
          decoded.context should contain allOf (
            "account_id" -> alice,
            "balance" -> "5",
            "traffic_cost" -> "10",
          )
        case Right(_) => fail("expected the submission to be rejected")
      }
    }

    "check the estimate times the multiplier, rounded up" in {
      val client = mock[RichTrafficServiceClient]
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
        .thenReturn(
          EitherT.rightT[FutureUnlessShutdown, GrpcError](
            GetAccountResponse(accountId = alice, balance = 13L)
          ),
          EitherT.rightT[FutureUnlessShutdown, GrpcError](
            GetAccountResponse(accountId = alice, balance = 14L)
          ),
        )
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          estimateMultiplier = 1.5,
        )

      for {
        rejected <- checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 9L).value
        accepted <- checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 9L).value
      } yield {
        inside(rejected) { case Left(err) =>
          err.context should contain("traffic_cost" -> "14")
        }
        accepted shouldBe Right(())
      }
    }

    "scale with exact decimal rounding, capped at Long.MaxValue" in {
      def scale(cost: Long, multiplier: Double): Long =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = mock[RichTrafficServiceClient],
          estimateMultiplier = multiplier,
        ).scaleEstimate(cost)

      scale(100L, 1.1) shouldBe 110L
      scale(9L, 1.5) shouldBe 14L
      scale(Long.MaxValue, 2.0) shouldBe Long.MaxValue
    }

    "reject a non-singleton actAs submission when configured to, without looking up an account" in {
      val client = mock[RichTrafficServiceClient]
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      checkEstimatedCost(backend, actAs = Seq(alice, bob), trafficCost = 10L).value.map {
        case Left(err) =>
          verifyZeroInteractions(client)
          err.code.id shouldBe "TRAFFIC_MULTI_PARTY_SUBMISSION_REJECTED"
        case Right(_) => fail("expected the submission to be rejected")
      }
    }

    "reject a non-singleton actAs submission when configured to, even when cost enforcement is disabled overall" in {
      val client = mock[RichTrafficServiceClient]
      val backend =
        newBackend(
          enforceCostOnSubmissions = false,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      checkEstimatedCost(backend, actAs = Seq(alice, bob), trafficCost = 10L).value.map {
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

      checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map { result =>
        verifyZeroInteractions(client)
        result shouldBe Right(())
      }
    }

    "return a failed account lookup as a Left preserving the original error id" in {
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
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map {
        _.left.value.code.id shouldBe "TRAFFIC_TRANSIENT_FAILURE"
      }
    }

    "return a bare transport failure normalized to a participant-owned error" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError =
        GrpcError("get-account", "traffic-service", Status.UNAVAILABLE.asRuntimeException())
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
        )

      checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map {
        _.left.value.code.id shouldBe "SERVICE_NOT_RUNNING"
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
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      loggerFactory.assertLogs(
        checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map { result =>
          result shouldBe Right(())
        },
        _.warningMessage should include("degrading"),
      )
    }

    "degrade a bare DEADLINE_EXCEEDED account lookup failure, since it is not a client cancellation" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError =
        GrpcError("get-account", "traffic-service", Status.DEADLINE_EXCEEDED.asRuntimeException())
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      loggerFactory.assertLogs(
        checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map { result =>
          result shouldBe Right(())
        },
        _.warningMessage should include("degrading"),
      )
    }

    "never degrade a bare CANCELLED account lookup failure, an explicit client cancellation" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError =
        GrpcError("get-account", "traffic-service", Status.CANCELLED.asRuntimeException())
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map {
        _.left.value.code.id shouldBe "REQUEST_TIME_OUT"
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
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map {
        _.left.value.code.id shouldBe "PROTO_DESERIALIZATION_FAILURE"
      }
    }

    "never degrade a bare PERMISSION_DENIED account lookup failure, a deterministic refusal" in {
      val client = mock[RichTrafficServiceClient]
      val grpcError =
        GrpcError("get-account", "traffic-service", Status.PERMISSION_DENIED.asRuntimeException())
      when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](grpcError))
      val backend =
        newBackend(
          enforceCostOnSubmissions = true,
          rejectMultiPartySubmissions = true,
          trafficServiceClient = client,
          allowSubmissionsOnDegradation = true,
        )

      loggerFactory.assertLogs(
        checkEstimatedCost(backend, actAs = Seq(alice), trafficCost = 10L).value.map {
          _.left.value.asGrpcError.getStatus.getCode shouldBe Status.Code.INTERNAL
        },
        _.errorMessage should include("Error in submitting request to traffic service"),
      )
    }
  }

  "checksBalance" should {

    def checksBalance(enforce: Boolean, accounting: SubmissionAccounting): Boolean =
      newBackend(
        enforceCostOnSubmissions = enforce,
        rejectMultiPartySubmissions = true,
        trafficServiceClient = mock[RichTrafficServiceClient],
      ).checksBalance(accounting)

    "be true only for a charged account when enforcement is on" in {
      val explicit = SubmissionAccounting.forSubmitter(
        Some(AccountId.tryCreate("account")),
        Set(alice, bob),
        adminParty,
      )
      val fallbackParty = SubmissionAccounting.forSubmitter(None, Set(alice), adminParty)
      val admin = SubmissionAccounting.participantAdmin(adminParty)
      val noAccount = SubmissionAccounting.forSubmitter(None, Set(alice, bob), adminParty)

      checksBalance(enforce = true, explicit) shouldBe true
      checksBalance(enforce = true, fallbackParty) shouldBe true
      checksBalance(enforce = true, admin) shouldBe false
      checksBalance(enforce = true, noAccount) shouldBe false
      checksBalance(enforce = false, explicit) shouldBe false
      checksBalance(enforce = false, fallbackParty) shouldBe false
    }
  }

  "registerSubmission" should {

    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)

    val changeId = ChangeId(
      Ref.UserId.assertFromString("user"),
      Ref.CommandId.assertFromString("command"),
      Set(alice),
    )
    val submissionId = Ref.SubmissionId.assertFromString("submission")

    def info(
        actAs: Set[LfPartyId],
        accountId: Option[AccountId] = None,
        checkBalance: Boolean = true,
    ): SubmissionAccountingInfo =
      SubmissionAccountingInfo(
        changeId.copy(actAs = actAs),
        submissionId,
        SubmissionAccounting
          .forSubmitter(accountId, actAs, adminParty)
          .copy(checkBalance = checkBalance),
      )

    def stubRegistration(
        client: RichTrafficServiceClient,
        result: Either[GrpcError, RegisterSubmissionResponse],
    ): Unit =
      when(
        client.registerSubmission(any[RegisterSubmissionRequest])(any[TraceContext])
      ).thenReturn(EitherT.fromEither[FutureUnlessShutdown](result)).discard

    "send the account and the balance to check for the selection" in {
      val explicitAccount = "explicit-account"
      val cases = Seq[
        (Option[AccountId], Set[LfPartyId], Boolean, Boolean, Option[String], Option[Long])
      ](
        (
          Some(AccountId.tryCreate(explicitAccount)),
          Set(alice, bob),
          true,
          true,
          Some(explicitAccount),
          Some(10L),
        ),
        (None, Set(alice), true, true, Some(alice), Some(10L)),
        (None, Set(alice), false, true, Some(alice), None),
        (None, Set(alice), true, false, Some(alice), None),
        (None, Set(adminParty), true, true, Some(adminParty), None),
        (None, Set(alice, bob), true, true, None, None),
      )

      MonadUtil
        .sequentialTraverse(cases) {
          case (accountId, actAs, enforce, checkBalance, expectedAccount, expectedBalance) =>
            val client = mock[RichTrafficServiceClient]
            stubRegistration(client, Right(RegisterSubmissionResponse()))
            val backend = newBackend(
              enforceCostOnSubmissions = enforce,
              rejectMultiPartySubmissions = false,
              trafficServiceClient = client,
            )

            backend
              .registerSubmission(info(actAs, accountId, checkBalance), trafficCost = 10L)
              .value
              .map { result =>
                result shouldBe Right(())
                val captor = ArgCaptor[RegisterSubmissionRequest]
                verify(client).registerSubmission(captor)(any[TraceContext])
                captor.value shouldBe RegisterSubmissionRequest(
                  changeIdHash = changeId.copy(actAs = actAs).hash.toHexString,
                  submissionId = submissionId,
                  account = expectedAccount.map(ChargedAccount(_, expectedBalance)),
                )
              }
        }
        .map(_ => succeed)
    }

    "pass a TEA rejection through unchanged, even when degradation is allowed" in {
      val rejection = TrafficEnforcementErrors.InsufficientBalance.Reject(alice, 5L, 10L)
      val client = mock[RichTrafficServiceClient]
      stubRegistration(
        client,
        Left(GrpcError("register-submission", "traffic-service", rejection.asGrpcError)),
      )
      val backend = newBackend(
        enforceCostOnSubmissions = true,
        rejectMultiPartySubmissions = false,
        trafficServiceClient = client,
        allowSubmissionsOnDegradation = true,
      )

      backend.registerSubmission(info(Set(alice)), trafficCost = 10L).value.map { result =>
        val decoded = DecodedCantonError
          .fromGrpcStatus(RpcStatus.fromJavaProto(result.left.value.asGrpcStatus))
          .value
        decoded.code.id shouldBe rejection.code.id
        decoded.context should contain allOf (
          "account_id" -> alice,
          "balance" -> "5",
          "traffic_cost" -> "10",
        )
      }
    }

    "reject, or degrade only when allowed, when TEA can't answer or the answer is lost" in {
      val cases = for {
        (status, normalizedId) <- Seq(
          Status.UNAVAILABLE -> "SERVICE_NOT_RUNNING",
          Status.DEADLINE_EXCEEDED -> "REQUEST_TIME_OUT",
        )
        enforce <- Seq(true, false)
        degrade <- Seq(true, false)
      } yield (status, normalizedId, enforce, degrade)

      MonadUtil
        .sequentialTraverse(cases) { case (status, normalizedId, enforce, degrade) =>
          val client = mock[RichTrafficServiceClient]
          stubRegistration(
            client,
            Left(GrpcError("register-submission", "traffic-service", status.asRuntimeException())),
          )
          val backend = newBackend(
            enforceCostOnSubmissions = enforce,
            rejectMultiPartySubmissions = false,
            trafficServiceClient = client,
            allowSubmissionsOnDegradation = degrade,
          )

          def result = backend.registerSubmission(info(Set(alice)), trafficCost = 10L).value
          if (degrade)
            loggerFactory.assertLogs(
              result.map(_ shouldBe Right(())),
              _.warningMessage should include("degrading"),
            )
          else result.map(_.left.value.code.id shouldBe normalizedId)
        }
        .map(_ => succeed)
    }
  }

  "registerIfEnabled" should {

    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)

    val changeId = ChangeId(
      Ref.UserId.assertFromString("user"),
      Ref.CommandId.assertFromString("command"),
      Set(alice),
    )
    val submissionId = Ref.SubmissionId.assertFromString("submission")
    val accounting = SubmissionAccounting.forSubmitter(None, Set(alice), adminParty)

    def register(
        backendO: Option[TrafficEnforcementBackend],
        submissionIdO: Option[Ref.SubmissionId] = Some(submissionId),
    ) =
      TrafficEnforcementBackend
        .registerIfEnabled(backendO, changeId, submissionIdO, accounting, trafficCost = 10L)
        .value

    "succeed without a backend" in {
      register(None).map(_ shouldBe Right(()))
    }

    "register the send and pass the backend's rejection through" in {
      val rejection = TrafficEnforcementErrors.InsufficientBalance.Reject(alice, 5L, 10L)
      val backend = mock[TrafficEnforcementBackend]
      when(backend.registerSubmission(any[SubmissionAccountingInfo], any[Long])(any[TraceContext]))
        .thenReturn(EitherT.leftT[FutureUnlessShutdown, Unit](rejection: RpcError))

      register(Some(backend)).map { result =>
        result shouldBe Left(rejection)
        verify(backend).registerSubmission(
          eqTo(SubmissionAccountingInfo(changeId, submissionId, accounting)),
          eqTo(10L),
        )(any[TraceContext])
        succeed
      }
    }

    "reject a send without a submission id before calling the backend" in {
      val backend = mock[TrafficEnforcementBackend]

      loggerFactory
        .assertLogs(
          register(Some(backend), submissionIdO = None).unwrap,
          _.shouldBeCantonErrorCode(TrafficEnforcementErrors.FatalFailure),
        )
        .map { result =>
          verifyZeroInteractions(backend)
          inside(result) { case UnlessShutdown.Outcome(Left(error)) =>
            error.code.id shouldBe TrafficEnforcementErrors.FatalFailure.id
          }
        }
    }
  }
}
