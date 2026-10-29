// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackendTelemetryTest.*
import com.digitalasset.canton.platform.apiserver.services.metrics.{
  TrafficEnforcementInventory,
  TrafficEnforcementMetrics,
}
import com.digitalasset.canton.tea.TrafficEnforcementErrors
import com.digitalasset.canton.tea.v1.{
  GetAccountRequest,
  GetAccountResponse,
  RegisterSubmissionRequest,
  RegisterSubmissionResponse,
}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import io.grpc.{Status, StatusRuntimeException}
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.trace.{StatusCode, Tracer}
import io.opentelemetry.sdk.trace.data.SpanData
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, BeforeAndAfterEach}

import scala.jdk.CollectionConverters.*

class TrafficEnforcementBackendTelemetryTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  private val adminParty = LfPartyId.assertFromString("participantAdmin")
  private val alice = LfPartyId.assertFromString("Alice")
  private val bob = LfPartyId.assertFromString("Bob")
  private val trafficCost = 10L

  private val OutcomeKey = stringKey("canton.traffic_enforcement_outcome")
  private val ReasonKey = stringKey("canton.traffic_enforcement_reason")
  private val SuccessStatus = "success"

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  private val changeId = ChangeId(
    Ref.UserId.assertFromString("user"),
    Ref.CommandId.assertFromString("command"),
    Set(alice),
  )
  private val submissionId = Ref.SubmissionId.assertFromString("submission")

  private def registrationError(e: StatusRuntimeException): GrpcError =
    GrpcError("register-submission", "traffic-service", e)

  private val transientFailureError = registrationError(
    TrafficEnforcementErrors.TransientFailure.Reject(new RuntimeException("db down")).asGrpcError
  )

  private val insufficientBalanceError = registrationError(
    TrafficEnforcementErrors.InsufficientBalance
      .Reject(accountId = alice, balance = trafficCost - 1, trafficCost = trafficCost)
      .asGrpcError
  )

  private val correlationConflictError = registrationError(
    TrafficEnforcementErrors.CorrelationConflict
      .Reject(changeId.hash.toHexString, submissionId)
      .asGrpcError
  )

  private val unavailableError = registrationError(Status.UNAVAILABLE.asRuntimeException())

  private val undecodableServerError = GrpcError.GrpcServerError(
    request = "register-submission",
    serverName = "traffic-service",
    status = Status.UNKNOWN,
    optTrailers = None,
    decodedCantonError = None,
  )

  private val refusedByServerError =
    registrationError(Status.PERMISSION_DENIED.asRuntimeException())

  private val clientError = GrpcError.GrpcClientError(
    request = "register-submission",
    serverName = "traffic-service",
    status = Status.PERMISSION_DENIED,
    optTrailers = None,
    decodedCantonError = None,
  )

  private var testTelemetrySetup: TestTelemetrySetup = _
  private var metricsFactory: InMemoryMetricsFactory = _
  private var metrics: TrafficEnforcementMetrics = _

  override def beforeEach(): Unit = {
    testTelemetrySetup = new TestTelemetrySetup()
    metricsFactory = new InMemoryMetricsFactory
    metrics = new TrafficEnforcementMetrics(
      inventory = new TrafficEnforcementInventory(MetricName("test"))(new HistogramInventory()),
      metricsFactory = metricsFactory,
    )(metricsContext)
  }

  override def afterEach(): Unit =
    testTelemetrySetup.close()

  private def newBackend(
      trafficServiceClient: RichTrafficServiceClient,
      enforceCostOnSubmissions: Boolean = true,
      rejectMultiPartySubmissions: Boolean = true,
      allowSubmissionsOnDegradation: Boolean = false,
  ): TrafficEnforcementBackend = {
    implicit val tracer: Tracer = testTelemetrySetup.tracer
    new TrafficEnforcementBackendImpl(
      enforceCostOnSubmissions = enforceCostOnSubmissions,
      rejectMultiPartySubmissions = rejectMultiPartySubmissions,
      allowSubmissionsOnDegradation = allowSubmissionsOnDegradation,
      trafficServiceClient = trafficServiceClient,
      adminParty = adminParty,
      metrics = metrics,
      timeouts = ProcessingTimeout(),
      loggerFactory = loggerFactory,
    )
  }

  /** The span is ended by a callback registered on the same future the test blocks on, so its
    * arrival in the exporter can lag slightly behind the blocking call returning.
    */
  private def singleReportedSpan(): SpanData = eventually() {
    val spans = testTelemetrySetup.reportedSpans()
    spans should have size 1
    spans.head
  }

  private def decisionMarkers = metricsFactory.metrics
    .meters(metrics.decisions.info.name)
    .getOrElse(metricsContext, sys.error(s"Metric ${metrics.decisions.info.name} not found"))
    .markers

  private def enforcementCheckDurations = metricsFactory.metrics
    .timers(metrics.enforcementCheckDuration.info.name)
    .getOrElse(
      metricsContext,
      sys.error(s"Metric ${metrics.enforcementCheckDuration.info.name} not found"),
    )
    .data

  private def decisionContext(outcome: String, reason: Option[String]): MetricsContext = {
    val withOutcome =
      MetricsContext.Empty.withExtraLabels(TrafficEnforcementOutcome.OutcomeAttribute -> outcome)
    reason.fold(withOutcome)(r =>
      withOutcome.withExtraLabels(TrafficEnforcementOutcome.ReasonAttribute -> r)
    )
  }

  private def info(actAs: Set[LfPartyId]): SubmissionAccountingInfo =
    SubmissionAccountingInfo(
      changeId.copy(actAs = actAs),
      submissionId,
      SubmissionAccounting.forSubmitter(None, actAs, adminParty),
    )

  private def stubLookup(client: RichTrafficServiceClient, response: GetAccountResponse): Unit =
    when(client.getAccount(any[GetAccountRequest])(any[TraceContext]))
      .thenReturn(EitherT.rightT[FutureUnlessShutdown, GrpcError](response))
      .discard

  private def stubRegistration(
      client: RichTrafficServiceClient,
      result: Either[GrpcError, RegisterSubmissionResponse],
  ): Unit =
    when(
      client.registerSubmission(any[RegisterSubmissionRequest])(any[TraceContext])
    ).thenReturn(EitherT.fromEither[FutureUnlessShutdown](result)).discard

  private def verifyDecision(row: DecisionRow): Assertion = {
    val client = mock[RichTrafficServiceClient]
    row.registerResult.foreach(stubRegistration(client, _))
    row.estimateLookup.foreach(stubLookup(client, _))
    val backend = newBackend(
      trafficServiceClient = client,
      enforceCostOnSubmissions = row.enforceCostOnSubmissions,
      rejectMultiPartySubmissions = row.rejectMultiPartySubmissions,
      allowSubmissionsOnDegradation = row.allowSubmissionsOnDegradation,
    )

    def call(): Either[RpcError, Unit] =
      if (row.estimateLookup.isDefined)
        backend
          .checkEstimatedCost(
            SubmissionAccounting.forSubmitter(None, row.actAs, adminParty),
            trafficCost,
          )
          .futureValueUS
      else backend.registerSubmission(info(row.actAs), trafficCost).futureValueUS

    def invoke(): Assertion = row.expectedResult match {
      case Allowed => call() shouldBe Right(())
      case RejectedWith(errorCodeId) => call().left.value.code.id shouldBe errorCodeId
    }

    row.expectedLog.fold(invoke())(assertion => loggerFactory.assertLogs(invoke(), assertion))

    // A row without a stubbed call decides without asking the traffic service.
    if (row.registerResult.isEmpty && row.estimateLookup.isEmpty) verifyZeroInteractions(client)

    val span = singleReportedSpan()
    span.getName shouldBe row.expectedSpanName
    span.getAttributes.get(OutcomeKey) shouldBe row.expectedOutcome
    Option(span.getAttributes.get(ReasonKey)) shouldBe row.expectedReason
    // TEA errors come back as a Left, so the span itself never fails.
    span.getStatus.getStatusCode shouldBe StatusCode.UNSET
    span.getEvents.asScala.map(_.getName) shouldBe row.expectedSpanEvents

    // Size, not just the marker: a second recordOutcome under other labels has to fail here.
    decisionMarkers should have size 1
    decisionMarkers(
      decisionContext(row.expectedOutcome, row.expectedReason)
    ).longValue() shouldBe 1L

    enforcementCheckDurations.recordedValues(
      MetricsContext("status" -> row.expectedTimerStatus)
    ) should have size 1
  }

  private val registered = Some(Right(RegisterSubmissionResponse()))

  /** The same `recordOutcome` call feeds the span and metrics, so each scenario asserts both to
    * catch any drift.
    */
  private val decisionRows = Seq(
    DecisionRow(
      description = "record an accepted outcome with no reason, on sufficient balance",
      actAs = Set(alice),
      registerResult = registered,
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Accepted,
      expectedReason = None,
      expectedTimerStatus = SuccessStatus,
    ),
    DecisionRow(
      description = "record a rejected/insufficient_balance outcome, without failing the span",
      actAs = Set(alice),
      registerResult = Some(Left(insufficientBalanceError)),
      expectedResult = RejectedWith(TrafficEnforcementErrors.InsufficientBalance.code.id),
      expectedOutcome = TrafficEnforcementOutcome.Rejected,
      expectedReason = Some(TrafficEnforcementOutcome.InsufficientBalance),
      expectedTimerStatus = TrafficEnforcementErrors.InsufficientBalance.code.id,
    ),
    DecisionRow(
      description =
        "record a rejected/correlation_conflict outcome for a marker, even when degradation is on",
      actAs = Set(alice, bob),
      rejectMultiPartySubmissions = false,
      allowSubmissionsOnDegradation = true,
      registerResult = Some(Left(correlationConflictError)),
      expectedResult = RejectedWith(TrafficEnforcementErrors.CorrelationConflict.code.id),
      expectedOutcome = TrafficEnforcementOutcome.Rejected,
      expectedReason = Some(TrafficEnforcementOutcome.CorrelationConflict),
      expectedTimerStatus = TrafficEnforcementErrors.CorrelationConflict.code.id,
    ),
    DecisionRow(
      description =
        "record a rejected/multi_party_submission outcome, without contacting the traffic service",
      actAs = Set(alice, bob),
      expectedResult = RejectedWith(TrafficEnforcementErrors.MultiPartySubmissionRejected.code.id),
      expectedOutcome = TrafficEnforcementOutcome.Rejected,
      expectedReason = Some(TrafficEnforcementOutcome.MultiPartySubmission),
      expectedTimerStatus = TrafficEnforcementErrors.MultiPartySubmissionRejected.code.id,
    ),
    DecisionRow(
      description = "record a skipped/admin_party outcome",
      actAs = Set(adminParty),
      registerResult = registered,
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Skipped,
      expectedReason = Some(TrafficEnforcementOutcome.AdminParty),
      expectedTimerStatus = SuccessStatus,
    ),
    DecisionRow(
      description = "record a skipped/non_singleton_act_as outcome",
      actAs = Set(alice, bob),
      rejectMultiPartySubmissions = false,
      registerResult = registered,
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Skipped,
      expectedReason = Some(TrafficEnforcementOutcome.NonSingletonActAs),
      expectedTimerStatus = SuccessStatus,
    ),
    DecisionRow(
      description = "record a degraded/marker_write_failed outcome, even when degradation is off",
      actAs = Set(alice, bob),
      rejectMultiPartySubmissions = false,
      registerResult = Some(Left(unavailableError)),
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Degraded,
      expectedReason = Some(TrafficEnforcementOutcome.MarkerWriteFailed),
      expectedTimerStatus = SuccessStatus,
      expectedLog = Some(_.warningMessage should include("no-account marker")),
    ),
    DecisionRow(
      description = "record a skipped/enforcement_disabled outcome",
      actAs = Set(alice),
      enforceCostOnSubmissions = false,
      rejectMultiPartySubmissions = false,
      registerResult = registered,
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Skipped,
      expectedReason = Some(TrafficEnforcementOutcome.EnforcementDisabled),
      expectedTimerStatus = SuccessStatus,
    ),
    DecisionRow(
      description = "record a degraded/lookup_unavailable outcome on a decoded transient failure",
      actAs = Set(alice),
      allowSubmissionsOnDegradation = true,
      registerResult = Some(Left(transientFailureError)),
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Degraded,
      expectedReason = Some(TrafficEnforcementOutcome.LookupUnavailable),
      expectedTimerStatus = SuccessStatus,
      expectedLog = Some(_.warningMessage should include("degrading")),
    ),
    DecisionRow(
      description = "record a degraded/lookup_unavailable outcome on an undecodable server error",
      actAs = Set(alice),
      allowSubmissionsOnDegradation = true,
      registerResult = Some(Left(undecodableServerError)),
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Degraded,
      expectedReason = Some(TrafficEnforcementOutcome.LookupUnavailable),
      expectedTimerStatus = SuccessStatus,
      expectedLog = Some(_.warningMessage should include("degrading")),
    ),
    DecisionRow(
      description = "record a failed/lookup_failed outcome and a redacted Left on a refusal",
      actAs = Set(alice),
      allowSubmissionsOnDegradation = true,
      registerResult = Some(Left(refusedByServerError)),
      // A redacted status decodes without its error id.
      expectedResult = RejectedWith("NA"),
      expectedOutcome = TrafficEnforcementOutcome.Failed,
      expectedReason = Some(TrafficEnforcementOutcome.LookupFailed),
      expectedTimerStatus = TrafficEnforcementOutcome.Failed,
      // Only this construction is redacted by normalizeTeaError, which is what logs at ERROR.
      expectedLog =
        Some(_.errorMessage should include("Error in submitting request to traffic service")),
    ),
    DecisionRow(
      description = "record a failed/lookup_failed outcome on an undecodable client error",
      actAs = Set(alice),
      allowSubmissionsOnDegradation = true,
      registerResult = Some(Left(clientError)),
      expectedResult = RejectedWith(LedgerApiErrors.InternalError.id),
      expectedOutcome = TrafficEnforcementOutcome.Failed,
      expectedReason = Some(TrafficEnforcementOutcome.LookupFailed),
      expectedTimerStatus = TrafficEnforcementOutcome.Failed,
      expectedLog =
        Some(_.errorMessage should include("Error in submitting request to traffic service")),
    ),
    DecisionRow(
      description = "record an accepted outcome for an estimated cost check",
      actAs = Set(alice),
      estimateLookup = Some(GetAccountResponse(accountId = alice, balance = trafficCost)),
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Accepted,
      expectedReason = None,
      expectedTimerStatus = SuccessStatus,
    ),
  )

  "the enforcement decision telemetry" should {

    decisionRows.foreach(row => row.description in verifyDecision(row))

    "run the registration under a trace context that is a child of the enforcement span" in {
      val client = mock[RichTrafficServiceClient]
      stubRegistration(client, Right(RegisterSubmissionResponse()))
      val backend = newBackend(trafficServiceClient = client)

      backend.registerSubmission(info(Set(alice)), trafficCost).futureValueUS shouldBe Right(())

      val traceContextCaptor = ArgCaptor[TraceContext]
      verify(client).registerSubmission(any[RegisterSubmissionRequest])(traceContextCaptor)

      val span = singleReportedSpan()
      traceContextCaptor.value.spanId shouldBe Some(span.getSpanId)
    }

    "record one latency sample per enforcement check" in {
      val client = mock[RichTrafficServiceClient]
      when(
        client.registerSubmission(any[RegisterSubmissionRequest])(any[TraceContext])
      ).thenAnswer { (_: RegisterSubmissionRequest) =>
        Threading.sleep(scala.util.Random.nextLong(10) + 1)
        EitherT.rightT[FutureUnlessShutdown, GrpcError](RegisterSubmissionResponse())
      }.discard
      val backend = newBackend(trafficServiceClient = client)

      val times = 5
      (1 to times).foreach { _ =>
        backend.registerSubmission(info(Set(alice)), trafficCost).futureValueUS shouldBe Right(())
      }

      val values = enforcementCheckDurations.recordedValues(
        MetricsContext("status" -> SuccessStatus)
      )
      values.size shouldEqual times
      forAll(values)(_ should be >= 1L)
    }
  }
}

object TrafficEnforcementBackendTelemetryTest {

  private sealed trait ExpectedResult
  private case object Allowed extends ExpectedResult
  private final case class RejectedWith(errorCodeId: String) extends ExpectedResult

  private final case class DecisionRow(
      description: String,
      actAs: Set[LfPartyId],
      enforceCostOnSubmissions: Boolean = true,
      rejectMultiPartySubmissions: Boolean = true,
      allowSubmissionsOnDegradation: Boolean = false,
      registerResult: Option[Either[GrpcError, RegisterSubmissionResponse]] = None,
      estimateLookup: Option[GetAccountResponse] = None,
      expectedResult: ExpectedResult,
      expectedOutcome: String,
      expectedReason: Option[String],
      expectedTimerStatus: String,
      expectedLog: Option[LogEntry => Assertion] = None,
  ) {
    def expectedSpanName: String =
      if (estimateLookup.isDefined) TrafficEnforcementBackend.CheckEstimatedCostSpanName
      else TrafficEnforcementBackend.RegisterSubmissionSpanName

    def expectedSpanEvents: Seq[String] =
      if (
        expectedOutcome == TrafficEnforcementOutcome.Degraded ||
        expectedOutcome == TrafficEnforcementOutcome.Failed
      ) Seq("exception")
      else Seq.empty
  }
}
