// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
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
import com.digitalasset.canton.tea.v1.{GetAccountRequest, GetAccountResponse}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import io.grpc.Status
import io.opentelemetry.api.common.AttributeKey.stringKey
import io.opentelemetry.api.trace.{StatusCode, Tracer}
import io.opentelemetry.sdk.trace.data.SpanData
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{Assertion, BeforeAndAfterEach}

import scala.concurrent.ExecutionContext
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

  private val SpanName = TrafficEnforcementBackend.EnforcementSpanName
  private val OutcomeKey = stringKey("canton.traffic_enforcement_outcome")
  private val ReasonKey = stringKey("canton.traffic_enforcement_reason")
  private val SuccessStatus = "success"

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  private val transientFailureError = GrpcError(
    request = "get-account",
    serverName = "traffic-service",
    e = TrafficEnforcementErrors.TransientFailure
      .Reject(new RuntimeException("db down"))
      .asGrpcError,
  )

  private val undecodableServerError = GrpcError.GrpcServerError(
    request = "get-account",
    serverName = "traffic-service",
    status = Status.UNKNOWN,
    optTrailers = None,
    decodedCantonError = None,
  )

  private val refusedByServerError =
    GrpcError("get-account", "traffic-service", Status.PERMISSION_DENIED.asRuntimeException())

  private val clientError = GrpcError.GrpcClientError(
    request = "get-account",
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
    new TrafficEnforcementBackend(
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

  private def stubLookup(
      client: RichTrafficServiceClient,
      result: Either[GrpcError, GetAccountResponse],
  ): Unit =
    when(client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext]))
      .thenReturn(EitherT.fromEither[FutureUnlessShutdown](result))
      .discard

  private def verifyDecision(row: DecisionRow): Assertion = {
    val client = mock[RichTrafficServiceClient]
    row.lookupResult.foreach(stubLookup(client, _))
    val backend = newBackend(
      trafficServiceClient = client,
      enforceCostOnSubmissions = row.enforceCostOnSubmissions,
      rejectMultiPartySubmissions = row.rejectMultiPartySubmissions,
      allowSubmissionsOnDegradation = row.allowSubmissionsOnDegradation,
    )

    def invoke(): Assertion = row.expectedResult match {
      case Allowed =>
        backend.validateTraffic(row.actAs, trafficCost).futureValueUS shouldBe Right(())
      case RejectedWith(errorCodeId) =>
        backend
          .validateTraffic(row.actAs, trafficCost)
          .futureValueUS
          .left
          .value
          .code
          .id shouldBe errorCodeId
      case ThrowsException =>
        a[Exception] should be thrownBy backend
          .validateTraffic(row.actAs, trafficCost)
          .futureValueUS
    }

    row.expectedLog.fold(invoke())(assertion => loggerFactory.assertLogs(invoke(), assertion))

    // A row without a stubbed lookup decides without asking the traffic service.
    if (row.lookupResult.isEmpty) verifyZeroInteractions(client)

    val span = singleReportedSpan()
    span.getName shouldBe SpanName
    span.getAttributes.get(OutcomeKey) shouldBe row.expectedOutcome
    Option(span.getAttributes.get(ReasonKey)) shouldBe row.expectedReason
    span.getStatus.getStatusCode shouldBe row.expectedSpanStatus

    // Size, not just the marker: a second recordOutcome under other labels has to fail here.
    decisionMarkers should have size 1
    decisionMarkers(
      decisionContext(row.expectedOutcome, row.expectedReason)
    ).longValue() shouldBe 1L

    enforcementCheckDurations.recordedValues(
      MetricsContext("status" -> row.expectedTimerStatus)
    ) should have size 1
  }

  /** The same `recordOutcome` call feeds the span and metrics, so each scenario asserts both to
    * catch any drift.
    */
  private val decisionRows = Seq(
    DecisionRow(
      description = "record an accepted outcome with no reason, on sufficient balance",
      actAs = Seq(alice),
      lookupResult = Some(Right(GetAccountResponse(accountId = alice, balance = trafficCost))),
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Accepted,
      expectedReason = None,
      expectedTimerStatus = SuccessStatus,
    ),
    DecisionRow(
      description = "record a rejected/insufficient_balance outcome, without failing the span",
      actAs = Seq(alice),
      lookupResult = Some(Right(GetAccountResponse(accountId = alice, balance = trafficCost - 1))),
      expectedResult = RejectedWith(TrafficEnforcementErrors.InsufficientBalance.code.id),
      expectedOutcome = TrafficEnforcementOutcome.Rejected,
      expectedReason = Some(TrafficEnforcementOutcome.InsufficientBalance),
      expectedTimerStatus = TrafficEnforcementErrors.InsufficientBalance.code.id,
    ),
    DecisionRow(
      description =
        "record a rejected/multi_party_submission outcome, without looking up an account",
      actAs = Seq(alice, bob),
      expectedResult = RejectedWith(TrafficEnforcementErrors.MultiPartySubmissionRejected.code.id),
      expectedOutcome = TrafficEnforcementOutcome.Rejected,
      expectedReason = Some(TrafficEnforcementOutcome.MultiPartySubmission),
      expectedTimerStatus = TrafficEnforcementErrors.MultiPartySubmissionRejected.code.id,
    ),
    DecisionRow(
      description = "record a skipped/admin_party outcome, without looking up an account",
      actAs = Seq(adminParty),
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Skipped,
      expectedReason = Some(TrafficEnforcementOutcome.AdminParty),
      expectedTimerStatus = SuccessStatus,
    ),
    DecisionRow(
      description = "record a skipped/non_singleton_act_as outcome, without looking up an account",
      actAs = Seq(alice, bob),
      rejectMultiPartySubmissions = false,
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Skipped,
      expectedReason = Some(TrafficEnforcementOutcome.NonSingletonActAs),
      expectedTimerStatus = SuccessStatus,
    ),
    DecisionRow(
      description = "record a skipped/enforcement_disabled outcome, without looking up an account",
      actAs = Seq(alice),
      enforceCostOnSubmissions = false,
      rejectMultiPartySubmissions = false,
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Skipped,
      expectedReason = Some(TrafficEnforcementOutcome.EnforcementDisabled),
      expectedTimerStatus = SuccessStatus,
    ),
    DecisionRow(
      description = "record a degraded/lookup_unavailable outcome on a decoded transient failure",
      actAs = Seq(alice),
      allowSubmissionsOnDegradation = true,
      lookupResult = Some(Left(transientFailureError)),
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Degraded,
      expectedReason = Some(TrafficEnforcementOutcome.LookupUnavailable),
      expectedTimerStatus = SuccessStatus,
      expectedLog = Some(_.warningMessage should include("degrading")),
    ),
    DecisionRow(
      description = "record a degraded/lookup_unavailable outcome on an undecodable server error",
      actAs = Seq(alice),
      allowSubmissionsOnDegradation = true,
      lookupResult = Some(Left(undecodableServerError)),
      expectedResult = Allowed,
      expectedOutcome = TrafficEnforcementOutcome.Degraded,
      expectedReason = Some(TrafficEnforcementOutcome.LookupUnavailable),
      expectedTimerStatus = SuccessStatus,
      expectedLog = Some(_.warningMessage should include("degrading")),
    ),
    DecisionRow(
      description = "record a failed/lookup_failed outcome and fail the span on a redacted refusal",
      actAs = Seq(alice),
      allowSubmissionsOnDegradation = true,
      lookupResult = Some(Left(refusedByServerError)),
      expectedResult = ThrowsException,
      expectedOutcome = TrafficEnforcementOutcome.Failed,
      expectedReason = Some(TrafficEnforcementOutcome.LookupFailed),
      expectedTimerStatus = TrafficEnforcementOutcome.Failed,
      // Only this construction is redacted by normalizeTeaError, which is what logs at ERROR.
      expectedLog =
        Some(_.errorMessage should include("Error in submitting request to traffic service")),
    ),
    DecisionRow(
      description =
        "record a failed/lookup_failed outcome and fail the span on a passed-through client error",
      actAs = Seq(alice),
      allowSubmissionsOnDegradation = true,
      lookupResult = Some(Left(clientError)),
      expectedResult = ThrowsException,
      expectedOutcome = TrafficEnforcementOutcome.Failed,
      expectedReason = Some(TrafficEnforcementOutcome.LookupFailed),
      expectedTimerStatus = TrafficEnforcementOutcome.Failed,
    ),
  )

  "the enforcement decision telemetry" should {

    decisionRows.foreach(row => row.description in verifyDecision(row))

    "attach the lookup failure to the degraded span as span data, not as a span failure" in {
      val client = mock[RichTrafficServiceClient]
      stubLookup(client, Left(transientFailureError))
      val backend = newBackend(trafficServiceClient = client, allowSubmissionsOnDegradation = true)

      loggerFactory.assertLogs(
        backend.validateTraffic(actAs = Seq(alice), trafficCost = trafficCost).futureValueUS,
        _.warningMessage should include("degrading"),
      )

      val span = singleReportedSpan()
      span.getStatus.getStatusCode shouldBe StatusCode.UNSET
      span.getEvents.asScala.toList.loneElement.getName shouldBe "exception"
    }

    "run the account lookup under a trace context that is a child of the enforcement span" in {
      val client = mock[RichTrafficServiceClient]
      stubLookup(client, Right(GetAccountResponse(accountId = alice, balance = trafficCost)))
      val backend = newBackend(trafficServiceClient = client)

      backend
        .validateTraffic(actAs = Seq(alice), trafficCost = trafficCost)
        .futureValueUS shouldBe Right(())

      val traceContextCaptor = ArgCaptor[TraceContext]
      verify(client).getAccount(any[GetAccountRequest])(traceContextCaptor, any[ExecutionContext])

      val span = singleReportedSpan()
      traceContextCaptor.value.spanId shouldBe Some(span.getSpanId)
    }

    "record one latency sample per enforcement check" in {
      val client = mock[RichTrafficServiceClient]
      when(
        client.getAccount(any[GetAccountRequest])(any[TraceContext], any[ExecutionContext])
      ).thenAnswer { (_: GetAccountRequest) =>
        Threading.sleep(scala.util.Random.nextLong(10) + 1)
        EitherT.rightT[FutureUnlessShutdown, GrpcError](
          GetAccountResponse(accountId = alice, balance = trafficCost)
        )
      }.discard
      val backend = newBackend(trafficServiceClient = client)

      val times = 5
      (1 to times).foreach { _ =>
        backend
          .validateTraffic(actAs = Seq(alice), trafficCost = trafficCost)
          .futureValueUS shouldBe Right(())
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
  private case object ThrowsException extends ExpectedResult

  private final case class DecisionRow(
      description: String,
      actAs: Seq[LfPartyId],
      enforceCostOnSubmissions: Boolean = true,
      rejectMultiPartySubmissions: Boolean = true,
      allowSubmissionsOnDegradation: Boolean = false,
      lookupResult: Option[Either[GrpcError, GetAccountResponse]] = None,
      expectedResult: ExpectedResult,
      expectedOutcome: String,
      expectedReason: Option[String],
      expectedTimerStatus: String,
      expectedLog: Option[LogEntry => Assertion] = None,
  ) {
    def expectedSpanStatus: StatusCode =
      if (expectedResult == ThrowsException) StatusCode.ERROR else StatusCode.UNSET
  }
}
