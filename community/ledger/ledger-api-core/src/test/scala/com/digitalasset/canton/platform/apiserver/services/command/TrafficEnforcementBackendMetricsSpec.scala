// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.platform.apiserver.services.metrics.{
  TrafficEnforcementInventory,
  TrafficEnforcementMetrics,
}
import com.digitalasset.canton.tea.TrafficEnforcementErrors.InsufficientBalance
import com.digitalasset.canton.tea.v1.{GetAccountRequest, GetAccountResponse}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.flatspec.AnyFlatSpec

class TrafficEnforcementBackendMetricsSpec
    extends AnyFlatSpec
    with BaseTest
    with HasExecutionContext {

  class TestContext {
    val trafficServiceClient = mock[RichTrafficServiceClient]
    val adminParty = LfPartyId.assertFromString("admin-party")
    val accountId1 = LfPartyId.assertFromString("test-account-1")
    val accountId2 = LfPartyId.assertFromString("test-account-2")
    val trafficCost = 100L

    val metricsFactory = new InMemoryMetricsFactory
    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    val parentName = MetricName("test")
    val metrics = new TrafficEnforcementMetrics(
      inventory = new TrafficEnforcementInventory(parentName)(new HistogramInventory()),
      metricsFactory = metricsFactory,
    )(metricsContext)

    val backend = new TrafficEnforcementBackend(
      enforceCostOnSubmissions = true,
      rejectMultiPartySubmissions = false,
      allowSubmissionsOnDegradation = true,
      trafficServiceClient = trafficServiceClient,
      adminParty = adminParty,
      metrics = metrics,
      timeouts = DefaultProcessingTimeouts.testing,
      loggerFactory = loggerFactory,
    )

    def metricMeter(name: MetricName) =
      metricsFactory.metrics
        .meters(name)
        .getOrElse(metricsContext, sys.error(s"Metric $name not found"))
        .markers

    lazy val balanceLookupMetrics = metricMeter(metrics.balanceLookups.info.name)
    lazy val insufficientBalanceMetrics = metricMeter(
      metrics.insufficientBalanceRejections.info.name
    )
    lazy val allowedSubmissionOnLookupFailuresMetrics =
      metricMeter(metrics.allowedSubmissionOnLookupFailures.info.name)
    lazy val enforcementCheckDurationMetrics = metricsFactory.metrics
      .timers(metrics.enforcementCheckDuration.info.name)
      .getOrElse(
        metricsContext,
        sys.error(s"Metric ${metrics.enforcementCheckDuration.info.name} not found"),
      )
      .data

    val successContext = MetricsContext("status" -> "success")

    def mockTeaResponse(accountId: String, balance: Long) =
      when(trafficServiceClient.getAccount(GetAccountRequest(accountId)))
        .thenReturn(
          EitherT.pure[FutureUnlessShutdown, GrpcError](
            GetAccountResponse(
              accountId = accountId,
              balance = balance,
            )
          )
        )

    def mockTeaResponseWithDelay(accountId: String, balance: Long, minDelay: Long, maxDelay: Long) =
      when(trafficServiceClient.getAccount(GetAccountRequest(accountId)))
        .thenAnswer { (_: GetAccountRequest) =>
          val delay = scala.util.Random.nextLong(maxDelay - minDelay + 1) + minDelay
          Threading.sleep(delay)
          EitherT.pure[FutureUnlessShutdown, GrpcError](
            GetAccountResponse(
              accountId = accountId,
              balance = balance,
            )
          )
        }
  }

  behavior of "TrafficEnforcementBackend Metrics"

  it should "register metrics for balance lookups when account is checked" in new TestContext {
    mockTeaResponse(accountId1, trafficCost)

    backend.validateTraffic(Seq(accountId1), trafficCost).value.futureValueUS

    balanceLookupMetrics should have size 1
    balanceLookupMetrics(MetricsContext.Empty).longValue() shouldBe 1L

    insufficientBalanceMetrics shouldBe empty

    allowedSubmissionOnLookupFailuresMetrics shouldBe empty

    enforcementCheckDurationMetrics.recordedValues(successContext) should have size 1
  }

  it should "register metrics for insufficient balance rejections when account balance is insufficient" in new TestContext {
    mockTeaResponse(accountId1, trafficCost - 1)

    backend.validateTraffic(Seq(accountId1), trafficCost).value.futureValueUS

    balanceLookupMetrics should have size 1
    balanceLookupMetrics(MetricsContext.Empty).longValue() shouldBe 1L

    insufficientBalanceMetrics should have size 1
    insufficientBalanceMetrics(MetricsContext.Empty).longValue() shouldBe 1L

    allowedSubmissionOnLookupFailuresMetrics shouldBe empty

    enforcementCheckDurationMetrics.recordedValues(
      MetricsContext("status" -> InsufficientBalance.code.id)
    ) should have size 1
  }

  it should "register metrics for allowed submission on lookup failures when degradation is allowed" in new TestContext {
    val error = GrpcError.GrpcServerError(
      request = "test-request",
      serverName = "test-server",
      status = io.grpc.Status.UNKNOWN,
      optTrailers = None,
      decodedCantonError = None,
    )
    when(trafficServiceClient.getAccount(GetAccountRequest(accountId1)))
      .thenReturn(
        EitherT.leftT[FutureUnlessShutdown, GetAccountResponse](error)
      )

    loggerFactory.assertLogs(
      backend.validateTraffic(Seq(accountId1), trafficCost).value.futureValueUS,
      _.warningMessage should include("degrading"),
    )
    balanceLookupMetrics should have size 1
    balanceLookupMetrics(MetricsContext.Empty).longValue() shouldBe 1L

    insufficientBalanceMetrics shouldBe empty

    allowedSubmissionOnLookupFailuresMetrics should have size 1
    allowedSubmissionOnLookupFailuresMetrics(MetricsContext.Empty).longValue() shouldBe 1L

    enforcementCheckDurationMetrics.recordedValues(successContext) should have size 1
  }

  it should "register latency metrics for enforcement checks" in new TestContext {
    mockTeaResponseWithDelay(accountId1, trafficCost, minDelay = 1, maxDelay = 10)

    // Repeat call a few times to ensure we have enough data points for the histogram
    val times = 5
    (1 to times).foreach { _ =>
      backend.validateTraffic(Seq(accountId1), trafficCost).value.futureValueUS
    }

    val values =
      enforcementCheckDurationMetrics.recordedValues(MetricsContext("status" -> "success"))
    values.size shouldEqual times

    forAll(values) { value =>
      value should be >= 1L
    }
  }

  it should "register latency metrics for failures" in new TestContext {}
}
