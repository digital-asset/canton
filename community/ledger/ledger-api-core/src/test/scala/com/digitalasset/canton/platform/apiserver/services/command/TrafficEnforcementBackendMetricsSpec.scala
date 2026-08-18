// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import cats.data.EitherT
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{MetricName, MetricsContext}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.platform.apiserver.client.RichTrafficServiceClient
import com.digitalasset.canton.platform.apiserver.services.metrics.TrafficEnforcementMetrics
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
    val accountId1 = "test-account-1"
    val accountId2 = "test-account-2"
    val party = LfPartyId.assertFromString(accountId1)
    val trafficCost = 100L

    val metricsFactory = new InMemoryMetricsFactory
    implicit val metricsContext: MetricsContext = MetricsContext.Empty
    val metrics = new TrafficEnforcementMetrics(
      parent = MetricName("test"),
      metricsFactory = metricsFactory,
    )

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
        .get(metricsContext)
        .fold(sys.error(s"Metric $name not found")) { c =>
          c.markers
        }

    lazy val balanceLookupMetrics = metricMeter(metrics.balanceLookupsName)
    lazy val notEnoughTrafficMetrics = metricMeter(metrics.notEnoughTrafficName)

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
  }

  behavior of "Metrics"

  it should "register metrics for balance lookups when account is checked" in new TestContext {
    mockTeaResponse(accountId1, trafficCost)

    backend.validateTraffic(accountId1, trafficCost).value.futureValueUS

    balanceLookupMetrics should have size 1
    balanceLookupMetrics(MetricsContext.Empty).longValue() shouldBe 1L

    notEnoughTrafficMetrics shouldBe empty
  }

  it should "register metrics for not enough traffic when account balance is insufficient" in new TestContext {
    mockTeaResponse(accountId1, trafficCost - 1)

    backend.validateTraffic(accountId1, trafficCost).value.futureValueUS

    balanceLookupMetrics should have size 1
    balanceLookupMetrics(MetricsContext.Empty).longValue() shouldBe 1L

    notEnoughTrafficMetrics should have size 1
    notEnoughTrafficMetrics(MetricsContext.Empty).longValue() shouldBe 1L
  }
}
