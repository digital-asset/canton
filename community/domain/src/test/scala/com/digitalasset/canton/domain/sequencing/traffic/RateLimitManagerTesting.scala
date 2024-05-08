// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.DamlGrpcServerMetrics
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.domain.sequencing.traffic.EnterpriseSequencerRateLimitManager.BalanceUpdateClient
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficBalanceStore
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficBalanceStore
import com.digitalasset.canton.metrics.CantonLabeledMetricsFactory.{
  CantonOpenTelemetryMetricsFactory,
  NoOpMetricsFactory,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.traffic.EventCostCalculator
import com.digitalasset.canton.{BaseTest, HasExecutionContext}

trait RateLimitManagerTesting { this: BaseTest with HasExecutionContext =>
  lazy val trafficBalanceStore = new InMemoryTrafficBalanceStore(loggerFactory)
  def mkTrafficBalanceManager(store: TrafficBalanceStore) = new TrafficBalanceManager(
    store,
    new SimClock(CantonTimestamp.Epoch, loggerFactory),
    SequencerTrafficConfig(),
    futureSupervisor,
    SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
    timeouts,
    loggerFactory,
  )
  lazy val defaultTrafficBalanceManager = mkTrafficBalanceManager(trafficBalanceStore)
  def mkBalanceUpdateClient(manager: TrafficBalanceManager): BalanceUpdateClient =
    new BalanceUpdateClientImpl(manager, loggerFactory)
  lazy val defaultBalanceUpdateClient = mkBalanceUpdateClient(defaultTrafficBalanceManager)

  lazy val defaultRateLimiter = mkRateLimiter(trafficBalanceStore)
  def defaultRateLimiterWithEventCostCalculator(eventCostCalculator: EventCostCalculator) =
    new EnterpriseSequencerRateLimitManager(
      defaultBalanceUpdateClient,
      loggerFactory,
      futureSupervisor,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      eventCostCalculator = eventCostCalculator,
    )

  def mkRateLimiter(store: TrafficBalanceStore) =
    new EnterpriseSequencerRateLimitManager(
      mkBalanceUpdateClient(mkTrafficBalanceManager(store)),
      loggerFactory,
      futureSupervisor,
      timeouts,
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      eventCostCalculator = new EventCostCalculator(loggerFactory),
    )

  def mkRateLimiter(
      manager: TrafficBalanceManager,
      metricsFactory: CantonOpenTelemetryMetricsFactory,
  ) = {
    new EnterpriseSequencerRateLimitManager(
      mkBalanceUpdateClient(manager),
      loggerFactory,
      futureSupervisor,
      timeouts,
      new SequencerMetrics(
        MetricName("sequencer-rate-limit-manager-test"),
        metricsFactory,
        new DamlGrpcServerMetrics(NoOpMetricsFactory, "sequencer"),
        new HealthMetrics(NoOpMetricsFactory),
      ),
      eventCostCalculator = new EventCostCalculator(loggerFactory),
    )
  }
}
