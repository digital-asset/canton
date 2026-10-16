// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic

import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.sequencing.traffic.EventCostCalculator
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.store.InMemorySequencerStore
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.memory.{
  InMemoryTrafficConsumedStore,
  InMemoryTrafficPurchasedStore,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.{
  TrafficConsumedStore,
  TrafficPurchasedStore,
}
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingTopology}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext}

trait RateLimitManagerTesting { this: BaseTest & HasExecutionContext =>
  lazy val trafficPurchasedStore = new InMemoryTrafficPurchasedStore(loggerFactory)
  lazy val trafficConsumedStore = new InMemoryTrafficConsumedStore(loggerFactory)
  lazy val sequencerTrafficConfig = SequencerTrafficConfig()
  private val sequencerStore = new InMemorySequencerStore(
    protocolVersion = testedProtocolVersion,
    sequencerMember = DefaultTestIdentities.sequencerId,
    blockSequencerMode = true,
    loggerFactory = loggerFactory,
    timeouts = timeouts,
    sequencerMetrics = SequencerMetrics.noop("rate-limit-manager-testing"),
  )
  // Tests purchase traffic for members that are not registered in `sequencerStore`, which the
  // manager only allows from PV37 onwards (below that, an onboarding member cannot be represented
  // in the v30 sequencer snapshot).
  def mkTrafficPurchasedManager(store: TrafficPurchasedStore) = new TrafficPurchasedManager(
    store,
    sequencerStore,
    sequencerTrafficConfig,
    futureSupervisor,
    SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
    timeouts,
    lsuSequencingBounds = None,
    ProtocolVersion.v37,
    loggerFactory,
  )
  lazy val defaultTrafficPurchasedManager = mkTrafficPurchasedManager(trafficPurchasedStore)

  lazy val defaultRateLimiter = mkRateLimiter(trafficPurchasedStore)

  lazy val cryptoClient =
    TestingTopology()
      .build(loggerFactory)
      .forOwnerAndSynchronizer(DefaultTestIdentities.participant1)

  def defaultRateLimiterWithEventCostCalculator(eventCostCalculator: EventCostCalculator) =
    new SequencerRateLimitManagerImpl(
      defaultTrafficPurchasedManager,
      trafficConsumedStore,
      loggerFactory,
      timeouts,
      BatchingConfig(),
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      cryptoClient,
      testedProtocolVersion,
      sequencerTrafficConfig,
      eventCostCalculator = eventCostCalculator,
      lsuSequencingBounds = None,
    )

  def mkRateLimiter(store: TrafficPurchasedStore) =
    new SequencerRateLimitManagerImpl(
      mkTrafficPurchasedManager(store),
      trafficConsumedStore,
      loggerFactory,
      timeouts,
      BatchingConfig(),
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      cryptoClient,
      testedProtocolVersion,
      sequencerTrafficConfig,
      eventCostCalculator = new EventCostCalculator(loggerFactory),
      lsuSequencingBounds = None,
    )

  def mkRateLimiter(
      manager: TrafficPurchasedManager,
      trafficConsumedStore: TrafficConsumedStore = trafficConsumedStore,
      eventCostCalculator: EventCostCalculator = new EventCostCalculator(loggerFactory),
  ) =
    new SequencerRateLimitManagerImpl(
      manager,
      trafficConsumedStore,
      loggerFactory,
      timeouts,
      BatchingConfig(),
      SequencerMetrics.noop("sequencer-rate-limit-manager-test"),
      cryptoClient,
      testedProtocolVersion,
      sequencerTrafficConfig,
      eventCostCalculator = eventCostCalculator,
      lsuSequencingBounds = None,
    )
}
