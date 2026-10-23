// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea

import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.platform.apiserver.services.metrics.{
  TrafficEnforcementInventory,
  TrafficEnforcementMetrics,
}
import com.digitalasset.canton.tea.TrafficEnforcementErrors.UnsafeToPrune
import com.digitalasset.canton.tea.projection.memory.TeaMemoryTrafficStore
import com.digitalasset.canton.tea.projection.{
  AccountId,
  CorrelationAccount,
  CorrelationKey,
  EventId,
  EventSource,
  SubmissionCorrelation,
  TrafficDelta,
}
import com.digitalasset.canton.tea.v1.{PruneRequest, PruneResponse}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, DefaultDamlValues, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

class TrafficEnforcementServiceTest extends AsyncWordSpec with BaseTest with FailOnShutdown {

  "prune" should {
    "only prune data older than the minimum retention" in {
      val retention = NonNegativeFiniteDuration.ofDays(7)
      val alice = AccountId.tryCreate("alice")
      val clock = new SimClock(loggerFactory = loggerFactory)
      val store = new TeaMemoryTrafficStore(loggerFactory)
      val service = new TrafficEnforcementService(
        store,
        clock,
        retention,
        new TrafficEnforcementMetrics(
          inventory = new TrafficEnforcementInventory(MetricName("test"))(new HistogramInventory()),
          metricsFactory = new InMemoryMetricsFactory,
        )(MetricsContext.Empty),
        loggerFactory,
      )
      val seededAt = clock.now
      val request = PruneRequest(Some(seededAt.toProtoTimestamp))

      for {
        _ <- store
          .persistTrafficDelta(
            alice,
            EventId.tryCreate("event"),
            EventSource.TeaAPI,
            TrafficDelta.creditBalanceDelta(10),
            seededAt,
          )
          .valueOrFail("persist")
        _ <- store
          .insertCorrelation(
            SubmissionCorrelation(
              CorrelationKey(DefaultDamlValues.lfhash(), "submission"),
              CorrelationAccount.Account(alice),
              seededAt,
            )
          )
          .valueOrFail("insert correlation")
        // Try pruning before the retention period has passed. This should be rejected.
        rejected <- service.prune(request)
        // Move the clock outside the retention boundary is so pruning is allowed.
        _ = clock.advance(retention.asJava)
        pruned <- service.prune(request)
      } yield {
        inside(rejected) { case Left(error) =>
          error.code.id shouldBe UnsafeToPrune.id
          error.context should contain(
            "safe_before_or_at" -> seededAt.minus(retention.asJava).toString
          )
        }
        pruned shouldBe Right(PruneResponse(prunedEventCount = 1, prunedCorrelationCount = 1))
      }
    }
  }
}
