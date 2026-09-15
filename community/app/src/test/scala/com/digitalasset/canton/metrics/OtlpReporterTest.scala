// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.digitalasset.canton.metrics.ClientCredentialsTokenProvider.CachedToken
import com.digitalasset.canton.metrics.OtlpReporter.{DeferredMetricExporter, DeferredValue}
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.`export`.MetricExporter
import io.opentelemetry.sdk.metrics.data.{AggregationTemporality, MetricData}
import org.scalatest.wordspec.AnyWordSpec

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.{Future, Promise}

class OtlpReporterTest
    extends AnyWordSpec
    with HasExecutionContext
    with HasActorSystem
    with BaseTest {

  "DeferredMetricExporter" should {

    "export metrics after obtaining a token" in {
      val tokenReady = Promise[CachedToken]()
      val exported = Promise[Boolean]()

      val tokenProvider = new TokenProvider {
        override def ensureToken(): Future[CachedToken] = tokenReady.future
        override def authorizationHeader(): String = "Bearer token"
      }

      val delegate = new MetricExporter {
        override def `export`(metrics: util.Collection[MetricData]): CompletableResultCode = {
          exported.success(true)
          CompletableResultCode.ofSuccess()
        }

        override def flush(): CompletableResultCode = CompletableResultCode.ofSuccess()

        override def shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()

        override def getAggregationTemporality(
            instrumentType: InstrumentType
        ): AggregationTemporality = AggregationTemporality.CUMULATIVE
      }

      val exporter = new DeferredMetricExporter(
        delegate,
        new DeferredValue[TokenProvider]("token provider"),
        loggerFactory,
      )
      exporter.initialize(tokenProvider)

      val result = exporter.export(util.Collections.emptyList())
      exported.future.isCompleted shouldBe false

      tokenReady.success(CachedToken("dummy token", wallClock.now.toInstant))
      exported.future.futureValue shouldBe true

      result.join(1, TimeUnit.SECONDS)
      result.isSuccess shouldBe true
    }
  }
}
