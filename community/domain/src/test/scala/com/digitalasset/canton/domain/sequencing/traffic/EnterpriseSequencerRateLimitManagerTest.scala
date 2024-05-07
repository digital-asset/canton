// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitError.AboveTrafficLimit
import com.digitalasset.canton.domain.sequencing.traffic.EnterpriseSequencerRateLimitManager.TrafficStateUpdateResult
import com.digitalasset.canton.domain.sequencing.traffic.store.memory.InMemoryTrafficBalanceStore
import com.digitalasset.canton.metrics.CantonLabeledMetricsFactory.CantonOpenTelemetryMetricsFactory
import com.digitalasset.canton.metrics.{MetricValue, OpenTelemetryOnDemandMetricsReader}
import com.digitalasset.canton.sequencing.TrafficControlParameters
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.DefaultTestIdentities.*
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.scalatest.FutureOutcome
import org.scalatest.flatspec.FixtureAsyncFlatSpec

import scala.reflect.ClassTag

class EnterpriseSequencerRateLimitManagerTest
    extends FixtureAsyncFlatSpec
    with BaseTest
    with HasExecutionContext
    with RateLimitManagerTesting {

  behavior of "EnterpriseSequencerRateLimiter"

  private val trafficConfig: TrafficControlParameters = TrafficControlParameters(
    maxBaseTrafficAmount = NonNegativeLong.zero
  )
  private val sender: Member = mediatorIdX.member
  private val recipients: Recipients = Recipients.cc(participant1, participant2)
  private val envelope1: ClosedEnvelope = ClosedEnvelope.create(
    ByteString.copyFromUtf8("hello"),
    recipients,
    Seq.empty,
    testedProtocolVersion,
  )
  private val eventCost = NonNegativeLong.tryCreate(5L)
  private val batch: Batch[ClosedEnvelope] = Batch(List(envelope1), testedProtocolVersion)
  private val sequencingTs = CantonTimestamp.Epoch.plusSeconds(1)
  private val inState = TrafficState.empty(CantonTimestamp.Epoch)

  case class Env(
      trafficConfig: TrafficControlParameters,
      batch: Batch[ClosedEnvelope],
      rlm: EnterpriseSequencerRateLimitManager,
      balanceManager: TrafficBalanceManager,
      onDemandMetricsReader: OpenTelemetryOnDemandMetricsReader,
  ) {
    def getMetricValues[TargetType <: MetricValue](name: String)(implicit
        M: ClassTag[TargetType]
    ) = {
      MetricValue
        .fromMetricData(
          onDemandMetricsReader
            .read()
            .find(_.getName.contains(name))
            .value
        )
        .flatMap { metricData =>
          metricData.select[TargetType]
        }
    }

    def assertMemberAndTypeAreInContext(name: String) = {
      val values = getMetricValues[MetricValue.LongPoint](name).headOption
      values
        .flatMap(_.attributes.get("member")) shouldBe Some(sender.toString)
      values
        .flatMap(_.attributes.get("type")) shouldBe Some("request_type")
    }

    def assertOnlyMemberIsInContext(name: String) = {
      getMetricValues[MetricValue.LongPoint](name).headOption
        .flatMap(_.attributes.get("member")) shouldBe Some(sender.toString)
    }

    def assertLongValue(name: String, expected: Long) = {
      getMetricValues[MetricValue.LongPoint](name).headOption.map(_.value) shouldBe Some(expected)
    }
  }

  override type FixtureParam = Env

  it should "consume from traffic balance" in { implicit f =>
    val expectedState = TrafficState(
      extraTrafficRemainder = NonNegativeLong.tryCreate(10L),
      extraTrafficConsumed = NonNegativeLong.tryCreate(5L),
      baseTrafficRemainder = NonNegativeLong.zero,
      sequencingTs,
    )
    for {
      _ <- f.balanceManager.addTrafficBalance(
        TrafficBalance(
          sender,
          PositiveInt.one,
          NonNegativeLong.tryCreate(15L),
          sequencingTs.immediatePredecessor,
        )
      )
      res <- f.rlm
        .consume(
          sender,
          f.batch,
          sequencingTs,
          inState,
          trafficConfig,
          Map.empty,
          () => MetricsContext("type" -> "request_type"),
        )
        .value
        .failOnShutdown
        .map { state =>
          state shouldBe Right(expectedState)
        }
    } yield {
      eventually() {
        f.assertMemberAndTypeAreInContext("event-delivered-cost")
        f.assertOnlyMemberIsInContext("last-traffic-update")
        f.assertOnlyMemberIsInContext("extra-traffic-limit")
        f.assertOnlyMemberIsInContext("extra-traffic-consumed")
        f.assertOnlyMemberIsInContext("base-traffic-remainder")

        f.assertLongValue("last-traffic-update", sequencingTs.getEpochSecond)
        f.assertLongValue("event-delivered-cost", eventCost.value)
        f.assertLongValue("extra-traffic-limit", expectedState.extraTrafficLimit.value.value)
        f.assertLongValue("extra-traffic-consumed", expectedState.extraTrafficConsumed.value)
        f.assertLongValue("base-traffic-remainder", expectedState.baseTrafficRemainder.value)
      }
      res
    }
  }

  it should "consume from traffic balance and base rate" in { implicit f =>
    for {
      _ <- f.balanceManager.addTrafficBalance(
        TrafficBalance(
          sender,
          PositiveInt.one,
          NonNegativeLong.tryCreate(15L),
          sequencingTs.immediatePredecessor,
        )
      )
      res <- f.rlm
        .consume(
          sender,
          f.batch,
          sequencingTs,
          inState,
          trafficConfig.copy(
            maxBaseTrafficAccumulationDuration = NonNegativeFiniteDuration.tryOfSeconds(1),
            maxBaseTrafficAmount = NonNegativeLong.tryCreate(2),
          ),
          Map.empty,
          () => MetricsContext.Empty,
        )
        .value
        .failOnShutdown
        .map { state =>
          state shouldBe Right(
            TrafficState(
              extraTrafficRemainder = NonNegativeLong.tryCreate(12L),
              extraTrafficConsumed = NonNegativeLong.tryCreate(3L),
              baseTrafficRemainder = NonNegativeLong.zero,
              sequencingTs,
            )
          )
        }
    } yield res
  }

  it should "consume from base rate only" in { implicit f =>
    for {
      _ <- f.balanceManager.addTrafficBalance(
        TrafficBalance(
          sender,
          PositiveInt.one,
          NonNegativeLong.tryCreate(8L),
          sequencingTs.immediatePredecessor,
        )
      )
      res <- f.rlm
        .consume(
          sender,
          f.batch,
          sequencingTs,
          inState,
          trafficConfig.copy(
            maxBaseTrafficAccumulationDuration = NonNegativeFiniteDuration.tryOfSeconds(1),
            maxBaseTrafficAmount = NonNegativeLong.tryCreate(10),
          ),
          Map.empty,
          () => MetricsContext.Empty,
        )
        .value
        .failOnShutdown
        .map { state =>
          state shouldBe Right(
            TrafficState(
              extraTrafficRemainder = NonNegativeLong.tryCreate(8L),
              extraTrafficConsumed = NonNegativeLong.zero,
              baseTrafficRemainder = NonNegativeLong.tryCreate(5L),
              sequencingTs,
            )
          )
        }
    } yield res
  }

  it should "update traffic state correctly" in { implicit f =>
    val trafficConfigWithBaseRate = trafficConfig.copy(
      maxBaseTrafficAccumulationDuration = NonNegativeFiniteDuration.tryOfSeconds(2),
      maxBaseTrafficAmount = NonNegativeLong.tryCreate(6),
    )
    for {
      initState <- f.rlm
        .createNewTrafficStateAt(sender, sequencingTs, trafficConfigWithBaseRate)
        .failOnShutdown
      _ = initState shouldBe TrafficState(
        extraTrafficRemainder = NonNegativeLong.zero,
        extraTrafficConsumed = NonNegativeLong.zero,
        baseTrafficRemainder = NonNegativeLong.tryCreate(6L), // Should have full base rate upfront
        sequencingTs,
      )
      _ = f.balanceManager.tick(sequencingTs)
      // Consume 5 immediately, should use base rate
      state1 <- f.rlm
        .consume(
          sender,
          f.batch,
          sequencingTs.immediateSuccessor,
          initState,
          trafficConfigWithBaseRate,
          Map.empty,
          () => MetricsContext.Empty,
          Option(sequencingTs),
        )
        .valueOrFail("Consuming from base rate")
        .failOnShutdown
      _ = state1 shouldBe TrafficState(
        extraTrafficRemainder = NonNegativeLong.zero,
        extraTrafficConsumed = NonNegativeLong.zero,
        baseTrafficRemainder =
          NonNegativeLong.tryCreate(1L), // Should have consumed 5 from base rate
        sequencingTs.immediateSuccessor,
      )
      _ <- f.balanceManager.addTrafficBalance(
        TrafficBalance(
          sender,
          PositiveInt.one,
          NonNegativeLong.tryCreate(8L),
          sequencingTs.immediateSuccessor,
        )
      )
      // Update state one second later
      state2 <- f.rlm
        .getUpdatedTrafficStatesAtTimestamp(
          Map(sender -> state1),
          sequencingTs.immediateSuccessor.plusSeconds(1),
          trafficConfigWithBaseRate,
          Option(sequencingTs.immediateSuccessor),
          warnIfApproximate = true,
        )
        .failOnShutdown
      _ = state2.get(sender).value shouldBe TrafficStateUpdateResult(
        TrafficState(
          extraTrafficRemainder = NonNegativeLong.tryCreate(8L),
          extraTrafficConsumed = NonNegativeLong.zero,
          // Should have half of the max base rate added back after 1 second
          baseTrafficRemainder = NonNegativeLong.tryCreate(
            4L
          ),
          sequencingTs.immediateSuccessor.plusSeconds(1),
        ),
        Some(PositiveInt.one),
      )
    } yield succeed
  }

  it should "fail if not enough traffic" in { implicit f =>
    f.balanceManager.tick(sequencingTs)
    for {
      res <- f.rlm
        .consume(
          sender,
          f.batch,
          sequencingTs,
          inState,
          trafficConfig,
          Map.empty,
          () => MetricsContext.Empty,
          Option(sequencingTs),
        )
        .value
        .failOnShutdown
        .map { state =>
          state shouldBe Left(
            AboveTrafficLimit(
              sender,
              NonNegativeLong.tryCreate(5),
              inState.copy(timestamp = sequencingTs),
            )
          )
        }
    } yield res
  }

  it should "create new traffic state" in { implicit f =>
    f.rlm
      .createNewTrafficStateAt(sender, sequencingTs, trafficConfig)
      .map { state =>
        state.timestamp shouldBe sequencingTs
        state.extraTrafficConsumed.value shouldBe 0L
        state.extraTrafficRemainder.value shouldBe 0L
        state.baseTrafficRemainder.value shouldBe trafficConfig.maxBaseTrafficAmount.value
      }
      .failOnShutdown
  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val store = new InMemoryTrafficBalanceStore(loggerFactory)
    val manager = mkTrafficBalanceManager(store)
    val onDemandMetricsReader = new OpenTelemetryOnDemandMetricsReader
    val sdkBuilder = OpenTelemetrySdk.builder()
    val meterProvider = SdkMeterProvider.builder()
    meterProvider.registerMetricReader(onDemandMetricsReader)
    sdkBuilder.setMeterProvider(meterProvider.build())
    val openTelemetry = ConfiguredOpenTelemetry(
      sdkBuilder.build(),
      SdkTracerProvider.builder(),
      onDemandMetricsReader,
      metricsEnabled = false,
    )

    val factory = new CantonOpenTelemetryMetricsFactory(
      openTelemetry.openTelemetry.meterBuilder("test").build(),
      MetricsContext.Empty,
    )
    val rateLimiter = mkRateLimiter(manager, factory)
    val env = Env(
      trafficConfig,
      batch,
      rateLimiter,
      manager,
      onDemandMetricsReader,
    )

    withFixture(test.toNoArgAsyncTest(env))
  }
}
