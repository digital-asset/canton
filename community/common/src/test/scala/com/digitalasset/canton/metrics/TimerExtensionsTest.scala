// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import cats.data.EitherT
import com.daml.metrics.api.testing.InMemoryMetricsFactory.InMemoryTimer
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.flatspec.AsyncFlatSpec

class TimerExtensionsTest extends AsyncFlatSpec with BaseTest with HasExecutionContext {

  private val metricInfo = MetricInfo(
    MetricName("test-timer"),
    summary = "A timer used for testing",
    qualification = MetricQualification.Latency,
  )

  private val SuccessLabel = "success"
  private val FailureLabel = "failure"
  private val StatusLabel = "status"
  private val ShutdownStatus = "fut_shutdown"
  private val FailedStatus = "fut_failure"

  private def newTimer(): InMemoryTimer = InMemoryTimer(metricInfo, MetricsContext.Empty)

  private def statusContext(status: String): MetricsContext =
    MetricsContext(StatusLabel -> status)

  private val successContext = statusContext(SuccessLabel)
  private val failureContext = statusContext(FailureLabel)
  private val shutdownContext = statusContext(ShutdownStatus)
  private val failedContext = statusContext(FailedStatus)

  behavior of "timeEitherFUSWithLabels"

  it should "record the mapped label of a right result" in {
    val timer = newTimer()
    val timed = timer.timeEitherFUSWithLabels(
      EitherT.pure[FutureUnlessShutdown, String](42)
    )
    timer.data.recordedValues.keySet shouldBe Set(successContext)
    timer.data.recordedValues(successContext) should have size 1

    timed.value.unwrap.map(_ shouldBe Outcome(Right(42)))
  }

  it should "record the mapped label of a left result" in {
    val timer = newTimer()
    val timed = timer.timeEitherFUSWithLabels(
      EitherT.leftT[FutureUnlessShutdown, Int]("boom")
    )

    timer.data.recordedValues.keySet shouldBe Set(failureContext)
    timer.data.recordedValues(failureContext) should have size 1

    timed.value.unwrap.map(_ shouldBe Outcome(Left("boom")))
  }

  it should "record a shutdown as such" in {
    val timer = newTimer()
    val timed = timer.timeEitherFUSWithLabels(
      EitherT(
        FutureUnlessShutdown.abortedDueToShutdown: FutureUnlessShutdown[Either[String, Int]]
      )
    )

    timer.data.recordedValues.keySet shouldBe Set(shutdownContext)
    timer.data.recordedValues(shutdownContext) should have size 1

    timed.value.unwrap.map(_ shouldBe AbortedDueToShutdown)
  }

  it should "record a failure as such and propagate the exception" in {
    val timer = newTimer()
    val ex = new RuntimeException("boom")
    val timed = timer.timeEitherFUSWithLabels(
      EitherT(FutureUnlessShutdown.failed[Either[String, Int]](ex))
    )

    timer.data.recordedValues.keySet shouldBe Set(failedContext)
    timer.data.recordedValues(failedContext) should have size 1

    timed.value.unwrap.failed.map(_ shouldBe ex)
  }

  it should "merge an ambient MetricsContext with the status label" in {
    val timer = newTimer()
    implicit val ambient: MetricsContext = MetricsContext("api" -> "ping")
    val timed = timer.timeEitherFUSWithLabels(
      EitherT.pure[FutureUnlessShutdown, String](42)
    )
    timer.data.recordedValues.keySet shouldBe Set(
      MetricsContext("api" -> "ping", StatusLabel -> "success")
    )

    timed.value.unwrap.map(_ shouldBe Outcome(Right(42)))
  }
}
