// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.InstrumentedGraphSpec.MaxValueCounter
import com.daml.metrics.api.noop.NoOpCounter
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{InMemoryCounter, InMemoryTimer}
import com.daml.metrics.api.testing.MetricValues
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.daml.testing.utils.PekkoBeforeAndAfterAll
import org.apache.pekko.stream.QueueOfferResult
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

final class InstrumentedGraphSpec
    extends AsyncFlatSpec
    with Matchers
    with PekkoBeforeAndAfterAll
    with MetricValues {

  behavior of "InstrumentedSource.queue"

  it should "correctly enqueue and measure queue delay" in {
    val capacityCounter =
      NoOpCounter(MetricInfo(MetricName("capacity"), "", MetricQualification.Debug))
    val maxBuffered = NoOpCounter(MetricInfo(MetricName("buffered"), "", MetricQualification.Debug))
    val delayTimer = InMemoryTimer(
      MetricInfo(MetricName("test"), "", MetricQualification.Debug),
      MetricsContext.Empty,
    )
    val bufferSize = 2

    val (source, sink) =
      InstrumentedGraph
        .queue[Int](bufferSize, capacityCounter, maxBuffered, delayTimer)
        .mapAsync(1) { x =>
          org.apache.pekko.pattern.after(5.millis, system.scheduler)(Future(x))
        }
        .toMat(Sink.seq)(Keep.both)
        .run()

    val input = Seq.fill(bufferSize)(util.Random.nextInt())

    val result = input.map(source.offer)
    source.complete()
    sink.map { output =>
      all(result) shouldBe QueueOfferResult.Enqueued
      output shouldEqual input
      delayTimer.count shouldEqual bufferSize
      delayTimer.values.max should be >= 5.millis.toMillis
    }
  }

  it should "track the buffer saturation correctly" in {

    val bufferSize = 500

    // Due to differences in scheduling, we expect the highest
    // possible recorded saturation value to be more or less equal
    // to the buffer size. See the ScalaDoc of `InstrumentedQueue.source`
    // for more details
    val acceptanceTolerance = bufferSize * 0.05
    val lowAcceptanceThreshold = bufferSize - acceptanceTolerance
    val highAcceptanceThreshold = bufferSize + acceptanceTolerance

    val maxBuffered = new MaxValueCounter
    val capacityCounter = InMemoryCounter(
      MetricInfo(MetricName("test"), "", MetricQualification.Debug),
      MetricsContext.Empty,
    )
    val delayTimer = InMemoryTimer(
      MetricInfo(MetricName("test"), "", MetricQualification.Debug),
      MetricsContext.Empty,
    )

    val stop = Promise[Unit]()

    val (source, termination) =
      InstrumentedGraph
        .queue[Int](bufferSize, capacityCounter, maxBuffered, delayTimer)
        .mapAsync(1)(_ => stop.future) // Block until completed to overflow queue.
        .watchTermination()(Keep.both)
        .toMat(Sink.ignore)(Keep.left)
        .run()

    // We to enqueue double the items that fit in the buffer
    // so to force items to be dropped from the queue
    val inputSize = bufferSize * 2
    val input = Seq.fill(inputSize)(util.Random.nextInt())

    val results = input.map(source.offer)
    capacityCounter.value shouldEqual bufferSize
    stop.success(())
    source.complete()
    val enqueued = results.count {
      case QueueOfferResult.Enqueued => true
      case _ => false
    }
    val dropped = results.count {
      case QueueOfferResult.Dropped => true
      case _ => false
    }
    termination.map { _ =>
      inputSize shouldEqual (enqueued + dropped)
      assert(enqueued >= bufferSize)
      assert(dropped <= bufferSize)
      assert(maxBuffered.value >= lowAcceptanceThreshold)
      assert(maxBuffered.value <= highAcceptanceThreshold)
      capacityCounter.value shouldEqual 0
    }
  }
}

object InstrumentedGraphSpec extends MetricValues {
  // For testing only, this counter will never decrease
  // so that we can test the maximum value read
  private final class MaxValueCounter
      extends InMemoryCounter(
        MetricInfo(MetricName("test"), "", MetricQualification.Debug),
        MetricsContext.Empty,
      ) {
    override def dec(value: Long)(implicit mc: MetricsContext): Unit = ()

  }
}
