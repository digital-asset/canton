// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.Snapshot
import com.daml.metrics.MetricHandle
import com.daml.metrics.MetricHandle.{Counter, Histogram, Meter, Timer}

trait MetricValues {

  import scala.language.implicitConversions

  implicit def convertCounterToValuable(counter: Counter): CounterValues = new CounterValues(
    counter
  )

  implicit def convertMeterToValuable(meter: Meter): MeterValues = new MeterValues(
    meter
  )

  implicit def convertHistogramToValuable(histogram: Histogram): HistogramValues =
    new HistogramValues(
      histogram
    )

  implicit def convertTimerToValuable(timer: Timer): TimerValues =
    new TimerValues(
      timer
    )

  class CounterValues(counter: Counter) {
    def value: Long = counter match {
      case MetricHandle.DropwizardCounter(_, metric) => metric.getCount
    }
  }

  class MeterValues(meter: Meter) {
    def value: Long = meter match {
      case MetricHandle.DropwizardMeter(_, metric) => metric.getCount
    }
  }

  class HistogramValues(histogram: Histogram) {
    def snapshot: Snapshot = histogram match {
      case MetricHandle.DropwizardHistogram(_, metric) => metric.getSnapshot
    }
  }

  class TimerValues(timer: Timer) {
    def snapshot: Snapshot = timer match {
      case MetricHandle.DropwizardTimer(_, metric) => metric.getSnapshot
      case Timer.NoOpTimer(_) =>
        throw new IllegalArgumentException("Snapshot not supported for noOp")
    }
  }

}
