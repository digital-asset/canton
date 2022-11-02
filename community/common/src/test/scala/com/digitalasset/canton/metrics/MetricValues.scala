// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.Snapshot
import com.daml.metrics.api.MetricHandle.{Counter, Histogram, Meter, Timer}
import com.daml.metrics.api.dropwizard.{
  DropwizardCounter,
  DropwizardHistogram,
  DropwizardMeter,
  DropwizardTimer,
}

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
      case DropwizardCounter(_, metric) => metric.getCount
      case other =>
        throw new IllegalArgumentException(s"Value not supported for $other")
    }
  }

  class MeterValues(meter: Meter) {
    def value: Long = meter match {
      case DropwizardMeter(_, metric) => metric.getCount
      case other =>
        throw new IllegalArgumentException(s"Value not supported for $other")
    }
  }

  class HistogramValues(histogram: Histogram) {
    def snapshot: Snapshot = histogram match {
      case DropwizardHistogram(_, metric) => metric.getSnapshot
      case other =>
        throw new IllegalArgumentException(s"Snapshot not supported for $other")
    }
  }

  class TimerValues(timer: Timer) {
    def snapshot: Snapshot = timer match {
      case DropwizardTimer(_, metric) => metric.getSnapshot
      case other =>
        throw new IllegalArgumentException(s"Snapshot not supported for $other")
    }
  }

}
