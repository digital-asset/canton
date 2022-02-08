// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricName
import com.digitalasset.canton.metrics.MetricHandle.{CounterM, GaugeM, TimerM, VarGaugeM}

import scala.concurrent.duration._

class SequencerClientMetrics(basePrefix: MetricName, val registry: MetricRegistry)
    extends MetricHandle.Factory {

  override val prefix: MetricName = basePrefix :+ "sequencer-client"

  @MetricDoc.Tag(
    summary = "Timer monitoring time and rate of sequentially handling the event application logic",
    description = """All events are received sequentially. This handler records the 
        |the rate and time it takes the application (participant or domain) to handle the events.""",
  )
  val applicationHandle: TimerM = timer(prefix :+ "application-handle")

  @MetricDoc.Tag(
    summary = "Timer monitoring time and rate of entire event handling",
    description =
      """Most event handling cost should come from the application-handle. This timer measures
        |the full time (which should just be marginally more than the application handle.""",
  )
  val processingTime: TimerM = timer(prefix :+ "event-handle")

  @MetricDoc.Tag(
    summary = "The load on the event subscription",
    description = """The event subscription processor is a sequential process. The load is a factor between 
                    |0 and 1 describing how much of an existing interval has been spent in the event handler.""",
  )
  val load: GaugeM[TimedLoadGauge, Double] =
    loadGauge(prefix :+ "load", 1.second, processingTime.metric)

  @MetricDoc.Tag(
    summary = "The delay on the event processing",
    description = """Every message received from the sequencer carries a timestamp. The delay
        |provides the difference between the sequencing time and the processing time.
        |The difference can be a result of either clock-skew or if the system is overloaded
        |and doesn't manage to keep up with processing events.""",
  )
  val delay: VarGaugeM[Long] = varGauge(prefix :+ "delay", 0L)

  object submissions {
    val prefix: MetricName = SequencerClientMetrics.this.prefix :+ "submissions"

    @MetricDoc.Tag(
      summary =
        "Number of sequencer send requests we have that are waiting for an outcome or timeout",
      description = """Incremented on every successful send to the sequencer.
          |Decremented when the event or an error is sequenced, or when the max-sequencing-time has elapsed.""",
    )
    val inFlight: GaugeM[IntGauge, Integer] = intGauge(prefix :+ "in-flight", initial = 0)

    @MetricDoc.Tag(
      summary = "Rate and timings of send requests to the sequencer",
      description =
        """Provides a rate and time of how long it takes for send requests to be accepted by the sequencer.
          |Note that this is just for the request to be made and not for the requested event to actually be sequenced.
          |""",
    )
    val sends: TimerM = timer(prefix :+ "sends")

    @MetricDoc.Tag(
      summary = "Rate and timings of sequencing requests",
      description =
        """This timer is started when a submission is made to the sequencer and then completed when a corresponding event
          |is witnessed from the sequencer, so will encompass the entire duration for the sequencer to sequence the
          |request. If the request does not result in an event no timing will be recorded.
          |""",
    )
    val sequencingTime: TimerM = timer(prefix :+ "sequencing")

    @MetricDoc.Tag(
      summary = "Count of send requests which receive an overloaded response",
      description =
        "Counter that is incremented if a send request receives an overloaded response from the sequencer.",
    )
    val overloaded: CounterM = counter(prefix :+ "overloaded")

    @MetricDoc.Tag(
      summary = "Count of send requests that did not cause an event to be sequenced",
      description = """Counter of send requests we did not witness a corresponding event to be sequenced by the
                      |supplied max-sequencing-time. There could be many reasons for this happening: the request may
                      |have been lost before reaching the sequencer, the sequencer may be at capacity and the
                      |the max-sequencing-time was exceeded by the time the request was processed, or the supplied 
                      |max-sequencing-time may just be too small for the sequencer to be able to sequence the request.""",
    )
    val dropped: CounterM = counter(prefix :+ "dropped")
  }
}
