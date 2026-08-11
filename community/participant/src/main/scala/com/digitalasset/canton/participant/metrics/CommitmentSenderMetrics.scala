// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.{
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.digitalasset.canton.data.CantonTimestamp

class CommitmentSenderMetrics private[metrics] (
    parent: MetricName,
    metricsFactory: MetricHandle.LabeledMetricsFactory,
)(implicit context: MetricsContext) {
  private val prefix = parent :+ "sender"

  val watermarkTimestamp: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "watermark-timestamp",
      summary = "Timestamp (record time) of the latest watermark that has been persisted",
      description = """Measures up to how far the participant has sent the ACS commitments.
          |If this metric falls behind the tick watermarks
          |for the given synchronizer for more significantly than the configured send delay, the sequencer(s) can be overloaded or there's another type of issue (for example, invalid configuration).""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val watermarkOffset: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "watermark-offset",
      summary = "Offset of the latest watermark that has been persisted",
      description =
        """Similar to watermark-timestamp, but stores the numeric offset instead. Cannot be compared
          |with the time values of the ledger, but can be compare with the previous values of the offset.
          |If it's not increasing steadily, the sequencer(s) can be overloaded
          |or there's another type of issue (for example, invalid configuration).""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    0L,
  )

  val batchSendingErrorCount: MetricHandle.Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "batch-sending-error-count",
      summary =
        "Number of the errors (both retriable and non-retriable) that occurred when sending batches",
      description = """The value should stay very low compared to sent-batch-count.
          |Any spike is the indicator of problems: either transient (like overload of the sequencer(s))
          |or persistent (example: invalid configuration).""".stripMargin,
      qualification = MetricQualification.Traffic,
    )
  )

  val sentBatchCount: MetricHandle.Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "sent-batch-count",
      summary = "Number of sent batches containing ACS commitments and summaries",
      description =
        "The value should increase at each tick proportional to the number of counterparticipants.",
      qualification = MetricQualification.Traffic,
    )
  )

  val sendFailureCount: MetricHandle.Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "send-failure-count",
      summary = "Number of the failures in the entire send process (not single batch)",
      description = """The value should stay very low compared to send-attempt-count.
                      |Any spike is the indicator of problems: either transient (like overload of the sequencer(s))
                      |or persistent (example: invalid configuration).""".stripMargin,
      qualification = MetricQualification.Traffic,
    )
  )

  val sendAttemptCount: MetricHandle.Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "send-attempt-count",
      summary = "Number of attempts of sending the ACS commitments",
      description = """The value should increase by one for each tick in the happy scenario.
          |Faster increases indicate failures (send-failure-count), which will trigger retries.""".stripMargin,
      qualification = MetricQualification.Traffic,
    )
  )
}
