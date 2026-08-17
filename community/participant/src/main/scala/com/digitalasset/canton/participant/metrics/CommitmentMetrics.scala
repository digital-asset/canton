// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import cats.Eval
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, LabeledMetricsFactory, Meter, Timer}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.ParticipantId

import scala.collection.concurrent.TrieMap

class CommitmentHistograms(parent: MetricName)(implicit inventory: HistogramInventory) {
  private[metrics] val prefix = parent :+ "commitments"
  private[metrics] val compute = Item(
    prefix :+ "compute",
    summary = "Measures the time that the participant node spends computing commitments.",
    description =
      """Participant nodes compute bilateral commitments at regular intervals, i.e., reconciliation intervals.
        |This metric exposes the time spent on each computation in milliseconds. There are two cases that the
        |operator should pay attention to. First, fluctuations in this value are expected if the number of
        |counter-participants or common stakeholder groups changes. However, changes with no apparent reason
        |could indicate a bug and the operator should monitor closely. Second, it is a cause of concern if the
        |value starts approaching or is greater than the reconciliation interval: The participant will
        |perpetually lag behind, because it needs to compute commitments more frequently than it can manage.
        |The operator should consider asking the synchronizer operator to increase the reconciliation interval
        |if the increase in commitment computation is expected, or otherwise investigate the cause.""",
    qualification = MetricQualification.Debug,
  )
}

class RunningDigestProcessorMetrics private[metrics] (
    parent: MetricName,
    metricsFactory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {
  private val prefix = parent :+ "running-digest-processor"

  val latestAcsUpdate: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "latest-acs-update-record-time",
      summary = "Record time of the latest event that was emitted by the internal index service.",
      description =
        "Record time of the latest event that was emitted by the internal index service.",
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val latestCheckpointedRecordTime: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "latest-checkpointed-record-time",
      summary =
        "Record time of the latest event that went through the checkpointing stage in the running digest processor.",
      description =
        "Record time of the latest event that went through the checkpointing stage in the running digest processor.",
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val latestClassifiedRecordTime: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "latest-classified-record-time",
      summary =
        "Record time of the latest event that went through the classification stage in the running digest processor.",
      description =
        "Record time of the latest event that went through the classification stage in the running digest processor.",
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )
  val latestAccumulatedRecordTime: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "latest-accumulated-record-time",
      summary =
        "Record time of the latest event that went through the digest accumulation stage in the running digest processor.",
      description =
        "Record time of the latest event that went through the digest accumulation stage in the running digest processor.",
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val loadedDigests: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "loaded-digests",
      summary =
        "Counts the number of digests that the running digest processor keeps in memory in the accumulator.",
      description =
        "Measures the memory consumption of the running digest processor. Each digest takes 2kB of raw data plus overhead for the data structures.",
      qualification = MetricQualification.Debug,
    )
  )

  val localPartyChangeCounterparties: Gauge[Int] = metricsFactory.gauge[Int](
    MetricInfo(
      prefix :+ "local-party-change-counterparties",
      summary =
        "Counts the number of counterparties that have been identified for sharing contracts with a local party hosting change",
      description = """This metric measures progress in the number of counterparties when the running digest processor
          |ingests a local party hosting change. This metric is reset to 0 at the start of processing each local party hosting change.
          |""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    0,
  )

  val localPartyChangeContractChanges: Gauge[Int] = metricsFactory.gauge[Int](
    MetricInfo(
      prefix :+ "local-party-change-contract-changes",
      summary =
        "Counts the number of contract changes that have been identified for a local party hosting change.",
      description =
        """This metric measures the progress in the number of identified contract changes when the running digest processor
          |ingests a local party hosting change. This metric is reset to 0 at the start of processing each local party hosting change.
          |Since the same contract of the local party may be processed multiple times (for different counterparties),
          |this metric may exceed the number of contracts of the local party.
          |""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    0,
  )
}

class CommitmentMetrics private[metrics] (
    histograms: CommitmentHistograms,
    metricsFactory: LabeledMetricsFactory,
)(implicit context: MetricsContext) {
  private val prefix = histograms.prefix

  val compute: Timer = metricsFactory.timer(histograms.compute.info)

  val sequencingTime: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ "sequencing-time",
        summary =
          "Measures the time between the end of a commitment period, and the time when the sequencer observes " +
            "the corresponding commitment.",
        description = """Participant nodes compute bilateral commitments at regular intervals. After a participant
                        |computes a commitment, it sends it for sequencing. The time between the end of a
                        |commitment interval and sequencing is measured in milliseconds. Because commitment computation
                        |is comprised within the measured time, the value is always greater than the
                        |`daml.participant.sync.commitments.compute` metric. The operator should pay attention to
                        |fluctuations of this value. An increase can be expected, e.g., because the computation time
                        |increases. However, a value increase can be a cause of concern, because it can indicate that
                        |the participant is lagging behind in processing messages and computing commitments, which is
                        |accompanied by `ACS_COMMITMENT_DEGRADATION` warnings in the participant logs. An increase can
                        |also indicate that the sequencer is slow in sequencing the commitment messages. The operator
                        |should cross-correlate with sequencing metrics such as `daml.sequencer-client.submissions.sequencing`
                        |and `daml.sequencer-client.handler.delay.` In this case, the operator should consider changing
                        |the preferred sequencer configuration.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  private val commonCounterParticipantLatencyDescription: String =
    """The metric exposes the highest latency of a counter-participant, measured by subtracting the highest known
    |counter-participant latency from the most recent period processed by the participant. A counter-participant has to
    |send a commitment at least once in order to appear here. The operator of a participant can configure a default
    |threshold per synchronizer that the participant connects to. The smaller the threshold, the more sensitive the
    |metric is to even small delays in receiving commitments from counter-participants. For example, for a threshold of
    |5 intervals and a reconciliation interval of 1 minute, the metric measures the latency of counter-participants
    |that have sent no commitments for periods covering the last 5 minutes observed by the participant."""

  private val defaultCounterParticipantLatencyDescription: String =
    """Participant nodes compute bilateral commitments at regular intervals and send them. This metric
    |is the default indicator of a counter-participant being slow.""" + commonCounterParticipantLatencyDescription

  private val distinguishedCounterParticipantLatencyDescription: String =
    """Participant nodes compute bilateral commitments at regular intervals and send them. This metric
    |indicates that a distinguished counter-participant is slow, i.e., the participant cannot confirm that
    |its state is the same with that of a counter-participant with whom the operator has an important
    |business relation.""" + commonCounterParticipantLatencyDescription

  val largestDistinguishedCounterParticipantLatency: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ "largest-distinguished-counter-participant-latency",
        summary =
          "The highest latency in micros for commitments outstanding from distinguished counter-participants " +
            "for more than a threshold-number of reconciliation intervals.",
        description = distinguishedCounterParticipantLatencyDescription,
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val largestCounterParticipantLatency: Gauge[Long] =
    metricsFactory.gauge(
      MetricInfo(
        prefix :+ "largest-counter-participant-latency",
        summary =
          "The highest latency in micros for commitments outstanding from counter-participants for more than a " +
            "threshold-number of reconciliation intervals.",
        description = defaultCounterParticipantLatencyDescription,
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  private val individuallyMonitoredCounterParticipantLatencies
      : TrieMap[ParticipantId, Eval[Gauge[Long]]] =
    TrieMap.empty[ParticipantId, Eval[Gauge[Long]]]

  def counterParticipantLatency(participant: ParticipantId): Gauge[Long] = {
    def createMonitoredParticipant: Gauge[Long] = {
      val mc = context.withExtraLabels("counter-participant" -> participant.uid.toProtoPrimitive)
      metricsFactory.gauge(
        MetricInfo(
          prefix :+ "counter-participant-latency",
          summary =
            "The latency of commitments outstanding from the given counter-participants measured in micros.",
          description =
            """Participant nodes compute bilateral commitments at regular intervals and send them. This metric shows
              |the latency of the given counter-participant, measured by subtracting the counter-participant latency from
              |the most recent period processed by the participant. The counter-participant has to send a commitment at
              |least once in order to appear here.""",
          qualification = MetricQualification.Debug,
        ),
        0L,
      )(mc)
    }

    individuallyMonitoredCounterParticipantLatencies
      .getOrElseUpdate(participant, Eval.later(createMonitoredParticipant))
      .value
  }

  val catchupModeEnabled: Meter = metricsFactory.meter(
    MetricInfo(
      prefix :+ "catchup-mode-enabled",
      summary =
        "Measures how many times the commitment processor catch-up mode has been triggered.",
      description =
        """Participant nodes compute bilateral commitments at regular intervals. This metric exposes how often the
          |catch-up mode has been activated. The catch-up mode is triggered according to catch-up config and happens
          |if the participant lags behind on computation. A healthy value is 0. An increasing value indicates
          |intermittent periods when a participant alternates between healthy and struggling to keep up with commitment
          |computation. However, we do not see a constantly increasing value for a participant that is consistently
          |behind commitment computation because, once catch-up mode is activated, the participant remains in catch-up
          |mode until it has completely caught up, and only triggers the metric once. In order to troubleshoot non-zero
          |values, the operator should cross-correlate this value with the
          |`daml.participant.sync.commitments.compute` metric.""",
      qualification = MetricQualification.Debug,
    )
  )

  val lastIncomingReceived: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "last-incoming-received",
      summary =
        "Timestamp of the latest received incoming ACS commitment period end in microseconds since unix epoch",
      description =
        """Timestamp of the latest incoming ACS commitment period end that has been received and enqueued,
          |but not yet processed by the participant. To measure the latency of particular counter participants,
          |use one of the counter-participant-latency metrics.""".stripMargin,
      qualification = MetricQualification.Latency,
    ),
    0L,
  )

  val lastIncomingProcessed: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "last-incoming-processed",
      summary =
        "Timestamp of the latest processed incoming ACS commitment period end in microseconds since unix epoch",
      description =
        """Timestamp of the latest incoming ACS commitment period end that was fully processed by the participant.""",
      qualification = MetricQualification.Latency,
    ),
    0L,
  )

  val checkpointWatermark: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "checkpoint-watermark",
      summary = "Record time of the latest checkpoint that has been persisted",
      description =
        """Measures up to how far the participant has produced and persisted ACS digests from the ACS and topology changes.
          |If this metric falls significantly behind the ledger end's record time for the given synchronizer,
          |digest processing is likely overloaded.""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val tickWatermark: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "tick-watermark",
      summary =
        "Record time of the latest (reconciliation or affirmation) tick for which ACS digests have been persisted",
      description =
        "The record time of the latest tick for which ACS digests have been computed and persisted.",
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val receivedWatermark: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "received-watermark",
      summary = "Sequencing time of the latest received incoming ACS commitment",
      description = "The sequencing time of the latest received incoming ACS commitment.",
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val matchingWatermark: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "matching-watermark",
      summary = "The sequencing time of the latest processed incoming ACS commitment",
      description = """The sequencing time of the latest processed incoming ACS commitment.
          |If this watermark falls behind the `tick-watermark` and the `received-watermark`,
          |then the matching cannot keep up.""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val runningDigestProcessor = new RunningDigestProcessorMetrics(prefix, metricsFactory)

  val lastLocallyCompleted: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "last-locally-completed",
      summary =
        "Timestamp of the latest locally completed ACS commitment interval in microseconds since unix epoch",
      description =
        """Timestamp of the latest locally completed ACS commitment interval. Crash recovery will start reingesting from this timestamp on or from the latest checkpointed ACS commitment interval on, whichever is later.""",
      qualification = MetricQualification.Latency,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val lastLocallyCheckpointed: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "last-locally-checkpointed",
      summary =
        "Record time of the latest checkpointed ACS commitment in microseconds since unix epoch",
      description =
        """Timestamp of the latest checkpointed ACS commitment in microseconds. Crash recovery will start reingesting from this timestamp on or from the latest locally completed ACS commitment interval on, whichever is later.""",
      qualification = MetricQualification.Latency,
    ),
    CantonTimestamp.MinValue.toMicros,
  )

  val activeStakeholderGroups: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "active-stakeholder-groups",
      summary =
        "Record the number of stakeholder groups with active contracts on this participants",
      description =
        "The number of stakeholder groups for which the participant has at least one active contract in the current active contract store.",
      qualification = MetricQualification.Saturation,
    ),
    0L,
  )

  val sender: CommitmentSenderMetrics = new CommitmentSenderMetrics(prefix, metricsFactory)

  val bufferDigestPipelineSize: Gauge[Long] = metricsFactory.gauge(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-size",
      summary = "Measures the size of the buffers in the digest processor pipeline",
      description =
        """This value changes only when the buffer size is reconfigured as part of a participant node restart.
          |It helps to visualize in dashboards when buffers are full.""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    0L,
  )

  val bufferDigestPipelineCheckpointing: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-1-checkpointing",
      summary = "Measures the buffer usage in the digest processor pipeline before checkpointing",
      description = """If this value is at the buffer size (see `buffer-digest-pipeline-size`),
          |then the checkpointing stage backpressures towards the indexer.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val bufferDigestPipelineBeforeClassification: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-2-classification",
      summary = "Measures the buffer usage in the digest processor pipeline before classification",
      description = """If this value is at the buffer size (see `buffer-digest-pipeline-size`),
          |then the classification stage backpressures towards the checkpointing stage.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val bufferDigestPipelineBeforeAccumulation: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-3-accumulation",
      summary = "Measures the buffer usage in the digest processor pipeline before accumulation",
      description = """If this value is at the buffer size (see `buffer-digest-pipeline-size`),
          |then the accumulation stage (in particular the `ensurePresent` substage)
          |backpressures towards the classification stage.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val bufferDigestPipelineBeforeComputeDigestsChanges: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-3.1-compute-digest-changes",
      summary =
        "Measures the buffer usage in the digest processor's accumulator pipeline before computing the digest changes",
      description = """If this value is at the buffer size (see `buffer-digest-pipeline-size`),
           |then the computation stage backpressures towards the `ensurePresent` stage.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val bufferDigestPipelineBeforeJoinLoading: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-3.2-ompute-digest-join-loading",
      summary =
        "Measures the buffer usage in the digest processor's accumulator pipeline before joining the loading futures",
      description = """If this value is at the buffer size (see `buffer-digest-pipeline-size`),
           |then the join loading stage backpressures towards the computation stage.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val bufferDigestPipelineBeforeAggregation: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-3.3-aggregate",
      summary =
        "Measures the buffer usage in the digest processor's accumulator pipeline before aggregating the digest changes",
      description = """If this value is at the buffer size (see `buffer-digest-pipeline-size`),
           |then the aggregation stage backpressures towards the join loading stage.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val bufferDigestPipelineBeforePersistence: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-3.4-persist-changes",
      summary =
        "Measures the buffer usage in the digest processor pipeline before persisting the digest changes",
      description = """If this value is at the buffer size (see `buffer-digest-pipeline-size`),
           |then the persistence stage triggers conflation in the aggregation state.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val bufferDigestPipelineBeforeOutstanding: Counter = metricsFactory.counter(
    MetricInfo(
      prefix :+ "buffer-digest-pipeline-4-persist-outstanding",
      summary =
        "Measures the buffer usage in the digest processor pipeline before persisting outstanding periods",
      description = """If this value is at the buffer size (see `buffer-digest-pipeline-size`),
           |then the persisting outstanding period stage backpressures towards the accumulator stage.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val reinitializeParties: Gauge[Int] = metricsFactory.gauge[Int](
    MetricInfo(
      prefix :+ "reinitialize-parties",
      summary =
        "Counts the number of parties that have been identified as part of reinitializing the digests.",
      description =
        """This metric measures progress in the number of counterparties during the digest processor reinitialization.
          |This metric is reset to 0 at the start of reinitialization.
          |""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    0,
  )

  val reinitializeContractChanges: Gauge[Int] = metricsFactory.gauge[Int](
    MetricInfo(
      prefix :+ "reinitialize-contract-changes",
      summary =
        "Counts the number of contract changes that have been identified as part of reinitializing the digests.",
      description =
        """This metric measures the progress in the number of identified contract changes upon reinitializing the digests.
          |This metric is reset to 0 at the start of reinitialization.
          |Since the same contract may be processed multiple times (for different counterparties),
          |this metric may exceed the number of active contracts.
          |""".stripMargin,
      qualification = MetricQualification.Debug,
    ),
    0,
  )
}
