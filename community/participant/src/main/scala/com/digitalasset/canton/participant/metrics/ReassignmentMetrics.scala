// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Counter, Histogram, LabeledMetricsFactory}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

class ReassignmentHistograms(parent: MetricName)(implicit inventory: HistogramInventory) {

  private[metrics] val prefix: MetricName = parent :+ "reassignments"

  private[metrics] val batchSize: Item = Item(
    prefix :+ "batch-size",
    summary = "Number of contracts per reassignment request",
    description =
      """Records the number of contracts carried by each unassignment and assignment request. The
        |`type` label tells the two apart.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val localTargetTimestampLag: Item = Item(
    prefix :+ "unassignment" :+ "local-target-timestamp-lag",
    summary = "Lag of the target topology behind the requested target timestamp",
    description =
      """Milliseconds by which this participant's view of the target synchronizer's topology lags behind
        |the target timestamp of an unassignment request, measured during validation. Zero means there is
        |no lag.""",
    qualification = MetricQualification.Latency,
  )
}

class ReassignmentMetrics private[metrics] (
    histograms: ReassignmentHistograms,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  private val prefix: MetricName = histograms.prefix

  val submitted: Counter = factory.counter(
    MetricInfo(
      prefix :+ "submitted",
      summary = "Number of unassignments and assignments submitted",
      description =
        """Records the unassignments and the assignments this participant submits, including the
          |assignments submitted on this participant's own initiative once the exclusivity timeout of
          |an unassignment has elapsed. The `type` label tells unassignments and assignments apart. A
          |submission that is retried is recorded once per attempt.""",
      qualification = MetricQualification.Debug,
    )
  )

  val requests: Counter = factory.counter(
    MetricInfo(
      prefix :+ "requests",
      summary = "Number of unassignment and assignment requests validated",
      description =
        """Records the unassignment and assignment requests this participant validates. The `type`
          |label tells the two apart.""",
      qualification = MetricQualification.Debug,
    )
  )

  val finalized: Counter = factory.counter(
    MetricInfo(
      prefix :+ "finalized",
      summary = "Number of unassignments and assignments finalized",
      description =
        """Records the unassignments and the assignments this reassigning participant finalizes. The
          |`type` label tells the two apart.""",
      qualification = MetricQualification.Debug,
    )
  )

  val batchSize: Histogram = factory.histogram(histograms.batchSize.info)

  val localTargetTimestampLag: Histogram =
    factory.histogram(histograms.localTargetTimestampLag.info)
}

object ReassignmentMetrics {

  private val Unassignment: String = "unassignment"
  private val Assignment: String = "assignment"

  /** Labels source and target synchronizer, so that the unassignment and the assignment of one
    * reassignment aggregate on the same key.
    */
  def synchronizers(
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
  ): MetricsContext =
    MetricsContext(
      "source" -> source.unwrap.toProtoPrimitive,
      "target" -> target.unwrap.toProtoPrimitive,
    )

  def unassignment(
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
  ): MetricsContext = synchronizers(source, target).withExtraLabels("type" -> Unassignment)

  def assignment(
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
  ): MetricsContext = synchronizers(source, target).withExtraLabels("type" -> Assignment)

  /** Labels a request this participant validates. The synchronizer it is validated on is already a
    * label of the connected synchronizer metrics, and the counterpart is submitter-chosen.
    */
  val unassignmentRequest: MetricsContext = MetricsContext("type" -> Unassignment)

  val assignmentRequest: MetricsContext = MetricsContext("type" -> Assignment)
}
