// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Gauge, Meter, Timer}
import com.daml.metrics.api.MetricQualification.{Latency, Traffic}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricsContext,
}
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementOutcome

class TrafficEnforcementMetrics(
    inventory: TrafficEnforcementInventory,
    metricsFactory: MetricHandle.LabeledMetricsFactory,
)(implicit val metricsContext: MetricsContext) {
  val prefix: MetricName = inventory.prefix

  /** One mark per enforcement decision, labeled with the same outcome/reason vocabulary as the
    * enforcement decision span, so the two can't drift apart.
    */
  val decisions: Meter = metricsFactory.meter(
    MetricInfo(
      name = prefix :+ "decisions",
      summary = "Number of enforcement decisions made",
      description =
        "The number of enforcement decisions the system has made, labeled by outcome and, where" +
          " applicable, reason.",
      qualification = Traffic,
      labelsWithDescription = Map(
        TrafficEnforcementOutcome.OutcomeAttribute -> "The outcome of the enforcement decision.",
        TrafficEnforcementOutcome.ReasonAttribute -> "Why the decision was reached, present only for outcomes that have a reason.",
      ),
    )
  )

  val completions: Meter = metricsFactory.meter(
    MetricInfo(
      name = prefix :+ "completions",
      summary = "Number of completions processed by traffic enforcement",
      description =
        "The number of Ledger API completions traffic enforcement processed, labeled by outcome" +
          " and, where applicable, reason.",
      qualification = Traffic,
      labelsWithDescription = Map(
        TrafficEnforcementOutcome.OutcomeAttribute -> "The outcome of processing the completion: charged, skipped or failed.",
        TrafficEnforcementOutcome.ReasonAttribute -> "Why the completion wasn't charged, present only for skipped completions.",
      ),
    )
  )

  private val unchargedLabels = Map(
    TrafficEnforcementOutcome.ReasonAttribute -> "Why the completion wasn't charged.",
    TrafficEnforcementOutcome.ActAsCategoryAttribute -> "Which kind of acting parties the completion had.",
  )

  val unchargedCompletions: Meter = metricsFactory.meter(
    MetricInfo(
      name = prefix :+ "uncharged-completions",
      summary = "Number of completions with a positive cost that weren't charged",
      description =
        "The number of completions with a positive traffic cost that traffic enforcement recorded" +
          " without charging an account, labeled by reason and acting party category.",
      qualification = Traffic,
      labelsWithDescription = unchargedLabels,
    )
  )

  val unchargedCompletionCost: Meter = metricsFactory.meter(
    MetricInfo(
      name = prefix :+ "uncharged-completion-cost",
      summary = "Traffic cost of completions that weren't charged",
      description =
        "The total traffic cost of completions that traffic enforcement recorded without charging" +
          " an account, labeled by reason and acting party category.",
      qualification = Traffic,
      labelsWithDescription = unchargedLabels,
    )
  )

  val prunedRows: Meter = metricsFactory.meter(
    MetricInfo(
      name = prefix :+ "pruned-rows",
      summary = "Number of traffic enforcement rows pruned",
      description = "The number of traffic enforcement rows pruned, labeled by table.",
      qualification = Traffic,
      labelsWithDescription = Map("table" -> "The pruned table."),
    )
  )

  val enforcementCheckDuration: Timer = metricsFactory.timer(inventory.enforcementCheckItem.info)

  val projectionTimestamp: Gauge[Long] = metricsFactory.gauge[Long](
    MetricInfo(
      name = prefix :+ "projection-timestamp",
      summary = "The consumed timestamp of the traffic enforcement projection",
      description =
        "The consumed timestamp of the traffic enforcement projection, indicating how far the system has processed traffic enforcement events.",
      qualification = Latency,
    ),
    0L,
  )

  val projectionOffset: Gauge[Long] = metricsFactory.gauge[Long](
    MetricInfo(
      name = prefix :+ "projection-offset",
      summary = "The saved offset of the traffic enforcement projection",
      description = "The saved offset of the traffic enforcement projection.",
      qualification = Latency,
    ),
    0L,
  )

  def markDecision(outcome: String, reason: Option[String]): Unit =
    decisions.mark()(outcomeLabels(outcome, reason))

  def markCompletion(outcome: String, reason: Option[String]): Unit =
    completions.mark()(outcomeLabels(outcome, reason))

  def markUnchargedCompletion(reason: String, actAsCategory: String, cost: Long): Unit = {
    val labeled = metricsContext.withExtraLabels(
      TrafficEnforcementOutcome.ReasonAttribute -> reason,
      TrafficEnforcementOutcome.ActAsCategoryAttribute -> actAsCategory,
    )
    unchargedCompletions.mark()(labeled)
    unchargedCompletionCost.mark(cost)(labeled)
  }

  private def outcomeLabels(outcome: String, reason: Option[String]): MetricsContext = {
    val withOutcome =
      metricsContext.withExtraLabels(TrafficEnforcementOutcome.OutcomeAttribute -> outcome)
    reason.fold(withOutcome)(r =>
      withOutcome.withExtraLabels(TrafficEnforcementOutcome.ReasonAttribute -> r)
    )
  }

  def markPrunedRows(table: String, count: Int): Unit =
    prunedRows.mark(count.toLong)(metricsContext.withExtraLabels("table" -> table))
}

class TrafficEnforcementInventory(parent: MetricName)(implicit inventory: HistogramInventory) {
  private[metrics] val prefix = parent :+ "traffic-enforcement"
  val enforcementCheckItem = Item(
    name = prefix :+ "enforcement-check-duration",
    summary = "Duration of traffic enforcement checks",
    description =
      "The time taken to perform traffic enforcement checks, including balance lookups and decision making.",
    qualification = Latency,
  )
}
