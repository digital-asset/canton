// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Gauge, LabeledMetricsFactory}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricQualification, MetricsContext}

class TransactionNodeHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private val prefix = parent :+ "transaction-node"

  val signatorySize: Item =
    Item(
      name = prefix :+ "signatories",
      summary = "The number of signatories in a transaction node or input contract",
      description =
        "The number of signatories in a transaction create node, exercise node, fetch node or input contract",
      qualification = MetricQualification.Saturation,
    )

  val stakeholderSize: Item =
    Item(
      name = prefix :+ "stakeholders",
      summary = "The number of stakeholders in a transaction node or input contract",
      description =
        "The number of stakeholders in a transaction create node, exercise node, fetch node or input contract",
      qualification = MetricQualification.Saturation,
    )

  val actingPartySize: Item =
    Item(
      name = prefix :+ "acting-parties",
      summary = "The number of acting parties in a transaction node",
      description = "The number of acting parties in a transaction exercise node or fetch node",
      qualification = MetricQualification.Saturation,
    )

  val choiceObserverSize: Item =
    Item(
      name = prefix :+ "choice-observers",
      summary = "The number of choice observers in a transaction exercise node",
      description = "The number of choice observers in a transaction exercise node",
      qualification = MetricQualification.Saturation,
    )

  val choiceAuthorizerSize: Item =
    Item(
      name = prefix :+ "choice-authorizers",
      summary = "The number of choice authorizers in a transaction exercise node",
      description = "The number of choice authorizers in a transaction exercise node",
      qualification = MetricQualification.Saturation,
    )

  val keyMaintainerSize: Item =
    Item(
      name = prefix :+ "key-maintainers",
      summary = "The number of contract key maintainers in a transaction node or input contract",
      description =
        "The maximal number of contract key maintainers in a transaction create node, exercise node, fetch node, query by key node or input contract",
      qualification = MetricQualification.Saturation,
    )

  val lfValueSize: Item =
    Item(
      name = prefix :+ "lf-value",
      summary = "The byte size of an LF value in a transaction node or input contract",
      description =
        "The maximal byte size of an LF value create argument, create key, exercise choice argument, exercise choice result, exercise contract key, fetch contract key, query by key contract key, input contract create argument or input contract contract key",
      qualification = MetricQualification.Saturation,
    )

  val externalCallResults: Item =
    Item(
      name = prefix :+ "external-call-results",
      summary = "The number of external call results in a transaction exercise node",
      description = "The number of external call results in a transaction exercise node",
      qualification = MetricQualification.Saturation,
    )

  val externalCallResultSize: Item =
    Item(
      name = prefix :+ "external-call-result-size",
      summary = "The byte size of an external call result in a transaction exercise node",
      description =
        "The maximal byte size of an external call result in a transaction exercise node",
      qualification = MetricQualification.Saturation,
    )

  val nodeChildrenSize: Item =
    Item(
      name = prefix :+ "node-children-size",
      summary = "The number child nodes to a transaction exercise node",
      description = "The number child nodes to a transaction exercise node",
      qualification = MetricQualification.Saturation,
    )

  val totalInformeeSize: Item =
    Item(
      name = prefix :+ "total-informees",
      summary = "The number informees for a transaction tree",
      description = "The number informees for a transaction tree",
      qualification = MetricQualification.Saturation,
    )

  val rootNodeCount: Item =
    Item(
      name = prefix :+ "root-node-count",
      summary = "The number of root nodes in the transaction tree",
      description = "The number of root nodes in the transaction tree",
      qualification = MetricQualification.Saturation,
    )

  val nodeCount: Item =
    Item(
      name = prefix :+ "node-count",
      summary = "The total number of nodes in the transaction tree",
      description = "The total number of nodes in the transaction tree",
      qualification = MetricQualification.Saturation,
    )

  val inputContractCount: Item =
    Item(
      name = prefix :+ "input-contract-count",
      summary = "The number of input contracts",
      description = "The number of input contracts",
      qualification = MetricQualification.Saturation,
    )
}

class TransactionNodeMetrics(
    histograms: TransactionNodeHistograms,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {
  val signatoriesGauge: Gauge[Int] = factory.gauge[Int](histograms.signatorySize.info, 0)

  val stakeholdersGauge: Gauge[Int] = factory.gauge[Int](histograms.stakeholderSize.info, 0)

  val actingPartiesGauge: Gauge[Int] = factory.gauge[Int](histograms.actingPartySize.info, 0)

  val choiceObserversGauge: Gauge[Int] = factory.gauge[Int](histograms.choiceObserverSize.info, 0)

  val choiceAuthorizersGauge: Gauge[Int] =
    factory.gauge[Int](histograms.choiceAuthorizerSize.info, 0)

  val keyMaintainersGauge: Gauge[Int] = factory.gauge[Int](histograms.keyMaintainerSize.info, 0)

  val lfValueGauge: Gauge[Int] = factory.gauge[Int](histograms.lfValueSize.info, 0)

  val externalCallResultsGauge: Gauge[Int] =
    factory.gauge[Int](histograms.externalCallResults.info, 0)

  val externalCallResultSizeGauge: Gauge[Int] =
    factory.gauge[Int](histograms.externalCallResultSize.info, 0)

  val nodeChildrenGauge: Gauge[Int] = factory.gauge[Int](histograms.nodeChildrenSize.info, 0)

  val totalInformeesGauge: Gauge[Long] = factory.gauge[Long](histograms.totalInformeeSize.info, 0)

  val transactionRootsGauge: Gauge[Int] = factory.gauge[Int](histograms.rootNodeCount.info, 0)

  val transactionNodesGauge: Gauge[Int] = factory.gauge[Int](histograms.nodeCount.info, 0)

  val inputContractsGauge: Gauge[Int] = factory.gauge[Int](histograms.inputContractCount.info, 0)
}
