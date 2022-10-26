// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.MetricHandle.{Counter, Meter}
import com.daml.metrics.{MetricName, Metrics as LedgerApiServerMetrics}
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.data.TaskSchedulerMetrics
import com.digitalasset.canton.metrics.MetricHandle.NodeMetrics
import com.digitalasset.canton.metrics.*
import io.opentelemetry.api.metrics

import scala.collection.concurrent.TrieMap

class ParticipantMetrics(
    override val prefix: MetricName,
    override val registry: MetricRegistry,
    meter: metrics.Meter,
) extends NodeMetrics {

  object dbStorage extends DbStorageMetrics(prefix, registry)

  val ledgerApiServer: LedgerApiServerMetrics = new LedgerApiServerMetrics(registry, meter)
  private val clients = TrieMap[DomainAlias, SyncDomainMetrics]()

  object pruning extends PruningMetrics(prefix, registry)

  def domainMetrics(alias: DomainAlias): SyncDomainMetrics = {
    clients.getOrElseUpdate(alias, new SyncDomainMetrics(prefix :+ alias.unwrap, registry))
  }

  @MetricDoc.Tag(
    summary = "Number of updates published through the read service to the indexer",
    description =
      """When an update is published through the read service, it has already been committed to the ledger.
        |The indexer will subsequently store the update in a form that allows for querying the ledger efficiently.""",
  )
  val updatesPublished: Meter = meter(prefix :+ "updates-published")

}

class SyncDomainMetrics(override val prefix: MetricName, val registry: MetricRegistry)
    extends MetricHandle.Factory {

  object sequencerClient extends SequencerClientMetrics(prefix, registry)

  object conflictDetection extends TaskSchedulerMetrics {

    private val prefix = SyncDomainMetrics.this.prefix :+ "conflict-detection"

    @MetricDoc.Tag(
      summary = "Size of conflict detection sequencer counter queue",
      description =
        """The task scheduler will work off tasks according to the timestamp order, scheduling
          |the tasks whenever a new timestamp has been observed. This metric exposes the number of
          |un-processed sequencer messages that will trigger a timestamp advancement.""",
    )
    val sequencerCounterQueue: Counter =
      counter(prefix :+ "sequencer-counter-queue")

    @MetricDoc.Tag(
      summary = "Size of conflict detection task queue",
      description = """The task scheduler will schedule tasks to run at a given timestamp. This metric
                      |exposes the number of tasks that are waiting in the task queue for the right time to pass.
                      |A huge number does not necessarily indicate a bottleneck;
                      |it could also mean that a huge number of tasks have not yet arrived at their execution time.""",
    )
    val taskQueue: RefGauge[Int] = refGauge(prefix :+ "task-queue", 0)

  }

  object transactionProcessing extends TransactionProcessingMetrics(prefix, registry)

  @MetricDoc.Tag(
    summary = "Size of conflict detection task queue",
    description = """The task scheduler will schedule tasks to run at a given timestamp. This metric
                    |exposes the number of tasks that are waiting in the task queue for the right time to pass.
                    |A huge number does not necessarily indicate a bottleneck;
                    |it could also mean that a huge number of tasks have not yet arrived at their execution time.""",
  )
  val numDirtyRequests: Counter = counter(prefix :+ "dirty-requests")

  object recordOrderPublisher extends TaskSchedulerMetrics {

    private val prefix = SyncDomainMetrics.this.prefix :+ "request-tracker"

    @MetricDoc.Tag(
      summary = "Size of record order publisher sequencer counter queue",
      description = """Same as for conflict-detection, but measuring the sequencer counter
          |queues for the publishing to the ledger api server according to record time.""",
    )
    val sequencerCounterQueue: Counter =
      counter(prefix :+ "sequencer-counter-queue")

    @MetricDoc.Tag(
      summary = "Size of record order publisher task queue",
      description = """The task scheduler will schedule tasks to run at a given timestamp. This metric
                      |exposes the number of tasks that are waiting in the task queue for the right time to pass.""",
    )
    val taskQueue: RefGauge[Int] = refGauge(prefix :+ "task-queue", 0)
  }

}
