// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Meter}
import com.daml.metrics.api.dropwizard.DropwizardGauge
import com.daml.metrics.api.{MetricDoc, MetricHandle as ApiMetricHandle, MetricName, MetricsContext}
import com.daml.metrics.Metrics as LedgerApiServerMetrics
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.data.TaskSchedulerMetrics
import com.digitalasset.canton.metrics.MetricHandle.NodeMetrics
import com.digitalasset.canton.metrics.*
import io.opentelemetry.api.metrics

import scala.collection.concurrent.TrieMap

class ParticipantMetrics(
    name: String,
    override val prefix: MetricName,
    override val registry: MetricRegistry,
    meter: metrics.Meter,
    factory: ApiMetricHandle.Factory,
) extends NodeMetrics {

  private implicit val mc: MetricsContext = MetricsContext("participant_name" -> name)

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
    qualification = Debug,
  )
  val updatesPublished: Meter = meter(prefix :+ "updates-published")

  @MetricDoc.Tag(
    summary = "Number of requests being validated.",
    description = """Number of requests that are currently being validated.
                    |This also covers requests submitted by other participants.
                    |""",
    qualification = Debug,
  )
  val dirtyRequests: Gauge[Int] =
    factory.gauge(prefix :+ "dirty_requests", 0, "Number of requests being validated.")

  @MetricDoc.Tag(
    summary = "Configured maximum number of requests currently being validated.",
    description =
      """Configuration for the maximum number of requests that are currently being validated.
        |This also covers requests submitted by other participants.
        |A negative value means no configuration value was provided and no limit is enforced.
        |""",
    qualification = Debug,
  )
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val maxDirtyRequestGaugeForDocs: Gauge[Int] =
    DropwizardGauge(prefix :+ "max_dirty_requests", null)

  def registerMaxDirtyRequest(value: () => Option[Int]): Unit =
    factory.gaugeWithSupplier(
      prefix :+ "max_dirty_requests",
      () => value().getOrElse(-1),
      """
        |Configuration for the maximum number of requests that are currently being validated.
        | If the value is negative then there is no configuration value and no limit is enforced.""".stripMargin,
    )

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
      qualification = Debug,
    )
    val sequencerCounterQueue: Counter =
      counter(prefix :+ "sequencer-counter-queue")

    @MetricDoc.Tag(
      summary = "Size of conflict detection task queue",
      description = """The task scheduler will schedule tasks to run at a given timestamp. This metric
                      |exposes the number of tasks that are waiting in the task queue for the right time to pass.
                      |A huge number does not necessarily indicate a bottleneck;
                      |it could also mean that a huge number of tasks have not yet arrived at their execution time.""",
      qualification = Debug,
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
    qualification = Debug,
  )
  val numDirtyRequests: Counter = counter(prefix :+ "dirty-requests")

  object recordOrderPublisher extends TaskSchedulerMetrics {

    private val prefix = SyncDomainMetrics.this.prefix :+ "request-tracker"

    @MetricDoc.Tag(
      summary = "Size of record order publisher sequencer counter queue",
      description = """Same as for conflict-detection, but measuring the sequencer counter
          |queues for the publishing to the ledger api server according to record time.""",
      qualification = Debug,
    )
    val sequencerCounterQueue: Counter =
      counter(prefix :+ "sequencer-counter-queue")

    @MetricDoc.Tag(
      summary = "Size of record order publisher task queue",
      description = """The task scheduler will schedule tasks to run at a given timestamp. This metric
                      |exposes the number of tasks that are waiting in the task queue for the right time to pass.""",
      qualification = Debug,
    )
    val taskQueue: RefGauge[Int] = refGauge(prefix :+ "task-queue", 0)
  }

}
