// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.metrics

import cats.Eval
import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.*
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.MetricHandle.Gauge.CloseableGauge
import com.daml.metrics.api.noop.NoOpGauge
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.TaskSchedulerMetrics
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.http.metrics.{HttpApiHistograms, HttpApiMetrics}
import com.digitalasset.canton.metrics.*
import com.digitalasset.canton.metrics.ActiveRequestsMetrics.GrpcServerMetricsX
import com.digitalasset.canton.participant.metrics.PruningMetrics as ParticipantPruningMetrics
import com.digitalasset.canton.topology.PhysicalSynchronizerId

import scala.collection.concurrent.TrieMap

class ParticipantHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "participant"

  private[metrics] val ledgerApiServer: LedgerApiServerHistograms =
    new LedgerApiServerHistograms(prefix :+ "api")

  private[metrics] val httpApi: HttpApiHistograms =
    new HttpApiHistograms(prefix)

  private[metrics] val dbStorage: DbStorageHistograms =
    new DbStorageHistograms(parent)
  private[metrics] val sequencerClient: SequencerClientHistograms = new SequencerClientHistograms(
    parent
  )
  private[metrics] val connectedSynchronizer: ConnectedSynchronizerHistograms =
    new ConnectedSynchronizerHistograms(
      prefix,
      sequencerClient,
    )
  private[metrics] val pruning: PruningHistograms = new PruningHistograms(parent)

  private[metrics] val consolePrefix: MetricName = prefix :+ "console"
  private[metrics] val consoleNodeCount: Item =
    Item(
      consolePrefix :+ "tx-node-count",
      "Number of nodes per transaction histogram, measured using canton console ledger_api.updates.start_measure",
      MetricQualification.Debug,
    )
  private[metrics] val consoleTransactionSize: Item =
    Item(
      consolePrefix :+ "tx-size",
      "Transaction size histogram, measured using canton console ledger_api.updates.start_measure ",
      MetricQualification.Debug,
    )

  private[metrics] val phase: Item =
    Item(
      prefix :+ "phase",
      summary =
        "Phase metrics measuring the time for the various command submission processing stages",
      description =
        """Time from receipt of command to submission such as interpretation and view computation.""",
      qualification = MetricQualification.Latency,
      labelsWithDescription = Map(
        "synchronizer" -> "synchronizer",
        "phase" -> "phase",
      ),
    )

}

class ParticipantMetrics(
    inventory: ParticipantHistograms,
    override val openTelemetryMetricsFactory: LabeledMetricsFactory,
) extends BaseMetrics {

  private implicit val mc: MetricsContext = MetricsContext.Empty

  // The metrics documentation generation requires all metrics to be registered in the factory.
  // However, the following metric is registered on-demand during normal operation. Therefore,
  // we use this environment variable approach to guard against instantiation in production; but
  // register the metric for the documentation generation.
  if (sys.env.contains("GENERATE_METRICS_FOR_DOCS")) {
    new ConnectedSynchronizerMetrics(
      SynchronizerAlias.tryCreate("synchronizer"),
      inventory.connectedSynchronizer,
      openTelemetryMetricsFactory,
    )
  }

  override val prefix: MetricName = inventory.prefix

  override val declarativeApiMetrics: DeclarativeApiMetrics =
    new DeclarativeApiMetrics(prefix, openTelemetryMetricsFactory)
  override def grpcMetrics: GrpcServerMetricsX = (ledgerApiServer.grpc, ledgerApiServer.requests)
  override def healthMetrics: HealthMetrics = ledgerApiServer.health
  override def storageMetrics: DbStorageMetrics = dbStorage
  val dbStorage = new DbStorageMetrics(inventory.dbStorage, openTelemetryMetricsFactory)
  val kmsMetrics: KmsMetrics = new KmsMetrics(prefix, openTelemetryMetricsFactory)
  val phase: Timer = openTelemetryMetricsFactory.timer(inventory.phase.info)

  // Private constructor to avoid being instantiated multiple times by accident
  final class ConsoleThroughputMetrics private[ParticipantMetrics] {
    private val prefix = ParticipantMetrics.this.prefix :+ "console"
    val metric: Meter =
      openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "tx-nodes-emitted",
          "Total number of nodes emitted, measured using canton console ledger_api.updates.start_measure",
          MetricQualification.Debug,
        )
      )
    val nodeCount: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.consoleNodeCount.info)
    val transactionSize: Histogram =
      openTelemetryMetricsFactory.histogram(inventory.consoleTransactionSize.info)
  }

  val consoleThroughput = new ConsoleThroughputMetrics

  val ledgerApiServer: LedgerApiServerMetrics =
    new LedgerApiServerMetrics(
      inventory.ledgerApiServer,
      openTelemetryMetricsFactory,
    )

  val httpApiServer: HttpApiMetrics =
    new HttpApiMetrics(inventory.httpApi, openTelemetryMetricsFactory)

  private val clients = TrieMap[SynchronizerAlias, Eval[ConnectedSynchronizerMetrics]]()

  val pruning = new ParticipantPruningMetrics(inventory.pruning, openTelemetryMetricsFactory)

  def connectedSynchronizerMetrics(alias: SynchronizerAlias): ConnectedSynchronizerMetrics =
    clients
      .getOrElseUpdate(
        alias,
        // Two concurrent calls with the same synchronizer alias may cause getOrElseUpdate to evaluate the new value expression twice,
        // even though only one of the results will be stored in the map.
        // Eval.later ensures that we actually create only one instance of ConnectedSynchronizerMetrics in such a case
        // by delaying the creation until the getOrElseUpdate call has finished.
        Eval.later(
          new ConnectedSynchronizerMetrics(
            alias,
            inventory.connectedSynchronizer,
            openTelemetryMetricsFactory,
          )(
            mc.withExtraLabels("synchronizer" -> alias.unwrap)
          )
        ),
      )
      .value

  val inflightValidationRequests: Gauge[Int] =
    openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "inflight_validation_requests",
        summary = "Number of requests being validated.",
        description = """Number of requests that are currently being validated.
                        |This also covers requests submitted by other participants.
                        |""",
        qualification = MetricQualification.Saturation,
        labelsWithDescription = Map(
          "participant" -> "The id of the participant for which the value applies."
        ),
      ),
      0,
    )

  private val maxInflightValidationRequestGaugeForDocs: Gauge[Int] =
    NoOpGauge(
      MetricInfo(
        prefix :+ "max_inflight_validation_requests",
        summary = "Configured maximum number of requests currently being validated.",
        description =
          """Configuration for the maximum number of requests that are currently being validated.
          |This also covers requests submitted by other participants.
          |A negative value means no configuration value was provided and no limit is enforced.
          |""",
        qualification = MetricQualification.Debug,
        labelsWithDescription = Map(
          "participant" -> "The id of the participant for which the value applies."
        ),
      ),
      0,
    )

  def registerMaxInflightValidationRequest(value: () => Option[Int]): Gauge.CloseableGauge =
    openTelemetryMetricsFactory.closeableGaugeWithSupplier(
      maxInflightValidationRequestGaugeForDocs.info,
      () => value().getOrElse(-1),
    )

  // Since gauges don't support metrics context per update, create a map with a gauge per successor psid.
  private val lsuStatus: TrieMap[PhysicalSynchronizerId, Gauge[Int]] = TrieMap.empty

  /** Update the value of the metric if the provided value is bigger than the stored value. Create
    * the metric otherwise.
    */
  def setLsuStatus(
      value: NonNegativeInt,
      successorPsid: PhysicalSynchronizerId,
  ): Unit =
    lsuStatus
      .updateWith(successorPsid) {
        case None =>
          Some(
            newLsuStatus(
              MetricsContext("successor_psid" -> successorPsid.toProtoPrimitive),
              value = value.unwrap,
            )
          )
        case Some(gauge) =>
          gauge.updateValue(_.max(value.unwrap))
          Some(gauge)
      }
      .discard

  /** Update the value of the metric to zero. Create the metric otherwise.
    */
  def resetLsuStatus(
      successorPsid: PhysicalSynchronizerId
  ): Unit = {
    val value = ParticipantMetrics.LsuStatus.NoLsu.unwrap

    lsuStatus
      .updateWith(successorPsid) {
        case None =>
          Some(
            newLsuStatus(
              MetricsContext("successor_psid" -> successorPsid.toProtoPrimitive),
              value = value,
            )
          )
        case Some(gauge) =>
          gauge.updateValue(value)
          Some(gauge)
      }
      .discard
  }

  private def newLsuStatus(mc: MetricsContext, value: Int) = openTelemetryMetricsFactory.gauge(
    info = MetricInfo(
      name = prefix :+ "lsu_status",
      summary = "Tracks the state of the LSU for a specific successor",
      qualification = MetricQualification.Debug,
      description = """The value represents the progress of LSU from the participant point of view.
          |0: Unset / initial
          |1: LSU announcement received
          |2: Relevant sequencer successors known
          |3: Handshake with successor done
          |4: Topology local copy done
          |5: LSU is done (node ready to connect to new synchronizer)
          |""".stripMargin,
      labelsWithDescription = Map(
        "successor_psid" -> "The physical synchronizer id of the successor"
      ),
    ),
    initial = value,
  )(mc)
}

object ParticipantMetrics {
  object LsuStatus {
    val NoLsu: NonNegativeInt = NonNegativeInt.zero // Only after a cancellation
    val LsuAnnounced: NonNegativeInt = NonNegativeInt.one
    val SequencerSuccessorsKnown: NonNegativeInt = NonNegativeInt.two
    val HandshakeDone: NonNegativeInt = NonNegativeInt.three
    val LocalCopyDone: NonNegativeInt = NonNegativeInt.tryCreate(4)
    val LsuDone: NonNegativeInt = NonNegativeInt.tryCreate(5)
  }
}

class ConnectedSynchronizerHistograms private[metrics] (
    val parent: MetricName,
    val sequencerClient: SequencerClientHistograms,
)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix: MetricName = parent :+ "sync"

  private[metrics] val transactionProcessing: TransactionProcessingHistograms =
    new TransactionProcessingHistograms(prefix)

  private[metrics] val commitments: CommitmentHistograms = new CommitmentHistograms(prefix)

}

class ConnectedSynchronizerMetrics private[metrics] (
    synchronizerAlias: SynchronizerAlias,
    histograms: ConnectedSynchronizerHistograms,
    factory: LabeledMetricsFactory,
)(implicit metricsContext: MetricsContext) {

  val sequencerClient: SequencerClientMetrics =
    new SequencerClientMetrics(histograms.sequencerClient, factory)

  val conflictDetection: TaskSchedulerMetrics = new TaskSchedulerMetrics {

    private val prefix = histograms.prefix :+ "conflict-detection"

    val sequencerCounterQueue: Counter =
      factory.counter(
        MetricInfo(
          prefix :+ "sequencer-counter-queue",
          summary = "Size of conflict detection sequencer counter queue",
          description =
            """The task scheduler will work off tasks according to the timestamp order, scheduling
              |the tasks whenever a new timestamp has been observed. This metric exposes the number of
              |un-processed sequencer messages that will trigger a timestamp advancement.""",
          qualification = MetricQualification.Debug,
        )
      )

    val taskQueueForDoc: Gauge[Int] = NoOpGauge(
      MetricInfo(
        prefix :+ "task-queue",
        summary = "Size of conflict detection task queue",
        description = """This metric measures the size of the queue for conflict detection between
            |concurrent transactions.
            |A huge number does not necessarily indicate a bottleneck;
            |it could also mean that a huge number of tasks have not yet arrived at their execution time.""",
        qualification = MetricQualification.Debug,
      ),
      0,
    )
    def taskQueue(size: () => Int): CloseableGauge =
      factory.gauge(taskQueueForDoc.info, 0)

  }

  val commitments: CommitmentMetrics =
    new CommitmentMetrics(synchronizerAlias, histograms.commitments, factory)

  val transactionProcessing: TransactionProcessingMetrics =
    new TransactionProcessingMetrics(histograms.transactionProcessing, factory)

  val numInflightValidations: Counter = factory.counter(
    MetricInfo(
      histograms.prefix :+ "inflight-validations",
      summary = "Number of requests being validated on the synchronizer.",
      description = """Number of requests that are currently being validated on the synchronizer.
                    |This also covers requests submitted by other participants.
                    |""",
      qualification = MetricQualification.Saturation,
    )
  )

  val recordOrderPublisher: TaskSchedulerMetrics = new TaskSchedulerMetrics {

    private val prefix = histograms.prefix :+ "request-tracker"

    val sequencerCounterQueue: Counter =
      factory.counter(
        MetricInfo(
          prefix :+ "sequencer-counter-queue",
          summary = "Size of record order publisher sequencer counter queue",
          description = """Same as for conflict-detection, but measuring the sequencer counter
              |queues for the publishing to the ledger api server according to record time.""",
          qualification = MetricQualification.Debug,
        )
      )

    val taskQueueForDoc: Gauge[Int] = NoOpGauge(
      MetricInfo(
        prefix :+ "task-queue",
        summary = "Size of record order publisher task queue",
        description = """The task scheduler will schedule tasks to run at a given timestamp. This metric
            |exposes the number of tasks that are waiting in the task queue for the right time to pass.""",
        qualification = MetricQualification.Debug,
      ),
      0,
    )

    def taskQueue(size: () => Int): CloseableGauge =
      factory.closeableGaugeWithSupplier(taskQueueForDoc.info, size)
  }

  // Private constructor to avoid being instantiated multiple times by accident
  final class InFlightSubmissionSynchronizerTrackerMetrics private[ConnectedSynchronizerMetrics] {

    private val prefix = histograms.prefix :+ "in-flight-submission-synchronizer-tracker"

    val unsequencedInFlight: Gauge[Int] =
      factory.gauge(
        MetricInfo(
          prefix :+ "unsequenced-in-flight-submissions",
          summary = "Number of unsequenced submissions in-flight.",
          description = """Number of unsequenced submissions in-flight.
                          |Unsequenced in-flight submissions are tracked in-memory, so high amount here will boil down to memory pressure.
                          |""",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )
  }

  val inFlightSubmissionSynchronizerTracker: InFlightSubmissionSynchronizerTrackerMetrics =
    new InFlightSubmissionSynchronizerTrackerMetrics

}
