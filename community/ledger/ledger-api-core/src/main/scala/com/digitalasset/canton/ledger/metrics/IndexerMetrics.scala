// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.DatabaseMetrics
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{Counter, Gauge, Histogram, LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{
  HistogramInventory,
  MetricHandle,
  MetricInfo,
  MetricName,
  MetricQualification,
  MetricsContext,
}

class IndexerHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val inputMappingPrefix: MetricName = prefix :+ "inputmapping"

  private[metrics] val inputMappingBatchSize: Item = Item(
    inputMappingPrefix :+ "batch_size",
    summary = "The batch sizes in the indexer.",
    description = """The number of state updates contained in a batch used in the indexer for
                    |database submission.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val inputMappingBatchWeight: Item = Item(
    inputMappingPrefix :+ "batch_weight",
    summary = "The batch weights in the indexer.",
    description =
      """The calculated weights of state updates contained in a batch used in the indexer for
                    |database submission, if useWeightedBatching was specified.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val seqMappingDuration: Item = Item(
    prefix :+ "seqmapping" :+ "duration",
    summary = "The duration of the seq-mapping stage.",
    description = """The time that a batch of updates spends in the seq-mapping stage of the
                    |indexer.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val ingestionBlockeByPruningDuration: Item = Item(
    prefix :+ "ingestion_blocked_by_pruning" :+ "duration",
    summary = "The duration of ingestions DB execution is blocked by pruning.",
    description =
      "The time that a batch of updates spends in blocked waiting for the pruning DB operation to finish.",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val deactivationDistances: Item = Item(
    prefix :+ "deactivation_distances",
    summary = "Event sequence id distances between activations and deactivations.",
    description = "Histogram to collect the statistics of how long individual contracts lived.",
    qualification = MetricQualification.Debug,
  )
}

class IndexerMetrics(
    histograms: IndexerHistograms,
    factory: LabeledMetricsFactory,
) {

  import MetricsContext.Implicits.empty

  private val prefix = histograms.prefix

  val initialization = new DatabaseMetrics(prefix :+ "initialization", factory)

  // Number of state updates persisted to the database
  // (after the effect of the corresponding Update is persisted into the database,
  // and before this effect is visible via moving the ledger end forward)
  val updates: Counter = factory.counter(
    MetricInfo(
      prefix :+ "updates",
      summary = "The number of the state updates persisted to the database.",
      description = """The number of the state updates persisted to the database. There are
          |updates such as accepted transactions, configuration changes,
          |party allocations, rejections, etc, but they also include synthetic events
          |when the node learned about the sequencer clock advancing without any actual
          |ledger event such as due to submission receipts or time proofs.""",
      qualification = MetricQualification.Traffic,
    )
  )

  val deactivationDistances: Histogram = factory.histogram(histograms.deactivationDistances.info)

  val outputBatchedBufferLength: Counter =
    factory.counter(
      MetricInfo(
        prefix :+ "output_batched_buffer_length",
        summary =
          "The size of the queue between the indexer and the in-memory state updating flow.",
        description =
          """This counter counts batches of updates passed to the in-memory flow. Batches
            |are dynamically-sized based on amount of backpressure exerted by the
            |downstream stages of the flow.""",
        qualification = MetricQualification.Debug,
      )
    )

  // Number of times the Indexer needed to restart due to missing referenced contracts (likely because of pruning)
  val indexerRestartDueToMissingReferencedContracts: Counter = factory.counter(
    MetricInfo(
      prefix :+ "indexer_restart_due_to_missing_contract",
      summary =
        "Number of times the Indexer needed to restart due to missing referenced contracts.",
      description = """Under seldom circumstances the indexer could be forced to restart if pruning removed
                      |referenced contracts. If this happens the missing contracts will be re-inserted to the DB
                      |and indexing continues. This is part for the normal operation and should happen very rarely.""",
      qualification = MetricQualification.Traffic,
    )
  )

  val ingestionBlockeByPruningDuration: Timer =
    factory.timer(histograms.ingestionBlockeByPruningDuration.info)

  // Input mapping stage
  // Translating state updates to data objects corresponding to individual SQL insert statements
  object inputMapping {

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = histograms.inputMappingPrefix :+ "executor"

    val batchSize: Histogram =
      factory.histogram(histograms.inputMappingBatchSize.info)

    val batchWeight: Histogram =
      factory.histogram(histograms.inputMappingBatchWeight.info)

    val submissionBatchConfiguredWeight: Gauge[Long] =
      factory.gauge(
        MetricInfo(
          prefix :+ "submission_batch_configured_weight",
          summary = "The configured weight of each submission batch.",
          description =
            """This value is calculated from `submissionBatchInsertionSize` of the indexer config by multiplying
              |the default weight of one insert operation.""".stripMargin,
          qualification = MetricQualification.Debug,
        ),
        0L,
      )

  }

  // Batching stage
  // Translating batch data objects to db-specific DTO batches
  object batching {
    private val prefix: MetricName = IndexerMetrics.this.prefix :+ "batching"

    // Bundle of metrics coming from instrumentation of the underlying thread-pool
    val executor: MetricName = prefix :+ "executor"
  }

  // Sequence Mapping stage
  object seqMapping {

    val duration: Timer = factory.timer(histograms.seqMappingDuration.info)
  }

  // Ingestion stage
  // Parallel ingestion of prepared data into the database
  val ingestion = new DatabaseMetrics(prefix :+ "ingestion", factory)

  // Tail ingestion stage
  // The throttled update of ledger end parameters
  val tailIngestion = new DatabaseMetrics(prefix :+ "tail_ingestion", factory)

  // Post Processing end ingestion stage
  // The throttled update of post processing end parameter
  val postProcessingEndIngestion =
    new DatabaseMetrics(prefix :+ "post_processing_end_ingestion", factory)

  val achsProcessing =
    new DatabaseMetrics(prefix :+ "achs_processing", factory)

  val achsBufferLength: Counter =
    factory.counter(
      MetricInfo(
        prefix :+ "achs_buffer_length",
        summary = "The size of the queue between the indexer and the ACHS maintenance pipe.",
        description =
          """This counter counts batches of updates queued before the ACHS maintenance pipe.
            |When the buffer is mostly full, it indicates that ACHS maintenance is creating
            |backpressure on the indexing pipeline.""",
        qualification = MetricQualification.Debug,
      )
    )

  val achsValidAt: Gauge[Long] =
    factory.gauge(
      MetricInfo(
        prefix :+ "achs_valid_at",
        summary = "The event sequential id at which the ACHS is valid.",
        description =
          """The event sequential id at which the ACHS is currently valid. It may contain some
            |deactivated events but they will anyway be removed when fetched.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val achsLastPopulated: Gauge[Long] =
    factory.gauge(
      MetricInfo(
        prefix :+ "achs_last_populated",
        summary = "The last event sequential id populated into the ACHS.",
        description =
          """The last event sequential id for which activations were added to the ACHS.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val achsLastRemoved: Gauge[Long] =
    factory.gauge(
      MetricInfo(
        prefix :+ "achs_last_removed",
        summary =
          "The last event sequential id for which deactivations were removed from the ACHS.",
        description = """The last event sequential id for which deactivations were looked up and the
            |corresponding activations were removed from the ACHS.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val indexerQueueBlocked: MetricHandle.Meter = factory.meter(
    MetricInfo(
      prefix :+ "indexer_queue_blocked",
      summary = "The amount of blocked enqueue operations for the indexer queue.",
      description =
        """Indexer queue exerts backpressure by blocking asynchronous enqueue operations.
          |This meter measures the amount of such blocked operations, signalling backpressure
          |materializing from downstream.""",
      qualification = MetricQualification.Debug,
    )
  )

  val indexerQueueBuffered: MetricHandle.Meter = factory.meter(
    MetricInfo(
      prefix :+ "indexer_queue_buffered",
      summary = "The size of the buffer before the indexer.",
      description =
        """This buffer is located before the indexer, increasing amount signals backpressure mounting.""",
      qualification = MetricQualification.Debug,
    )
  )

  val indexerQueueUncommitted: MetricHandle.Meter = factory.meter(
    MetricInfo(
      prefix :+ "indexer_queue_uncommitted",
      summary = "The amount of entries which are uncommitted for the indexer.",
      description =
        """Uncommitted entries contain all blocked, buffered and submitted, but not yet committed entries.
          |This amount signals the momentum of stream processing, and has a theoretical maximum defined by all
          |the queue perameters.""".stripMargin,
      qualification = MetricQualification.Debug,
    )
  )

  val ledgerEndSequentialId: Gauge[Long] =
    factory.gauge(
      MetricInfo(
        prefix :+ "ledger_end_sequential_id",
        summary = "The sequential id of the current ledger end kept in the database.",
        description = """The ledger end's sequential id is a monotonically increasing integer value
                      |representing the sequential id ascribed to the most recent ledger event
                      |ingested by the index db. Please note, that only a subset of all ledger events
                      |are ingested and given a sequential id. These are: creates, consuming
                      |exercises, non-consuming exercises and divulgence events. This value can be
                      |treated as a counter of all such events visible to a given participant. This
                      |metric exposes the latest ledger end's sequential id registered in the
                      |database.""",
        qualification = MetricQualification.Debug,
      ),
      0L,
    )

  val meteredEventsMeter: MetricHandle.Meter = factory.meter(
    MetricInfo(
      prefix :+ "metered_events",
      summary = "Number of individual ledger events (create, exercise, archive).",
      description =
        """Represents the number of individual ledger events constituting a transaction.""",
      qualification = MetricQualification.Debug,
      labelsWithDescription = Map(
        "participant_id" -> "The id of the participant.",
        "user_id" -> "The user generating the events.",
      ),
    )
  )

  val eventsMeter: MetricHandle.Meter =
    factory.meter(
      MetricInfo(
        prefix :+ "events",
        summary = "Number of ledger events processed.",
        description =
          "Represents the total number of ledger events processed (transactions, reassignments, party allocations).",
        qualification = MetricQualification.Debug,
        labelsWithDescription = Map(
          "participant_id" -> "The id of the participant.",
          "user_id" -> "The user generating the events.",
          "event_type" -> "The type of ledger event processed (transaction, reassignment, party_allocation).",
          "status" -> "Indicates if the event was accepted or not. Possible values accepted|rejected.",
        ),
      )
    )
}

object IndexerMetrics {

  object Labels {
    val userId = "user_id"
    val grpcCode = "grpc_code"
    object eventType {

      val key = "event_type"

      val partyAllocation = "party_allocation"
      val transaction = "transaction"
      val reassignment = "reassignment"
      val topologyTransaction = "topology_transaction"
    }

    object status {
      val key = "status"

      val accepted = "accepted"
      val rejected = "rejected"
    }

  }
}
