// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.metrics

import com.daml.metrics.api.*
import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.{CacheMetrics, HealthMetrics}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.BaseMetrics
import com.digitalasset.canton.logging.pretty.PrettyNameOnlyCase
import com.digitalasset.canton.metrics.ActiveRequestsMetrics.GrpcServerMetricsX
import com.digitalasset.canton.metrics.{
  CryptoMetrics,
  DbStorageHistograms,
  DbStorageMetrics,
  DeclarativeApiMetrics,
  DecryptionHistograms,
  DecryptionMetrics,
  KmsMetrics,
  SigningHistograms,
  SigningMetrics,
}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerConnectionStatus,
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
  PeerNetworkStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  ModuleContext,
}
import com.digitalasset.canton.util.Mutex

import java.time.{Duration, Instant}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

private[metrics] final class BftOrderingHistograms(val parent: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val prefix = parent :+ BftOrderingMetrics.Prefix

  private[metrics] val dbStorage = new DbStorageHistograms(parent)

  private[metrics] val signingHistograms = new SigningHistograms(parent)
  private[metrics] val decryptionHistograms = new DecryptionHistograms(parent)

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class PerformanceHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "performance"

    private[metrics] val orderingStageLatency: Item = Item(
      prefix :+ "ordering-stage-latency",
      summary = "Ordering stage latency",
      description =
        """Records the rate and latency it takes for an ordering stage, which is recorded as a label.
          |This metric is meaningful only when sequencers' clocks are kept synchronized.""",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val performance = new PerformanceHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class GlobalHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "global"

    private[metrics] val requestsOrderingLatency: Item = Item(
      prefix :+ "requests-ordering-latency",
      summary = "Requests ordering latency",
      description =
        """Records the rate and latency it takes to order requests. This metric is always meaningful
          |when queried on and restricted to the receiving sequencer; in other cases, it is meaningful only
          |when the receiving and reporting sequencers' clocks are kept synchronized.""",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val global = new GlobalHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class IngressHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "ingress"

    private[metrics] val requestsSize: Item = Item(
      prefix :+ "requests-size",
      summary = "Requests size",
      description = "Records the size of requests to the BFT ordering service.",
      qualification = MetricQualification.Traffic,
    )
  }
  private[metrics] val ingress = new IngressHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class AvailabilityHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "availability"

    private[metrics] val outputFetchLatency = Item(
      prefix :+ "output-fetch-latency",
      summary = "Output fetch latency",
      description =
        "Records the rate and latency it takes for one output fetch request to get an response (requests that timeout are not included)",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val availability = new AvailabilityHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class ConsensusHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "consensus"

    private[metrics] val consensusCommitLatency: Item = Item(
      prefix :+ "commit-latency",
      summary = "Consensus commit latency",
      description =
        "Records the rate and latency it takes to commit a block at the consensus level.",
      qualification = MetricQualification.Latency,
    )

    private[metrics] val viewChangeProgressLatency: Item = Item(
      prefix :+ "view-change-progress-latency",
      summary = "View change progress latency",
      description = "Records the rate and latency it takes to make progress on a view.",
      qualification = MetricQualification.Latency,
    )

    private[metrics] val relativeSegmentLatency: Item = Item(
      prefix :+ "relative-segment-latency",
      summary = "Relative segment latency",
      description =
        "Records the rate and latency it takes to complete a segment after the segment led by this node completed",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val consensus = new ConsensusHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class OutputHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "output"

    private[metrics] val blockSizeBytes: Item = Item(
      prefix :+ "block-size-bytes",
      summary = "Block size (bytes)",
      description = "Records the size (in bytes) of blocks ordered.",
      qualification = MetricQualification.Traffic,
    )

    private[metrics] val blockSizeRequests: Item = Item(
      prefix :+ "block-size-requests",
      summary = "Block size (requests)",
      description = "Records the size (in requests) of blocks ordered.",
      qualification = MetricQualification.Traffic,
    )

    private[metrics] val blockSizeBatches: Item = Item(
      prefix :+ "block-size-batches",
      summary = "Block size (batches)",
      description = "Records the size (in batches) of blocks ordered.",
      qualification = MetricQualification.Traffic,
    )

    private[metrics] val blockDelay: Item = Item(
      prefix :+ "block-delay",
      summary = "Block delay",
      description =
        "Wall-clock time of the ordered block being provided to the sequencer minus BFT time of the block.",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val output = new OutputHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class TopologyHistograms private[BftOrderingHistograms] {
    private[metrics] val prefix = BftOrderingHistograms.this.prefix :+ "topology"

    private[metrics] val queryLatency: Item = Item(
      prefix :+ "query-latency",
      summary = "Topology query latency",
      description = "Records the rate and latency when querying the topology client.",
      qualification = MetricQualification.Latency,
    )
  }
  private[metrics] val topology = new TopologyHistograms

  // Private constructor to avoid being instantiated multiple times by accident
  private[metrics] final class P2PHistograms private[BftOrderingHistograms] {
    val p2pPrefix: MetricName = BftOrderingHistograms.this.prefix :+ "p2p"

    // Private constructor to avoid being instantiated multiple times by accident
    private[metrics] class SendMetrics private[P2PHistograms] {
      val prefix: MetricName = p2pPrefix :+ "send"

      private[metrics] val grpcOnNextLatency: Item = Item(
        prefix :+ "network-write-latency",
        summary = "P2P gRPC `onNext` latency",
        description =
          "Records the rate and latency when sending messages to the P2P stream via gRPC's `onNext`.",
        qualification = MetricQualification.Latency,
      )

      private[metrics] val grpcLatency: Item = Item(
        prefix :+ "grpc-latency",
        summary = "Total latency of a P2P gRPC message send",
        description =
          "Records the rate of gRPC message sends and their latency (up to receiving them on the other side).",
        qualification = MetricQualification.Latency,
      )

      private[metrics] val grpcFlowControlNotReadyLatency: Item = Item(
        prefix :+ "grpc-flow-control-not-ready",
        summary = "Duration of gRPC flow control not being ready",
        description = "Records the rate and duration of gRPC flow control not being ready.",
        qualification = MetricQualification.Latency,
      )

      private[metrics] val messageSize: Item = Item(
        prefix :+ "sent-message-size",
        summary = "Sent message size",
        description =
          "Records the distribution of serialized gRPC message sizes (in bytes) sent to each peer.",
        qualification = MetricQualification.Traffic,
      )
    }
    private[metrics] val send = new SendMetrics

    // Private constructor to avoid being instantiated multiple times by accident
    private[metrics] final class ReceiveMetrics private[P2PHistograms] {
      val prefix: MetricName = p2pPrefix :+ "receive"

      private[metrics] val processingLatency: Item = Item(
        prefix :+ "processing-latency",
        summary = "Message receive processing latency",
        description = "Records the rate and latency when processing incoming P2P network messages.",
        qualification = MetricQualification.Latency,
      )

      private[metrics] val messageSize: Item = Item(
        prefix :+ "received-message-size",
        summary = "Received message size",
        description =
          "Records the distribution of serialized gRPC message sizes (in bytes) received from each peer.",
        qualification = MetricQualification.Traffic,
      )
    }
    private[metrics] val receive = new ReceiveMetrics

    // Private constructor to avoid being instantiated multiple times by accident
    private[metrics] final class StreamMetrics private[P2PHistograms] {
      val prefix: MetricName = p2pPrefix :+ "stream"

      private[metrics] val roundTripLatency: Item = Item(
        prefix :+ "round-trip-latency",
        summary = "P2P application-level round-trip time",
        description =
          "Records the distribution of the application-level round-trip time to each peer, measured " +
            "by periodically sending a low-frequency `ConnectionOpened` echo probe that the peer reflects " +
            "back. Unlike the TCP-level RTT (see the transport metrics), this is connection-direction- and " +
            "transport-independent, so it is always available, but it also includes application-level queuing " +
            "and processing time.",
        qualification = MetricQualification.Latency,
      )
    }
    private[metrics] val stream = new StreamMetrics
  }
  private[metrics] val p2p = new P2PHistograms
}

class BftOrderingMetrics private[metrics] (
    histograms: BftOrderingHistograms,
    override val openTelemetryMetricsFactory: LabeledMetricsFactory,
    override val grpcMetrics: GrpcServerMetricsX,
    override val healthMetrics: HealthMetrics,
) extends BaseMetrics {

  private implicit val metricsContext: MetricsContext = MetricsContext.Empty

  override val prefix: MetricName = histograms.prefix

  val dbStorage: DbStorageMetrics =
    new DbStorageMetrics(histograms.dbStorage, openTelemetryMetricsFactory)

  val crypto = new CryptoMetrics(
    new SigningMetrics(histograms.signingHistograms, openTelemetryMetricsFactory),
    new DecryptionMetrics(histograms.decryptionHistograms, openTelemetryMetricsFactory),
    Some(new KmsMetrics(prefix, openTelemetryMetricsFactory)),
  )

  override val declarativeApiMetrics: DeclarativeApiMetrics =
    new DeclarativeApiMetrics(prefix, openTelemetryMetricsFactory)

  override def storageMetrics: DbStorageMetrics = dbStorage

  override def cryptoMetrics: CryptoMetrics = crypto

  // Private constructor to avoid being instantiated multiple times by accident
  final class PerformanceMetrics private[BftOrderingMetrics] {

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    @volatile var enabled: Boolean = true

    // Private constructor to avoid being instantiated multiple times by accident
    final class OrderingStageLatencyMetrics private[BftOrderingMetrics] {
      object labels {

        object stage {
          val Key: String = "ordering-stage"

          object values {

            object mempool {
              // Insertion in batch queue until extraction and batch send to availability
              val RequestQueuedForBatchInclusion = "request-queued-for-batch-inclusion"
            }

            val BatchAvailabilityTotal = "batch-availability-total"

            object availability {
              // Time waited for a batch while there are outstanding consensus proposal requests and no dissemination is in progress
              val BatchWait = "batch-wait"

              // Cumulative latency of a batch dissemination until a PoA is formed and
              //  it can be present multiple times for a given batch if it regresses due to topology changes
              val BatchDissemination = "batch-dissemination-total"

              object hashing {
                // Duration of computing a batch ID (i.e. hashing the whole batch payload) when a
                //  batch is created locally; performed synchronously on the actor thread
                val LocalBatchIdComputation = "availability-local-batch-id-computation"

                // Duration of validating a batch ID (i.e. hashing the whole batch payload) of a
                //  batch fetched from a remote node, e.g. during state transfer; performed
                //  asynchronously off the actor thread
                val FetchedBatchIdValidation = "availability-fetched-batch-id-validation"
              }

              object dissemination {
                // The following latencies can be present multiple times for a given batch
                //  in case of multiple ack collections and regressions due to topology changes

                // Duration of batch validation when a node receives a batch store
                //  request (emitted 0 or more times per batch)
                val BatchValidation = "batch-validation"

                // Insertion in proposal queue until next event, like re-sign, re-dissemination, [re-]proposal
                //  or batch ordered (emitted 1 or more times per batch)
                val BatchQueuedForBlockInclusion = "batch-queued-for-block-inclusion"
              }
            }

            object consensus {
              // Time waited for a block proposal from availability
              val BlockProposalWait = "consensus-block-proposal-wait"
              // Time waited for a new epoch to complete
              val EpochCompletionWait = "consensus-epoch-completion-wait"
              // Time waited for a new epoch to start
              val EpochStartWait = "consensus-epoch-start-wait"
              // Time elapsed between sending a PrePrepare and seeing that the block has been ordered
              val SegmentProposalToCommitLatency = "consensus-segment-proposal-to-commit-latency"
              // Time elapsed between ordered blocks proposed by a segment
              val SegmentBlockCommitLatency = "consensus-segment-block-commit-latency"

              // Time spent by view messages in the postponed queue
              val PostponedViewMessagesQueueLatency =
                "consensus-postponed-view-messages-queue-latency"

              // Time waited for the first relative block number of each segment in an epoch to
              // be ordered, associated to the original segment leader of that block.
              val FirstSegmentBlockOrderingLatency = "consensus-first-segment-block-order-latency"

              object stateTransfer {
                // Time spent by consensus messages in the postponed queue during state transfer
                val PostponedMessagesQueueLatency =
                  "state-transfer-postponed-consensus-messages-queue-latency"
                val TotalEpochTransferLatency = "state-transfer-total-epoch-transfer-latency"
              }
            }

            object output {
              val Fetch = "output-block-fetch-batches"
              val Inspection = "output-block-inspection"
              val Backpressure = "output-backpressure"
              val OutputStageDuration = "output-stage-duration"

              // Time a block spends waiting in the output Peano queue for lower-numbered blocks
              //  ("holes") to be ordered and fetched before it can be extracted. Emitted once per
              //  extracted block whose delivery to post-ordering actually had to wait, split
              //  into one sample per leader that ordered a delaying hole (tagged with the `leader` label).
              val InterleavingWait = "output-block-interleaving-wait"

              // Similar to `interleaving-wait`, but emitted once per extracted block and also considering those whose
              //  delivery to post-ordering _didn't_ have to wait behind a lower-numbered block.
              //  Also, a single observation is emitted for each block that is equal to that block's total wait
              //  summed over all delaying leaders (there's no `leader` label).
              //  The per-leader `interleaving-wait` averages are not additive for comparison with the output stage
              //  duration's average; that is due to the sample population being different.
              //  By contrast, the average of this metric highlights the portion of the output stage duration's
              //  average that is due to blocks' delivery to post-ordering waiting behind lowered-numbered "holes".
              val InterleavingTotalWait = "output-block-interleaving-total-wait"
            }
          }
        }

        // Assigned segment leader to which a latency sample is attributed (e.g., interleaving wait
        //  or first segment block ordering latency).
        object leader {
          val Key: String = "leader"
        }
      }

      private val timer: Timer =
        openTelemetryMetricsFactory.timer(histograms.performance.orderingStageLatency.info)

      private val queueSizeGauges = new ConcurrentHashMap[String, Gauge[Int]]()

      private def queueSize(
          moduleName: String
      )(implicit metricsContext: MetricsContext): Gauge[Int] =
        queueSizeGauges
          .computeIfAbsent(
            moduleName,
            _ =>
              openTelemetryMetricsFactory.gauge(
                MetricInfo(
                  histograms.performance.prefix :+ "moduleQueueSize" :+ moduleName,
                  summary = s"Module queue size for $moduleName",
                  description = s"Size of the module queue for $moduleName.",
                  qualification = MetricQualification.Latency,
                ),
                0,
              ),
          )

      def time[T](
          call: => T
      )(implicit metricsContext: MetricsContext = MetricsContext.Empty): T =
        if (enabled)
          timer.time(call)(metricsContext)
        else
          call

      def timeFuture[T](call: => Future[T])(implicit
          context: MetricsContext = MetricsContext.Empty
      ): Future[T] =
        if (enabled)
          timer.timeFuture(call)(context)
        else
          call

      def timeFuture[E <: Env[E], X](
          moduleContext: ModuleContext[E, ?],
          futureUnlessShutdown: E#FutureUnlessShutdownT[X],
          orderingStage: String,
      )(implicit metricsContext: MetricsContext): E#FutureUnlessShutdownT[X] =
        if (enabled)
          moduleContext.timeFuture(timer, futureUnlessShutdown)(
            metricsContext.withExtraLabels(
              labels.stage.Key -> orderingStage
            )
          )
        else
          futureUnlessShutdown

      def emitModuleQueueLatency(
          moduleName: String,
          sendInstant: Instant,
          maybeDelay: Option[FiniteDuration],
      )(implicit metricsContext: MetricsContext): Unit =
        if (enabled) {
          val latency =
            Duration
              .between(sendInstant, Instant.now)
              .minus(maybeDelay.fold(Duration.ZERO)(_.toJava))
          updateTimer(timer, latency)(
            metricsContext.withExtraLabels(labels.stage.Key -> s"module-queue-$moduleName")
          )
        }

      def emitModuleQueueSize(
          moduleName: String,
          size: Int,
      )(implicit metricsContext: MetricsContext): Unit =
        if (enabled) {
          queueSize(moduleName).updateValue(size)
        }

      def emitOrderingStageLatency[R](
          stage: String,
          op: () => R,
      )(implicit metricsContext: MetricsContext): R =
        if (enabled) {
          val startInstant = Instant.now()
          val result = op()
          val duration = Duration.between(startInstant, Instant.now())
          emitOrderingStageLatency(stage, duration)
          result
        } else {
          op()
        }

      def emitOrderingStageLatency(
          stage: String,
          startInstant: Option[Instant],
          endInstant: Instant = Instant.now(),
          cleanup: () => Unit = () => (),
      )(implicit metricsContext: MetricsContext): Unit = {
        startInstant.foreach(start =>
          emitOrderingStageLatency(
            stage,
            Duration.between(start, endInstant),
          )(metricsContext)
        )
        cleanup()
      }

      def emitOrderingStageLatency(
          stage: String,
          duration: Duration,
      )(implicit metricsContext: MetricsContext): Unit =
        if (enabled)
          updateTimer(performance.orderingStageLatency.timer, duration)(
            metricsContext.withExtraLabels(
              performance.orderingStageLatency.labels.stage.Key ->
                stage
            )
          )
    }
    val orderingStageLatency = new OrderingStageLatencyMetrics
  }
  val performance = new PerformanceMetrics
  // Private constructor to avoid being instantiated multiple times by accident
  final class GlobalMetrics private[BftOrderingMetrics] {

    object labels {
      val IsBlockEmpty: String = "is-block-empty" // true or false
    }

    private val prefix = histograms.global.prefix

    val blocksOrdered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "ordered-blocks",
        summary = "Blocks ordered on synchronizer",
        description = "Reports the number of blocks ordered on the synchronizer.",
        qualification = MetricQualification.Traffic,
      )
    )

    val batchesOrdered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "ordered-batches",
        summary = "Batches ordered since startup",
        description = "Measures the total batches ordered since this node last started up.",
        qualification = MetricQualification.Traffic,
      )
    )

    val requestsOrdered: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "ordered-requests",
        summary = "Requests ordered since startup",
        description = "Measures the total requests ordered since this node last started up.",
        qualification = MetricQualification.Traffic,
      )
    )

    // Private constructor to avoid being instantiated multiple times by accident
    final class RequestsOrderingLatencyMetrics private[BftOrderingMetrics] {
      object labels {
        val ReceivingSequencer: String = "receiving-sequencer"
      }

      val timer: Timer =
        openTelemetryMetricsFactory.timer(histograms.global.requestsOrderingLatency.info)
    }
    val requestsOrderingLatency = new RequestsOrderingLatencyMetrics
  }
  val global = new GlobalMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class IngressMetrics private[BftOrderingMetrics] {
    private val prefix = histograms.ingress.prefix

    object labels {
      val Tag: String = "tag"
      val Sender: String = "sender"

      object outcome {
        val Key: String = "outcome"

        object values {
          sealed trait OutcomeValue extends PrettyNameOnlyCase
          case object Success extends OutcomeValue
          case object QueueFull extends OutcomeValue
          case object RequestTooBig extends OutcomeValue
          case object InvalidTag extends OutcomeValue
          case object P2PNotReady extends OutcomeValue
          case object BlackListed extends OutcomeValue
        }
      }
    }

    val requestsReceived: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "received-requests",
        summary = "Requests received",
        description = "Measures the total requests received.",
        qualification = MetricQualification.Traffic,
      )
    )

    val requestsQueued: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "requests-queued",
        summary = "Requests queued",
        description = "Measures the size of the mempool in requests.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val bytesQueued: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "bytes-queued",
        summary = "Bytes queued",
        description = "Measures the size of the mempool in bytes.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val bytesReceived: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "received-bytes",
        summary = "Bytes received",
        description = "Measures the total bytes received.",
        qualification = MetricQualification.Traffic,
      )
    )

    val requestsSize: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.ingress.requestsSize.info)
  }
  val ingress = new IngressMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class MempoolMetrics private[BftOrderingMetrics] {
    private val prefix = BftOrderingMetrics.this.prefix :+ "mempool"

    val requestedBatches: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "requested-batches",
        summary = "Requested batches",
        description = "Number of batches requested from the mempool by the availability module.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )
  }
  val mempool = new MempoolMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class AvailabilityMetrics private[BftOrderingMetrics] {
    private val prefix = BftOrderingMetrics.this.prefix :+ "availability"

    object requested {
      val proposals: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "requested-proposals",
          summary = "Requested proposals",
          description = "Number of proposals requested from availability by the consensus module.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

      val batches: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "requested-batches",
          summary = "Requested batches",
          description =
            "Maximum number of batches requested from availability by the consensus module.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )
    }

    object dissemination {

      object labels {
        val ReadyForConsensus = "ready-for-consensus" // true or false
      }

      val bytes: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "disseminating-bytes",
          summary = "Bytes being disseminated",
          description = "Number of bytes being disseminated.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

      val requests: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "disseminating-requests",
          summary = "Requests being disseminated",
          description = "Number of requests being disseminated.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

      val batches: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "disseminating-batches",
          summary = "Batches being disseminated",
          description = "Number of batches being disseminated.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

      val batchCache: CacheMetrics =
        new CacheMetrics("batch-cache", openTelemetryMetricsFactory)
    }

    object outputFetch {
      object labels {
        val Leader = "Leader"
        val From = "From"
      }

      val missingBatchesNeedOutputFetch: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "missing-batches-need-output-fetch",
          summary = "Missing batches that need output fetch",
          description =
            "Measures amount of batches from other nodes that we did not have locally so we need to fetch from network",
          qualification = MetricQualification.Traffic,
        )
      )

      val timeouts: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "output-fetch-timeouts",
          summary = "Output fetch timeouts",
          description = "Measures amount of timeouts during output fetch",
          qualification = MetricQualification.Errors,
        )
      )

      val latency: Timer = openTelemetryMetricsFactory.timer(
        histograms.availability.outputFetchLatency.info
      )
    }

    object regression {
      object labels {
        object stage {
          val Key = "stage"

          object values {
            sealed trait RegressionStageValue extends PrettyNameOnlyCase
            case object Signing extends RegressionStageValue
            case object Dissemination extends RegressionStageValue
          }
        }
      }

      val batch: Counter = openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "batch-regressions",
          summary = "Batch regressions",
          description = "Count of regressions of a single batch due to topology changes",
          qualification = MetricQualification.Saturation,
        )
      )
    }
  }
  val availability = new AvailabilityMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class SecurityMetrics private[BftOrderingMetrics] {
    private val prefix = BftOrderingMetrics.this.prefix :+ "security"

    object noncompliant {
      private val prefix = security.prefix :+ "noncompliant"

      object labels {
        val Endpoint: String = "endpoint"
        val Sequencer: String = "sequencer"

        object violationType {
          val Key: String = "violation-type"

          object values {
            sealed trait ViolationTypeValue extends PrettyNameOnlyCase

            case object AuthIdentityEquivocation extends ViolationTypeValue
            case object DisseminationInvalidMessage extends ViolationTypeValue
            case object ConsensusInvalidMessage extends ViolationTypeValue
            case object ConsensusDataEquivocation extends ViolationTypeValue
            case object ConsensusRoleEquivocation extends ViolationTypeValue
            case object StateTransferInvalidMessage extends ViolationTypeValue
            case object RetransmissionResponseInvalidMessage extends ViolationTypeValue
            case object WrongGrpcMessageSentByBftNodeId extends ViolationTypeValue
          }
        }
      }

      val behavior: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "behavior",
          summary = "Non-compliant behaviors",
          description = "Number of non-compliant (potentially malicious) behaviors detected.",
          qualification = MetricQualification.Errors,
        )
      )
    }
  }
  val security = new SecurityMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class ConsensusMetrics private[BftOrderingMetrics] {
    private val prefix = histograms.consensus.prefix

    object labels {
      val Leader = "Leader"
    }

    val epoch: Gauge[Long] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "epoch",
        summary = "Epoch number",
        description = "Current epoch number for the node.",
        qualification = MetricQualification.Traffic,
      ),
      0,
    )

    val epochLength: Gauge[Long] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "epoch-length",
        summary = "Epoch length",
        description = "Length of the current epoch in number of blocks.",
        qualification = MetricQualification.Traffic,
      ),
      0,
    )

    val epochViewChanges: Gauge[Long] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "epoch-view-changes",
        summary = "Number of view changes occurred",
        description = "Number of view changes occurred.",
        qualification = MetricQualification.Latency,
      ),
      0,
    )

    val postponedViewMessagesQueueSize: Gauge[Int] =
      openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "postponed-view-messages-queue-size",
          summary = "Size of the queue containing postponed view messages",
          description = "Size of the queue containing postponed view messages.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

    val postponedViewMessagesQueueMaxSize: Gauge[Int] =
      openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "postponed-view-messages-queue-max-size",
          summary = "Actual maximum size of the queue containing postponed view messages",
          description = "Actual maximum size of the queue containing postponed view messages.",
          qualification = MetricQualification.Saturation,
        ),
        0,
      )

    val postponedViewMessagesQueueDuplicatesMeter: Meter =
      openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "postponed-view-messages-duplicates",
          summary =
            "Count of messages dropped as duplicates by queue containing postponed view messages",
          description =
            "Count of messages dropped as duplicates by queue containing postponed view messages.",
          qualification = MetricQualification.Saturation,
        )
      )

    val postponedViewMessagesQueueDropMeter: Meter =
      openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "postponed-view-messages-dropped",
          summary = "Count of messages dropped by queue containing postponed view messages",
          description = "Count of messages dropped by queue containing postponed view messages.",
          qualification = MetricQualification.Saturation,
        )
      )

    val flushedBlocks: Meter = openTelemetryMetricsFactory.meter(
      MetricInfo(
        prefix :+ "flushed-blocks",
        summary = "Flushed blocks",
        description = "Total blocks flushed.",
        qualification = MetricQualification.Debug,
      )
    )

    val commitLatency: Timer =
      openTelemetryMetricsFactory.timer(histograms.consensus.consensusCommitLatency.info)

    val viewChangeProgressLatency: Timer =
      openTelemetryMetricsFactory.timer(histograms.consensus.viewChangeProgressLatency.info)

    val relativeSegmentLatency: Timer =
      openTelemetryMetricsFactory.timer(histograms.consensus.relativeSegmentLatency.info)

    // Private constructor to avoid being instantiated multiple times by accident
    final class RetransmissionsMetrics private[BftOrderingMetrics] {

      val incomingRetransmissionsRequestsMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "incoming-retransmission-requests",
          summary = "Incoming retransmissions requests",
          description = "Retransmissions requests received during an epoch",
          qualification = MetricQualification.Traffic,
        )
      )

      val outgoingRetransmissionsRequestsMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "outgoing-retransmission-requests",
          summary = "Outgoing retransmissions requests",
          description = "Retransmissions requests sent during an epoch",
          qualification = MetricQualification.Traffic,
        )
      )

      val retransmittedMessagesMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "retransmitted-messages",
          summary = "Retransmitted PBFT messages",
          description = "Number of PBFT messages retransmitted during an epoch",
          qualification = MetricQualification.Traffic,
        )
      )

      val retransmittedCommitCertificatesMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "retransmitted-commit-certificates",
          summary = "Retransmitted commit certificates",
          description = "Number of commit certificates retransmitted during an epoch",
          qualification = MetricQualification.Traffic,
        )
      )

      val discardedWrongEpochRetransmissionResponseMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "discarded-wrong-epoch-retransmission-responses",
          summary = "Discarded retransmission response messages",
          description =
            "Discarded retransmission response messages for epoch different than current one",
          qualification = MetricQualification.Traffic,
        )
      )

      val discardedRateLimitedRetransmissionRequestMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "discarded-rate-limited-retransmission-requests",
          summary = "Discarded rate limited retransmission requests",
          description = "Discarded retransmission requests messages due to rate limiting",
          qualification = MetricQualification.Traffic,
        )
      )
    }

    // Private constructor to avoid being instantiated multiple times by accident
    final class VotesMetrics private[BftOrderingMetrics] {

      object labels {
        val VotingSequencer: String = "voting-sequencer"
      }

      private val prepareGauges = mutable.Map[BftNodeId, Gauge[Double]]()
      private val commitGauges = mutable.Map[BftNodeId, Gauge[Double]]()
      private val lock = new Mutex()

      val discardedRepeatedMessageMeter: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "discarded-messages",
          summary = "Discarded messages",
          description =
            "Discarded network messages received during an epoch, either due to being repeated (too many retransmissions), invalid or from a stale view",
          qualification = MetricQualification.Traffic,
        )
      )

      def prepareVotesPercent(node: BftNodeId): Gauge[Double] =
        getOrElseUpdateGauge(
          prepareGauges,
          node,
          "prepare-votes-percent",
          "Block vote % during prepare",
          "Percentage of BFT sequencers that voted for a block in the PBFT prepare stage.",
        )

      def commitVotesPercent(node: BftNodeId): Gauge[Double] =
        getOrElseUpdateGauge(
          commitGauges,
          node,
          "commit-votes-percent",
          "Block vote % during commit",
          "Percentage of BFT sequencers that voted for a block in the PBFT commit stage.",
        )

      def cleanupVoteGauges(keepOnly: Set[BftNodeId]): Unit =
        lock.exclusive {
          cleanupGauges(prepareGauges, keepOnly)
          cleanupGauges(commitGauges, keepOnly)
        }

      private def getOrElseUpdateGauge(
          gauges: mutable.Map[BftNodeId, Gauge[Double]],
          node: BftNodeId,
          name: String,
          summary: String,
          description: String,
      ): Gauge[Double] =
        getOrCreateGauge(
          gauges,
          lock,
          node,
          metricsContext.withExtraLabels(labels.VotingSequencer -> node),
          MetricInfo(prefix :+ name, summary, MetricQualification.Traffic, description),
          0.0d,
        )
    }

    // Private constructor to avoid being instantiated multiple times by accident
    final class StateTransferMetrics private[BftOrderingMetrics] {
      private val prefix = ConsensusMetrics.this.prefix :+ "state-transfer"

      val postponedMessagesQueueSize: Gauge[Int] =
        openTelemetryMetricsFactory.gauge(
          MetricInfo(
            prefix :+ "postponed-consensus-messages-queue-size",
            summary =
              "Size of the queue containing consensus messages postponed during state transfer",
            description =
              "Size of the queue containing consensus messages postponed during state transfer.",
            qualification = MetricQualification.Saturation,
          ),
          0,
        )

      val postponedMessagesQueueMaxSize: Gauge[Int] =
        openTelemetryMetricsFactory.gauge(
          MetricInfo(
            prefix :+ "postponed-consensus-messages-queue-max-size",
            summary =
              "Actual maximum size of the queue containing consensus messages postponed during state transfer",
            description =
              "Actual maximum size of the queue containing consensus messages postponed during state transfer.",
            qualification = MetricQualification.Saturation,
          ),
          0,
        )

      val postponedMessagesQueueDropMeter: Meter =
        openTelemetryMetricsFactory.meter(
          MetricInfo(
            prefix :+ "postponed-consensus-messages-dropped",
            summary =
              "Count of messages dropped by queue containing consensus messages postponed during state transfer",
            description =
              "Count of messages dropped by queue containing consensus messages postponed during state transfer.",
            qualification = MetricQualification.Saturation,
          )
        )
    }

    val votes = new VotesMetrics
    val retransmissions = new RetransmissionsMetrics
    val stateTransfer = new StateTransferMetrics
  }
  val consensus = new ConsensusMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class OutputMetrics private[BftOrderingMetrics] {

    object labels {
      object mode {
        val Key: String = "mode"

        object values {
          sealed trait ModeValue extends PrettyNameOnlyCase with Product with Serializable
          case object Consensus extends ModeValue
          case object StateTransfer extends ModeValue
        }
      }
    }

    val blockSizeBytes: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeBytes.info)

    val blockSizeRequests: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeRequests.info)

    val blockSizeBatches: Histogram =
      openTelemetryMetricsFactory.histogram(histograms.output.blockSizeBatches.info)

    val blockDelay: Timer =
      openTelemetryMetricsFactory.timer(histograms.output.blockDelay.info)

    val sequencerCoreSubscriptionBufferSize: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "sequencer-core-subscription-buffer-size",
        summary = "Sequencer core subscription buffer size",
        description = "Size of the buffer for the subscription to the sequencer core output, " +
          "which is used to apply backpressure to the sequencer core when the output is not consumed fast enough.",
        qualification = MetricQualification.Saturation,
      ),
      0,
    )

    val currentSequencerCoreBackpressureDelayMillis: Gauge[Long] =
      openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "sequencer-core-backpressure-current-delay-millis",
          summary = "Current sequencer core backpressure delay (ms)",
          description = "Current sequencer core backpressure delay in milliseconds.",
          qualification = MetricQualification.Latency,
        ),
        0L,
      )
  }
  val output = new OutputMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class TopologyMetrics private[BftOrderingMetrics] {
    private val prefix = histograms.topology.prefix

    val validators: Gauge[Int] = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "validators",
        summary = "Active validators",
        description = "Number of BFT sequencers actively involved in consensus.",
        qualification = MetricQualification.Traffic,
      ),
      0,
    )

    val queryLatency: Timer =
      openTelemetryMetricsFactory.timer(histograms.topology.queryLatency.info)

    val blacklistedEpochsCounter: Counter =
      openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "blacklisted-epochs",
          "Number of epochs a node has been blacklisted for",
          MetricQualification.Traffic,
          "Number of epochs a node has been blacklisted for after failing to timely lead a segment",
        )
      )

    object labels {
      val sequencerId: String = "sequencer-id"
    }
    // We assign different values to different nodes just to make it easier to distinguish them in Grafana
    private val topologyGauges = mutable.Map[BftNodeId, Gauge[Int]]()

    private val leadersGauges = mutable.Map[BftNodeId, Gauge[Int]]()
    private val maxToleratedFaultsGauge =
      openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "max-tolerated-faults",
          "Maximum number of tolerated faults",
          MetricQualification.Traffic,
          "Maximum number of tolerated faults",
        ),
        0,
      )
    private val weakQuorumGauge =
      openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "weak-quorum",
          "Number of non-faulty nodes required for a weak quorum",
          MetricQualification.Traffic,
          "Number of non-faulty nodes required for a weak quorum, like for batch dissemination",
        ),
        0,
      )
    private val strongQuorumGauge = openTelemetryMetricsFactory.gauge(
      MetricInfo(
        prefix :+ "strong-quorum",
        "Number of non-faulty nodes required for a strong quorum",
        MetricQualification.Traffic,
        "Number of non-faulty nodes required for a strong quorum, like for consensus",
      ),
      0,
    )

    private val lock = new Mutex()
    def update(newMembership: Membership)(implicit metricsContext: MetricsContext): Unit = {
      val orderingTopology = newMembership.orderingTopology
      val members = orderingTopology.nodes
      val sortedMembersWithIndex = members.toSeq.sorted.zipWithIndex
      val sortedLeadersWithIndex = newMembership.leaders.toSeq.sorted.zipWithIndex

      maxToleratedFaultsGauge.updateValue(orderingTopology.maxToleratedFaults)
      weakQuorumGauge.updateValue(orderingTopology.weakQuorum)
      strongQuorumGauge.updateValue(orderingTopology.strongQuorum)

      newMembership.blacklistedNodes.foreach { nodeId =>
        blacklistedEpochsCounter.inc()(metricsContext.withExtraLabels(labels.sequencerId -> nodeId))
      }

      {
        lock.exclusive {
          cleanupGauges(topologyGauges, members)
          cleanupGauges(leadersGauges, members)
          updateMetrics(
            sortedMembersWithIndex,
            topologyGauges,
            labels.sequencerId,
            prefix,
            "topology-member",
            "Topology members",
            "Topology members sorted by node ID with their index",
          )
          updateMetrics(
            sortedLeadersWithIndex,
            leadersGauges,
            labels.sequencerId,
            prefix,
            "topology-leader",
            "Topology leaders",
            "Topology leaders sorted by node ID with their index",
          )
        }
      }
    }
  }
  val topology = new TopologyMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class BlacklistLeaderSelectionPolicyMetrics private[BftOrderingMetrics] {
    object labels {
      val blacklistNode: String = "blacklist-sequencer"
    }

    private val blacklistGauges = mutable.Map[BftNodeId, Gauge[Long]]()
    private val lock = new Mutex()
    def blacklist(node: BftNodeId): Gauge[Long] =
      getOrCreateGauge(
        blacklistGauges,
        lock,
        node,
        metricsContext.withExtraLabels(labels.blacklistNode -> node),
        MetricInfo(
          prefix :+ "blacklist-sequencer",
          "Amount of epochs the node is blacklisted for",
          MetricQualification.Traffic,
          "The amount of epochs an BFT sequencer is blacklisted from being a leader",
        ),
        0L,
      )

    def cleanupBlacklistGauges(keepOnly: Set[BftNodeId]): Unit =
      lock.exclusive {
        cleanupGauges(blacklistGauges, keepOnly)
      }
  }
  val blacklistLeaderSelectionPolicyMetrics = new BlacklistLeaderSelectionPolicyMetrics

  // Private constructor to avoid being instantiated multiple times by accident
  final class P2PMetrics private[BftOrderingMetrics] {
    private val p2pPrefix = histograms.p2p.p2pPrefix

    object labels {
      val endpoint: String = "endpoint"
    }

    // We assign different values to different endpoints just to make it easier to distinguish them in Grafana
    private val authenticatedGauges = mutable.Map[String, Gauge[Int]]()
    private val unauthenticatedGauges = mutable.Map[String, Gauge[Int]]()
    private val disconnectedGauges = mutable.Map[String, Gauge[Int]]()
    private val lock = new Mutex()

    def update(status: PeerNetworkStatus)(implicit metricsContext: MetricsContext): Unit = {
      val statusView = status.endpointStatuses.view
      val authenticated =
        statusView
          .flatMap {
            case PeerConnectionStatus.PeerIncomingConnection(sequencerId) =>
              Some(sequencerId.toProtoPrimitive)
            case PeerConnectionStatus.PeerEndpointStatus(
                  p2pEndpointId,
                  isOutgoingConnection,
                  PeerEndpointHealth(
                    PeerEndpointHealthStatus.Authenticated(sequencerId),
                    _description,
                  ),
                ) =>
              Some(
                s"${p2pEndpointId.url} (${sequencerId.toProtoPrimitive}, outgoing = $isOutgoingConnection)"
              )
            case _ => None
          }
      val unauthenticated =
        statusView
          .flatMap {
            case PeerConnectionStatus.PeerEndpointStatus(
                  p2pEndpointId,
                  _isOutgoingConnection,
                  PeerEndpointHealth(
                    PeerEndpointHealthStatus.Unauthenticated,
                    _description,
                  ),
                ) =>
              Some(p2pEndpointId.url)
            case _ => None
          }
      val disconnected =
        statusView
          .flatMap {
            case PeerConnectionStatus.PeerEndpointStatus(
                  p2pEndpointId,
                  _isOutgoingConnection,
                  PeerEndpointHealth(
                    PeerEndpointHealthStatus.Disconnected,
                    _description,
                  ),
                ) =>
              Some(p2pEndpointId.url)
            case _ => None
          }
      val authenticatedSortedWithIndex = authenticated.toSeq.sorted.zipWithIndex
      val connectedSortedWithIndex = unauthenticated.toSeq.sorted.zipWithIndex
      val disconnectedSortedWithIndex = disconnected.toSeq.sorted.zipWithIndex

      {
        lock.exclusive {
          cleanupGauges(authenticatedGauges, authenticated.toSet)
          cleanupGauges(unauthenticatedGauges, unauthenticated.toSet)
          cleanupGauges(disconnectedGauges, disconnected.toSet)

          updateMetrics(
            authenticatedSortedWithIndex,
            authenticatedGauges,
            labels.endpoint,
            p2pPrefix,
            "authenticated-endpoint",
            "Authenticated P2P endpoints",
            "P2P endpoints that are authenticated.",
          )
          updateMetrics(
            connectedSortedWithIndex,
            unauthenticatedGauges,
            labels.endpoint,
            p2pPrefix,
            "unauthenticated-endpoint",
            "Connected but unauthenticated P2P endpoints",
            "P2P endpoints that are connected but not yet authenticated.",
          )
          updateMetrics(
            disconnectedSortedWithIndex,
            disconnectedGauges,
            labels.endpoint,
            p2pPrefix,
            "disconnected-endpoint",
            "Disconnected P2P endpoints",
            "P2P endpoints that are disconnected.",
          )
        }
      }
    }

    // Private constructor to avoid being instantiated multiple times by accident
    final class ConnectionsMetrics private[P2PMetrics] {
      private val prefix = p2pPrefix :+ "connections"

      val connected: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "connected",
          summary = "Connected peers",
          description = "Number of connected P2P endpoints.",
          qualification = MetricQualification.Traffic,
        ),
        0,
      )

      val authenticated: Gauge[Int] = openTelemetryMetricsFactory.gauge(
        MetricInfo(
          prefix :+ "authenticated",
          summary = "Authenticated peers",
          description = "Number of connected P2P endpoints that are also authenticated.",
          qualification = MetricQualification.Traffic,
        ),
        0,
      )
    }
    val connections = new ConnectionsMetrics

    // Private constructor to avoid being instantiated multiple times by accident
    final class SendMetrics private[P2PMetrics] {
      private val prefix = histograms.p2p.send.prefix

      object labels {
        val SourceSequencer: String = "source-sequencer"
        val TargetSequencer: String = "target-sequencer"
        val DroppedAsUnauthenticated: String = "dropped-as-unauthenticated"

        object targetModule {
          val Key: String = "target-module"

          object values {
            sealed trait TargetModuleValue extends PrettyNameOnlyCase
            case object Availability extends TargetModuleValue
            case object Consensus extends TargetModuleValue
          }
        }
      }

      object failure {
        object labels {
          object reason {
            val Key: String = "send-failure-reason"

            object values {
              sealed trait SendFailureReasonValue extends PrettyNameOnlyCase
              case object ConnectionInitError extends SendFailureReasonValue
              case object FlowControl extends SendFailureReasonValue
              case object SendError extends SendFailureReasonValue
              case object Unauthenticated extends SendFailureReasonValue
              case object NoAuthenticatedRecipientCandidates extends SendFailureReasonValue
            }
          }
        }
      }

      val sentBytes: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "sent-bytes",
          summary = "Bytes sent",
          description = "Total P2P bytes sent.",
          qualification = MetricQualification.Traffic,
        )
      )

      val sentMessages: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "sent-messages",
          summary = "Messages sent",
          description = "Total P2P messages sent.",
          qualification = MetricQualification.Traffic,
        )
      )

      val sendsRetried: Counter = openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "sends-retried",
          summary = "P2P sends retried",
          description = "Total P2P network sends retried after a delay.",
          qualification = MetricQualification.Latency,
        )
      )

      val sendsDropped: Counter = openTelemetryMetricsFactory.counter(
        MetricInfo(
          prefix :+ "sends-dropped",
          summary = "P2P sends dropped",
          description = "Total P2P network sends dropped, labeled by reason.",
          qualification = MetricQualification.Latency,
        )
      )

      val grpcOnNextLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.send.grpcOnNextLatency.info)

      val grpcLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.send.grpcLatency.info)

      val grpcFlowControlNotReadyLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.send.grpcFlowControlNotReadyLatency.info)

      val messageSize: Histogram =
        openTelemetryMetricsFactory.histogram(histograms.p2p.send.messageSize.info)
    }
    val send = new SendMetrics

    // Private constructor to avoid being instantiated multiple times by accident
    final class ReceiveMetrics private[P2PMetrics] {
      private val prefix = histograms.p2p.receive.prefix

      object labels {
        val SourceSequencer: String = "source-sequencer"

        object source {
          val Key: String = "source"

          object values {
            sealed trait SourceValue extends PrettyNameOnlyCase
            case object SourceParsingFailed extends SourceValue
            case class Empty(from: BftNodeId) extends SourceValue
            case class ConnectionOpener(from: BftNodeId) extends SourceValue
            case class Availability(from: BftNodeId) extends SourceValue
            case class Consensus(from: BftNodeId) extends SourceValue
            case class Retransmissions(from: BftNodeId) extends SourceValue
            case class StateTransfer(from: BftNodeId) extends SourceValue
          }
        }
      }

      val receivedBytes: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "received-bytes",
          summary = "Bytes received",
          description = "Total P2P bytes received.",
          qualification = MetricQualification.Traffic,
        )
      )

      val receivedMessages: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "received-messages",
          summary = "Messages received",
          description = "Total P2P messages received.",
          qualification = MetricQualification.Traffic,
        )
      )

      val processingLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.receive.processingLatency.info)

      val messageSize: Histogram =
        openTelemetryMetricsFactory.histogram(histograms.p2p.receive.messageSize.info)
    }
    val receive = new ReceiveMetrics

    /** gRPC/networking-level P2P stream metrics. Unlike the [[TransportMetrics]], these are a plain
      * gRPC networking signal rather than a transport-level one, so they are always emitted,
      * independently of whether the optional (conditionally enabled) transport metrics are active.
      */
    // Private constructor to avoid being instantiated multiple times by accident
    final class StreamMetrics private[P2PMetrics] {
      private val prefix = p2pPrefix

      object labels {
        val Peer: String = "peer"

        /** Connection direction from this node's point of view: `outgoing` for connections this
          * node dialed and `incoming` for connections a peer dialed to this node.
          */
        val Direction: String = "direction"

        object direction {
          val Outgoing: String = "outgoing"
          val Incoming: String = "incoming"
        }

        /** The gRPC status code observed when a P2P stream is torn down as an error. */
        val GrpcStatus: String = "grpc-status"
      }

      val teardowns: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "stream-teardowns",
          summary = "P2P stream teardowns",
          description =
            "Count of P2P gRPC stream teardowns observed as errors, labeled by peer, connection direction and gRPC status code. Useful to diagnose remote- or infrastructure-initiated resets (e.g. a proxy GOAWAY/RST_STREAM surfaced as UNAVAILABLE). This is a gRPC/networking-level signal, always emitted regardless of whether transport metrics are enabled.",
          qualification = MetricQualification.Errors,
        )
      )

      /** Application-level round-trip time to a peer, labeled by peer. Measured by the dialer
        * (outgoing) side, which periodically sends a low-frequency `ConnectionOpened` echo probe
        * that the peer's acceptor (incoming) side reflects back. Only the dialer measures, so there
        * is no meaningful connection-direction label. Unlike the TCP-level RTT (see
        * [[TransportMetrics.tcpRttMicros]]), this is connection-direction- and
        * transport-independent, so it is always available.
        */
      val roundTripLatency: Timer =
        openTelemetryMetricsFactory.timer(histograms.p2p.stream.roundTripLatency.info)
    }
    val stream = new StreamMetrics

    /** HTTP/2- and TCP-level transport metrics for P2P gRPC connections, all labeled by peer node
      * ID and connection direction. They are populated by a periodic sampler (see the P2P transport
      * metrics publisher) that reads gRPC Channelz and, when the Netty epoll transport is in use,
      * per-socket `TcpInfo`.
      *
      * These metrics are meant to troubleshoot latency of gRPC bidirectional streaming with
      * (possibly third-party) peers.
      */
    // Private constructor to avoid being instantiated multiple times by accident
    final class TransportMetrics private[P2PMetrics] {
      private val prefix = p2pPrefix :+ "transport"

      object labels {
        val Peer: String = "peer"

        val Direction: String = "direction"
        object direction {
          val Outgoing: String = "outgoing"
          val Incoming: String = "incoming"
        }
      }

      // --- HTTP/2 stream lifecycle -------------------------------------------------------------

      val http2StreamsOpened: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "http2-streams-opened",
          summary = "HTTP/2 streams opened",
          description = "Rate at which HTTP/2 streams are opened on the P2P connection to a peer.",
          qualification = MetricQualification.Traffic,
        )
      )

      val http2StreamsClosed: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "http2-streams-closed",
          summary = "HTTP/2 streams closed",
          description =
            "Rate at which HTTP/2 streams are closed (succeeded or failed) on the P2P connection to a peer.",
          qualification = MetricQualification.Traffic,
        )
      )

      // --- gRPC messages -----------------------------------------------------------------------

      val grpcMessagesSent: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "grpc-messages-sent",
          summary = "gRPC messages sent",
          description =
            "Count of gRPC messages sent to a peer on the P2P connection (from Channelz message counts). Note that a single gRPC message may be split across multiple HTTP/2 DATA frames, so this is not a raw frame count.",
          qualification = MetricQualification.Traffic,
        )
      )

      val grpcMessagesReceived: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "grpc-messages-received",
          summary = "gRPC messages received",
          description =
            "Count of gRPC messages received from a peer on the P2P connection (from Channelz message counts). Note that a single gRPC message may be split across multiple HTTP/2 DATA frames, so this is not a raw frame count.",
          qualification = MetricQualification.Traffic,
        )
      )

      val http2KeepAlivePings: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "http2-keepalive-pings",
          summary = "HTTP/2 keepalive pings",
          description =
            "Count of HTTP/2 keepalive PING frames sent on the P2P connection to a peer.",
          qualification = MetricQualification.Traffic,
        )
      )

      // --- HTTP/2 flow control -----------------------------------------------------------------

      val http2FlowControlStalls: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "http2-flow-control-stalls",
          summary = "HTTP/2 flow control stalls",
          description =
            "Count of samples where the HTTP/2 flow control window to a peer was observed to be exhausted (zero or negative).",
          qualification = MetricQualification.Saturation,
        )
      )

      // --- TCP wire-level bytes and retransmissions --------------------------------------------

      val tcpWireBytesSent: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "tcp-wire-bytes-sent",
          summary = "TCP wire bytes sent",
          description =
            "Total wire-level bytes sent on the P2P TCP connection to a peer (including TLS and framing overhead). Wire-byte counting only works on the client (dialer/outgoing) side, so this is labeled by peer only (no connection-direction label).",
          qualification = MetricQualification.Traffic,
        )
      )

      val tcpWireBytesReceived: Meter = openTelemetryMetricsFactory.meter(
        MetricInfo(
          prefix :+ "tcp-wire-bytes-received",
          summary = "TCP wire bytes received",
          description =
            "Total wire-level bytes received on the P2P TCP connection from a peer (including TLS and framing overhead). Wire-byte counting only works on the client (dialer/outgoing) side, so this is labeled by peer only (no connection-direction label).",
          qualification = MetricQualification.Traffic,
        )
      )

      // TCP retransmitted segments are published as a per-peer gauge (see `tcpRetransmittedSegments`
      //  below), because the underlying `tcpi_retrans` is an instantaneous count of outstanding
      //  retransmissions, not a monotonic lifetime total.

      // --- Per-peer sampled gauges -------------------------------------------------------------

      // Gauges are keyed by (peer, direction) so that outgoing (client-side) and incoming
      //  (server-side) connections to the same peer are tracked and cleaned up independently.
      private val http2ActiveStreamsGauges = mutable.Map[(BftNodeId, String), Gauge[Int]]()
      private val http2LocalWindowGauges = mutable.Map[(BftNodeId, String), Gauge[Long]]()
      private val http2RemoteWindowGauges = mutable.Map[(BftNodeId, String), Gauge[Long]]()
      private val tcpSendWindowScaleGauges = mutable.Map[(BftNodeId, String), Gauge[Int]]()
      private val tcpReceiveWindowScaleGauges = mutable.Map[(BftNodeId, String), Gauge[Int]]()
      private val tcpRttMicrosGauges = mutable.Map[(BftNodeId, String), Gauge[Long]]()
      private val tcpRetransmittedSegmentsGauges = mutable.Map[(BftNodeId, String), Gauge[Long]]()
      private val lock = new Mutex()

      def http2ActiveStreams(peer: BftNodeId, direction: String): Gauge[Int] =
        intGauge(
          http2ActiveStreamsGauges,
          peer,
          direction,
          "http2-active-streams",
          "Active concurrent HTTP/2 streams",
          "Number of concurrently active HTTP/2 streams on the P2P connection to a peer.",
        )

      def http2LocalWindowBytes(peer: BftNodeId, direction: String): Gauge[Long] =
        longGauge(
          http2LocalWindowGauges,
          peer,
          direction,
          "http2-flow-control-local-window-bytes",
          "HTTP/2 local flow control window (bytes)",
          "Local HTTP/2 flow control window (in bytes) for the P2P connection to a peer.",
        )

      def http2RemoteWindowBytes(peer: BftNodeId, direction: String): Gauge[Long] =
        longGauge(
          http2RemoteWindowGauges,
          peer,
          direction,
          "http2-flow-control-remote-window-bytes",
          "HTTP/2 remote flow control window (bytes)",
          "Remote HTTP/2 flow control window (in bytes) for the P2P connection to a peer.",
        )

      def tcpSendWindowScale(peer: BftNodeId, direction: String): Gauge[Int] =
        intGauge(
          tcpSendWindowScaleGauges,
          peer,
          direction,
          "tcp-send-window-scale",
          "TCP send window scale",
          "TCP send window scaling factor for the P2P connection to a peer (requires the Netty epoll transport).",
        )

      def tcpReceiveWindowScale(peer: BftNodeId, direction: String): Gauge[Int] =
        intGauge(
          tcpReceiveWindowScaleGauges,
          peer,
          direction,
          "tcp-receive-window-scale",
          "TCP receive window scale",
          "TCP receive window scaling factor for the P2P connection to a peer (requires the Netty epoll transport).",
        )

      def tcpRttMicros(peer: BftNodeId, direction: String): Gauge[Long] =
        longGauge(
          tcpRttMicrosGauges,
          peer,
          direction,
          "tcp-rtt-micros",
          "TCP round-trip time (us)",
          "TCP-level smoothed round-trip time (in microseconds) to a peer (requires the Netty epoll transport).",
        )

      def tcpRetransmittedSegments(peer: BftNodeId, direction: String): Gauge[Long] =
        longGauge(
          tcpRetransmittedSegmentsGauges,
          peer,
          direction,
          "tcp-retransmitted-segments",
          "TCP retransmitted segments (current)",
          "Current number of outstanding (unacknowledged) retransmitted TCP segments on the P2P connection to a peer, sampled from `tcpi_retrans` (requires the Netty epoll transport). This is an instantaneous value, not a cumulative count.",
        )

      /** Close and drop all per-peer transport gauges for (peer, direction) pairs no longer
        * present.
        */
      def cleanupTransportGauges(keepOnly: Set[(BftNodeId, String)]): Unit =
        lock.exclusive {
          cleanupGauges(http2ActiveStreamsGauges, keepOnly)
          cleanupGauges(http2LocalWindowGauges, keepOnly)
          cleanupGauges(http2RemoteWindowGauges, keepOnly)
          cleanupGauges(tcpSendWindowScaleGauges, keepOnly)
          cleanupGauges(tcpReceiveWindowScaleGauges, keepOnly)
          cleanupGauges(tcpRttMicrosGauges, keepOnly)
          cleanupGauges(tcpRetransmittedSegmentsGauges, keepOnly)
        }

      private def intGauge(
          gauges: mutable.Map[(BftNodeId, String), Gauge[Int]],
          peer: BftNodeId,
          direction: String,
          name: String,
          summary: String,
          description: String,
      ): Gauge[Int] =
        getOrCreateGauge(
          gauges,
          lock,
          (peer, direction),
          metricsContext.withExtraLabels(labels.Peer -> peer, labels.Direction -> direction),
          MetricInfo(prefix :+ name, summary, MetricQualification.Traffic, description),
          0,
        )

      private def longGauge(
          gauges: mutable.Map[(BftNodeId, String), Gauge[Long]],
          peer: BftNodeId,
          direction: String,
          name: String,
          summary: String,
          description: String,
      ): Gauge[Long] =
        getOrCreateGauge(
          gauges,
          lock,
          (peer, direction),
          metricsContext.withExtraLabels(labels.Peer -> peer, labels.Direction -> direction),
          MetricInfo(prefix :+ name, summary, MetricQualification.Traffic, description),
          0L,
        )
    }
    val transport = new TransportMetrics
  }
  val p2p = new P2PMetrics

  private def cleanupGauges[K, T](
      gauges: mutable.Map[K, Gauge[T]],
      keepOnly: Set[K],
  ): Unit =
    gauges.view.filterKeys(!keepOnly.contains(_)).foreach { case (id, gauge) =>
      gauge.close()
      gauges.remove(id).discard
    }

  /** Returns the gauge registered under `key`, creating and registering it (with the given metrics
    * context, info and initial value) if absent. Registration happens under `lock` to keep the map
    * and the metrics factory consistent for concurrent callers.
    */
  private def getOrCreateGauge[K, T](
      gauges: mutable.Map[K, Gauge[T]],
      lock: Mutex,
      key: K,
      context: MetricsContext,
      info: MetricInfo,
      initial: T,
  ): Gauge[T] =
    lock.exclusive {
      locally {
        implicit val metricsContext: MetricsContext = context
        gauges.getOrElseUpdate(key, openTelemetryMetricsFactory.gauge(info, initial))
      }
    }

  private def updateMetrics[N <: String](
      sortedMembersWithIndex: Seq[(N, Int)],
      gauges: mutable.Map[N, Gauge[Int]],
      label: String,
      prefix: MetricName,
      metricName: String,
      metricSummary: String,
      metricDescription: String,
  )(implicit metricsContext: MetricsContext): Unit =
    sortedMembersWithIndex.foreach { case (txt, index) =>
      val mc1 = metricsContext.withExtraLabels(label -> txt)
      locally {
        implicit val metricsContext: MetricsContext = mc1
        gauges
          .getOrElseUpdate(
            txt,
            openTelemetryMetricsFactory.gauge(
              MetricInfo(
                prefix :+ metricName,
                metricSummary,
                MetricQualification.Traffic,
                metricDescription,
              ),
              index + 1,
            ),
          )
          .updateValue(index + 1)
      }
    }
}

object BftOrderingMetrics {

  val Prefix: MetricName = MetricName("bftordering")

  def updateTimer(
      timer: Timer,
      duration: Duration,
  )(implicit metricsContext: MetricsContext): Unit =
    // Java's `Instant` does not have to provide monotonically increasing times
    //  (see the documentation for details: https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html)
    //  and emitting negative durations is generally disallowed by metrics infrastructure, as it can
    //  result in warnings/errors and/or unexpected behavior.
    if (!duration.isNegative)
      timer.update(duration)
}
