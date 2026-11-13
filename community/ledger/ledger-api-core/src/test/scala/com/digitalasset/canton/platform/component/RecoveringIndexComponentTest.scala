// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.platform.component.IndexComponentTest.{ServiceParams, WithPostgres}
import com.digitalasset.canton.platform.indexer.IndexerParams
import com.digitalasset.canton.protocol.ContractInstance
import com.digitalasset.canton.util.PekkoUtil.{
  Commit,
  FutureQueueConsumer,
  RecoveringFutureQueueImpl,
  RecoveringQueueMetrics,
  ShutdownInProgress,
}
import com.digitalasset.canton.util.{Mutex, PekkoUtil}
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

class RecoveringIndexComponentTest extends AnyFlatSpec with IndexComponentTest with WithPostgres {

  private val commitGate = new CommitGate

  private val uncommittedGauge = new RecordingGauge

  override protected def serviceParams: ServiceParams =
    super.serviceParams.copy(buildFutureQueue = (indexerF, config) => {
      val consumerFactory
          : Commit => ShutdownInProgress => Future[Future[FutureQueueConsumer[Update]]] =
        (commit: Commit) =>
          (shutdownRequested: ShutdownInProgress) =>
            indexerF(
              IndexerParams(
                repairMode = false,
                commit = commitGate.wrap(commit),
                shutdownRequested = shutdownRequested,
              )
            )

      new RecoveringFutureQueueImpl[Update](
        maxBlockedOffer = config.queueMaxBlockedOffer,
        bufferSize = config.queueBufferSize,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = config.queueRecoveryRetryMinWaitMillis.toLong,
          multiplier = 2,
          cap = config.queueRecoveryRetryMaxWaitMillis.toLong,
        ),
        retryAttemptWarnThreshold = config.queueRecoveryRetryAttemptWarnThreshold,
        retryAttemptErrorThreshold = config.queueRecoveryRetryAttemptErrorThreshold,
        uncommittedWarnTreshold = config.queueUncommittedWarnThreshold,
        recoveringQueueMetrics = RecoveringQueueMetrics(
          blockedGauge = RecoveringQueueMetrics.NoOp.blocked,
          bufferedGauge = RecoveringQueueMetrics.NoOp.buffered,
          uncommittedGauge = uncommittedGauge,
        ),
        consumerFactory = consumerFactory,
        consumerName = "indexer",
        healthStateChanged = () => (),
        defaultLogLevelIsInfo = _ => true,
      )
    })

  private val nextRecordTimeFactory = new SingleStepIncreasingRecordTime

  private val uncommittedQueueWarning = "Uncommitted queue is growing too large"

  private def ingest(numberOfUpdates: Int): Unit = {
    logger.debug(s"preparing $numberOfUpdates updates...")
    val updatesWithContracts: Vector[(Update, Vector[ContractInstance])] =
      (1 to numberOfUpdates).toVector.map { _ =>
        creates(recordTime = nextRecordTimeFactory, payloadLength = 64)(1)
      }
    ingestUpdates(updatesWithContracts*).discard
  }

  /** Holds back the indexer's commits, ingests the updates, and checks that the indexer consumed
    * all of them without any being committed. Then releases the commits and waits until all updates
    * are committed.
    */
  private def ingestWithCommitsHeldBack(numberOfUpdates: Int): Unit = {
    uncommittedGauge.getValue shouldBe 0
    commitGate.close()
    try {
      ingest(numberOfUpdates)
      // the ledger end covers all updates, so the indexer consumed all of them, and none was committed
      uncommittedGauge.getValue shouldBe numberOfUpdates
    } finally commitGate.open()
    eventually() {
      uncommittedGauge.getValue shouldBe 0
    }
  }

  it should "keep consumed updates uncommitted until the indexer commits them" in {
    ingestWithCommitsHeldBack(numberOfUpdates = 2000)
  }

  it should "emit the uncommitted-queue warning when commit lags behind ingestion" in {
    restartServices(
      serviceParams.copy(indexerConfig =
        serviceParams.indexerConfig.copy(queueUncommittedWarnThreshold = 1000)
      )
    )

    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      ingestWithCommitsHeldBack(numberOfUpdates = 2000),
      (logs: Seq[LogEntry]) =>
        forAtLeast(1, logs) { log =>
          log.warningMessage should include(uncommittedQueueWarning)
        },
    )
  }
}

/** Forwards the indexer's commits, unless closed. While closed, it remembers the highest commit
  * index, and forwards it on opening.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class CommitGate {
  private val lock = Mutex()
  private var closed: Boolean = false
  private var heldBack: Option[(Commit, Long)] = None

  def wrap(commit: Commit): Commit = index =>
    lock.exclusive {
      if (closed) heldBack = Some(commit -> index)
      else commit(index)
    }

  def close(): Unit = lock.exclusive {
    closed = true
  }

  def open(): Unit = lock.exclusive {
    closed = false
    heldBack.foreach { case (commit, index) => commit(index) }
    heldBack = None
  }
}

final class RecordingGauge extends Gauge[Int] {
  private val current = new AtomicInteger(0)

  override val info: MetricInfo =
    MetricInfo(MetricName.Daml :+ "test_uncommitted", "", MetricQualification.Debug)

  override def updateValue(newValue: Int)(implicit mc: MetricsContext): Unit =
    current.set(newValue)

  override def updateValue(f: Int => Int): Unit =
    current.updateAndGet(f(_)).discard

  override def getValue: Int = current.get()

  override def getValueAndContext: (Int, MetricsContext) = current.get() -> MetricsContext.Empty

  override def close(): Unit = ()
}
