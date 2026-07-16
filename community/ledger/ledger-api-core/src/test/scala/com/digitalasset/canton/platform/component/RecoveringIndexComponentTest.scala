// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.platform.component.IndexComponentTest.{ServiceParams, WithPostgres}
import com.digitalasset.canton.platform.indexer.{IndexerConfig, IndexerParams}
import com.digitalasset.canton.protocol.ContractInstance
import com.digitalasset.canton.util.PekkoUtil.{
  Commit,
  FutureQueueConsumer,
  RecoveringFutureQueueImpl,
  RecoveringQueueMetrics,
  ShutdownInProgress,
}
import com.digitalasset.canton.util.{MonadUtil, PekkoUtil}
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.Span
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}

class RecoveringIndexComponentTest extends AnyFlatSpec with IndexComponentTest with WithPostgres {

  private val commitDelay: FiniteDuration = 25.millis

  private val maxUncommittedGauge = new MaxRecordingGauge

  override protected def serviceParams: ServiceParams =
    super.serviceParams.copy(buildFutureQueue = (indexerF, config) => {
      val consumerFactory
          : Commit => ShutdownInProgress => Future[Future[FutureQueueConsumer[Update]]] =
        (commit: Commit) =>
          (shutdownRequested: ShutdownInProgress) => {
            val effectiveCommit: Commit =
              new Commit {
                override def apply(index: Long): Unit = {
                  Threading.sleep(commitDelay.toMillis)
                  commit(index)
                }
              }
            indexerF(
              IndexerParams(
                repairMode = false,
                commit = effectiveCommit,
                shutdownRequested = shutdownRequested,
              )
            )
          }

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
          uncommittedGauge = maxUncommittedGauge,
        ),
        consumerFactory = consumerFactory,
      )
    })

  private val nextRecordTimeFactory = new SingleStepIncreasingRecordTime

  private val uncommittedQueueWarning = "Uncommitted queue is growing too large"

  private def ingest(numberOfUpdates: Int): Unit = {
    maxUncommittedGauge.reset()
    logger.debug(s"preparing $numberOfUpdates updates...")
    val updatesWithContracts: Vector[(Update, Vector[ContractInstance])] =
      (1 to numberOfUpdates).toVector.map { _ =>
        creates(recordTime = nextRecordTimeFactory, payloadLength = 64)(1)
      }

    val ledgerEndBefore =
      index.currentLedgerEnd().map(_.lastOffset.unwrap).getOrElse(0L)

    logger.debug("storing contracts...")
    val updates: Seq[Update] =
      MonadUtil
        .sequentialTraverse(updatesWithContracts) { case (update, contracts) =>
          storeContracts(update, contracts)
        }
        .futureValue(
          PatienceConfiguration.Timeout(Span.convertDurationToSpan(Duration(600, "seconds")))
        )

    logger.debug("offering updates...")
    MonadUtil
      .sequentialTraverse_(updates)(ingestUpdateAsync)
      .futureValue(
        PatienceConfiguration.Timeout(Span.convertDurationToSpan(Duration(600, "seconds")))
      )

    logger.debug("all updates offered; waiting for the indexer to drain (commit) everything...")
    val expectedOffset = Offset.tryFromLong(numberOfUpdates + ledgerEndBefore)
    eventually(timeUntilSuccess = 600.seconds, maxPollInterval = 200.millis) {
      index.currentLedgerEnd().map(_.lastOffset) shouldBe Some(expectedOffset)
    }
  }

  it should "not throw a warning for uncommited queue size growth when indexer pipeline is at full momentum and the commit is slow" in {
    ingest(numberOfUpdates = 25000)
    val minExpectedCommitedSize: Int = 12000

    val peak = maxUncommittedGauge.max
    // if the test flakes then minExpectedCommitedSize can be reduced to 10000
    peak should be >= minExpectedCommitedSize
    // this is also verified by the fact that no warning was logged
    peak should be < IndexerConfig.DefaultQueueUncommittedWarnThreshold
  }

  it should "emit the uncommitted-queue warning when commit lags behind ingestion" in {
    restartServices(
      serviceParams.copy(indexerConfig =
        serviceParams.indexerConfig.copy(queueUncommittedWarnThreshold = 1000)
      )
    )

    loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
      ingest(numberOfUpdates = 5000),
      (logs: Seq[LogEntry]) =>
        forAtLeast(1, logs) { log =>
          log.warningMessage should include(uncommittedQueueWarning)
        },
    )
  }
}

final class MaxRecordingGauge extends Gauge[Int] {
  private val current = new AtomicInteger(0)
  private val peak = new AtomicInteger(0)

  override val info: MetricInfo =
    MetricInfo(MetricName.Daml :+ "test_max_uncommitted", "", MetricQualification.Debug)

  override def updateValue(newValue: Int)(implicit mc: MetricsContext): Unit = {
    current.set(newValue)
    peak.accumulateAndGet(newValue, math.max)
  }

  override def updateValue(f: Int => Int): Unit = {
    val updated = current.updateAndGet(f(_))
    peak.accumulateAndGet(updated, math.max)
  }

  override def getValue: Int = current.get()

  override def getValueAndContext: (Int, MetricsContext) = current.get() -> MetricsContext.Empty

  override def close(): Unit = ()

  def max: Int = peak.get()

  def reset(): Unit = {
    current.set(0)
    peak.set(0)
  }
}
