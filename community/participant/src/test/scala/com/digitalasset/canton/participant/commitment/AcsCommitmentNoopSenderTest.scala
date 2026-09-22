// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import cats.syntax.option.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.metrics.{CommitmentSenderMetrics, TestCommitmentMetrics}
import com.digitalasset.canton.participant.store.AcsDigestStore.{Checkpoint, CheckpointType}
import com.digitalasset.canton.participant.store.db.{
  DbAcsCommitmentSenderWatermarkStore,
  DbAcsDigestStore,
}
import com.digitalasset.canton.participant.store.memory.{
  InMemoryAcsCommitmentSenderWatermarkStore,
  InMemoryAcsDigestStore,
}
import com.digitalasset.canton.participant.store.{
  AcsCommitmentSenderWatermarkStore,
  AcsDigestStore,
  TestDigestUtils,
}
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbTest, H2Test}
import com.digitalasset.canton.topology.DefaultTestIdentities.physicalSynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  HasActorSystem,
  HasExecutionContext,
  ProtocolVersionChecksAsyncWordSpec,
}
import org.scalatest.wordspec.AsyncWordSpec

trait AcsCommitmentNoopSenderTest
    extends AsyncWordSpec
    with BaseAcsCommitmentSenderTest
    with HasExecutionContext
    with HasActorSystem
    with ProtocolVersionChecksAsyncWordSpec {
  import AcsCommitmentNoopSenderTest.*

  implicit val mc: MetricsContext = MetricsContext.Empty

  "AcsCommitmentNoopSender" should {
    "increase the sender watermark" in {
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) = mkStoresAndSender(metrics)

      val checkpointTimestamp = ts(10)
      val checkpointOffset = off(10)
      val checkpointTimepoint = Timepoint(checkpointOffset)(checkpointTimestamp)

      digestStore
        .insertCheckpointTime(
          Checkpoint(checkpointTimepoint, CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.processCommitmentsUpTo(checkpointOffset).futureValueUS

      assertWatermarkValue(watermarkStore, checkpointTimepoint.some)
      assertWatermarkMetricsValue(metrics, checkpointTimepoint.some)

      // send metrics haven't changed
      assertInitialEmptySendMetricValues(metrics)
    }

    "increase the watermark to the latest reconciliation checkpoint up to the provided offset" in {
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) = mkStoresAndSender(metrics)

      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(5), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(10), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          // Not reconciliation checkpoint, should be ignored
          Checkpoint(tp(13), CheckpointType.MaxEventsWithoutCheckpoint)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(20), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      val targetCheckpoint = tp(10)

      // process checkpoints up to including offset=15
      sender.processCommitmentsUpTo(off(15)).futureValueUS

      assertWatermarkValue(watermarkStore, targetCheckpoint.some)
      assertWatermarkMetricsValue(metrics, targetCheckpoint.some)

      // send metrics haven't changed
      assertInitialEmptySendMetricValues(metrics)
    }

    "not increase the watermark if no new reconciliation checkpoint is found" in {
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) = mkStoresAndSender(metrics)

      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(5), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(10), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          // Not reconciliation checkpoint, should be ignored
          Checkpoint(tp(13), CheckpointType.MaxEventsWithoutCheckpoint)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(20), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      val targetCheckpoint = tp(10)

      // process reconciliation checkpoints up to including offset=15
      sender.processCommitmentsUpTo(off(15)).futureValueUS

      assertWatermarkValue(watermarkStore, targetCheckpoint.some)
      assertWatermarkMetricsValue(metrics, targetCheckpoint.some)
      assertInitialEmptySendMetricValues(metrics)

      // process reconciliation checkpoints up to including offset=18
      sender.processCommitmentsUpTo(off(18)).futureValueUS

      assertWatermarkValue(watermarkStore, targetCheckpoint.some)
      assertWatermarkMetricsValue(metrics, targetCheckpoint.some)
      assertInitialEmptySendMetricValues(metrics)
    }

    "not decrease the watermark" in {
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) = mkStoresAndSender(metrics)

      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(5), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(10), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          // Not reconciliation checkpoint, should be ignored
          Checkpoint(tp(13), CheckpointType.MaxEventsWithoutCheckpoint)
        )
        .futureValueUS
      digestStore
        .insertCheckpointTime(
          Checkpoint(tp(20), CheckpointType.ReconciliationIntervalBoundary)
        )
        .futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      val targetCheckpoint = tp(10)

      // process checkpoints up to including offset=15
      sender.processCommitmentsUpTo(off(15)).futureValueUS

      assertWatermarkValue(watermarkStore, targetCheckpoint.some)
      assertWatermarkMetricsValue(metrics, targetCheckpoint.some)
      assertInitialEmptySendMetricValues(metrics)

      // simulate crash recovery by processing an earlier offset
      sender.processCommitmentsUpTo(off(5)).futureValueUS

      assertWatermarkValue(watermarkStore, targetCheckpoint.some)
      assertWatermarkMetricsValue(metrics, targetCheckpoint.some)
      assertInitialEmptySendMetricValues(metrics)
    }
  }

  private def mkMetrics(): CommitmentSenderMetrics = TestCommitmentMetrics().sender

  private def mkStoresAndSender(
      metrics: CommitmentSenderMetrics
  ): (
      AcsDigestStore,
      AcsCommitmentSenderWatermarkStore,
      AcsCommitmentNoopSender,
  ) = {
    val (digestStore, watermarkStore) = mkStores()

    (
      digestStore,
      watermarkStore,
      new AcsCommitmentNoopSender(
        digestStore = digestStore,
        watermarkStore = watermarkStore,
        loggerFactory = loggerFactory,
        timeouts = timeouts,
        stringInterningEval = Eval.now(mockStringInterning),
        metrics = metrics,
        synchronizerId = psid,
      ),
    )
  }

  protected def mkStores(): (AcsDigestStore, AcsCommitmentSenderWatermarkStore)
}

object AcsCommitmentNoopSenderTest extends TestDigestUtils {
  private lazy val psid = physicalSynchronizerId

  lazy val defaultSync: IndexedSynchronizer = IndexedSynchronizer.tryCreate(psid.logical, 1)
  lazy val mockStringInterning = new MockStringInterning()
}

trait AcsCommitmentNoopSenderTestDb extends AcsCommitmentNoopSenderTest {
  self: DbTest =>

  import AcsCommitmentNoopSenderTest.*

  override protected def mkStores(): (AcsDigestStore, AcsCommitmentSenderWatermarkStore) = (
    new DbAcsDigestStore(
      indexedSynchronizer = defaultSync,
      Eval.now(mockStringInterning),
      storage,
      loggerFactory,
      timeouts,
    ),
    new DbAcsCommitmentSenderWatermarkStore(storage, timeouts, loggerFactory, defaultSync),
  )

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_acs_participant_running_digest",
        sqlu"truncate table par_acs_commitment_sender_watermark",
      ),
      functionFullName,
    )
  }
}

class AcsCommitmentNoopSenderTestH2 extends AcsCommitmentNoopSenderTestDb with H2Test

class AcsCommitmentNoopSenderTestInMemory extends AcsCommitmentNoopSenderTest {

  import AcsCommitmentNoopSenderTest.*

  override protected def mkStores(): (AcsDigestStore, AcsCommitmentSenderWatermarkStore) = (
    (InMemoryAcsDigestStore
      .create(Eval.now(mockStringInterning), loggerFactory)),
    new InMemoryAcsCommitmentSenderWatermarkStore(loggerFactory),
  )
}
