// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.metrics.api.MetricHandle.Counter
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.testing.InMemoryMetricsFactory.InMemoryCounter
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.{SyncCryptoApi, SynchronizerCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{LogEntry, NamedLoggerFactory}
import com.digitalasset.canton.participant.commitment.AcsCommitmentSender.RetryStrategy
import com.digitalasset.canton.participant.config.AcsCommitmentSenderConfig
import com.digitalasset.canton.participant.metrics.{CommitmentSenderMetrics, TestCommitmentMetrics}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigest,
  AcsDigestUpdate,
  HashedDigest,
  InternedParticipantId,
  RawDigest,
}
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
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  AcsCommitmentProtocolMessage,
  AcsCommitmentSummary,
  AcsCommitmentSummaryProtocolMessage,
  CommitmentPeriod,
  DefaultOpenEnvelope,
  DigestForCounterparticipant,
  ProtocolMessage,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.sequencing.client.TestSequencerClientSend.Request
import com.digitalasset.canton.sequencing.client.{SendResult, TestSequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  Deliver,
  DeliverError,
  MessageId,
  SequencerErrors,
}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  participant1,
  participant2,
  participant3,
  participant4,
  physicalSynchronizerId,
}
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.topology.{ParticipantId, TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasActorSystem,
  HasExecutionContext,
  ProtocolVersionChecksAsyncWordSpec,
}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

trait AcsCommitmentSenderTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasActorSystem
    with ProtocolVersionChecksAsyncWordSpec {
  import AcsCommitmentSenderTest.*

  implicit val mc: MetricsContext = MetricsContext.Empty

  private val sendTimestamp = t3
  private val sendTimepoint = Timepoint(offset3)(sendTimestamp)

  private val defaultCryptoApi = mkCryptoApi(allParticipantsTopology, loggerFactory)
  private val defaultSyncCryptoApi = defaultCryptoApi.snapshot(t3).futureValueUS

  "AcsCommitmentSender" should {
    "send the expected messages when all messages fit in one batch" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) = mkStoresAndSender(sequencerClient, metrics)

      digestStore.participant.upsertDigestUpdates(Seq(updateP2, updateP3, updateP4)).futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.sendAcsCommitments(sendTimepoint).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 1

      val request = requests.head
      val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
      val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

      acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(defaultSyncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(
        defaultSyncCryptoApi,
        acsCommitmentSummaryMessage,
      )

      acsCommitments shouldBe List(
        acsCommitmentP2,
        acsCommitmentP3,
        acsCommitmentP4,
      )

      acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant2, participant3, participant4),
        commitmentTick = sendTimestamp,
      )

      assertWatermarkValue(watermarkStore, sendTimepoint.some)
      assertWatermarkMetricsValue(metrics, sendTimepoint.some)

      assertCounterMetricValue(metrics.sentBatchCount, 1)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
      assertCounterMetricValue(metrics.sendFailureCount, 0)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }

    "skip the updates with empty digests and send messages for the rest" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) = mkStoresAndSender(sequencerClient, metrics)

      digestStore.participant
        .upsertDigestUpdates(Seq(updateP2Empty, updateP3, updateP4))
        .futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.sendAcsCommitments(sendTimepoint).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 1

      val request = requests.head
      val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
      val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

      acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(defaultSyncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(
        defaultSyncCryptoApi,
        acsCommitmentSummaryMessage,
      )

      acsCommitments shouldBe List(
        acsCommitmentP3,
        acsCommitmentP4,
      )

      acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant3, participant4),
        commitmentTick = sendTimestamp,
      )

      assertWatermarkValue(watermarkStore, sendTimepoint.some)
      assertWatermarkMetricsValue(metrics, sendTimepoint.some)

      assertCounterMetricValue(metrics.sentBatchCount, 1)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
      assertCounterMetricValue(metrics.sendFailureCount, 0)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }

    "skip the updates for inactive participants and include them in unsent digests" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val cryptoApi = mkCryptoApi(
        mkTopology(
          Seq(participant1, participant4) // Not including participants 2 and 3 on purpose
        ),
        loggerFactory,
      )
      val syncCryptoApi = cryptoApi.snapshot(t3).futureValueUS
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) =
        mkStoresAndSender(sequencerClient, metrics, cryptoApi)

      digestStore.participant.upsertDigestUpdates(Seq(updateP2, updateP3, updateP4)).futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.sendAcsCommitments(sendTimepoint).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 1

      val request = requests.head
      val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
      val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

      acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(syncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(
        syncCryptoApi,
        acsCommitmentSummaryMessage,
      )

      acsCommitments shouldBe List(
        acsCommitmentP4
      )

      acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant4),
        commitmentTick = sendTimestamp,
        unsentDigests = Seq(
          DigestForCounterparticipant(hashedDigest0, participant2.toLf),
          DigestForCounterparticipant(hashedDigest1, participant3.toLf),
        ),
      )

      assertWatermarkValue(watermarkStore, sendTimepoint.some)
      assertWatermarkMetricsValue(metrics, sendTimepoint.some)

      assertCounterMetricValue(metrics.sentBatchCount, 1)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
      assertCounterMetricValue(metrics.sendFailureCount, 0)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }

    "send nothing if all updates have empty digests" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) = mkStoresAndSender(sequencerClient, metrics)

      digestStore.participant
        .upsertDigestUpdates(Seq(updateP2Empty, updateP3Empty, updateP4Empty))
        .futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.sendAcsCommitments(sendTimepoint).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 0

      assertWatermarkValue(watermarkStore, sendTimepoint.some)
      assertWatermarkMetricsValue(metrics, sendTimepoint.some)

      assertCounterMetricValue(metrics.sentBatchCount, 0)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
      assertCounterMetricValue(metrics.sendFailureCount, 0)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }

    "send nothing if snapshot contains no updates" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val metrics = mkMetrics()
      val (_, watermarkStore, sender) = mkStoresAndSender(sequencerClient, metrics)

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.sendAcsCommitments(sendTimepoint).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 0

      assertWatermarkValue(watermarkStore, sendTimepoint.some)
      assertWatermarkMetricsValue(metrics, sendTimepoint.some)

      assertCounterMetricValue(metrics.sentBatchCount, 0)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
      assertCounterMetricValue(metrics.sendFailureCount, 0)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }

    "send the expected messages when messages are split into multiple batches" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) =
        mkStoresAndSender(sequencerClient, metrics, maxBatchSize = PositiveInt.tryCreate(2))

      digestStore.participant.upsertDigestUpdates(Seq(updateP2, updateP3, updateP4)).futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.sendAcsCommitments(sendTimepoint).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 2

      val request1 = requests.head
      val request2 = requests(1)

      val (acsCommitmentMessages1, acsCommitmentSummaryMessage1) = splitMessages(request1.batch)
      val acsCommitments1 = acsCommitmentMessages1.map(_.acsCommitment)

      acsCommitmentMessages1.foreach(assertCommitmentMessageValidSignature(defaultSyncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(
        defaultSyncCryptoApi,
        acsCommitmentSummaryMessage1,
      )

      acsCommitments1 shouldBe List(
        acsCommitmentP2,
        acsCommitmentP3,
      )

      acsCommitmentSummaryMessage1.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant2, participant3),
        commitmentTick = sendTimestamp,
        batchIndex = NonNegativeInt.zero,
        lastBatch = false,
      )

      val (acsCommitmentMessages2, acsCommitmentSummaryMessage2) = splitMessages(request2.batch)
      val acsCommitments2 = acsCommitmentMessages2.map(_.acsCommitment)

      acsCommitmentMessages2.foreach(assertCommitmentMessageValidSignature(defaultSyncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(
        defaultSyncCryptoApi,
        acsCommitmentSummaryMessage2,
      )

      acsCommitments2 shouldBe List(
        acsCommitmentP4
      )

      acsCommitmentSummaryMessage2.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant4),
        commitmentTick = sendTimestamp,
        batchIndex = NonNegativeInt.one,
        lastBatch = true,
      )

      assertWatermarkValue(watermarkStore, sendTimepoint.some)
      assertWatermarkMetricsValue(metrics, sendTimepoint.some)

      assertCounterMetricValue(metrics.sentBatchCount, 2)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
      assertCounterMetricValue(metrics.sendFailureCount, 0)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }

    "not try to send the next batch if sending the first batch fails with a non-retriable error" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient =
        new TestSequencerClientSend(wallClock, nonRetriableErrorSendResultFactory.some)
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) =
        mkStoresAndSender(sequencerClient, metrics, maxBatchSize = PositiveInt.tryCreate(2))

      digestStore.participant.upsertDigestUpdates(Seq(updateP2, updateP3, updateP4)).futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        sender.sendAcsCommitments(sendTimepoint).futureValueUS,
        LogEntry.assertLogSeq(
          mustContainWithClue = Seq(
            (
              _.errorMessage should include("An error occurred when sending ACS commitments"),
              "expected error message",
            )
          ),
          mayContain = Seq.empty,
        ),
      )

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 1

      val request = requests.head
      val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
      val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

      acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(defaultSyncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(
        defaultSyncCryptoApi,
        acsCommitmentSummaryMessage,
      )

      acsCommitments shouldBe List(
        acsCommitmentP2,
        acsCommitmentP3,
      )

      acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant2, participant3),
        commitmentTick = sendTimestamp,
        batchIndex = NonNegativeInt.zero,
        lastBatch = false,
      )

      assertWatermarkValue(watermarkStore, None)
      assertWatermarkMetricsValue(metrics, None)

      assertCounterMetricValue(metrics.sentBatchCount, 0)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 1)
      assertCounterMetricValue(metrics.sendFailureCount, 1)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }

    "keep attempting to send messages when getting retriable errors" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sendResultFactory = consecutiveSendResultsFactory(
        Seq(
          timeoutSendResultFactory,
          timeoutSendResultFactory,
          successfulSendResultFactory,
        )
      )
      val sequencerClient = new TestSequencerClientSend(wallClock, sendResultFactory.some)
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) = mkStoresAndSender(sequencerClient, metrics)

      digestStore.participant.upsertDigestUpdates(Seq(updateP2, updateP3, updateP4)).futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.sendAcsCommitments(sendTimepoint).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 3 // Failed attempts are also recorded

      val request = requests.last
      val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
      val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

      acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(defaultSyncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(
        defaultSyncCryptoApi,
        acsCommitmentSummaryMessage,
      )

      acsCommitments shouldBe List(
        acsCommitmentP2,
        acsCommitmentP3,
        acsCommitmentP4,
      )

      acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant2, participant3, participant4),
        commitmentTick = sendTimestamp,
      )

      assertWatermarkValue(watermarkStore, sendTimepoint.some)
      assertWatermarkMetricsValue(metrics, sendTimepoint.some)

      assertCounterMetricValue(metrics.sentBatchCount, 1)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 2)
      assertCounterMetricValue(metrics.sendFailureCount, 0)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }

    "not increase the batch index if all digests in the batch are empty" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val metrics = mkMetrics()
      val (digestStore, watermarkStore, sender) =
        mkStoresAndSender(sequencerClient, metrics, maxBatchSize = PositiveInt.tryCreate(2))

      digestStore.participant
        .upsertDigestUpdates(Seq(updateP2Empty, updateP3Empty, updateP4))
        .futureValueUS

      assertWatermarkValue(watermarkStore, None)
      assertInitialEmptyMetricValues(metrics)

      sender.sendAcsCommitments(sendTimepoint).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 1

      val request = requests.head
      val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
      val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

      acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(defaultSyncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(
        defaultSyncCryptoApi,
        acsCommitmentSummaryMessage,
      )

      acsCommitments shouldBe List(acsCommitmentP4)

      acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant4),
        commitmentTick = sendTimestamp,
        batchIndex = NonNegativeInt.zero,
        lastBatch = true,
      )

      assertWatermarkValue(watermarkStore, sendTimepoint.some)
      assertWatermarkMetricsValue(metrics, sendTimepoint.some)

      assertCounterMetricValue(metrics.sentBatchCount, 1)
      assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
      assertCounterMetricValue(metrics.sendFailureCount, 0)
      assertCounterMetricValue(metrics.sendAttemptCount, 1)
    }
  }

  "Retry delay calculation" should {
    val originalDelay = FiniteDuration(2, TimeUnit.SECONDS)
    val maxDelay = FiniteDuration(10, TimeUnit.SECONDS)

    "return None if there is no base delay" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val retryStrategy = RetryStrategy(retryDelay = None)

      AcsCommitmentSender.calculateFinalRetryDelay(
        retryStrategy,
        NonNegativeInt.two,
        maxDelay,
      ) shouldBe None
    }

    "return original delay if exponential backoff is disabled" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val retryStrategy =
        RetryStrategy(retryDelay = originalDelay.some, useExponentialBackoff = false)

      AcsCommitmentSender.calculateFinalRetryDelay(
        retryStrategy,
        NonNegativeInt.two,
        maxDelay,
      ) shouldBe originalDelay.some
    }

    "return increased delay within the limit if exponential backoff is enabled" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val retryStrategy =
        RetryStrategy(retryDelay = originalDelay.some, useExponentialBackoff = true)

      AcsCommitmentSender.calculateFinalRetryDelay(
        retryStrategy,
        NonNegativeInt.zero,
        maxDelay,
      ) shouldBe originalDelay.some

      AcsCommitmentSender.calculateFinalRetryDelay(
        retryStrategy,
        NonNegativeInt.one,
        maxDelay,
      ) shouldBe (originalDelay * 2).some

      AcsCommitmentSender.calculateFinalRetryDelay(
        retryStrategy,
        NonNegativeInt.two,
        maxDelay,
      ) shouldBe (originalDelay * 4).some

      AcsCommitmentSender.calculateFinalRetryDelay(
        retryStrategy,
        NonNegativeInt.three,
        maxDelay,
      ) shouldBe maxDelay.some
    }
  }

  private def assertWatermarkValue(
      watermarkStore: AcsCommitmentSenderWatermarkStore,
      expectedTimepoint: Option[Timepoint],
  )(implicit traceContext: TraceContext): Assertion =
    watermarkStore.lookupWatermark().futureValueUS.map(_.tupled) shouldBe expectedTimepoint.map(
      _.tupled
    )

  private def assertInitialEmptyMetricValues(metrics: CommitmentSenderMetrics): Assertion = {
    assertWatermarkMetricsValue(metrics, None)
    assertCounterMetricValue(metrics.sentBatchCount, 0)
    assertCounterMetricValue(metrics.batchSendingErrorCount, 0)
    assertCounterMetricValue(metrics.sendFailureCount, 0)
    assertCounterMetricValue(metrics.sendAttemptCount, 0)
  }

  private def assertWatermarkMetricsValue(
      metrics: CommitmentSenderMetrics,
      expectedTimepoint: Option[Timepoint],
  ): Assertion = {
    metrics.watermarkOffset.getValue shouldBe expectedTimepoint.fold(0L)(_.offset.unwrap)
    metrics.watermarkTimestamp.getValue shouldBe expectedTimepoint.fold(0L)(_.recordTime.toMicros)
  }

  private def assertCounterMetricValue(counter: Counter, expectedValue: Long): Assertion =
    inside(counter) { case inMemoryCounter: InMemoryCounter =>
      inMemoryCounter.markers.headOption.map(_._2.get()).getOrElse(0L) shouldBe expectedValue
    }

  private def assertCommitmentMessageValidSignature(
      syncCryptoApi: SyncCryptoApi,
      message: AcsCommitmentProtocolMessage,
  ): Assertion =
    AcsCommitmentProtocolMessage
      .verifySignature(syncCryptoApi, message)
      .futureValueUS
      .isRight shouldBe true

  private def assertCommitmentSummaryMessageValidSignature(
      syncCryptoApi: SyncCryptoApi,
      message: AcsCommitmentSummaryProtocolMessage,
  ): Assertion =
    AcsCommitmentSummaryProtocolMessage
      .verifySignature(syncCryptoApi, message, participant1)
      .futureValueUS
      .isRight shouldBe true

  private def mkMetrics(): CommitmentSenderMetrics = TestCommitmentMetrics().sender

  private def mkStoresAndSender(
      sequencerClient: TestSequencerClientSend,
      metrics: CommitmentSenderMetrics,
      cryptoApi: SynchronizerCryptoClient = defaultCryptoApi,
      maxBatchSize: PositiveInt = AcsCommitmentSenderConfig.defaultMaxBatchSize,
  ): (
      AcsDigestStore,
      AcsCommitmentSenderWatermarkStore,
      AcsCommitmentSender,
  ) = {
    val (digestStore, watermarkStore) = mkStores()

    (
      digestStore,
      watermarkStore,
      new AcsCommitmentSender(
        digestStore = digestStore,
        cryptoApi = cryptoApi,
        sequencerClient = sequencerClient,
        watermarkStore = watermarkStore,
        loggerFactory = loggerFactory,
        timeouts = timeouts,
        clock = new WallClock(timeouts, loggerFactory),
        stringInterningEval = Eval.now(mockStringInterning),
        metrics = metrics,
        synchronizerId = psid,
        participantId = participant1,
        config = AcsCommitmentSenderConfig(
          maxBatchSize = maxBatchSize
        ),
      ),
    )
  }

  protected def mkStores(): (AcsDigestStore, AcsCommitmentSenderWatermarkStore)
}

object AcsCommitmentSenderTest extends TestDigestUtils {
  private lazy val psid = physicalSynchronizerId
  private lazy val initialSynchronizerParameters = TestSynchronizerParameters.defaultDynamic

  lazy val defaultSync: IndexedSynchronizer = IndexedSynchronizer.tryCreate(psid.logical, 1)
  lazy val mockStringInterning = new MockStringInterning()

  lazy val (offset0, t0) = offsetTime(PositiveLong.tryCreate(10))
  lazy val (offset1, t1) = offsetTime(PositiveLong.tryCreate(20))
  lazy val (offset2, t2) = offsetTime(PositiveLong.tryCreate(30))
  lazy val (offset3, t3) = offsetTime(PositiveLong.tryCreate(40))

  private lazy val rawDigest0 = genRawDigest(0x2a)
  private lazy val hashedDigest0 = genHashedDigest(rawDigest0)
  private lazy val rawDigest1 = genRawDigest(0x3a)
  private lazy val hashedDigest1 = genHashedDigest(rawDigest1)
  private lazy val rawDigest2 = genRawDigest(0x4a)
  private lazy val hashedDigest2 = genHashedDigest(rawDigest2)

  private lazy val updateP2 =
    mkDigestUpdate(participant2, rawDigest0.some, offset0, t0)
  private lazy val updateP3 =
    mkDigestUpdate(participant3, rawDigest1.some, offset1, t1)
  private lazy val updateP4 =
    mkDigestUpdate(participant4, rawDigest2.some, offset2, t2)

  private lazy val updateP2Empty = mkDigestUpdate(participant2, None, offset0, t0)
  private lazy val updateP3Empty = mkDigestUpdate(participant3, None, offset1, t1)
  private lazy val updateP4Empty = mkDigestUpdate(participant4, None, offset2, t2)

  private lazy val acsCommitmentP2 = mkAcsCommitment(
    counterparticipantId = participant2,
    hashedDigest = hashedDigest0,
    fromExclusive = t0.immediatePredecessor,
    toInclusive = t3,
  )

  private lazy val acsCommitmentP3 = mkAcsCommitment(
    counterparticipantId = participant3,
    hashedDigest = hashedDigest1,
    fromExclusive = t1.immediatePredecessor,
    toInclusive = t3,
  )

  private lazy val acsCommitmentP4 = mkAcsCommitment(
    counterparticipantId = participant4,
    hashedDigest = hashedDigest2,
    fromExclusive = t2.immediatePredecessor,
    toInclusive = t3,
  )

  private def mkTopology(activeParticipants: Seq[ParticipantId]): TestingTopology =
    TestingTopology.from(
      Set(psid),
      participants =
        activeParticipants.map(_ -> ParticipantAttributes(ParticipantPermission.Submission)).toMap,
    )

  private def mkCryptoApi(
      topology: TestingTopology,
      loggerFactory: NamedLoggerFactory,
  ): SynchronizerCryptoClient = {
    val identityFactory = TestingIdentityFactory(
      topology,
      loggerFactory,
      dynamicSynchronizerParameters = initialSynchronizerParameters,
    )

    identityFactory.forOwnerAndSynchronizer(participant1, psid)
  }

  private lazy val allParticipantsTopology = mkTopology(
    Seq(participant1, participant2, participant3, participant4)
  )

  private def mkDigestUpdate(
      participantId: ParticipantId,
      digest0: Option[RawDigest],
      offset: Offset,
      timestamp: CantonTimestamp,
  ) = AcsDigestUpdate(
    digestUpdate = AcsDigest[InternedParticipantId](
      key = mockStringInterning.participantId.internalize(participantId.toLf),
      offset = offset,
      timestamp = timestamp,
      digestO = digest0,
      trace = None,
    ),
    replacesOffset = None,
  )

  private def mkAcsCommitment(
      counterparticipantId: ParticipantId,
      hashedDigest: HashedDigest,
      fromExclusive: CantonTimestamp,
      toInclusive: CantonTimestamp,
  ) = AcsCommitment.create(
    synchronizerId = psid,
    sender = participant1.toLf, // Can be hardcoded, as we only use this participant
    counterparticipant = counterparticipantId.toLf,
    period = CommitmentPeriod.tryCreate(
      fromExclusive = fromExclusive,
      toInclusive = toInclusive,
    ),
    digest = hashedDigest,
    protocolVersion = ProtocolVersion.dev,
  )

  private def mkAcsCommitmentSummary(
      participants: Seq[ParticipantId],
      commitmentTick: CantonTimestamp,
      batchIndex: NonNegativeInt = NonNegativeInt.zero,
      lastBatch: Boolean = true,
      unsentDigests: Seq[DigestForCounterparticipant] = Seq.empty,
  ) = AcsCommitmentSummary.create(
    psid = psid,
    commitmentTick = commitmentTick,
    addressedCounterparticipants = participants.map(_.toLf),
    unsentDigests = unsentDigests,
    batchIndex = batchIndex,
    lastBatch = lastBatch,
    protocolVersion = ProtocolVersion.dev,
  )

  private def splitMessages(
      batch: Batch[DefaultOpenEnvelope]
  ): (List[AcsCommitmentProtocolMessage], AcsCommitmentSummaryProtocolMessage) = {
    val envelopes = batch.envelopes

    val acsCommitmentMessages = envelopes
      .mapFilter(ProtocolMessage.select[AcsCommitmentProtocolMessage])
      .map(_.protocolMessage)
    val acsCommitmentSummaryMessage = envelopes
      .mapFilter(ProtocolMessage.select[AcsCommitmentSummaryProtocolMessage])
      .map(_.protocolMessage)
      .head

    (acsCommitmentMessages, acsCommitmentSummaryMessage)
  }

  private lazy val successfulSendResultFactory: Request => UnlessShutdown[SendResult] = { request =>
    UnlessShutdown.Outcome(
      SendResult.Success(
        // The specific values are not important for the tests at this point, we only need the instance of Success
        Deliver.create(
          previousTimestamp = None,
          timestamp = request.maxSequencingTime,
          synchronizerId = psid,
          messageIdO = None,
          batch = request.batch,
          topologyTimestampO = None,
          trafficReceipt = None,
        )
      )
    )
  }

  private def consecutiveSendResultsFactory(
      sendResults: Seq[Request => UnlessShutdown[SendResult]]
  ): Request => UnlessShutdown[SendResult] = {
    var index = 0

    { request =>
      val result = sendResults(index)(request)

      index += 1

      result
    }
  }

  private lazy val timeoutSendResultFactory: Request => UnlessShutdown[SendResult] = { request =>
    UnlessShutdown.Outcome(
      SendResult.Timeout(request.maxSequencingTime)
    )
  }

  private lazy val nonRetriableErrorSendResultFactory: Request => UnlessShutdown[SendResult] = {
    request =>
      UnlessShutdown.Outcome(
        SendResult.Error(
          DeliverError.create(
            previousTimestamp = None,
            timestamp = request.maxSequencingTime,
            synchronizerId = psid,
            messageId = MessageId.randomMessageId(),
            sequencerError = SequencerErrors.SenderUnknown("Sender Unknown"),
            trafficReceipt = None,
          )
        )
      )
  }
}

trait AcsCommitmentSenderTestDb extends AcsCommitmentSenderTest {
  self: DbTest =>

  import AcsCommitmentSenderTest.*

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

//@AcsCommitmentTest
//class AcsCommitmentSenderTestPostgres extends AcsCommitmentSenderTestDb with PostgresTest
//
//@AcsCommitmentTest
//class AcsCommitmentSenderTestH2 extends AcsCommitmentSenderTestDb with H2Test

@AcsCommitmentTest
class AcsCommitmentSenderTestInMemory extends AcsCommitmentSenderTest {

  import AcsCommitmentSenderTest.*

  override protected def mkStores(): (AcsDigestStore, AcsCommitmentSenderWatermarkStore) = (
    (InMemoryAcsDigestStore
      .create(Eval.now(mockStringInterning), loggerFactory)),
    new InMemoryAcsCommitmentSenderWatermarkStore(loggerFactory),
  )
}
