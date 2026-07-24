// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.Eval
import cats.syntax.functorFilter.*
import cats.syntax.option.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.SyncCryptoApi
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.UnlessShutdown
import com.digitalasset.canton.participant.config.AcsCommitmentSenderConfig
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigest,
  AcsDigestUpdate,
  HashedDigest,
  InternedParticipantId,
  RawDigest,
}
import com.digitalasset.canton.participant.store.db.{BaseDbAcsDigestStoreTest, DbAcsDigestStore}
import com.digitalasset.canton.participant.store.{AcsDigestStore, TestDigestUtils}
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  AcsCommitmentProtocolMessage,
  AcsCommitmentSummary,
  AcsCommitmentSummaryProtocolMessage,
  CommitmentPeriod,
  DefaultOpenEnvelope,
  ProtocolMessage,
}
import com.digitalasset.canton.sequencing.client.TestSequencerClientSend.Request
import com.digitalasset.canton.sequencing.client.{SendResult, TestSequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver}
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  participant1,
  participant2,
  participant3,
  participant4,
  physicalSynchronizerId,
}
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, ParticipantPermission}
import com.digitalasset.canton.topology.{ParticipantId, TestingIdentityFactory, TestingTopology}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

trait AcsCommitmentSenderTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAsyncWordSpec {
  import AcsCommitmentSenderTest.*

  implicit val mc: MetricsContext = MetricsContext.Empty

  private val topology = TestingTopology.from(
    Set(psid),
    participants = Map(
      participant1 -> ParticipantAttributes(
        ParticipantPermission.Submission
      ),
      participant2 -> ParticipantAttributes(
        ParticipantPermission.Submission
      ),
      participant3 -> ParticipantAttributes(
        ParticipantPermission.Submission
      ),
    ),
  )

  private val sendOffset = offset3
  private val sendTimestamp = t3

  private val cryptoApi = {
    val identityFactory = TestingIdentityFactory(
      topology,
      loggerFactory,
      dynamicSynchronizerParameters = initialSynchronizerParameters,
    )
    identityFactory.forOwnerAndSynchronizer(participant1, psid)
  }
  private val syncCryptoApi = cryptoApi.snapshot(t3).futureValueUS

  "AcsCommitmentSender" should {
    "send the expected messages when all messages fit in one batch" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val (store, sender) = mkStoreAndSender(sequencerClient)

      store.participant.upsertDigestUpdates(Seq(updateP2, updateP3, updateP4)).futureValueUS

      sender.sendAcsCommitments(sendOffset, sendTimestamp).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 1

      val request = requests.head
      val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
      val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

      acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(syncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(syncCryptoApi, acsCommitmentSummaryMessage)

      acsCommitments shouldBe List(
        acsCommitmentP2,
        acsCommitmentP3,
        acsCommitmentP4,
      )

      acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant2, participant3, participant4),
        commitmentTick = sendTimestamp,
      )
    }

    "skip the updates with empty digests and send messages for the rest" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val (store, sender) = mkStoreAndSender(sequencerClient)

      store.participant.upsertDigestUpdates(Seq(updateP2Empty, updateP3, updateP4)).futureValueUS

      sender.sendAcsCommitments(sendOffset, sendTimestamp).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 1

      val request = requests.head
      val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
      val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

      acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(syncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(syncCryptoApi, acsCommitmentSummaryMessage)

      acsCommitments shouldBe List(
        acsCommitmentP3,
        acsCommitmentP4,
      )

      acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant3, participant4),
        commitmentTick = sendTimestamp,
      )
    }

    "send nothing if all updates have empty digests" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val (store, sender) = mkStoreAndSender(sequencerClient)

      store.participant
        .upsertDigestUpdates(Seq(updateP2Empty, updateP3Empty, updateP4Empty))
        .futureValueUS

      sender.sendAcsCommitments(sendOffset, sendTimestamp).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 0
    }

    "send nothing if snapshot contains no updates" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val (_, sender) = mkStoreAndSender(sequencerClient)

      sender.sendAcsCommitments(sendOffset, sendTimestamp).futureValueUS

      val requests = sequencerClient.requests.toSeq

      requests.length shouldBe 0
    }

    "send the expected messages when messages are split into multiple batches" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val sequencerClient = new TestSequencerClientSend(wallClock, successfulSendResultFactory.some)
      val (store, sender) =
        mkStoreAndSender(sequencerClient, snapshotLimit = PositiveInt.tryCreate(2))

      store.participant.upsertDigestUpdates(Seq(updateP2, updateP3, updateP4)).futureValueUS

      sender.sendAcsCommitments(sendOffset, sendTimestamp).futureValueUS

      val requests = sequencerClient.requests.toSeq
      requests.length shouldBe 2

      val request1 = requests.head
      val request2 = requests(1)

      val (acsCommitmentMessages1, acsCommitmentSummaryMessage1) = splitMessages(request1.batch)
      val acsCommitments1 = acsCommitmentMessages1.map(_.acsCommitment)

      acsCommitmentMessages1.foreach(assertCommitmentMessageValidSignature(syncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(syncCryptoApi, acsCommitmentSummaryMessage1)

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

      acsCommitmentMessages2.foreach(assertCommitmentMessageValidSignature(syncCryptoApi, _))
      assertCommitmentSummaryMessageValidSignature(syncCryptoApi, acsCommitmentSummaryMessage2)

      acsCommitments2 shouldBe List(
        acsCommitmentP4
      )

      acsCommitmentSummaryMessage2.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
        participants = Seq(participant4),
        commitmentTick = sendTimestamp,
        batchIndex = NonNegativeInt.one,
        lastBatch = true,
      )
    }
  }

  "not try to send the next batch if sending the first batch fails" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
    val sequencerClient = new TestSequencerClientSend(wallClock, timeoutSendResultFactory.some)
    val (store, sender) =
      mkStoreAndSender(sequencerClient, snapshotLimit = PositiveInt.tryCreate(2))

    store.participant.upsertDigestUpdates(Seq(updateP2, updateP3, updateP4)).futureValueUS

    sender.sendAcsCommitments(sendOffset, sendTimestamp).futureValueUS

    val requests = sequencerClient.requests.toSeq
    requests.length shouldBe 1

    val request = requests.head
    val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
    val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

    acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(syncCryptoApi, _))
    assertCommitmentSummaryMessageValidSignature(syncCryptoApi, acsCommitmentSummaryMessage)

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
  }

  "not increase the batch index if all digests in the batch are empty" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
    val sequencerClient = new TestSequencerClientSend(wallClock, timeoutSendResultFactory.some)
    val (store, sender) =
      mkStoreAndSender(sequencerClient, snapshotLimit = PositiveInt.tryCreate(2))

    store.participant.upsertDigestUpdates(Seq(updateP2Empty, updateP3Empty, updateP4)).futureValueUS

    sender.sendAcsCommitments(sendOffset, sendTimestamp).futureValueUS

    val requests = sequencerClient.requests.toSeq
    requests.length shouldBe 1

    val request = requests.head
    val (acsCommitmentMessages, acsCommitmentSummaryMessage) = splitMessages(request.batch)
    val acsCommitments = acsCommitmentMessages.map(_.acsCommitment)

    acsCommitmentMessages.foreach(assertCommitmentMessageValidSignature(syncCryptoApi, _))
    assertCommitmentSummaryMessageValidSignature(syncCryptoApi, acsCommitmentSummaryMessage)

    acsCommitments shouldBe List(acsCommitmentP4)

    acsCommitmentSummaryMessage.acsCommitmentSummary shouldBe mkAcsCommitmentSummary(
      participants = Seq(participant4),
      commitmentTick = sendTimestamp,
      batchIndex = NonNegativeInt.zero,
      lastBatch = true,
    )
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

  private def mkStoreAndSender(
      sequencerClient: TestSequencerClientSend,
      snapshotLimit: PositiveInt = AcsCommitmentSenderConfig.defaultMaxBatchSize,
  ): (
      AcsDigestStore,
      AcsCommitmentSender,
  ) = {
    val digestStore = mkDigestStore()

    (
      digestStore,
      new AcsCommitmentSender(
        digestStore = digestStore,
        cryptoApi = cryptoApi,
        sequencerClient = sequencerClient,
        stringInterningEval = Eval.now(mockStringInterning),
        synchronizerId = psid,
        participantId = participant1,
        config = AcsCommitmentSenderConfig(
          maxBatchSize = snapshotLimit
        ),
      ),
    )
  }

  protected def mkDigestStore(): AcsDigestStore
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

  private lazy val (rawDigest0, hashedDigest0) = genParticipantDigest(genRawDigest(0x2a))
  private lazy val (rawDigest1, hashedDigest1) = genParticipantDigest(genRawDigest(0x3a))
  private lazy val (rawDigest2, hashedDigest2) = genParticipantDigest(genRawDigest(0x4a))

  private lazy val updateP2 =
    mkDigestUpdate(participant2, (rawDigest0, hashedDigest0).some, offset0, t0)
  private lazy val updateP3 =
    mkDigestUpdate(participant3, (rawDigest1, hashedDigest1).some, offset1, t1)
  private lazy val updateP4 =
    mkDigestUpdate(participant4, (rawDigest2, hashedDigest2).some, offset2, t2)

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

  private def mkDigestUpdate(
      participantId: ParticipantId,
      digest0: Option[(RawDigest, HashedDigest)],
      offset: Offset,
      timestamp: CantonTimestamp,
  ) = AcsDigestUpdate(
    digestUpdate = AcsDigest[InternedParticipantId, (RawDigest, HashedDigest)](
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
  ) = AcsCommitmentSummary.create(
    psid = psid,
    commitmentTick = commitmentTick,
    addressedCounterparticipants = participants.map(_.toLf),
    unsentDigests = Seq.empty,
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

  private lazy val timeoutSendResultFactory: Request => UnlessShutdown[SendResult] = { request =>
    UnlessShutdown.Outcome(
      SendResult.Timeout(request.maxSequencingTime)
    )
  }
}

trait AcsCommitmentSenderTestDb extends AcsCommitmentSenderTest with BaseDbAcsDigestStoreTest {
  self: DbTest =>

  import AcsCommitmentSenderTest.*

  override protected def mkDigestStore(): AcsDigestStore = new DbAcsDigestStore(
    indexedSynchronizer = defaultSync,
    Eval.now(mockStringInterning),
    storage,
    loggerFactory,
    timeouts,
  )
}

@AcsCommitmentTest
class AcsCommitmentSenderTestPostgres extends AcsCommitmentSenderTestDb with PostgresTest

//@AcsCommitmentTest
//class AcsCommitmentSenderTestH2 extends AcsCommitmentSenderTestDb with H2Test

//@AcsCommitmentTest
//class AcsCommitmentSenderTestInMemory extends AcsCommitmentSenderTest {
//  import AcsCommitmentSenderTest.*
//
//  override protected def mkDigestStore(): AcsDigestStore = InMemoryAcsDigestStore
//    .create(Eval.now(mockStringInterning), loggerFactory)
//
//}
