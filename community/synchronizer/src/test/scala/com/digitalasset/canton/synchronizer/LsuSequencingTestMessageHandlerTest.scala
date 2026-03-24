// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer

import cats.data.EitherT
import cats.syntax.option.*
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  HashPurpose,
  Signature,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.protocol.messages.{
  LsuSequencingTestMessage,
  LsuSequencingTestMessageContent,
}
import com.digitalasset.canton.sequencing.WithCounter
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.ErrorCode
import com.digitalasset.canton.sequencing.client.{RequestSigner, SendAsyncClientError}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  Deliver,
  MediatorGroupRecipient,
  MessageId,
  OpenEnvelope,
  Recipients,
  SignedContent,
}
import com.digitalasset.canton.serialization.HasCryptographicEvidence
import com.digitalasset.canton.synchronizer.metrics.{MediatorHistograms, MediatorMetrics}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  MediatorId,
  Member,
  PhysicalSynchronizerId,
  SequencerGroup,
  SequencerId,
  TestingIdentityFactory,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

import DefaultTestIdentities.{mediatorId, namespace, physicalSynchronizerId, sequencerId}

final class LsuSequencingTestMessageHandlerTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext {

  private lazy val testingIdentityFactory = TestingIdentityFactory(
    TestingTopology.from(sequencerGroup =
      SequencerGroup(
        active = Seq(sequencerId, sequencerId2),
        passive = Nil,
        threshold = PositiveInt.one,
      )
    ),
    loggerFactory,
    TestSynchronizerParameters.defaultDynamic,
    SymbolicCrypto
      .create(testedReleaseProtocolVersion, DefaultProcessingTimeouts.testing, loggerFactory),
  )
  private lazy val mediatorId2: MediatorId = MediatorId(
    UniqueIdentifier.tryCreate("mediator2", namespace)
  )
  private lazy val sequencerId2: SequencerId = SequencerId(
    UniqueIdentifier.tryCreate("sequencer2", namespace)
  )

  private lazy val metricsFactory = new InMemoryMetricsFactory
  private lazy val histogramInventory = new HistogramInventory
  private lazy val mediatorHistograms = new MediatorHistograms(MetricName.Daml)(
    histogramInventory
  )

  private lazy val metrics = new MediatorMetrics(mediatorHistograms, metricsFactory)
  private lazy val cryptoApi = testingIdentityFactory.forOwnerAndSynchronizer(
    sequencerId,
    physicalSynchronizerId,
  )

  private lazy val handler =
    new LsuSequencingTestMessageHandler(metrics, cryptoApi, loggerFactory)

  private def getLsuSequencingTestMetricValues: Map[Member, Long] = metricsFactory.metrics.meters
    .get(MetricName.Daml :+ "received-lsu-sequencing-test-messages")
    .value
    .get(MetricsContext())
    .value
    .markers
    .map { case (ctx, value) =>
      Member.fromProtoPrimitive_(ctx.labels("sender")).value -> value.get()
    }
    .toMap

  private def createAndHandleMessage(
      psid: PhysicalSynchronizerId = physicalSynchronizerId,
      sender: Member = sequencerId,
      signatureOverride: Option[Signature] = None,
  ): FutureUnlessShutdown[Unit] = {
    val cryptoClient =
      testingIdentityFactory.forOwnerAndSynchronizer(sender, physicalSynchronizerId)

    for {
      snapshot <- cryptoClient.currentSnapshotApproximation

      content = LsuSequencingTestMessageContent.create(psid = psid, sender = sender)
      signedContent <- sign(
        cryptoClient,
        snapshot,
        HashPurpose.LsuSequencingTestMessageContent,
        content,
      ).value.map(_.value)

      message = LsuSequencingTestMessage(
        signedContent.content,
        signatureOverride.getOrElse(signedContent.signature),
      )

      openEnvelope: OpenEnvelope[LsuSequencingTestMessage] = OpenEnvelope(
        message,
        Recipients.cc(MediatorGroupRecipient(NonNegativeInt.zero)),
      )(testedProtocolVersion)

      deliver: Deliver[OpenEnvelope[LsuSequencingTestMessage]] = Deliver.create(
        previousTimestamp = None,
        timestamp = CantonTimestamp.Epoch,
        psid,
        MessageId.tryCreate("test").some,
        Batch(List(openEnvelope), testedProtocolVersion),
        topologyTimestampO = None,
        trafficReceipt = None,
      )

      tracedBatch = Traced(Seq(WithCounter(SequencerCounter(42), Traced(deliver))))

      _ <- handler(tracedBatch).futureValueUS.unwrap
    } yield ()
  }

  private def sign[A <: HasCryptographicEvidence](
      cryptoClient: SynchronizerCryptoClient,
      syncCryptoApi: SynchronizerSnapshotSyncCryptoApi,
      hashPurpose: HashPurpose,
      request: A,
  ): EitherT[FutureUnlessShutdown, ErrorCode.Wrap, SignedContent[A]] =
    RequestSigner(cryptoClient, loggerFactory)
      .signRequest(
        request = request,
        hashPurpose = hashPurpose,
        snapshot = syncCryptoApi,
        approximateTimestampOverride = None,
      )
      .leftMap { err =>
        val message = s"Error signing submission request $err"
        logger.error(message)
        SendAsyncClientError.ErrorCode
          .Wrap(SendAsyncClientError.RequestFailed(message))
      }

  "LsuSequencingTestMessageHandler" should {
    "update received-lsu-sequencing-test-messages metric for correct messages" in {
      mediatorId should not be mediatorId2
      sequencerId should not be sequencerId2

      val wrongPsid = physicalSynchronizerId.incrementSerial

      // first message -> value is 1
      createAndHandleMessage().futureValueUS
      getLsuSequencingTestMetricValues shouldBe Map(sequencerId -> 1)

      // second message -> value is 2
      createAndHandleMessage().futureValueUS
      getLsuSequencingTestMetricValues shouldBe Map(sequencerId -> 2)

      // wrong psid
      loggerFactory.assertLogs(
        createAndHandleMessage(psid = wrongPsid).futureValueUS,
        _.errorMessage should include(
          s"Received test lsu sequencing messages with wrong synchronizer ids: List($wrongPsid)"
        ),
      )
      // metric is not changed
      getLsuSequencingTestMetricValues shouldBe Map(sequencerId -> 2)

      // different sender
      createAndHandleMessage(sender = sequencerId2).futureValueUS
      getLsuSequencingTestMetricValues shouldBe Map(sequencerId -> 2, sequencerId2 -> 1)

      // incorrect signature
      loggerFactory.assertLogs(
        createAndHandleMessage(signatureOverride = Some(Signature.noSignature)).futureValueUS,
        _.warningMessage should include("INVALID_LSU_SEQUENCING_TEST_MESSAGE_SIGNATURE"),
      )
      // metric is not changed
      getLsuSequencingTestMetricValues shouldBe Map(sequencerId -> 2, sequencerId2 -> 1)
    }
  }
}
