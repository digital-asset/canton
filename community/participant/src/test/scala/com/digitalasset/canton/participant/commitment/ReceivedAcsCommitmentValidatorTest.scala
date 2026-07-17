// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{
  BatchingConfig,
  CachingConfigs,
  CryptoConfig,
  CryptoProvider,
  SessionEncryptionKeyCacheConfig,
}
import com.digitalasset.canton.crypto.{
  Crypto,
  RequiredEncryptionSpecs,
  RequiredSigningSpecs,
  SynchronizerCryptoClient,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.Update.ReceivedAcsCommitment
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.participant.commitment.ReceivedAcsCommitmentValidatorTest.Publisher
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.AcsCommitmentAlarm
import com.digitalasset.canton.protocol.Phase37Processor.PublishUpdateViaRecordOrderPublisher
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  AcsCommitmentProtocolMessage,
  CommitmentPeriod,
}
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  TestingIdentityFactory,
  TestingTopology,
}
import com.digitalasset.canton.tracing.NoReportingTracerProvider
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

@AcsCommitmentTest
class ReceivedAcsCommitmentValidatorTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

  import DefaultTestIdentities.{physicalSynchronizerId, participant1, participant2, participant3}

  private lazy val jceStaticSynchronizerParameters: StaticSynchronizerParameters =
    StaticSynchronizerParameters(
      requiredSigningSpecs = RequiredSigningSpecs(
        CryptoProvider.Jce.signingAlgorithms.supported,
        CryptoProvider.Jce.signingKeys.supported,
      ),
      requiredEncryptionSpecs = RequiredEncryptionSpecs(
        CryptoProvider.Jce.encryptionAlgorithms.supported,
        CryptoProvider.Jce.encryptionKeys.supported,
      ),
      requiredSymmetricKeySchemes = CryptoProvider.Jce.symmetric.supported,
      requiredHashAlgorithms = CryptoProvider.Jce.hash.supported,
      requiredCryptoKeyFormats = CryptoProvider.Jce.supportedCryptoKeyFormats,
      requiredSignatureFormats = CryptoProvider.Jce.supportedSignatureFormats,
      topologyChangeDelay = StaticSynchronizerParameters.defaultTopologyChangeDelay,
      enableTransparencyChecks = false,
      protocolVersion = testedProtocolVersion,
      serial = NonNegativeInt.zero,
    )

  private lazy val crypto: Crypto = Crypto
    .create(
      CryptoConfig(),
      CachingConfigs.defaultKmsMetadataCache,
      SessionEncryptionKeyCacheConfig(),
      CachingConfigs.defaultPublicKeyConversionCache,
      new MemoryStorage(loggerFactory, timeouts),
      Option.empty[ReplicaManager],
      testedReleaseProtocolVersion,
      futureSupervisor,
      wallClock,
      CommonMockMetrics.cryptoMetrics,
      executorService,
      timeouts,
      BatchingConfig(),
      loggerFactory,
      NoReportingTracerProvider,
      new ExecutorServiceMetrics(NoOpMetricsFactory),
    )
    .valueOrFailShutdown("Failed to create crypto object")
    .futureValue

  private lazy val testingTopology: TestingIdentityFactory =
    TestingTopology()
      .withSynchronizers(physicalSynchronizerId)
      .withStaticSynchronizerParams(jceStaticSynchronizerParameters)
      .withSimpleParticipants(participant1, participant2, participant3)
      .build(crypto, loggerFactory)

  private lazy val p1client: SynchronizerCryptoClient =
    testingTopology.forOwnerAndSynchronizer(participant1)
  private lazy val p2client: SynchronizerCryptoClient =
    testingTopology.forOwnerAndSynchronizer(participant2)
  private lazy val p3client: SynchronizerCryptoClient =
    testingTopology.forOwnerAndSynchronizer(participant3)

  private def mkValidator =
    new ReceivedAcsCommitmentValidatorImpl(
      physicalSynchronizerId,
      participant1,
      p1client,
      ParticipantTestMetrics.synchronizer.commitments,
      PositiveInt.one,
      loggerFactory,
    )

  private lazy val goodCommitmentP2 = AcsCommitment.create(
    physicalSynchronizerId,
    participant2.toLf,
    participant1.toLf,
    CommitmentPeriod
      .tryCreate(CantonTimestamp.ofEpochSecond(1), CantonTimestamp.ofEpochSecond(3)),
    ByteString.copyFromUtf8("Good digest"),
    testedProtocolVersion,
  )

  private lazy val goodCommitmentP3 = AcsCommitment.create(
    physicalSynchronizerId,
    sender = participant3.toLf,
    counterparticipant = participant1.toLf,
    CommitmentPeriod
      .tryCreate(CantonTimestamp.ofEpochSecond(3), CantonTimestamp.ofEpochSecond(8)),
    ByteString.copyFromUtf8("Another digest"),
    testedProtocolVersion,
  )
  private lazy val invalidRecipientCommitment = AcsCommitment.create(
    physicalSynchronizerId,
    sender = participant2.toLf,
    // Invalid recipient, the expected recipient is participant1
    counterparticipant = participant3.toLf,
    CommitmentPeriod
      .tryCreate(CantonTimestamp.ofEpochSecond(1), CantonTimestamp.ofEpochSecond(3)),
    ByteString.copyFromUtf8("invalid recipient"),
    testedProtocolVersion,
  )

  "ReceivedAcsCommitmentValidatorImpl" should {
    "pass protocol messages to publish" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val validator = mkValidator
      val publisher = new Publisher

      val messages = NonEmpty(
        Seq,
        AcsCommitmentProtocolMessage.signAndCreate(p2client, goodCommitmentP2).futureValueUS.value,
        AcsCommitmentProtocolMessage.signAndCreate(p3client, goodCommitmentP3).futureValueUS.value,
      )
      val envelopes = messages.map(message =>
        OpenEnvelope(
          message,
          Recipients.cc(participant1),
        )(testedProtocolVersion)
      )

      validator
        .validateAndPublish(CantonTimestamp.ofEpochSecond(10), envelopes, publisher)
        .handlerResultValue

      val expectedCommitments = ReceivedAcsCommitments(messages)
      publisher.publishRef.get.value shouldBe Some(
        ReceivedAcsCommitment(
          synchronizerId = physicalSynchronizerId.logical,
          recordTime = CantonTimestamp.ofEpochSecond(10),
          payload = expectedCommitments.toByteString(testedProtocolVersion),
          updateId = ReceivedAcsCommitmentValidator.updateId(
            localParticipantId = participant1,
            synchronizerId = physicalSynchronizerId,
            recordTime = CantonTimestamp.ofEpochSecond(10),
          ),
        )
      )
    }

    "not publish invalid protocol messages" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val validator = mkValidator
      val publisher = new Publisher

      val futurePeriodCommitment = AcsCommitment.create(
        physicalSynchronizerId,
        sender = participant2.toLf,
        counterparticipant = participant1.toLf,
        CommitmentPeriod
          .tryCreate(
            CantonTimestamp.ofEpochSecond(3),
            CantonTimestamp.ofEpochSecond(10).immediateSuccessor,
          ),
        ByteString.copyFromUtf8("future period end"),
        testedProtocolVersion,
      )
      val messages = NonEmpty(
        Seq,
        AcsCommitmentProtocolMessage.signAndCreate(p2client, goodCommitmentP2).futureValueUS.value,
        AcsCommitmentProtocolMessage
          .signAndCreate(p2client, invalidRecipientCommitment)
          .futureValueUS
          .value,
        AcsCommitmentProtocolMessage
          .signAndCreate(p2client, futurePeriodCommitment)
          .futureValueUS
          .value,
        AcsCommitmentProtocolMessage.signAndCreate(p3client, goodCommitmentP2).futureValueUS.value,
      )
      val envelopes = messages.map(message =>
        OpenEnvelope(
          message,
          Recipients.cc(participant1),
        )(testedProtocolVersion)
      )

      loggerFactory.assertLogs(
        validator
          .validateAndPublish(CantonTimestamp.ofEpochSecond(10), envelopes, publisher)
          .handlerResultValue,
        invalidRecipientLogAssertion,
        _.shouldBeCantonError(
          AcsCommitmentAlarm.code,
          _ should include("Received an ACS commitment with a future timestamp."),
        ),
        _.shouldBeCantonError(
          AcsCommitmentAlarm.code,
          _ should include("SignatureWithWrongKey"),
        ),
      )

      val expectedCommitments = ReceivedAcsCommitments(NonEmpty(Seq, messages.head1))
      publisher.publishRef.get.value shouldBe Some(
        ReceivedAcsCommitment(
          synchronizerId = physicalSynchronizerId.logical,
          recordTime = CantonTimestamp.ofEpochSecond(10),
          payload = expectedCommitments.toByteString(testedProtocolVersion),
          updateId = ReceivedAcsCommitmentValidator.updateId(
            localParticipantId = participant1,
            synchronizerId = physicalSynchronizerId,
            recordTime = CantonTimestamp.ofEpochSecond(10),
          ),
        )
      )
    }

    "not publish anything if no envelopes pass validation" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
      val validator = mkValidator
      val publisher = new Publisher

      val messages = NonEmpty(
        Seq,
        AcsCommitmentProtocolMessage
          .signAndCreate(p2client, invalidRecipientCommitment)
          .futureValueUS
          .value,
      )
      val envelopes = messages.map(message =>
        OpenEnvelope(message, Recipients.cc(participant1))(testedProtocolVersion)
      )

      loggerFactory.assertLogs(
        validator
          .validateAndPublish(CantonTimestamp.ofEpochSecond(10), envelopes, publisher)
          .handlerResultValue,
        invalidRecipientLogAssertion,
      )

      publisher.publishRef.get.value shouldBe empty
    }
  }

  private def invalidRecipientLogAssertion(entry: LogEntry): Assertion =
    entry.shouldBeCantonError(
      AcsCommitmentAlarm.code,
      _ should include(
        s"${participant2.toLf} sent an ACS commitment to me, but the commitment lists ${participant3.toLf} as the counterparticipant"
      ),
    )
}

object ReceivedAcsCommitmentValidatorTest {
  private class Publisher extends PublishUpdateViaRecordOrderPublisher[ReceivedAcsCommitment] {
    val publishRef: SingleUseCell[Option[ReceivedAcsCommitment]] =
      new SingleUseCell[Option[ReceivedAcsCommitment]]()

    override def apply(event: Option[ReceivedAcsCommitment]): Unit =
      publishRef.putIfAbsent(event).foreach { previous =>
        throw new IllegalStateException(
          s"Publisher was called twice. Previous: $previous, new: $event"
        )
      }
  }
}
