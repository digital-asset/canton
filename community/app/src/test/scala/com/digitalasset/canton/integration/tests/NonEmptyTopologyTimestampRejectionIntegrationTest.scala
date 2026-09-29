// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MessageId,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.NonEmptyTopologyTimestamp
import com.digitalasset.canton.version.ProtocolVersion

final class NonEmptyTopologyTimestampRejectionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private def submissionRequest(topologyTimestamp: Option[CantonTimestamp])(implicit
      env: TestConsoleEnvironment
  ): SignedContent[SubmissionRequest] = {
    import env.*

    val request = SubmissionRequest.tryCreate(
      sender = participant1.member,
      messageId = MessageId.randomMessageId(),
      batch = Batch(Nil, testedProtocolVersion),
      maxSequencingTime = CantonTimestamp.MaxValue,
      topologyTimestamp = topologyTimestamp,
      aggregationRule = None,
      submissionCost = None,
      protocolVersion = testedProtocolVersion,
    )

    val cryptoSnapshot: SyncCryptoApi =
      participant1.underlying.value.sync.syncCrypto
        .forSynchronizer(daId, staticSynchronizerParameters1)
        .value
        .currentSnapshotApproximation
        .futureValueUS
    SignedContent
      .create(
        cryptoApi = cryptoSnapshot.pureCrypto,
        cryptoPrivateApi = cryptoSnapshot,
        content = request,
        timestampOfSigningKey = Some(cryptoSnapshot.ipsSnapshot.timestamp),
        signingTimestampOverrides = None,
        purpose = HashPurpose.SubmissionRequestSignature,
        protocolVersion = testedProtocolVersion,
      )
      .futureValueUS
      .value
  }

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1

  "Non-empty topology timestamp" should {
    "be rejected synchronously for pv >= 36" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, daName)

      val resEmpty = sequencer1.underlying.value.sequencer.sequencer
        .sendAsyncSigned(submissionRequest(None))
        .futureValueUS

      val resNonEmpty = sequencer1.underlying.value.sequencer.sequencer
        .sendAsyncSigned(submissionRequest(Some(environment.clock.now)))
        .futureValueUS

      resEmpty.value shouldBe ()

      if (testedProtocolVersion <= ProtocolVersion.v35)
        resNonEmpty.value shouldBe ()
      else
        resNonEmpty.left.value shouldBe NonEmptyTopologyTimestamp.Error
    }
  }
}
