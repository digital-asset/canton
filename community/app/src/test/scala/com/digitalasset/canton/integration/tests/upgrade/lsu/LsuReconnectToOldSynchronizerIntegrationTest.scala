// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
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
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, PositiveFiniteDuration}
import monocle.macros.syntax.lens.*

/** This test validates that participants can receive multiple time proofs on the old synchronizer
  * and disconnect from and reconnect to the old synchronizer as well.
  *
  * This test doesn't actually perform an LSU.
  */
final class LsuReconnectToOldSynchronizerIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.addConfigTransform(
      ConfigTransforms.updateAllParticipantConfigs_(
        // Ensure PNs stay connected to the old synchronizer
        _.focus(_.parameters.lsu.automaticallyPerformLsu).replace(false)
      )
    )

  "reconnect to the old synchronizer" in { implicit env =>
    import env.*

    sequencer1.topology.synchronizer_parameters.propose_update(
      synchronizerId = daId,
      // reduce the decision timeout to 20 seconds to lower time to run the test
      _.update(
        confirmationResponseTimeout = NonNegativeFiniteDuration.tryOfSeconds(10).toConfig,
        mediatorReactionTimeout = NonNegativeFiniteDuration.tryOfSeconds(10).toConfig,
      ),
      mustFullyAuthorize = true,
    )

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant1.health.ping(participant1)

    // announce the synchronizer upgrade
    val successorPsid = daId.incrementSerial
    val upgradeTime = environment.clock.now + PositiveFiniteDuration.tryOfSeconds(15)
    synchronizerOwners1.foreach { node =>
      node.topology.lsu.announcement.propose(successorPsid, upgradeTime, daId)
    }

    clue("awaiting announcement to become active") {
      eventually() {
        val announcement = participant1.topology.lsu.announcement.list(daId).loneElement
        announcement.item.upgradeTime shouldBe upgradeTime
        announcement.context.validFrom should be < upgradeTime.toInstant
      }
    }

    clue(s"waiting for time to pass until after $upgradeTime") {
      eventually() {
        val timestamp = participant1.testing.fetch_synchronizer_time(daId)
        timestamp should be > upgradeTime
      }
    }

    // Create a bunch of time proofs beyond the decision timeout

    val startOfTest = environment.clock.now
    List.tabulate(7) { i =>
      val offset = NonNegativeFiniteDuration.tryOfSeconds(5L * i.toLong)
      val targetTimestamp = startOfTest + offset
      clue(s"trying to provoke a tick with target time $targetTimestamp (offset of $offset)") {
        eventually() {
          val timestamp = participant1.testing.fetch_synchronizer_time(daId)
          timestamp should be > targetTimestamp
        }
      }
    }

    /*
    Creating a time proof request that is sent manually so that the participant does not process it before the restart.
    Has to be done here before the participant disconnects so that we can access crypto.
     */
    val request = signedRequestForP1()

    clue("disconnecting from synchronizer") {
      participant1.synchronizers.disconnect_all()
    }

    // trigger a time proof for the participant, that the participant doesn't actually process
    sequencer1.underlying.value.sequencer.sequencer.sendAsyncSigned(request).futureValueUS

    // reconnecting should work
    clue("reconnecting from synchronizer") {
      participant1.synchronizers.reconnect_all()
    }

    // triggering more timeproofs should work as well
    clue("trying to provoke a tick after reconnect") {
      eventually() {
        val timestamp = participant1.testing.fetch_synchronizer_time(daId)
        timestamp should be > upgradeTime
      }
    }

  }

  private def signedRequestForP1()(implicit
      env: TestConsoleEnvironment
  ): SignedContent[SubmissionRequest] = {
    import env.*

    val request = SubmissionRequest.tryCreate(
      sender = participant1.member,
      messageId = MessageId.randomMessageId(),
      batch = Batch(Nil, testedProtocolVersion),
      maxSequencingTime = CantonTimestamp.MaxValue,
      topologyTimestamp = None,
      aggregationRule = None,
      submissionCost = None,
      protocolVersion = testedProtocolVersion,
    )

    sequencer1.underlying.value.sequencer

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

}
