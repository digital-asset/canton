// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import com.digitalasset.canton.annotations.UnstableTest
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
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MessageId,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.store.SequencedEventStore.SearchCriterion
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGeneratorImpl
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import java.time.Duration
import scala.concurrent.duration.DurationInt

/** Checks that the config flag max sequencing time is taken into account. */
@UnstableTest // TODO(i31786): mark stable again
final class MaxSequencingTimeIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.useStaticTime)
      .addConfigTransform(
        ConfigTransforms.updateSequencerConfig("sequencer1")(
          _.focus(_.parameters.lsuRepair.globalMaxSequencingTimeExclusive)
            .replace(Some(maxSequencingTime))
        )
      )

  private lazy val maxSequencingTime = CantonTimestamp.ofEpochSecond(30)

  "Sequencer node" should {
    "enforce max sequencing time config value" in { implicit env =>
      import env.*

      def getLatestKnownSequencingTime(): CantonTimestamp =
        participant1.underlying.value.sync.syncPersistentStateManager
          .get(daId)
          .value
          .sequencedEventStore
          .find(SearchCriterion.Latest)
          .futureValueUS
          .value
          .timestamp

      participant1.synchronizers.connect_local(sequencer1, daName)
      participant1.health.ping(participant1)

      val initialTs = getLatestKnownSequencingTime()

      sequencer1.underlying.value.sequencer.sequencer
        .sendAsyncSigned(signedRequestForP1())
        .value
        .futureValueUS
        .value

      // wait until the message was received
      eventually() {
        getLatestKnownSequencingTime() should be > initialTs
      }

      environment.simClock.value.advanceTo(maxSequencingTime)

      /*
       We cannot rely on the standard waitForTargetTimeOnSequencer: as soon as the sequencing time is past
       the target time, nothing gets sequenced (and so waitForTargetTimeOnSequencer foes not complete)
       */
      eventually() {
        val ts =
          sequencer1.underlying.value.sequencer.sequencer.sequencingTime.futureValueUS.value
        ts should be >= maxSequencingTime
      }

      /*
        Time needs to progress for the message to be logged.
        Hence, we retry a few times.
       */
      eventually(retryOnTestFailuresOnly = false) {
        environment.simClock.value.advance(Duration.ofMillis(10))

        loggerFactory
          .assertEventuallyLogsSeq(
            SuppressionRule.Level(Level.INFO) && SuppressionRule.forLogger[BlockUpdateGeneratorImpl]
          )(
            sequencer1.underlying.value.sequencer.sequencer
              .sendAsyncSigned(signedRequestForP1())
              .value
              .futureValueUS
              .value,
            forExactly(1, _)(
              _.infoMessage should include(
                s"is not admissible (sequencing time is at or after the upper bound ($maxSequencingTime)"
              )
            ),
            // To avoid the test taking too long
            timeUntilSuccess = 2.seconds,
          )
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
