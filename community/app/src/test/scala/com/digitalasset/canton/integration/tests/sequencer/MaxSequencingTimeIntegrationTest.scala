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
import com.digitalasset.canton.logging.SuppressionRule.LoggerNameContains
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  MessageId,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.store.SequencedEventStore.SearchCriterion
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGeneratorImpl
import com.digitalasset.canton.synchronizer.sequencer.WritePayloadsFlow
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import java.time.Duration
import scala.concurrent.duration.DurationInt

/** Checks that the config flag max sequencing time is taken into account. */
@UnstableTest // TODO(#31786) Remove from unstable
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

      /*
        Time needs to progress for the message to be logged.
        Hence, we retry a few times.
       */
      eventually(retryOnTestFailuresOnly = false) {
        /*
          In the standard scenario, the message is logged by the BlockUpdateGeneratorImpl.

          However, it can happen that a topology tick is the first event sequencer >= maxSequencingTime.
          Such a topology tick:
            - Is not handled by the BlockUpdateGeneratorImpl: the log is emitted by WritePayloadsFlow.
            - If dropped (which is the case if >= max sequencing time), completely blocks the orderer,
              which prevents subsequent attempts to succeed.
            - Can be emitted as soon as the clock is advanced.
         */
        loggerFactory
          .assertEventuallyLogsSeq(
            SuppressionRule.Level(Level.INFO) && (SuppressionRule
              .forLogger[BlockUpdateGeneratorImpl] || LoggerNameContains(
              WritePayloadsFlow.getClass.getSimpleName
            ))
          )(
            {
              environment.simClock.value.advanceTo(maxSequencingTime)
              val ts =
                sequencer1.underlying.value.sequencer.sequencer.sequencingTime.futureValueUS.value
              ts should be >= maxSequencingTime

              environment.simClock.value.advance(Duration.ofMillis(10))

              sequencer1.underlying.value.sequencer.sequencer
                .sendAsyncSigned(signedRequestForP1())
                .value
                .futureValueUS
                .value
            },
            forAtLeast(1, _)(
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
