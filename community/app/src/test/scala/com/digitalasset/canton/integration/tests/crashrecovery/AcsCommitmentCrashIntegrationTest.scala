// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.crashrecovery

import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnections,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.NonNegativeProportion
import com.digitalasset.canton.config.{CommitmentSendDelay, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.tests.acs.commitment.util.{
  CommitmentTestUtil,
  IntervalDuration,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.ReceivedCmtState.Match
import monocle.macros.syntax.lens.*

class AcsCommitmentCrashIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with CommitmentTestUtil {

  private val interval: java.time.Duration = java.time.Duration.ofSeconds(60)
  private val checkpointInterval = config.PositiveDurationSeconds.ofSeconds(6)
  private implicit val intervalDuration: IntervalDuration = IntervalDuration(interval)

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.commitmentCheckpointInterval).replace(checkpointInterval)
        ),
      )
      .updateTestingConfig(
        _.focus(_.commitmentSendDelay)
          .replace(
            Some(
              CommitmentSendDelay(
                Some(NonNegativeProportion.zero),
                Some(NonNegativeProportion.zero),
              )
            )
          )
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronisation.await_idle()
        initializedSynchronizers foreach { case (_, initializedSynchronizer) =>
          initializedSynchronizer.synchronizerOwners.foreach(
            _.topology.synchronizer_parameters
              .propose_update(
                initializedSynchronizer.synchronizerId,
                _.update(reconciliationInterval = config.PositiveDurationSeconds(interval)),
              )
          )
        }

        // Set the observation latency to 0 such that `await_time` works in sim clock
        val daSequencerConnection =
          SequencerConnections.single(sequencer1.sequencerConnection.withAlias(daName.toString))
        participants.all.synchronizers.connect(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = daSequencerConnection,
            timeTracker = SynchronizerTimeTrackerConfig(
              observationLatency = config.NonNegativeFiniteDuration.Zero
            ),
          )
        )
        participants.all.foreach { p =>
          p.dars.upload(CantonExamplesPath, synchronizerId = daId)
        }

        passTopologyRegistrationTimeout()
      }

  "checkpoints are at a grid" in { implicit env =>
    import env.*

    val simClock = environment.simClock.value
    val checkpointGridMicros = checkpointInterval.underlying.toMicros
    // start is a timestamp on the checkpoint interval grid that is after now.`
    val start = CantonTimestamp.assertFromLong(
      (simClock.uniqueTime().underlying.micros + checkpointGridMicros)
        / checkpointGridMicros * checkpointGridMicros
    )

    val ts1 = start.plus(checkpointInterval.asJava.dividedBy(2))
    logger.info(s"Deploying first round of contracts at $ts1")
    simClock.advanceTo(ts1)
    IouSyntax.createIou(participant1)(participant1.adminParty, participant2.adminParty)
    IouSyntax.createIou(participant1)(participant1.adminParty, participant2.adminParty)

    val ts2 = start.plus(checkpointInterval.asJava.multipliedBy(4).dividedBy(3))
    logger.info(
      s"Deploying two more contracts to trigger a checkpoint within the checkpoint duration since the first round of contracts at $ts2"
    )
    simClock.advanceTo(ts2)
    IouSyntax.createIou(participant1)(participant1.adminParty, participant2.adminParty)
    IouSyntax.createIou(participant1)(participant1.adminParty, participant2.adminParty)

    logger.info("Creating a topology event that is not going to be reordered like the ACS changes")
    participant2.parties.enable("Bob")

    logger.info("Check that we have persisted a checkpoint")
    val storeP1 = participant1.underlying.value.sync.syncPersistentStateManager
      .get(daId)
      .value
      .acsCommitmentStore
    eventually() {
      // The watermark is at the record time of the last change prior to the checkpoint.
      val watermark =
        storeP1.runningCommitments.watermark.futureValueUS.timestamp
      watermark should be >= ts1
      watermark should be < start.plus(checkpointInterval.asJava)
    }

    val ts3 = start.plus(checkpointInterval.asJava.multipliedBy(5).dividedBy(3))
    logger.info(
      s"Deploying two more contracts without triggering a checkpoint, but more than the checkpoint duration since the first round of contracts at $ts3"
    )
    simClock.advanceTo(ts3)
    IouSyntax.createIou(participant1)(participant1.adminParty, participant2.adminParty)
    IouSyntax.createIou(participant1)(participant1.adminParty, participant2.adminParty)

    logger.info("Reconnecting participant1 for the first time")
    participant1.synchronizers.disconnect_all()
    participant1.synchronizers.reconnect_all()

    // Wait a bit so that the ACS commitment processor gets a chance to reprocess the changes.
    Threading.sleep(100)

    logger.info("Reconnecting participant1 for the second time")
    participant1.synchronizers.disconnect_all()
    participant1.synchronizers.reconnect_all()

    logger.info("Deploy another contract to without exceeding the next checkpoint grid")
    IouSyntax.createIou(participant1)(participant1.adminParty, participant2.adminParty)
    IouSyntax.createIou(participant1)(participant1.adminParty, participant2.adminParty)

    val period = awaitNextTick(participant1, participant2)
    checkReceivedCommitment(period, participant2, daId, Match)
  }
}
