// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.acs.commitment

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
  SynchronizerTimeRange,
  TimeRange,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.{NonNegativeProportion, PositiveInt}
import com.digitalasset.canton.config.{CommitmentSendDelay, NonNegativeDuration}
import com.digitalasset.canton.console.LocalSequencerReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.tests.acs.commitment.util.{
  CommitmentTestUtil,
  IntervalDuration,
}
import com.digitalasset.canton.integration.util.TestUtils
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.MismatchError.CommitmentsMismatch
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.ReceivedCmtState.{
  Match,
  Mismatch,
}
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsHelpers
import com.digitalasset.canton.participant.store.UpdateMode
import com.digitalasset.canton.protocol.messages.{AcsCommitment, CommitmentPeriod}
import com.digitalasset.canton.topology.SynchronizerId
import monocle.Monocle.toAppliedFocusOps
import org.slf4j.event.Level

import java.time.Duration as JDuration
import scala.util.chaining.scalaUtilChainingOps

/** This test showcases the scenario of re-initializing the ACS commitments of a participant to a
  * recent point in time by skipping the intermediate checkpoints.
  *
  * It showcases a runbook that can be used in case of emergency when the participant's ACS
  * commitment processor cannot progress due to resource utilization or ACS commitment corruption.
  *
  * =ACS commitment re-initialization runbook=
  *
  * Step 1: Disconnect the participant from the synchronizer:
  *   - Canton console:
  *     {{{
  *     participant1.synchronizers.disconnect_all()
  *     }}}
  *
  * Step 2: Stop the participant node.
  *
  * Step 3: Get the current synchronizer index and checkpoint time by inspecting the query below:
  *   - PSQL:
  *     {{{
  *     SELECT * FROM par_commitment_checkpoint_snapshot_time;
  *     }}}
  *
  * Step 4: Drop the commitment checkpoints:
  *   - PSQL:
  *     {{{
  *     DELETE FROM par_commitment_checkpoint_snapshot WHERE synchronizer_idx = <synchronizer_idx>;
  *     }}}
  *     (use the synchronizer index from step 3)
  *
  * Step 5: Find the latest synchronizer record time:
  *   - PSQL:
  *     {{{
  *     SELECT le.synchronizer_id,
  *            si.external_string AS physical_synchronizer_id,
  *            le.record_time
  *     FROM lapi_ledger_end_synchronizer_index le
  *     JOIN lapi_string_interning si ON si.internal_id = le.synchronizer_id;
  *     }}}
  *
  * Step 6: Update the ACS commitment checkpoint time to the latest record time as observed by the
  * participant (from step 5):
  *   - PSQL:
  *     {{{
  *     UPDATE par_commitment_checkpoint_snapshot_time SET ts = <record_time> WHERE synchronizer_idx = <synchronizer_idx>;
  *     }}}
  *     (use the synchronizer index from step 3 and record time from step 5)
  *
  * Step 7: Start the participant node.
  *
  * Step 8: Re-connect the synchronizers:
  *   - Canton console:
  *     {{{
  *     participant1.synchronizers.reconnect_all()
  *     }}}
  *   - Verify connection:
  *     {{{
  *     participant1.synchronizers.list_connected()
  *     }}}
  *
  * Step 9: Re-initialize ACS commitments:
  *   - Canton console:
  *     {{{
  *     participant1.commitments.reinitialize_commitments(Seq.empty, Seq.empty, Seq.empty, config.NonNegativeDuration.ofSeconds(36000))
  *     }}}
  *   - Note: For the duration of the re-init operation, you will potentially observe
  *     ACS_COMMITMENT_MISMATCH messages. After the operation is done, mismatch warnings might
  *     continue for a while, but only for commitment periods predating the reinitialization
  *     timestamp. No commitment mismatch failures should appear with periods spanning after the
  *     re-initialization timestamp.
  *
  * Step 10: Restart participant node and execute a ping to check all works fine:
  *   - Restart the participant node.
  *   - Canton console:
  *     {{{
  *     participant1.synchronizers.reconnect_all()
  *     participant1.health.ping(participant1)
  *     }}}
  *   - Follow that the commitments are exchanged correctly in logs (every 30 minutes as that is the
  *     reconciliation interval on the global synchronizer). Additionally, check that the
  *     checkpoints advanced compared to the original query result in step 3 above.
  *   - Logs to check after re-init:
  *     - "Initialized from stored snapshot at RecordTime.."
  *     - "Looking for ACS changes to replay between [timestamp] and TimeOfChange(...) in batches of
  *       ..." (should have roughly 0 ACS changes)
  *     - "Persisted ACS commitments.." (check record time should be close to the present)
  *     - "DEBUG Task 'reinitialize running commitments.*finished completed"
  *   - Logs to check during steady state:
  *     - "Computed and stored.." (new commitments due to newly-ingested updates)
  *
  * *Note*: The test below showcases the runbook above, but with shell commands (e.g. psql) wherever
  * remote Console commands were not possible. Steps 2 and 7 - stopping and starting the participant
  * node are included in the runbook as best practices but excluded in the test.
  */
trait AcsCommitmentForceReinitIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SortedReconciliationIntervalsHelpers
    with CommitmentTestUtil {

  private val interval: JDuration = JDuration.ofSeconds(5)
  private implicit val intervalDuration: IntervalDuration = IntervalDuration(interval)

  private var alreadyDeployedContracts: Seq[Iou.Contract] = Seq.empty

  private lazy val maxDedupDuration = java.time.Duration.ofHours(1)

  private var p2LastComputed: RecordTime = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.enableAdditionalConsistencyChecks)
            .replace(true)
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

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.foreach { p =>
          p.dars.upload(CantonExamplesPath, synchronizerId = daId)
        }
        passTopologyRegistrationTimeout()
      }

  def createContractsAndCheck(sequencer: LocalSequencerReference, synchronizerId: SynchronizerId)(
      implicit env: FixtureParam
  ): (Seq[Iou.Contract], CommitmentPeriod, AcsCommitment.HashedCommitmentType) = {
    import env.*
    val nContracts = PositiveInt.three
    val simClock = environment.simClock.value

    val initialTimestamp =
      sequencer.underlying.value.sequencer.timeTracker.fetchTime().futureValueUS

    val createdCids =
      (1 to nContracts.value).map(_ =>
        deployOnTwoParticipantsAndCheckContract(
          synchronizerId,
          participant1,
          participant2,
        )
      )

    // bump the time past the next reconciliation interval
    val tick1 = tickAfter(initialTimestamp.immediateSuccessor)
    simClock.advanceTo(tick1.forgetRefinement.immediateSuccessor)
    val now = simClock.now

    val p1Computed = eventually() {
      logger.debug(
        s"Awaiting sequencer time tracker to reach ${tick1.forgetRefinement.immediateSuccessor} for commitment computation"
      )
      // make sure sequencer has reached the reconciliation interval
      sequencers.local.foreach(
        _.underlying.value.sequencer.timeTracker
          .awaitTick(tick1.forgetRefinement.immediateSuccessor)
          .foreach(
            _.futureValue
          )
      )
      // and the participant observes it as well, so that the commitments are computed
      participant1.testing.fetch_synchronizer_times()

      val p1Computed = participant1.commitments
        .computed(
          daName,
          initialTimestamp.toInstant,
          now.toInstant,
          Some(participant2),
        )

      p1Computed.size shouldBe 1
      p1Computed
    }

    val (period, _participant, commitment) = p1Computed.loneElement
    alreadyDeployedContracts = alreadyDeployedContracts.concat(createdCids)
    (createdCids, period, commitment)
  }

  def checkAfterCreate(sequencer: LocalSequencerReference)(implicit
      env: FixtureParam
  ): (CommitmentPeriod, AcsCommitment.HashedCommitmentType) = {
    import env.*
    val simClock = environment.simClock.value

    val initialTimestamp =
      sequencer.underlying.value.sequencer.timeTracker.fetchTime().futureValueUS

    // bump the time past the next reconciliation interval
    val tick1 = tickAfter(initialTimestamp.immediateSuccessor)
    simClock.advanceTo(tick1.forgetRefinement.immediateSuccessor)
    val now = simClock.now

    val p1Computed = eventually() {
      logger.debug(
        s"Awaiting sequencer time tracker to reach ${tick1.forgetRefinement.immediateSuccessor} for commitment computation"
      )
      // make sure sequencer has reached the reconciliation interval
      sequencers.local.foreach(
        _.underlying.value.sequencer.timeTracker
          .awaitTick(tick1.forgetRefinement.immediateSuccessor)
          .foreach(
            _.futureValue
          )
      )
      // and the participant observes it as well, so that the commitments are computed
      participant1.testing.fetch_synchronizer_times()

      val p1Computed = participant1.commitments
        .computed(
          daName,
          initialTimestamp.toInstant,
          now.toInstant,
          Some(participant2),
        )

      p1Computed.size shouldBe 1
      p1Computed
    }

    val (period, _participant, commitment) = p1Computed.loneElement
    (period, commitment)
  }

  "Running commitment reinitialization" should {
    "reinitialize commitments cleanly without loading checkpoints or ACS changes" in {
      implicit env =>
        import env.*

        val simClock = environment.simClock.value

        val now = simClock.now
        // make sure sequencer time is close to the sim clock time from the beginning
        sequencers.local.foreach(
          _.underlying.value.sequencer.timeTracker.awaitTick(now).foreach(_.futureValue)
        )

        // make sure participants have observed the latest time before starting
        participants.all.foreach { p =>
          p.testing.fetch_synchronizer_times()
        }

        // Deploy three contracts. P1 and P2 exchange commitments
        createContractsAndCheck(sequencer1, daId)

        val ts = simClock.now
        // da might not have progressed time when using BFTOrderer (since with BFT Time, the time of a block is
        // decided by the previous block). So we make sure the sequencers have observed this time.
        sequencers.local.foreach { s =>
          TestUtils.waitForTargetTimeOnSequencer(s, ts, logger)
        }

        // Wait a bit until everything quiets down. This ensures that there's a high chance that the reinitialization happens
        // when no further changes are queued that could move ledger end afterwards (in particular incoming ACS commitments).
        Threading.sleep(2000)

        // exchange commitments, all should be fine
        val (_, period1, _) = createContractsAndCheck(sequencer1, daId)

        eventually() {
          participant2.commitments.lastComputedAndSent(daName) should contain(period1.toInclusive)
        }

        deployOnTwoParticipantsAndCheckContract(
          daId,
          participant1,
          participant2,
        )

        // RUNBOOK: Step 1: Disconnect the participant from the synchronizer
        participant2.synchronizers.disconnect_all()
        eventually() {
          participant2.synchronizers.list_connected() shouldBe empty
        }

        checkAfterCreate(sequencer1)

        // RUNBOOK: Steps 3 and 4: Drop the commitment checkpoint tables
        val incrementalCommitmentStoreP2 =
          participant2.underlying.value.sync.syncPersistentStateManager
            .acsCommitmentStore(daId)
            .value
            .runningCommitments
        incrementalCommitmentStoreP2
          .get()
          .flatMap { case (recordTime, _runningCmts) =>
            p2LastComputed = recordTime
            incrementalCommitmentStoreP2.forgetCheckpoints()
          }
          .futureValueUS

        // RUNBOOK: Steps 5 and 6: Update the commitment checkpoint to the latest record time
        val recentCheckpoint = participant2.underlying.value.sync.stateInspection
          .lastSynchronizerOffset(daId)
          .value
          .recordTime
          .pipe(timestamp => RecordTime(CantonTimestamp(timestamp), 0))

        assert(recentCheckpoint.timestamp.compareTo(p2LastComputed.timestamp) > 0)
        incrementalCommitmentStoreP2
          .get()
          .flatMap { case (_recordTime, _runningCmts) =>
            incrementalCommitmentStoreP2.update(
              recentCheckpoint,
              updates = Map.empty,
              deletes = Set.empty,
              UpdateMode.Checkpoint,
            )
          }
          .futureValueUS

        // catch warnings about commitment mismatch and inconsistencies between running commitments and ACS
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            // RUNBOOK: Step 8: Re-connect the participant to the synchronizer
            participant2.synchronizers.reconnect_all()
            eventually() {
              participant2.synchronizers
                .list_connected()
                .map(_.physicalSynchronizerId) should contain(daId)
            }

            // exchange commitments
            val (_, period2, _) = createContractsAndCheck(sequencer1, daId)
            eventually() {
              val p1Received = participant1.commitments.lookup_received_acs_commitments(
                synchronizerTimeRanges = Seq(
                  SynchronizerTimeRange(
                    daId,
                    Some(
                      TimeRange(
                        period2.fromExclusive.forgetRefinement,
                        period2.toInclusive.forgetRefinement,
                      )
                    ),
                  )
                ),
                counterParticipants = Seq.empty,
                commitmentState = Seq(Mismatch),
                verboseMode = false,
              )

              val daCmtsP1 = p1Received.get(daId).value
              daCmtsP1.size shouldBe 1

              val p2Received = participant2.commitments.lookup_received_acs_commitments(
                synchronizerTimeRanges = Seq(
                  SynchronizerTimeRange(
                    daId,
                    Some(
                      TimeRange(
                        period2.fromExclusive.forgetRefinement,
                        period2.toInclusive.forgetRefinement,
                      )
                    ),
                  )
                ),
                counterParticipants = Seq.empty,
                commitmentState = Seq(Mismatch),
                verboseMode = false,
              )

              val daCmtsP2 = p2Received.get(daId).value
              daCmtsP2.size shouldBe (1)
            }

            logger.debug(
              s"Repair P2's commitments on $daId by reinitializing them based on the ACS"
            )
            // We expect that the running commitments are repaired, and there are no more inconsistencies or commitments mismatches
            // RUNBOOK: Step 9: Re-initialize ACS commitments
            val reinitCmtsResult2 =
              participant2.commitments.reinitialize_commitments(
                Seq(daId),
                Seq.empty,
                Seq.empty,
                NonNegativeDuration.ofSeconds(30),
              )
            // results should be for daId, and the timestamp should be defined

            reinitCmtsResult2.map(_.synchronizerId) should contain theSameElementsAs Seq(
              daId.logical
            )
            forAll(reinitCmtsResult2)(_.acsTimestamp.isDefined shouldBe true)
            forAll(reinitCmtsResult2)(_.acsTimestamp.value shouldBe >=(recentCheckpoint.timestamp))
          },
          logs => {
            forAtLeast(1, logs) { m =>
              m.message should include(CommitmentsMismatch.id)
            }
            forAtLeast(1, logs) { m =>
              m.message should include(
                "Detected an inconsistency between the running commitment and the ACS"
              )
            }
          },
        )

        def checkMatch(period: CommitmentPeriod) = {
          val timeRange = SynchronizerTimeRange(
            daId,
            Some(
              TimeRange(
                period.fromExclusive.forgetRefinement,
                period.toInclusive.forgetRefinement,
              )
            ),
          )
          Seq(participant1, participant2).foreach { participant =>
            withClue(s"For participant ${participant.name} in period $period:") {
              eventually() {
                val received = participant.commitments.lookup_received_acs_commitments(
                  synchronizerTimeRanges = Seq(timeRange),
                  counterParticipants = Seq.empty,
                  commitmentState = Seq.empty,
                  verboseMode = false,
                )

                val daCmts = received.get(daId).value
                daCmts.size shouldBe 1
                daCmts(0).state shouldBe Match
              }
            }
          }
        }

        logger.debug("Check that the commitments match again")
        val (_, period4da, _) = createContractsAndCheck(sequencer1, daId)
        checkMatch(period4da)

        logger.debug("Restart to verify that the repair survives a crash")
        // The in-memory running commitments produced by the reinitialization could gloss over
        // the bogus entry in the checkpoint table. We therefore restart the participant and
        // check again.
        // RUNBOOK: Step 10: Restart participant node and execute a ping to check all works fine
        participant2.stop()
        participant2.start()
        participant2.synchronizers.reconnect_all()
        eventually() {
          participant2.synchronizers
            .list_connected()
            .map(_.physicalSynchronizerId) should contain(daId)
        }

        val (_, period5da, _) = createContractsAndCheck(sequencer1, daId)
        checkMatch(period5da)
    }
  }
}

class AcsCommitmentForceReinitIntegrationTestPostgres2
    extends AcsCommitmentForceReinitIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1"))
        )
      ),
    )
  )
}

class AcsCommitmentForceReinitIntegrationTestH2 extends AcsCommitmentForceReinitIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1"))
        )
      ),
    )
  )

}
