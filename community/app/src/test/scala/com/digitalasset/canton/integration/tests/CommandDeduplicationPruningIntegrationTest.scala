// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import cats.syntax.option.*
import com.digitalasset.canton.config
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference}
import com.digitalasset.canton.data.{CantonTimestamp, DeduplicationPeriod, Offset}
import com.digitalasset.canton.examples.java.cycle as C
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.grpc.PruningServiceError.UnsafeToPrune
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.util.ShowUtil.*

import DeduplicationPeriod.{DeduplicationDuration, DeduplicationOffset}

/** Test setup:
  *   - Single participant connected to a BFT sequencer with Postgres storage.
  *   - The maximum deduplication duration is configured to 1 hour, and ACS commitments are set to a
  *     short interval (1s) to ensure they do not artificially block pruning.
  *
  * Test goal: Verify the interaction between command deduplication and ledger pruning. It
  * specifically ensures that:
  *   1. The system actively prevents pruning within the max deduplication duration to preserve
  *      necessary deduplication state.
  *   1. Duplicate command submissions are correctly caught and rejected.
  *   1. Commands specifying a deduplication period or offset that overlaps with already pruned data
  *      are correctly rejected with a FAILED_PRECONDITION error.
  *
  * Note: There is an interdependence between querying for the safe pruning offset and how far the
  * ACS commitment processor has caught up. If the `simClock` time is advanced by a large amount,
  * the ACS commitment processor may lag behind in resource-constrained environments (like CI). To
  * avoid flaky tests, always ensure the test waits for `noOutstandingCommitmentsTs` to advance
  * before asserting on the expected safe pruning offset.
  */
class CommandDeduplicationPruningIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with CommandDeduplicationTestHelpers {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  lazy val maxDedupDuration = java.time.Duration.ofHours(1)

  private val reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxDedupDuration),
      )
      .withSetup { implicit env =>
        import env.*

        // Make sure that ACS commitments do not block pruning
        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = reconciliationInterval),
            )
        )

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)

        participant1.parties.testing.enable("Alice")
      }

  private def findSafeOffset(
      testClue: String,
      participant: ParticipantReference,
      minExpectedOffset: Long,
  ): Long =
    clue(testClue) {
      eventually() {
        val safe = participant.pruning.find_safe_offset().value
        safe should be >= minExpectedOffset
        safe
      }
    }

  "block pruning for the max deduplication duration" in
    WithContext { (alice, simClock) => implicit env =>
      import env.*

      val createCycleContract =
        new C.Cycle(
          "Command-Dedup-Contract-Pruning",
          alice.toProtoPrimitive,
        ).create.commands.loneElement
      val commandId1 = "warm-up"
      val commandId2 = "at-max-dedup-boundary"
      val commandId3 = "yet-another-commandId"
      val commandId4DuplicateRejected = commandId2
      val commandId5OutsideDedupBoundaryAccepted = commandId2
      val commandId6DeduplicationPeriodAccepted = "fresh-command-id"
      val commandId7DeduplicationPeriodTooLong = "command-id-too-long-period"

      def submit(
          commandId: String,
          dedupPeriod: DeduplicationPeriod = DeduplicationDuration(java.time.Duration.ofMinutes(1)),
          submissionId: String = "",
      ): Long =
        participant1.ledger_api.javaapi.commands
          .submit(
            Seq(alice),
            Seq(createCycleContract),
            synchronizerId = Some(daId), // Run on DA synchronizer
            commandId = commandId,
            submissionId = submissionId,
            deduplicationPeriod = dedupPeriod.some,
          )
          .getOffset

      def submitAsync(
          commandId: String,
          dedupPeriod: DeduplicationPeriod,
          submissionId: String,
      ): Unit =
        participant1.ledger_api.javaapi.commands.submit_async(
          Seq(alice),
          Seq(createCycleContract),
          synchronizerId = Some(daId), // Run on DA synchronizer
          commandId = commandId,
          submissionId = submissionId,
          deduplicationPeriod = dedupPeriod.some,
        )

      // Send three transactions at t, t+1ms, and t+1ms+max_dedup_duration
      val after1 = submit(commandId1)
      simClock.advance(java.time.Duration.ofMillis(1))
      val before2 = simClock.now
      val after2 = submit(commandId2)
      simClock.advance(maxDedupDuration)
      val after3 = submit(commandId3)

      // Flake prevention: Wait for ACS commitments to be processed first before attempting to query the safe prune offset.
      eventually() {
        val noOutstandingCommitmentsO = participant1.testing.state_inspection
          .noOutstandingCommitmentsTs(daName, CantonTimestamp.MaxValue)
        logger.debug(s"No outstanding commitment at $noOutstandingCommitmentsO")
        noOutstandingCommitmentsO.value should be > before2
      }

      // We must not prune anything published within the max deduplication duration,
      // so we can prune only up to the first transaction
      val safe1 = findSafeOffset("safe1", participant1, after1)

      // Test that we get a meaningful error message if we try to prune with a too high offset
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.pruning.prune(after2),
        logEntry => {
          logEntry.errorMessage should include(UnsafeToPrune.id)
          logEntry.errorMessage should include(
            show"due to max deduplication duration of $maxDedupDuration"
          )
          logEntry.errorMessage should include(s"safe_offset=>$safe1")
        },
      )

      participant1.pruning.prune(after1)

      logger.debug("resubmitting second command")
      val submissionId = "resubmission"
      submitAsync(
        commandId4DuplicateRejected,
        DeduplicationDuration(maxDedupDuration),
        submissionId,
      )
      val (completion4, _offset4) =
        findCompletionFor(participant1, alice, after3, commandId2, submissionId)
      checkIsAlreadyExists(completion4, submissionId, after2)

      // Advance the time slightly so the participant's publication time moves higher.
      // This enables the participant to safely calculate a higher pruning offset.
      simClock.advance(java.time.Duration.ofMillis(1L))
      participant1.testing.fetch_synchronizer_time(daId)

      val safe2 = findSafeOffset("safe2", participant1, after2)
      participant1.pruning.prune(safe2)

      val after5 = submit(
        commandId5OutsideDedupBoundaryAccepted,
        DeduplicationDuration(maxDedupDuration),
      )
      val outsideDedupBoundaryTs =
        valueOrFail(
          participant1.testing.state_inspection
            .lookupPublicationTime(after5)
            .value
            .futureValueUS
        )(s"Failed to locate publication time")

      logger.debug("submit fresh command with long dedup duration")
      simClock.advanceTo(
        before2.plus(
          maxDedupDuration
            .multipliedBy(2)
            // +1 millisecond to account for the millisecond added above between command submissions 4 and 5:
            .plusMillis(1)
        )
      )

      // Required to avoid ABORTED/LOCAL_VERDICT_TIMEOUT: Rejected transaction due to a participant determined timeout
      // In particular: Time validation has failed: The delta of the ledger time 1970-01-01T02:00:00.002Z and the record time 1970-01-01T01:00:00.011Z exceeds the max of 1m
      participant1.testing.fetch_synchronizer_time(daId)

      val after6 = submit(
        commandId6DeduplicationPeriodAccepted,
        DeduplicationDuration(maxDedupDuration.multipliedBy(2)),
      )

      logger.debug("submit command with too long dedup duration")
      val submissionIdTooLongDuration = "submission-id-too-long-duration"
      val dedupPeriod7 = DeduplicationDuration(maxDedupDuration.multipliedBy(2).plusMillis(1))
      submitAsync(
        commandId7DeduplicationPeriodTooLong,
        dedupPeriod7,
        submissionIdTooLongDuration,
      )
      val (completion7, offset7) = findCompletionFor(
        participant1,
        alice,
        after6,
        commandId7DeduplicationPeriodTooLong,
        submissionIdTooLongDuration,
      )
      checkDedupPeriodTooLong(completion7, submissionIdTooLongDuration, after2)

      // Fast-forward the simulated clock to strictly exceed the max deduplication duration
      // since after5 was published. This ensures that the event at after5 becomes eligible
      // for safe pruning at the end of the test.
      if (outsideDedupBoundaryTs.plus(maxDedupDuration) > simClock.now) {
        simClock.advanceTo(outsideDedupBoundaryTs.plus(maxDedupDuration))
      }

      // Max-deduplication-time checks are inclusive. Adding one millisecond ensures
      // that the event at after5 crosses the boundary and can be safely pruned.
      simClock.advance(java.time.Duration.ofMillis(1L))

      logger.debug("submit command with too early deduplication offset")
      val submissionIdTooEarlyOffset = "submission-id-too-early-offset"
      val dedupPeriod8 = DeduplicationOffset(Some(Offset.tryFromLong(after1)))
      submitAsync(
        commandId7DeduplicationPeriodTooLong,
        dedupPeriod8,
        submissionIdTooEarlyOffset,
      )
      val (completion8, _offset8) = findCompletionFor(
        participant1,
        alice,
        offset7,
        commandId7DeduplicationPeriodTooLong,
        submissionIdTooEarlyOffset,
      )
      checkDedupPeriodTooLong(completion8, submissionIdTooEarlyOffset, after2)

      logger.debug("submit command with participant begin deduplication offset")
      val submissionIdParticipantBegin = "submission-id-participant-begin"
      val dedupPeriod9 = DeduplicationOffset(None)
      submitAsync(
        commandId7DeduplicationPeriodTooLong,
        dedupPeriod9,
        submissionIdParticipantBegin,
      )
      val (completion9, _offset9) = findCompletionFor(
        participant1,
        alice,
        offset7,
        commandId7DeduplicationPeriodTooLong,
        submissionIdParticipantBegin,
      )
      checkDedupPeriodTooLong(completion9, submissionIdParticipantBegin, after2)

      val safe5 = findSafeOffset("safe5", participant1, after5)
      participant1.pruning.prune(safe5)
    }
}
