// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.InvalidTimestamp
import com.digitalasset.canton.topology.transaction.ParticipantPermission

import java.time.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala

/** Intention:
  *   - Model the party replication as CN is currently doing to assert/investigate (see #29121)
  *   - Follow the recommended party replication flow as closely as possibly until the
  *     find_highest_offset_by_timestamp endpoint is used
  *
  * Setup:
  *   - Alice is hosted on participant1 (Source)
  *   - Bob is hosted on participant2 (Target)
  *   - 2 active IOU contract between Alice (signatory) and Bob (observer)
  *
  * Test: Activate Alice on target
  *   - Target participant authorizes Alice->target
  *   - Target participant disconnects from the synchronizer
  *   - A creation transaction to create a contract with Alice as signatory and Bob as observer is
  *     sequenced
  *   - Source participant approves/confirms transaction
  *   - Source participant authorizes Alice->target
  *   - Find the offset for the ACS export using the validForm timestamp of the effective topology
  *     transaction
  *   - Assert that the found offset for the given timestamp is as expected
  */
final class HighestOffsetByTimestampIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.withSetup { implicit env =>
      source = env.participant1
      target = env.participant2
    }

  "Party replication using the find highest offset by timestamp endpoint" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value

    // Advance the clock cleanly away from the epoch to prevent mediator skew warnings
    clock.advance(Duration.ofSeconds(5))

    IouSyntax.createIou(source)(alice, bob, 3.33)

    target.topology.party_to_participant_mappings
      .propose_delta(
        party = alice,
        adds = Seq(target.id -> ParticipantPermission.Submission),
        store = daId,
        requiresPartyToBeOnboarded = true,
      )

    target.synchronizers.disconnect_all()

    val createIouCmd = testIou(alice, bob, 2.20).create().commands().asScala.toSeq

    source.ledger_api.javaapi.commands.submit(
      Seq(alice),
      createIouCmd,
      daId,
      optTimeout = None,
    )

    val sourceLedgerEnd = source.ledger_api.state.end()

    alice.topology.party_to_participant_mappings.propose_delta(
      node = source,
      adds = Seq(target.id -> ParticipantPermission.Submission),
      store = daId,
      requiresPartyToBeOnboarded = true,
    )

    val effectiveTimestamp = eventually() {
      // Keep time ticking slowly while polling so the topology transaction can be sequenced
      clock.advance(Duration.ofMillis(100L))
      getOnboardingEffectiveAt(daId)
    }

    // Generate a ledger event and advance the clock so the sequencer processes past effectiveTimestamp.
    // We ping `source` since `target` was disconnected from all synchronizers above.
    source.health.ping(source)
    clock.advance(Duration.ofMillis(500L))

    val offsetByTimestamp =
      source.parties.find_highest_offset_by_timestamp(daId, effectiveTimestamp.toInstant)

    val maxActivationOffset = source.parties.find_party_max_activation_offset(
      alice,
      target,
      daId,
      Some(effectiveTimestamp.toInstant),
      beginOffsetExclusive = sourceLedgerEnd,
      completeAfter = PositiveInt.one,
    )

    offsetByTimestamp shouldBe maxActivationOffset
  }
}

final class HighestOffsetByTimestampForceIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.withSetup { implicit env =>
      source = env.participant1
      target = env.participant3

      IouSyntax.createIou(source)(alice, bob, 1.95)
      IouSyntax.createIou(source)(alice, bob, 2.95)
      IouSyntax.createIou(source)(alice, bob, 3.95)
      IouSyntax.createIou(source)(alice, bob, 4.95)
      IouSyntax.createIou(source)(alice, bob, 5.95)
    }

  "Find ledger offset by timestamp can be forced, but not return a larger ledger offset with subsequent transactions" in {
    implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      source.ledger_api.state.acs.of_party(alice) should have size 5

      // Advance the clock cleanly away from the epoch to prevent mediator skew warnings
      clock.advance(Duration.ofSeconds(5))
      val requestedTimestamp = clock.now.toInstant

      // Generate an event on the ledger
      source.health.ping(target)

      // Advance the clock so the sequencer can process the ping
      // and move its internal record time past the requestedTimestamp
      clock.advance(Duration.ofMillis(500))

      val startLedgerEndOffset = source.ledger_api.state.end()

      // Transaction that happens after the requested timestamp
      IouSyntax.createIou(source)(alice, bob, 99.95)

      val foundOffset = loggerFactory.assertLogsUnorderedOptional(
        eventually(retryOnTestFailuresOnly = false) {
          // Keep time ticking slowly while polling
          clock.advance(Duration.ofMillis(100L))

          val offset =
            source.parties.find_highest_offset_by_timestamp(daId, requestedTimestamp, force = true)
          offset should be > 0L
          offset
        },
        (
          LogEntryOptionality.OptionalMany,
          _.shouldBeCantonErrorCode(InvalidTimestamp),
        ),
      )

      // Another transaction that happens after the requested timestamp and having called the end point
      IouSyntax.createIou(source)(alice, bob, 1111.95)

      val forcedFoundOffset = source.parties
        .find_highest_offset_by_timestamp(daId, requestedTimestamp, force = true)

      // The following cannot check for equality because find_highest_offset_by_timestamp skips over unpersisted
      // SequencerIndexMoved updates.
      forcedFoundOffset should be <= startLedgerEndOffset
      foundOffset should be < startLedgerEndOffset
      foundOffset shouldBe forcedFoundOffset
  }

}

/** Test motivated by
  * [[https://github.com/DACH-NY/canton/issues/29121 GetHighestOffsetByTimestamp fails to return the expected offset for a given timestamp #29121]]
  * exercising thereafter implemented safeguards
  * [[https://github.com/DACH-NY/canton/pull/29539 Enhance debug logging for getHighestOffsetByTimestamp (#29121)]]
  *
  * Ensures that `find_highest_offset_by_timestamp` strictly validates the exact recordTime of the
  * found offset, protecting against returning a prior, mismatched offset instead of the requested
  * one.
  *
  * At its core, this addresses two scenarios where the underlying database query (`<= t1`) might
  * return a prior offset `o0` (at `t0`) when queried for `t1`:
  *   1. Hypothetical Race Condition (historical bug, #29121): The Indexer updates the database
  *      watermark (`cleanSynchronizerIndex`) and the in-memory `ledgerEndCache` asynchronously via
  *      different thread pools. There is a microscopic window where the watermark allows a query
  *      for `t1`, but the lagging in-memory cache artificially truncates the database read (`WHERE
  *      offset <= ledgerEnd`). This blinds the query to the newly written `t1` events, forcing the
  *      DB to incorrectly fall back to the prior event `o0`.
  *   1. Empty Timestamp (expected behavior): The system is fully synchronized, but the client
  *      queries a `t1` that simply has no events. The `<=` query naturally falls back to the prior
  *      offset `o0`.
  *
  * This test proves the fixes for these issues are active:
  *   - Safeguard 1 (Synchronizer Lag): If queried while the indexer is lagging, the endpoint
  *     detects the `cleanSynchronizerIndex` has not reached `t1` and refuses to answer.
  *   - Safeguard 2 (Exact Timestamp Match): This test deterministically exercises Scenario 2 (Empty
  *     Timestamp). By querying a `t1` with no events, the DB predictably finds `o0` at `t0`. The
  *     endpoint now explicitly checks that the found record time (`t0`) exactly matches the
  *     requested time (`t1`). Since `t0 != t1`, it throws a "Timestamp mismatch" error, completely
  *     preventing the return of the prior offset. This same strict validation identically protects
  *     against the cache-truncation race condition (Scenario 1) in production.
  */
final class HighestOffsetByTimestampSafeguardsIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.withSetup { implicit env =>
      source = env.participant1
      target = env.participant2
    }

  "The endpoint protects against race conditions and stale offsets" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value // = 1970-01-01T00:00:00Z

    // Create an event at T0
    clock.advance(Duration.ofSeconds(5))
    source.health.ping(source)

    // Let indexer catch up to T0 events
    clock.advance(Duration.ofSeconds(1))

    // Define T1 (a timestamp slightly in the future) = 1970-01-01T00:00:11Z
    val t1 = clock.now.plus(Duration.ofSeconds(5)).toInstant

    // First Safeguard: Indexer has NOT reached T1 yet
    // If we force the query, it detects the synchronizer is lagging and aborts
    // rather than returning the stale T0 offset.
    clue("First Safeguard") {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        source.parties.find_highest_offset_by_timestamp(daId, t1, force = true),
        logEntry => {
          logEntry.errorMessage should include("INVALID_STATE_PARTY_MANAGEMENT_ERROR")
          logEntry.errorMessage should include regex
            s"Synchronizer offset not found for offset \\d+ as determined by the requested timestamp $t1\\."
        },
      )
    }

    // Advance clock past T1 to T2, and create a new event
    clock.advance(Duration.ofSeconds(10))
    source.health.ping(source)

    // Let indexer catch up to T2
    clock.advance(Duration.ofSeconds(1))

    // Second Safeguard: Indexer is now past T1, but there was no exact event at T1
    // The DB query finds T0's offset, but the new strict validation catches
    // the mismatch (T0 != T1) and throws an error instead of returning stale offset.
    clue("Second Safeguard") {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        source.parties.find_highest_offset_by_timestamp(daId, t1, force = false),
        logEntry => {
          logEntry.errorMessage should include("INVALID_STATE_PARTY_MANAGEMENT_ERROR")
          logEntry.errorMessage should include regex
            s"Timestamp mismatch: requested=$t1 != recordTime=.*\\. \\(Context: offset=\\d+ -> synchronizer offset=\\d+\\)"

        },
      )
    }

    // Assert that an offset result is forceable without any error, knowingly accepting the returned offset
    // belongs a different timestamp (record time) than requested (this is required for disaster recovery).
    source.parties.find_highest_offset_by_timestamp(daId, t1, force = true) should be > 0L
  }
}
