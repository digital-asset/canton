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
import com.digitalasset.canton.version.ProtocolVersion

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
      onboarding = testedProtocolVersion >= ProtocolVersion.v35,
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
  * exercising the implemented safeguards from
  * [[https://github.com/DACH-NY/canton/pull/29539 Enhance debug logging for getHighestOffsetByTimestamp (#29121)]]
  *
  * This test ensures that `find_highest_offset_by_timestamp` correctly handles future timestamps,
  * strictly validates exact record times when not forced, and remains resilient against cache lag.
  *
  * The test proves the following safeguards are active:
  *
  *   1. Safeguard 1 (Future Timestamp + Force): When queried with a timestamp in the future (i.e.,
  *      the `cleanSynchronizerIndex` has not yet reached `t1`) and `force = true`, the endpoint
  *      gracefully and intentionally returns the current, stable `ledgerEnd` from a consistent
  *      database snapshot, rather than failing or returning an unpredictable offset.
  *
  *   1. Safeguard 2 (Exact Timestamp Match): When queried with a timestamp in the past (`t1`) that
  *      has no exact event, the underlying DB query naturally falls back to the prior offset at
  *      `t0`. If `force = false`, the endpoint detects this mismatch (`t0 != t1`) and explicitly
  *      throws an `INVALID_STATE_PARTY_MANAGEMENT_ERROR` to prevent silently returning an older
  *      offset.
  *
  *   1. Safeguard 3 (Disaster Recovery Override): Exercises the same scenario as Safeguard 2, but
  *      with `force = true`. It verifies that the exact timestamp validation is bypassed, allowing
  *      the system to safely return the prior offset for disaster recovery purposes.
  *
  * Note: By asserting on a stable `ledgerEnd` in the first safeguard, this test also inherently
  * exercises the endpoint's cache synchronization barrier, ensuring the returned persistent offset
  * is fully observable in the in-memory `ledgerEndCache` before the command returns.
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

    // First Safeguard: Indexer has NOT reached T1 yet.
    // By forcing the query, the endpoint intentionally returns the stable ledger-end
    // instead of failing or returning an unpredictable offset.
    clue("First Safeguard") {
      eventually() {
        val ledgerEnd = source.ledger_api.state.end()
        val result = source.parties.find_highest_offset_by_timestamp(daId, t1, force = true)
        // ensuring stable ledgerEnd first
        ledgerEnd shouldBe source.ledger_api.state.end()
        // with stable ledgerEnd we expect to see the result being the same
        result shouldBe ledgerEnd
      }
    }

    // Advance clock past T1 to T2, and create a new event
    clock.advance(Duration.ofSeconds(10))
    source.health.ping(source)

    // Let indexer catch up to T2
    clock.advance(Duration.ofSeconds(1))

    // Second Safeguard: Indexer is now past T1, but there was no exact event at T1.
    // The DB query falls back to T0's offset, but the strict validation catches
    // the mismatch (T0 != T1) and throws an error to prevent returning a stale offset.
    clue("Second Safeguard") {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        source.parties.find_highest_offset_by_timestamp(daId, t1, force = false),
        logEntry => {
          logEntry.errorMessage should include("INVALID_STATE_PARTY_MANAGEMENT_ERROR")
          logEntry.errorMessage should include regex
            s"Timestamp mismatch: requested=$t1 != recordTime=.*\\. \\(Context: offset=\\d+\\)"
        },
      )
    }

    // Third Safeguard: Disaster Recovery Override.
    // Assert that the exact timestamp validation is bypassed when forced. It knowingly accepts
    // and returns the prior offset (T0) even though the record time does not match T1.
    clue("Third Safeguard")(
      source.parties.find_highest_offset_by_timestamp(daId, t1, force = true) should be > 0L
    )
  }
}
