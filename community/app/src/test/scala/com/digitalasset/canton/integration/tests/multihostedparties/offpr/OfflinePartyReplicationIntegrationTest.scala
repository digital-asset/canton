// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.digitalasset.canton.admin.api.client.data.{FlagNotSet, FlagSet}
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.HasSimClockUtils.advanceClockInStepsTo
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.examples.IouSyntax.testIou
import com.digitalasset.canton.integration.tests.multihostedparties.hypr.PartyHostingStateTestHelpers.{
  awaitHostingStates,
  freezeParty,
  proposeAsParticipant,
  proposeAsParty,
}
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.participant.admin.party.PartyManagementServiceError.{
  AcsExportMissingTargetOnboardingMapping,
  EffectivePartyToParticipantMappingNotFound,
  OnboardingClearanceMissingMapping,
}
import com.digitalasset.canton.participant.protocol.TransactionProcessor
import com.digitalasset.canton.time.{DelegatingSimClock, PositiveFiniteDuration}
import com.digitalasset.canton.topology.transaction.HostingParticipant.HostingState
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{Party, PartyKind, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{HasExecutionContext, HasTempDirectory, SynchronizerAlias, config}

import java.time.Duration
import scala.jdk.CollectionConverters.CollectionHasAsScala

trait OfflinePartyReplicationIntegrationTestBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasTempDirectory
    with HasExecutionContext {

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // Alice's replication to the target participant may trigger ACS commitment mismatch warnings.
  // This is expected behavior. To reduce the frequency of these warnings and avoid associated
  // test flakes, `reconciliationInterval` is set to ten years.
  protected val reconciliationInterval: config.PositiveDurationSeconds =
    config.PositiveDurationSeconds.ofDays(10 * 365)

  protected var source: LocalParticipantReference = _
  protected var target: LocalParticipantReference = _
  protected var alice: Party = _
  protected var bob: Party = _

  // As an external party you actually do not want the participant to have the permission to submit (for you).
  // Therefore, resolve permissions based on the active `PartyKind`.
  protected def targetPermission: ParticipantPermission = partiesKind match {
    case PartyKind.External(_) => ParticipantPermission.Confirmation
    case PartyKind.Local => ParticipantPermission.Submission
  }

  // Determine if the current party type has submission rights on the target
  protected def canSubmitOnTarget: Boolean = partiesKind match {
    case PartyKind.External(_) => false
    case PartyKind.Local => true
  }

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(ConfigTransforms.useTestingTimeService),
      )
      .withSetup { implicit env =>
        import env.*
        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.local.dars.upload(CantonExamplesPath)
        sequencer1.topology.synchronizer_parameters
          .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval))

        alice =
          participant1.parties.testing.enable("Alice", synchronizeParticipants = Seq(participant2))
        bob =
          participant2.parties.testing.enable("Bob", synchronizeParticipants = Seq(participant1))
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val acsSnapshot =
    tempDirectory.toTempFile("offline_party_replication_test_acs_snapshot.gz")

  protected[offpr] val acsSnapshotPath: String = acsSnapshot.toString

  protected[this] def assertAcsAndContinuedOperation(
      participant: LocalParticipantReference,
      expectedNumActiveContracts: Long,
      numOfContractCreations: Int = 1,
  ): Unit = {
    participant.ledger_api.state.acs
      .active_contracts_of_party(alice) should have size expectedNumActiveContracts

    // Archive contract and create another one to assert regular operation after the completed party replication.
    // Note: This is skipped for External parties on the target participant as they lack submission permission.
    if (canSubmitOnTarget || participant == source) {
      val iou = participant.ledger_api.javaapi.state.acs.filter(M.iou.Iou.COMPANION)(alice).head
      IouSyntax.archive(participant)(iou, alice)

      (1 to numOfContractCreations).foreach { num =>
        IouSyntax.createIou(participant)(alice, bob, s"${num}42.95".toDouble).discard
      }
    }
  }

  /** Gets the `validFrom` timestamp of the PTP transaction that onboarded the party to the target.
    */
  protected[offpr] def getOnboardingEffectiveAt(
      daId: PhysicalSynchronizerId
  ): CantonTimestamp =
    CantonTimestamp
      .fromInstant(
        source.topology.party_to_participant_mappings
          .list(
            synchronizerId = daId,
            filterParty = alice.filterString,
            filterParticipant = target.id.filterString,
          )
          .loneElement
          .context
          .validFrom
      )
      .value

  /** Adds the target to the party's hosts as Onboarding, with the target's and the party's
    * authorization.
    *
    * @return
    *   The source's ledger end before the target's entry becomes effective.
    */
  protected[offpr] def targetAuthorizesHosting(
      party: Party,
      synchronizerId: PhysicalSynchronizerId,
      disconnectTarget: Boolean,
  ): Long = {
    target.topology.party_to_participant_mappings
      .propose_delta(
        party = party.partyId,
        adds = Seq(target.id -> targetPermission),
        store = synchronizerId,
        requiresPartyToBeOnboarded = true,
      )

    if (disconnectTarget) {
      target.synchronizers.disconnect_all()
    }

    val sourceLedgerEnd = source.ledger_api.state.end()

    // Authorize the PTP update from the party
    party.topology.party_to_participant_mappings.propose_delta(
      source,
      adds = Seq(target.id -> targetPermission),
      store = synchronizerId,
      requiresPartyToBeOnboarded = true,
    )

    sourceLedgerEnd
  }

  /** Advances the simulated clock in chunks and pings to prevent static time drift.
    *
    * In Canton's static time mode, advancing the local clock too far at once causes the
    * participant's ledger time to drift ahead of the sequencer's record time. This will result in
    * warnings like so: "Time validation has failed: The delta of the ledger time .* and the record
    * time .* exceeds the max of 1 minute"
    *
    * The sequencer will reject requests if this drift exceeds its 1-minute tolerance. By stepping
    * time in 30-second chunks and sending a ping, we force the sequencer to process an event and
    * update its record time, keeping the nodes synchronized.
    *
    * @param clock
    *   The simulated clock to advance.
    * @param totalDuration
    *   The total duration to advance the time by.
    * @param sourceNode
    *   The participant initiating the ping to drag the sequencer time forward.
    * @param targetNode
    *   The target participant receiving the ping.
    */
  protected[offpr] def advanceTimeAndPing(
      clock: DelegatingSimClock,
      totalDuration: Duration,
      sourceNode: LocalParticipantReference,
      targetNode: LocalParticipantReference,
  ): Unit =
    advanceClockInStepsTo(
      clock,
      clock.now.plus(totalDuration),
      maxStep = PositiveFiniteDuration.tryOfSeconds(30),
    ) {
      sourceNode.health.ping(targetNode.id)
    }

  /** Reconnects to synchronizer and unilaterally clears the onboarding flag. */
  protected[offpr] def reconnectAndEnsureOnboardingClearance(
      clock: DelegatingSimClock,
      party: Party,
      synchronizerAlias: SynchronizerAlias,
      providedTargetLedgerEnd: Option[Long] = None,
  )(implicit env: TestConsoleEnvironment, traceContext: TraceContext): Unit = {
    import env.*

    // Advance time to allow topology transactions to be processed
    clock.advance(Duration.ofSeconds(30))

    val targetLedgerEnd = providedTargetLedgerEnd.getOrElse(target.ledger_api.state.end())
    val psid = getInitializedSynchronizer(synchronizerAlias).physicalSynchronizerId
    target.synchronizers.reconnect(synchronizerAlias)

    source.health.ping(target)

    // Total duration needed for the asynchronous onboarding flag clearance to complete.
    // Depends on the historical decision timeout and of topology transaction actually removing the onboarding flag
    // becoming effective.
    val totalWaitTime = Duration.ofMinutes(2).plusSeconds(10)
    advanceTimeAndPing(clock, totalWaitTime, source, target)

    // Force both participants to process all messages up to the new clock time.
    // This ensures `source` processes the flag-clearing transaction sent by `target`.
    source.health.ping(target)
    target.health.ping(source)

    target.parties.clear_party_onboarding_flag(
      party,
      psid.logical,
      targetLedgerEnd,
    ) shouldBe FlagNotSet
  }

  protected[offpr] def replicateParty(
      clock: DelegatingSimClock,
      party: Party,
      workflowIdPrefix: String = "",
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    val beforeActivationOffset =
      targetAuthorizesHosting(party, daId, disconnectTarget = true)

    source.parties.export_party_acs(
      party = party,
      synchronizerId = daId.logical,
      targetParticipantId = target.id,
      beginOffsetExclusive = beforeActivationOffset,
      exportFilePath = acsSnapshotPath,
    )
    target.parties.import_party_acs(daId, Some(party), acsSnapshotPath, workflowIdPrefix)

    reconnectAndEnsureOnboardingClearance(clock, party, daName)
  }
}

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2
  *   - 2 active IOU contract between Alice (signatory) and Bob (observer)
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test Goal: Follow the Offline Party Replication operational steps as documented in
  * `docs-open/src/sphinx/participant/howtos/operate/parties/party_replication.rst`
  *
  * Test Steps: Replicate Alice to target using the onboarding flag
  *   - Target participant authorizes Alice->target setting the onboarding flag
  *   - Target participant disconnects from the synchronizer
  *   - A creation transaction to create a contract with Alice as signatory and Bob as observer is
  *     sequenced
  *   - Source participant approves/confirms transaction
  *   - Source participant authorizes Alice->target setting the onboarding flag
  *   - ACS snapshot for Alice is taken on source participant
  *   - ACS is imported on the target participant, which then reconnects to the synchronizers
  *   - Target participant clears the onboarding flag unilaterally
  *   - Assert successful clearing of onboarding flag by the target participant
  */
final class OfflinePartyReplicationIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3
      }

  "Regular offline party replication operational flow succeeds" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value

    IouSyntax.createIou(participant1)(alice, bob, 3.33).discard

    target.topology.party_to_participant_mappings
      .propose_delta(
        party = alice,
        adds = Seq(target.id -> targetPermission),
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

    // Alice also needs to authorize the transaction
    alice.topology.party_to_participant_mappings.propose_delta(
      source,
      adds = Seq(target.id -> targetPermission),
      store = daId,
      requiresPartyToBeOnboarded = true,
    )

    source.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = target,
      beginOffsetExclusive = sourceLedgerEnd,
      exportFilePath = acsSnapshotPath,
    )

    // Assert ACS snapshot is non-empty (no stakeholder filtering happened)
    repair.acs.read_from_file(acsSnapshotPath) should have size 2

    target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)

    // Advance time to allow topology transactions to be processed
    clock.advance(Duration.ofSeconds(30))
    checkOnboardingFlag(daId, setOnTarget = true)

    target.synchronizers.reconnect(daName)

    source.health.ping(target)
    target.ledger_api.state.acs.of_party(alice).size shouldBe 2

    // Assert contract archival and creation (submission) does NOT work
    // while the onboarding flag has not been cleared yet (local parties only).
    if (canSubmitOnTarget) {
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        assertAcsAndContinuedOperation(
          target,
          expectedNumActiveContracts = 2,
          numOfContractCreations = 7,
        ),
        _.shouldBeCantonErrorCode(
          TransactionProcessor.SubmissionErrors.PartyCurrentlyOnboarding
            .Rejection(Seq(alice.toLf))
            .code
        ),
      )
    } else {
      assertAcsAndContinuedOperation(target, expectedNumActiveContracts = 2)
    }

    // Advance time to allow the asynchronous onboarding flag clearance to complete
    advanceTimeAndPing(clock, Duration.ofMinutes(2).plusSeconds(10), source, target)

    // Force both participants to process all messages up to the new clock time.
    // This ensures `source` processes the flag-clearing transaction sent by `target`.
    source.health.ping(target)
    target.health.ping(source)

    // Now that we've advanced time and synced, the onboarding flag on the target participant should be cleared
    checkOnboardingFlag(daId, setOnTarget = false)

    // Assert contract archival and creation (submission) DOES work
    // after the onboarding flag has been cleared.
    assertAcsAndContinuedOperation(
      source,
      expectedNumActiveContracts = 2,
      numOfContractCreations = 3,
    )
  }

  private def checkOnboardingFlag(daId: => PhysicalSynchronizerId, setOnTarget: Boolean): Unit = {
    val lastPTP =
      source.topology.party_to_participant_mappings.list(synchronizerId = daId).last.item

    lastPTP.partyId shouldBe alice.partyId
    lastPTP.participants should have size 2

    forExactly(1, lastPTP.participants) { p =>
      p.participantId shouldBe source.id
      p.onboarding should be(false)
    }
    forExactly(1, lastPTP.participants) { p =>
      p.participantId shouldBe target.id
      p.onboarding should be(setOnTarget)
    }
  }
}

/** Setup:
  *   - Alice is hosted on participant1 (source)
  *   - Bob is hosted on participant2 (P2)
  *   - 5 active IOU contracts between Alice and Bob
  *   - Participant3 (target) is empty, and the target participant for replicating Alice
  *
  * Test Focus: Edge cases, failure modes, and state recovery during offline party replication (ACS
  * export/import).
  *
  * Note: These tests run sequentially on the same nodes and share state. Therefore, the tests rely
  * on proper state cleanup to verify that failures do not permanently corrupt the node's ability to
  * perform a successful replication.
  */
final class OfflinePartyReplicationEdgeCasesIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*

        source = participant1
        target = participant3

        IouSyntax.createIou(source)(alice, bob, 1.95).discard
        IouSyntax.createIou(source)(alice, bob, 2.95).discard
        IouSyntax.createIou(source)(alice, bob, 3.95).discard
        IouSyntax.createIou(source)(alice, bob, 4.95).discard
        IouSyntax.createIou(source)(alice, bob, 5.95).discard
      }

  /** Missing Party Activation Test Case:
    *
    * Asserts that a party ACS export aborts (times out) if the party (Alice) has not been
    * authorized/activated on the target participant prior to export.
    */
  "Missing party activation on the target participant aborts ACS export" in { implicit env =>
    import env.*

    val ledgerEndP1 = source.ledger_api.state.end()

    // no authorization for alice

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId,
        targetParticipantId = target.id,
        beginOffsetExclusive = ledgerEndP1,
        exportFilePath = acsSnapshotPath,
        waitForActivationTimeout = Some(config.NonNegativeFiniteDuration.ofMillis(5)),
      ),
      _.warningMessage should include(
        "Internal stream [Internal Topology Transaction Update Stream] completed with a failure"
      ),
      _.shouldBeCantonErrorCode(EffectivePartyToParticipantMappingNotFound.code),
    )
  }

  /** Missing Onboarding Flag Test Case:
    *   - Asserts that if Alice is authorized on the target participant but the `onboarding` flag is
    *     set to `false`, the party ACS export and the onboarding flag clearance explicitly abort
    *     with an expected error.
    *   - Reverses the invalid topology state by deactivating Alice on the target. This cleanup is
    *     strictly required to prepare the shared node state for the subsequent test.
    */
  "Party activation on the target participant with missing onboarding flag aborts ACS export and onboarding flag clearance" in {
    implicit env =>
      import env.*

      val ledgerEndP1 = source.ledger_api.state.end()
      val ledgerEndTarget = target.ledger_api.state.end()

      // Authorize Alice without onboarding flag set
      PartyToParticipantDeclarative.forParty(Set(source, target), daId)(
        source.id,
        alice,
        PositiveInt.one,
        Set(
          (source.id, targetPermission),
          (target.id, targetPermission),
        ),
      )(executionContext, env)

      forAll(
        source.topology.party_to_participant_mappings
          .list(
            daId,
            filterParty = alice.toProtoPrimitive,
            filterParticipant = target.toProtoPrimitive,
          )
          .loneElement
          .item
          .participants
      ) { p =>
        p.onboarding shouldBe false
      }

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        source.parties.export_party_acs(
          party = alice,
          synchronizerId = daId,
          targetParticipantId = target.id,
          beginOffsetExclusive = ledgerEndP1,
          exportFilePath = acsSnapshotPath,
          waitForActivationTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(1)),
        ),
        _.shouldBeCantonErrorCode(AcsExportMissingTargetOnboardingMapping.code),
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        target.parties.clear_party_onboarding_flag(
          alice,
          daId.logical,
          ledgerEndTarget,
          waitForActivationTimeout = Some(config.NonNegativeFiniteDuration.ofSeconds(1)),
        ),
        _.shouldBeCantonErrorCode(OnboardingClearanceMissingMapping.code),
      )

      // Undo activating Alice on the target participant without onboarding flag set; for the following test
      alice.topology.party_to_participant_mappings.propose_delta(
        source,
        removes = Seq(target.id),
        store = daId,
        mustFullyAuthorize = true,
      )

      // Wait for party deactivation to propagate to both source and target participants
      eventually() {
        val sourceMapping = source.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.toProtoPrimitive)
          .loneElement
          .item

        sourceMapping.participants should have size 1
        forExactly(1, sourceMapping.participants) { p =>
          p.participantId shouldBe source.id
          p.onboarding should be(false)
        }

        // Ensure the target has processed the "remove" transaction to prevent flakes
        val targetMapping = target.topology.party_to_participant_mappings
          .list(daId, filterParty = alice.toProtoPrimitive)
          .loneElement
          .item

        targetMapping.participants should have size 1
        forExactly(1, targetMapping.participants) { p =>
          p.participantId shouldBe source.id
          p.onboarding should be(false)
        }
      }

  }

  /** Successful Replication After Failure Recovery Test Case:
    *   - Verifies that a normal party replication still succeeds after the aborted attempt and
    *     cleanup from the previous test, proving the nodes remained in a healthy state.
    *   - Asserts the expected number of active contracts and verifies continued operations (Alice
    *     can archive and create contracts on the target).
    */
  "Party replication succeeds following cleanup of prior failed activations" in { implicit env =>
    executeSuccessfulReplicationFlow(expectedAcsSnapshotSize = 5)
  }

  /** Empty ACS Snapshot Import Test Case:
    *   - Verifies that party replication succeeds even when the generated ACS snapshot is empty.
    *   - This occurs when the target (participant2) already hosts a stakeholder (Bob) who shares
    *     all active contracts with the replicated party (Alice), resulting in an empty export due
    *     to stakeholder filtering.
    *   - Asserts that importing this empty snapshot acts as a safe no-op and the party is
    *     successfully onboarded.
    */
  "Party replication succeeds even when importing an empty ACS snapshot" in { implicit env =>
    import env.*

    // Participant2 hosts Bob who shares all active contracts with Alice.
    // Replicating Alice to participant2 will filter out all active contracts for Alice,
    // resulting in an empty ACS snapshot.
    target = participant2

    executeSuccessfulReplicationFlow(expectedAcsSnapshotSize = 0)
  }

  private def executeSuccessfulReplicationFlow(expectedAcsSnapshotSize: Long)(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*
    val clock = env.environment.simClock.value

    val beforeActivationOffset =
      targetAuthorizesHosting(alice, daId, disconnectTarget = true)

    source.parties.export_party_acs(
      party = alice,
      synchronizerId = daId,
      targetParticipantId = target.id,
      beginOffsetExclusive = beforeActivationOffset,
      exportFilePath = acsSnapshotPath,
    )

    repair.acs.read_from_file(acsSnapshotPath) should have size expectedAcsSnapshotSize

    target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)

    reconnectAndEnsureOnboardingClearance(clock, alice, daName)

    assertAcsAndContinuedOperation(target, expectedNumActiveContracts = 5)
  }

}

/** Setup:
  *   - Alice is hosted on participant1 (source), Bob on participant2.
  *   - Participant3 (target) is the target of the legacy offline party replication. Participant2
  *     replicates Alice with the new flow and freezes her.
  *
  * Test Goal: Offline party replication keeps working while another target freezes the party. The
  * ACS export and the onboarding flag clearance read the target's hosting state, not the hosting
  * view, which is empty while any entry of the party is Freezing. The ACS export leaves out only
  * the contracts shared with parties that the target hosts (Onboarding or Active).
  *
  * Test Steps:
  *   - Export during a freeze: participant2 is Freezing for Alice when the target's Onboarding
  *     entry becomes effective. The export of Alice's ACS succeeds.
  *   - Clearance during a freeze: the target imports Alice's ACS, reconnects and requests the
  *     onboarding flag clearance, which is accepted. The target becomes Active while participant2
  *     stays Freezing. Relies on the state of the previous test.
  *   - Excluded stakeholders: Carol (participant1) shares one contract each with Bob, Charlie, Dave
  *     and Erin (participant2). The target is Nominated for Bob, Freezing for Charlie, Onboarding
  *     for Dave and Active for Erin. Carol's export to the target includes only the contracts
  *     shared with Bob and Charlie.
  */
final class OfflinePartyReplicationDuringFreezeIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        source = participant1
        target = participant3
      }

  "Export Alice's ACS while another target freezes her" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      IouSyntax.createIou(source)(alice, bob, 1.95).discard

      freezeParty(alice, source, participant2, daId, participants.local).discard

      val beforeActivationOffset = targetAuthorizesHosting(
        alice,
        daId,
        disconnectTarget = true,
      )

      source.parties.export_party_acs(
        party = alice,
        synchronizerId = daId.logical,
        targetParticipantId = target.id,
        beginOffsetExclusive = beforeActivationOffset,
        exportFilePath = acsSnapshotPath,
      )

      repair.acs.read_from_file(acsSnapshotPath) should have size 1
  }

  "Clear Alice's onboarding flag while another target freezes her" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      target.parties.import_party_acs(daId, Some(alice), acsSnapshotPath)
      // The target receives its activation only after reconnecting
      val targetLedgerEnd = target.ledger_api.state.end()

      // Advance time to allow topology transactions to be processed
      clock.advance(Duration.ofSeconds(30))
      target.synchronizers.reconnect(daName)
      source.health.ping(target)

      target.parties.clear_party_onboarding_flag(
        alice,
        daId.logical,
        targetLedgerEnd,
      ) shouldBe a[FlagSet]

      // Pass the decision deadline, after which the target clears its onboarding flag
      advanceTimeAndPing(clock, Duration.ofMinutes(2).plusSeconds(10), source, target)

      awaitHostingStates(
        alice,
        daId,
        participants.local,
        Map(
          source.id -> HostingState.Active,
          participant2.id -> HostingState.Freezing,
          target.id -> HostingState.Active,
        ),
      ).discard
  }

  "Exclude from the ACS export only the contracts shared with parties the target hosts" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    implicit env =>
      import env.*

      val carol = source.parties.testing.enable(
        "Carol",
        synchronizeParticipants = Seq(participant2, target),
      )
      def enableOnParticipant2(name: String): Party =
        participant2.parties.testing.enable(name, synchronizeParticipants = Seq(source, target))
      val charlie = enableOnParticipant2("Charlie")
      val dave = enableOnParticipant2("Dave")
      val erin = enableOnParticipant2("Erin")

      // The party alone nominates the target, but needs the target to add it as Onboarding or Active
      proposeAsParty(bob, participant2, target, daId, Map(target.id -> HostingState.Nominated))
      proposeAsParty(charlie, participant2, target, daId, Map(target.id -> HostingState.Nominated))
      addHost(dave, participant2, target, requiresPartyToBeOnboarded = true)
      addHost(erin, participant2, target)
      Seq(
        bob -> HostingState.Nominated,
        charlie -> HostingState.Nominated,
        dave -> HostingState.Onboarding,
        erin -> HostingState.Active,
      ).foreach { case (party, targetState) =>
        awaitHostingStates(
          party,
          daId,
          participants.local,
          Map(participant2.id -> HostingState.Active, target.id -> targetState),
        ).discard
      }

      Seq(bob, charlie, dave, erin).foreach(owner =>
        IouSyntax.createIou(source)(carol, owner).discard
      )

      proposeAsParticipant(charlie, target, daId, Map(target.id -> HostingState.Freezing))
      awaitHostingStates(
        charlie,
        daId,
        participants.local,
        Map(participant2.id -> HostingState.Active, target.id -> HostingState.Freezing),
      ).discard

      val beforeActivationOffset = targetAuthorizesHosting(carol, daId, disconnectTarget = false)

      source.parties.export_party_acs(
        party = carol,
        synchronizerId = daId.logical,
        targetParticipantId = target.id,
        beginOffsetExclusive = beforeActivationOffset,
        exportFilePath = acsSnapshotPath,
      )

      repair.acs
        .read_from_file(acsSnapshotPath)
        .flatMap(_.getCreatedEvent.observers)
        .toSet shouldBe Set(bob.toLf, charlie.toLf)
  }

  /** Adds `participant` with Observation permission to the party's hosts, with the participant's
    * and the party's authorization.
    *
    * @param requiresPartyToBeOnboarded
    *   whether the participant should be in the Onboarding hosting state for the party
    */
  private def addHost(
      party: Party,
      partyHost: LocalParticipantReference,
      participant: LocalParticipantReference,
      requiresPartyToBeOnboarded: Boolean = false,
  )(implicit env: TestConsoleEnvironment): Unit = {
    proposeAsParticipant(
      party,
      participant,
      env.daId,
      requiresPartyToBeOnboarded = requiresPartyToBeOnboarded,
    )
    proposeAsParty(
      party,
      partyHost,
      participant,
      env.daId,
      requiresPartyToBeOnboarded = requiresPartyToBeOnboarded,
    )
  }
}
