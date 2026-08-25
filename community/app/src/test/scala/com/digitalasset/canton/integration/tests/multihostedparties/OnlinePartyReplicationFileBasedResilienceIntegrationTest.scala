// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor.*
import com.digitalasset.canton.participant.admin.party.{
  PartyReplicationTestInterceptor,
  PartyReplicator,
}
import com.digitalasset.canton.participant.party.PartyReplicationTestInterceptorImpl
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{HasTempDirectory, config}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/** Setup:
  *   - 2 participants: participant1 (SP) exports the ACS, participant2 (TP) imports it
  *   - The TP is deliberately paused mid-import and restarted to simulate a crash, OR
  *   - The TP encounters an error mid-import, OR
  *   - The TP imports an empty ACS snapshot.
  *
  * Test Goal: Retrying the file-based OnPR re-processes the ACS snapshot from the beginning without
  * issues in failure scenarios. Also validates importing an empty ACS snapshot.
  */
class OnlinePartyReplicationFileBasedResilienceIntegrationTest
    extends CommunityIntegrationTest
    with OnlinePartyReplicationTestHelpers
    with HasTempDirectory
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private var externalParty: ExternalParty = _
  private var externalParty2: ExternalParty = _
  private var emptyAcsParty: ExternalParty = _
  private var alice: PartyId = _
  private var sourceParticipant: ParticipantReference = _
  private var targetParticipant: LocalParticipantReference = _

  private lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)
  private lazy val acsSnapshotFilename =
    tempDirectory.toTempFile("canton-acs-export-retry-recovery.gz").toString

  private val totalContractsInBatch = 200

  // Holds the logic to evaluate per batch
  private val interceptorLogicRef = new AtomicReference[Long => ProceedOrWait](_ => Proceed)

  // Dynamically delegates to the current logic in the reference
  private val dynamicInterceptor: PartyReplicationTestInterceptor =
    PartyReplicationTestInterceptorImpl.targetParticipantEvaluates { progress =>
      interceptorLogicRef.get()(progress.processedContractCount.unwrap)
    }

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // Replication to the target participant may trigger ACS commitment mismatch warnings.
  // This is expected behavior. To reduce the frequency of these warnings and avoid associated
  // test flakes, `reconciliationInterval` is set to ten years.
  private val reconciliationInterval: config.PositiveDurationSeconds =
    config.PositiveDurationSeconds.ofDays(10 * 365)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableAlphaOnlinePartyReplicationSupport(
          Map("participant2" -> (() => dynamicInterceptor)),
          pauseIndexer = false,
        )*
      )
      .withSetup { implicit env =>
        import env.*

        sequencer1.topology.synchronizer_parameters
          .propose_update(daId, _.update(reconciliationInterval = reconciliationInterval))

        participants.all.synchronizers.connect_local(sequencer1, daName)
        darPaths.foreach(darPath => participants.all.foreach(_.dars.upload(darPath)))

        alice = participant1.parties.enable("alice", synchronizeParticipants = Seq(participant2))
        externalParty = participant1.parties.testing.external.enable("external-party")
        externalParty2 = participant1.parties.testing.external.enable("external-party-2")
        emptyAcsParty = participant1.parties.testing.external.enable("empty-external-party")

        sourceParticipant = participant1
        targetParticipant = participant2
      }

  private def setupAndExportAcs(
      party: ExternalParty,
      filename: String,
      createContracts: Boolean,
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    if (createContracts) {
      val amounts = 1 to totalContractsInBatch
      IouSyntax.createIous(sourceParticipant, alice, party, amounts)
    }

    val authorizedSetup = authorizeOnboardingTopologyForFileBasedOnPR(
      sourceParticipant,
      targetParticipant,
      party,
    )

    sourceParticipant.parties.export_party_acs(
      party,
      daId,
      targetParticipant,
      authorizedSetup.spOffsetBeforePartyOnboardingToTargetParticipant,
      filename,
    )

    authorizedSetup
  }

  "Retry after node shutdown (crash) mid ACS import stream" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      // Locally scoped state for this specific test run
      val reachedMidwayCheckpoint = Promise[Unit]()
      @volatile var retry = false

      // Inject logic to WAIT mid-stream
      interceptorLogicRef.set { processedCount =>
        if (processedCount >= (totalContractsInBatch / 2) && !retry) {
          reachedMidwayCheckpoint.trySuccess(())
          Wait
        } else {
          Proceed
        }
      }

      try {
        val authorizedOnPRSetup = setupAndExportAcs(
          party = externalParty,
          filename = acsSnapshotFilename,
          createContracts = true,
        )

        val firstImportF = Future {
          targetParticipant.parties.add_party_with_acs_async(
            importFilePath = acsSnapshotFilename,
            party = externalParty,
            synchronizerId = daId,
            sourceParticipant = sourceParticipant,
            serial = authorizedOnPRSetup.topologySerial,
            participantPermission = ParticipantPermission.Confirmation,
          )
        }

        reachedMidwayCheckpoint.future.futureValue

        loggerFactory.suppressWarningsAndErrors({
          targetParticipant.stop()

          try {
            firstImportF.futureValue
          } catch {
            case NonFatal(_) =>
              logger.info("First import failed cleanly as expected due to participant shutdown")
          }

          targetParticipant.start()
          targetParticipant.synchronizers.reconnect(daName)

          eventually() {
            targetParticipant.health.ping(sourceParticipant)
          }
        })

        retry = true

        val addPartyRequestId = clue("Retry to add party after node restart") {
          targetParticipant.parties.add_party_with_acs_async(
            importFilePath = acsSnapshotFilename,
            party = externalParty,
            synchronizerId = daId,
            sourceParticipant = sourceParticipant,
            serial = authorizedOnPRSetup.topologySerial,
            participantPermission = ParticipantPermission.Confirmation,
          )
        }

        val expectedNumContracts = NonNegativeInt.tryCreate(totalContractsInBatch)

        eventuallyOnPRCompletesOnTP(
          targetParticipant,
          addPartyRequestId,
          Some(expectedNumContracts),
          () => (),
        )

      } finally {
        // Reset interceptor to default for the next test
        interceptorLogicRef.set(_ => Proceed)
      }
  }

  "Retry after error mid ACS import stream" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    @volatile var retry = false

    // Inject logic to actively FAIL mid-stream
    interceptorLogicRef.set { processedCount =>
      if (processedCount >= (totalContractsInBatch / 2) && !retry) {
        Fail("Simulated error during ACS import batch processing")
      } else {
        Proceed
      }
    }

    try {
      val acsSnapshotFilename2 =
        tempDirectory.toTempFile("canton-acs-export-retry-recovery-2.gz").toString

      val authorizedOnPRSetup = setupAndExportAcs(
        party = externalParty2,
        filename = acsSnapshotFilename2,
        createContracts = true,
      )

      loggerFactory.assertLogsSeq(
        SuppressionRule.forLogger[PartyReplicator] || SuppressionRule
          .forLogger[EnvironmentDefinition]
      )(
        within = intercept[CommandFailure] {
          targetParticipant.parties.add_party_with_acs_async(
            importFilePath = acsSnapshotFilename2,
            party = externalParty2,
            synchronizerId = daId,
            sourceParticipant = sourceParticipant,
            serial = authorizedOnPRSetup.topologySerial,
            participantPermission = ParticipantPermission.Confirmation,
          )
        },
        assertion = logEntries => {
          val allMessages = logEntries.map(_.message).mkString("\n")
          allMessages should include("Simulated error during ACS import batch processing")
        },
      )

      retry = true

      // Attempt to retry with a mismatched parameter (Observation instead of Confirmation)
      clue("Retry with mismatched participant permission should fail immediately") {
        assertThrowsAndLogsCommandFailures(
          targetParticipant.parties.add_party_with_acs_async(
            importFilePath = acsSnapshotFilename2,
            party = externalParty2,
            synchronizerId = daId,
            sourceParticipant = sourceParticipant,
            serial = authorizedOnPRSetup.topologySerial,
            participantPermission = ParticipantPermission.Observation,
          ),
          _.errorMessage should include("is not marked as onboarding with permission Observation"),
        )
      }

      val addPartyRequestId = clue("Retry to add party after error") {
        targetParticipant.parties.add_party_with_acs_async(
          importFilePath = acsSnapshotFilename2,
          party = externalParty2,
          synchronizerId = daId,
          sourceParticipant = sourceParticipant,
          serial = authorizedOnPRSetup.topologySerial,
          participantPermission = ParticipantPermission.Confirmation,
        )
      }

      val expectedNumContracts = NonNegativeInt.tryCreate(totalContractsInBatch)

      eventuallyOnPRCompletesOnTP(
        targetParticipant,
        addPartyRequestId,
        Some(expectedNumContracts),
        () => (),
      )

    } finally {
      interceptorLogicRef.set(_ => Proceed)
    }
  }

  "Replicate a party using an empty ACS snapshot file" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val emptyAcsSnapshotFilename = tempDirectory.toTempFile("canton-empty-acs-export.gz").toString

      val authorizedOnPRSetup = setupAndExportAcs(
        party = emptyAcsParty,
        filename = emptyAcsSnapshotFilename,
        createContracts = false,
      )

      val addPartyRequestId = clue("Add party with empty acs snapshot file")(
        targetParticipant.parties.add_party_with_acs_async(
          importFilePath = emptyAcsSnapshotFilename,
          party = emptyAcsParty,
          synchronizerId = daId,
          sourceParticipant = sourceParticipant,
          serial = authorizedOnPRSetup.topologySerial,
          participantPermission = ParticipantPermission.Confirmation,
        )
      )

      val expectedNumContracts = NonNegativeInt.tryCreate(0)

      eventuallyOnPRCompletesOnTP(
        targetParticipant,
        addPartyRequestId,
        Some(expectedNumContracts),
        () => (),
      )
  }

  "Ensure source and target participant Ledger Api ACS match" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      Seq(
        (externalParty, totalContractsInBatch * 2), // Verified from the shutdown (crash) test
        (externalParty2, totalContractsInBatch * 2), // Verified from the error test
        (emptyAcsParty, 10), // Verified from empty ACS test
      ).foreach { case (party, limit) =>
        eventuallyLedgerApiAcsInSyncBetweenSPAndTP(
          sourceParticipant,
          targetParticipant,
          replicatedParty = party,
          limit = limit,
        )
      }
  }

  "Ensure target participant Ledger Api ACS and internal ACS match" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      ensureActiveContractsInSyncBetweenLedgerApiAndSyncService(
        targetParticipant,
        // Increase the limit to 3 (600) to safely cover the 400 total contracts (200 + 200) accumulated across all tests. (The empty ACS party adds 0).
        limit = totalContractsInBatch * 3,
      )
  }
}
