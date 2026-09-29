// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.commitments

import com.digitalasset.canton.TestPredicateFiltersFixtureAnyWordSpec
import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.ReinitCommitments.{
  DigestCommitmentReinitializationInfo,
  DigestCommitmentReinitializationStatusInfo,
}
import com.digitalasset.canton.admin.api.client.data.DynamicSynchronizerParameters
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.crypto.LtHash16Blake3
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.commitment.{
  DigestProcessor,
  ReinitializingDigestProcessor,
  RunningDigestProcessor,
}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  CheckpointType,
  InternedParticipantId,
  allCheckpointsFilter,
}
import com.digitalasset.canton.time.NonNegativeSeconds
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.syntax.all.*
import org.slf4j.event.Level

import scala.concurrent.duration.*

/** End to end integration test for ACS commitment processing pipeline */
sealed trait AcsCommitmentsEndToEndIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with TestPredicateFiltersFixtureAnyWordSpec
    with HasCycleUtils {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(
        ConfigTransforms.disableOldAcsCommitmentProcessor,
        // Trigger frequent garbage collections so that we can see that they are happening
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.journalGarbageCollectionMinimumGap)
            .replace(PositiveFiniteDuration.ofSeconds(1))
        ),
      )

  "the digest processor creates digests for counterparticipants" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
    implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, daName)
      participant2.synchronizers.connect_local(sequencer1, daName)
      Seq(participant1, participant2).dars.upload(CantonExamplesPath)

      // running a ping exchanges contracts
      participant1.health.ping(participant2, timeout = 30.seconds)

      // create parties and a contract using those parties
      val alice = participant1.parties.enable("alice")
      val bob = participant2.parties.enable("bob")

      val pruningBound = participant1.testing.fetch_synchronizer_time(daId)

      val iou = IouSyntax.createIou(participant1)(alice, alice, observers = List(bob))

      // party allocations always trigger a checkpoint
      participant1.parties.enable("checkpoint-trigger-1")

      eventually() {
        validateDigestAtOffsetOfSharedContract(participant1, participant2, iou.id.contractId)
      }

      // disconnect participant2
      participant2.synchronizers.disconnect_all()

      // create more contracts while participant2 is disconnected
      val latestIou =
        List
          .fill(3)(
            IouSyntax
              .createIou(participant1)(alice, alice, observers = List(bob), optTimeout = None)
          )
          .last

      // party allocations always trigger a checkpoint
      participant1.parties.enable("checkpoint-trigger-2")

      participant2.synchronizers.reconnect_all()
      // retry for all exceptions, in case the contract cannot be found in the first few retries until p2 has caught up
      eventually(retryOnTestFailuresOnly = false) {
        loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.ERROR))(
          validateDigestAtOffsetOfSharedContract(
            participant1,
            participant2,
            latestIou.id.contractId,
          ),
          LogEntry.assertLogSeq(
            Seq.empty,
            Seq(
              _.shouldBeCantonErrorCode(NotFound.ContractEvents)
            ),
          ),
        )
      }

      logger.info(
        "Check that the journal background collector prunes the ACS even if the legacy commitment processor is disabled"
      )
      val p1acsStore = participant1.underlying.value.sync.syncPersistentStateManager
        .activeContractStore(daId)
        .value
      eventually(timeUntilSuccess = 90.seconds) {
        participant1.health.ping(participant1)
        val acsPruningStatus = p1acsStore.pruningStatus.futureValueUS
        acsPruningStatus.value.lastSuccess.value should be >= pruningBound
      }
  }

  // the following test case should only run when the synchronizer actually runs with `ProtocolVersion.acsCommitmentRedesign`,
  // because otherwise a synchronizer parameter change doesn't trigger a checkpoint
  "synchronizer parameter changes trigger a checkpoint" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
    implicit env =>
      import env.*

      val beforeParams =
        participant1.topology.synchronizer_parameters.list(daId).loneElement

      var startOffset = Offset.tryFromLong(participant1.ledger_api.state.end())
      sequencer1.topology.synchronizer_parameters.propose(
        daId,
        DynamicSynchronizerParameters(
          beforeParams.item.update(reconciliationInterval =
            beforeParams.item.reconciliationInterval.add(NonNegativeSeconds.tryOfSeconds(1))
          )
        ),
        serial = Some(beforeParams.context.serial.increment.value),
      )

      val expectedCheckpointTime = eventually() {
        val params = participant1.topology.synchronizer_parameters.list(daId).loneElement
        params.context.serial shouldBe beforeParams.context.serial.increment.value
        params.context.validFrom
      }

      val digestStore =
        participant1.underlying.value.sync.syncPersistentStateManager.acsDigestStore(daId).value

      eventually() {
        // in case there is no next checkpoint, .value will trigger a retry of the eventually loop
        val cp =
          digestStore.firstCheckpointAfter(startOffset, allCheckpointsFilter).futureValueUS.value

        // if there was a checkpoint, update the offset to look for the next checkpoint
        startOffset = cp.offset
        // finally check whether we have reached the checkpoint with the expected checkpoint time or later
        // (as reconciliation checkpoints can be skipped).
        cp.recordTime.toInstant should be >= expectedCheckpointTime
        cp.checkpointType shouldBe CheckpointType.ReconciliationIntervalBoundary
      }
  }

  s"start reinitializing on one participant when running digest processor is active" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
    implicit env =>
      import env.*

      val alice = participant1.parties.enable("alice-reinit")
      val bob = participant2.parties.enable("bob-reinit")

      val iou = IouSyntax.createIou(participant1)(alice, alice, observers = List(bob))

      // Trigger a checkpoint so the running digest processor persists the digest updates
      // triggered by the new contract
      participant1.parties.enable("p1-checkpoint-party-trigger-before-reinit")

      eventually() {
        validateDigestAtOffsetOfSharedContract(participant1, participant2, iou.id.contractId)
      }

      // Kick off reinitialization while running digest processor is active
      // this command should stop the running digest processor and then once completes, should restart it
      // on participant1
      val DigestCommitmentReinitializationInfo(reinitTs) =
        participant1.commitments.reinitialize_digest_commitments(daId)

      eventually() {
        val DigestCommitmentReinitializationStatusInfo(lastCompletedTsO) =
          participant1.commitments.digest_commitments_reinitialization_status(daId)
        lastCompletedTsO shouldEqual Some(reinitTs)
      }

      val newIou = IouSyntax.createIou(participant1)(alice, alice, observers = List(bob))

      // Trigger a checkpoint so the running digest processor persists the digest updates
      // triggered by the new contract
      participant1.parties.enable("p1-checkpoint-party-trigger-after-reinit")

      participant1.ledger_api.state.acs.of_party(alice).map(_.contractId) should contain(
        newIou.id.contractId
      )

      eventually() {
        participant2.ledger_api.state.acs.of_party(bob).map(_.contractId) should contain(
          newIou.id.contractId
        )
      }

      eventually() {
        validateDigestAtOffsetOfSharedContract(participant1, participant2, newIou.id.contractId)
      }
  }

  private def validateDigestAtOffsetOfSharedContract(
      p1: LocalParticipantReference,
      p2: LocalParticipantReference,
      contractId: String,
  )(implicit env: TestConsoleEnvironment) = {
    val p1sViewOfP2 = getDigestFor(p1, p2.id).value
    val p2sViewOfP1 = getDigestFor(p2, p1.id).value

    // the assigned offset for a contract is local to the participant
    val p1IouOffset = getOffset(p1, contractId)
    val p2IouOffset = getOffset(p2, contractId)

    p1sViewOfP2.digestUpdate.offset.positive shouldBe p1IouOffset
    p2sViewOfP1.digestUpdate.offset.positive shouldBe p2IouOffset

    // the two participants must coincide
    p1sViewOfP2.digestUpdate.digestO.value shouldBe p2sViewOfP1.digestUpdate.digestO.value

    LtHash16Blake3.tryCreate(p1sViewOfP2.digestUpdate.digestO.value) should not be
      LtHash16Blake3.empty
  }

  private def getOffset(
      participant: ParticipantReference,
      contractId: String,
      parties: PartyId*
  ): Long =
    participant.ledger_api.javaapi.event_query
      .by_contract_id(contractId, parties)
      .getCreated
      .getCreatedEvent
      .getOffset

  def getDigestFor(source: LocalParticipantReference, target: ParticipantId)(implicit
      env: TestConsoleEnvironment
  ): Option[AcsDigestStore.AcsDigestUpdate[InternedParticipantId]] = {
    val si =
      source.underlying.value.sync.ledgerApiIndexer.asEval.value.ledgerApiStore.stringInterningView
    source.underlying.value.sync.syncPersistentStateManager
      .acsDigestStore(env.daId)
      .value
      .participant
      .lookup(si.participantId.internalize(target.toLf), Offset.MaxValue)
      .futureValueUS
  }
}

// the test case below stops and starts participant3, which doesn't work for InMemory storage
sealed trait AcsCommitmentsEndToEndIntegrationTestStorage {
  self: AcsCommitmentsEndToEndIntegrationTest =>
  "does not attempt automatic reinitialization if no data has been received by the synchronizer" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign in {
    implicit env =>
      import env.*

      participant3.config.parameters.acsCommitments.enableNewAcsCommitmentProcessor shouldBe true

      participant3.synchronizers.register(
        sequencer1,
        daName,
        // perform the handshake to initialize the persistent state
        performHandshake = true,
        // and manually connect, so that the participant has a persistent state, but no events on the synchronizer yet
        manualConnect = true,
      )

      participant3.stop()
      participant3.start()

      def currentProc: Option[DigestProcessor] =
        participant3.underlying.value.participantServices.acsCommitmentProcessorManagerO
          .flatMap(_.asEval.value.synchronizers.get(daId))
          .flatMap(_.digestProcessorManager.currentProcessor)

      clue(s"$participant3 hasn't started reinitialization") {
        always() {
          currentProc.foreach(_ should not be a[ReinitializingDigestProcessor])
        }
      }
      clue(s"$participant3 has started a running digest processor") {
        eventually() {
          currentProc.value shouldBe a[RunningDigestProcessor]
        }
      }

      // Now reconnect to the synchronizer and check that the commitment processing works correctly
      participant3.synchronizers.reconnect(daName)
      participant3.dars.upload(CantonExamplesPath)

      IouSyntax
        .createIou(participant3)(
          participant3.adminParty,
          participant3.adminParty,
          observers = List(participant1.adminParty),
        )
        .discard

      // trigger a checkpoint via a party allocation
      participant3.parties.enable("charlie")

      eventually() {
        getDigestFor(participant1, participant3.id).value.digestUpdate.digestO.value shouldBe
          getDigestFor(participant3, participant1.id).value.digestUpdate.digestO.value
      }

  }

}

class AcsCommitmentsEndToEndIntegrationTestInMemory extends AcsCommitmentsEndToEndIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(ConfigTransforms.allInMemory)
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  registerPlugin(new UseBftSequencer(loggerFactory))
}

class AcsCommitmentsBftOrderingEndToEndIntegrationTestH2
    extends AcsCommitmentsEndToEndIntegrationTest
    with AcsCommitmentsEndToEndIntegrationTestStorage {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class AcsCommitmentsBftOrderingEndToEndIntegrationTestPostgres
    extends AcsCommitmentsEndToEndIntegrationTest
    with AcsCommitmentsEndToEndIntegrationTestStorage {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
