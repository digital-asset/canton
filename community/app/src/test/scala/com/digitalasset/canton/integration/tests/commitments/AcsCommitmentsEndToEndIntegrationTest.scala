// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.commitments

import com.digitalasset.canton.TestPredicateFiltersFixtureAnyWordSpec
import com.digitalasset.canton.annotations.AcsCommitmentTest
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.crypto.LtHash16Blake3
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.NotFound
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  HashedDigest,
  InternedParticipantId,
  RawDigest,
}
import com.digitalasset.canton.topology.{ParticipantId, PartyId}
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseProtocolVersion}
import monocle.syntax.all.*
import org.slf4j.event.Level

import scala.concurrent.duration.*

/** End to end integration test for ACS commitment processing pipeline */
sealed trait AcsCommitmentsEndToEndIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with TestPredicateFiltersFixtureAnyWordSpec {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.enableDevVersionSupport*)
      .addConfigTransforms(ConfigTransforms.enableNewAcsDigestProcessorPipeline)

  "the digest processor creates digests for counterparticipants" onlyRunWithOrGreaterThan ReleaseProtocolVersion.acsCommitmentRedesignStorage.v in {
    implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, daName)
      participant2.synchronizers.connect_local(sequencer1, daName)
      participants.all.dars.upload(CantonExamplesPath)

      // running a ping exchanges contracts
      participant1.health.ping(participant2, timeout = 30.seconds)

      // create parties and a contract using those parties
      val alice = participant1.parties.enable("alice")
      val bob = participant2.parties.enable("bob")

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
  }

  // the following test case should only run when the synchronizer actually runs with $ProtocolVersion.acsCommitmentRedesign,
  // because otherwise a synchronizer parameter change doesn't trigger a checkpoint
  // TODO(#33326) enable this test, once the synchronizer parametes are properly persisted in the indexer
  "synchronizer parameter changes trigger a checkpoint" onlyRunWithOrGreaterThan ProtocolVersion.acsCommitmentRedesign ignore {
    implicit env =>
      import env.*

      val beforeSerial =
        participant1.topology.synchronizer_parameters.list(daId).loneElement.context.serial

      var startOffset = Offset.tryFromLong(participant1.ledger_api.state.end())
      synchronizerOwners1.foreach(
        _.topology.synchronizer_parameters.propose_update(
          daId,
          params =>
            params.update(reconciliationInterval = params.reconciliationInterval.plusSeconds(1)),
        )
      )

      val expectedCheckpointTime = eventually() {
        val params = participant1.topology.synchronizer_parameters.list(daId).loneElement
        params.context.serial shouldEqual beforeSerial.increment
        params.context.validFrom
      }

      participant1.underlying.value.sync.syncPersistentStateManager.acsDigestStore(daId).value
      val digestStore =
        participant1.underlying.value.sync.syncPersistentStateManager.acsDigestStore(daId).value

      // TODO(#34302): check the checkpoint for the specific checkpoint type
      eventually() {
        // in case there is no next checkpoint, .value will trigger a retry of the eventually loop
        val (checkpointOffset, checkpointTs) =
          digestStore.firstCheckpointAfter(startOffset).futureValueUS.value

        // if there was a checkpoint, update the offset to look for the next checkpoint
        startOffset = checkpointOffset
        // finally check whether we have reached the checkpoint with the expected checkpoint time
        checkpointTs.toInstant shouldBe expectedCheckpointTime
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

    LtHash16Blake3.tryCreate(
      p1sViewOfP2.digestUpdate.digestO.value._1
    ) should not be LtHash16Blake3.empty

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
  ): Option[AcsDigestStore.AcsDigestUpdate[InternedParticipantId, (RawDigest, HashedDigest)]] = {
    val si = source.underlying.value.sync.ledgerApiIndexer.asEval
      .flatMap(_.ledgerApiStore)
      .value
      .stringInterningView
    source.underlying.value.sync.syncPersistentStateManager
      .acsDigestStore(env.daId)
      .value
      .participant
      .lookup(si.participantId.internalize(target.toLf), Offset.MaxValue)
      .futureValueUS
  }

}

@AcsCommitmentTest
class AcsCommitmentsEndToEndIntegrationTestInMemory extends AcsCommitmentsEndToEndIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(ConfigTransforms.allInMemory)
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  registerPlugin(new UseBftSequencer(loggerFactory))
}

@AcsCommitmentTest
class AcsCommitmentsBftOrderingEndToEndIntegrationTestH2
    extends AcsCommitmentsEndToEndIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

@AcsCommitmentTest
class AcsCommitmentsBftOrderingEndToEndIntegrationTestPostgres
    extends AcsCommitmentsEndToEndIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
