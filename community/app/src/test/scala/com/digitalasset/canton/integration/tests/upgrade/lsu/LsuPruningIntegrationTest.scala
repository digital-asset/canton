// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.syntax.functor.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CommitmentSendDelay
import com.digitalasset.canton.config.RequireTypes.NonNegativeProportion
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.participant.store.{
  ActiveContractStore,
  LogicalSyncPersistentState,
  PhysicalSyncPersistentState,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.synchronizer.sequencer.SequencerUtils
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId, SynchronizerId}
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.*
import scala.util.chaining.*

/** This test verifies that pruning works well with LSU. It uses 2 participants, 2 sequencers and 2
  * mediators.
  *
  * Scenario:
  *   - LSU is performed
  *   - Wait until the sequencing time corresponding to the safe to prune offset is past the latest
  *     sequencing time of the old synchronizer
  *   - Check that data can be pruned:
  *     - old physical stores (sequenced event store, request journal)
  *     - new physical stores (sequenced event store, request journal)
  *     - ACS commitment store before upgrade time
  *     - ACS commitment store after upgrade time
  *   - Check that data can be garbage collected
  *     - ActiveContractStore is cleaned
  */
@nowarn("cat=deprecation")
final class LsuPruningIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-pruning"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] = Map(
    "sequencer2" -> "sequencer1"
  )
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  /*
  Big value compared to (upgradeTime - Epoch) so that contracts of the ActiveContractStore
  cannot be garbage collected before the LSU.
   */
  private lazy val journalGarbageCollectionDelay = config.NonNegativeFiniteDuration.ofDays(1)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .updateTestingConfig(
        _.focus(_.commitmentSendDelay).replace(
          Some(
            CommitmentSendDelay(
              Some(NonNegativeProportion.zero),
              Some(NonNegativeProportion.zero),
            )
          )
        )
      )
      .addConfigTransforms(
        /*
        Sufficiently small so that when advancing the clock to progress past
        dedup duration, we don't hit other limits (e.g., the ledger time to record
        time tolerance).
         */
        ConfigTransforms.updateMaxDeduplicationDurations(10.seconds.toJava)
      )
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.journalGarbageCollectionDelay)
            .replace(journalGarbageCollectionDelay)
        )
      )
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  private var fixture: Fixture = _
  private var alice: PartyId = _
  private var bank: PartyId = _

  private var iou1: Iou.Contract = _
  private var iou2: Iou.Contract = _

  private def noOutstandingCommitments(
      p: LocalParticipantReference,
      ts: CantonTimestamp,
  )(implicit env: TestConsoleEnvironment): CantonTimestamp =
    p.underlying.value.sync.syncPersistentStateManager
      .get(env.daId)
      .value
      .acsCommitmentStore
      .noOutstandingCommitments(ts)
      .futureValueUS
      .value

  private def getPhysicalState(
      p: LocalParticipantReference,
      psid: PhysicalSynchronizerId,
  ): PhysicalSyncPersistentState = p.underlying.value.sync.syncPersistentStateManager
    .get(psid)
    .value
    .physical

  private def getLogicalState(
      p: LocalParticipantReference,
      lsid: SynchronizerId,
  ): LogicalSyncPersistentState =
    p.underlying.value.sync.syncPersistentStateManager.getAllLogical.get(lsid).value

  /** Wait until the safe to prune timestamp reaches the given timestamp.
    *
    * Returns the corresponding offset and timestamp.
    */
  private def waitP2SafeTsReaches(
      targetTs: CantonTimestamp
  )(implicit env: TestConsoleEnvironment): (Long, CantonTimestamp) = {
    import env.*

    eventually() {
      environment.simClock.value.advance(15.seconds.toJava)
      participant1.health.ping(participant2)

      val computedSafeOffset =
        participant2.pruning.find_safe_offset(beforeOrAt = environment.clock.now.toInstant).value

      val safeTimestamp =
        participant2.underlying.value.sync.ledgerApiIndexer.asEval.value.ledgerApiStore.value
          .lastSynchronizerOffsetBeforeOrAt(
            fixture.lsid,
            Offset.tryFromLong(computedSafeOffset),
          )
          .futureValueUS
          .value
          .recordTime
          .pipe(t => CantonTimestamp.assertFromInstant(t.toInstant))

      safeTimestamp should be > targetTs

      (computedSafeOffset, safeTimestamp)
    }
  }

  "When an LSU is done" in { implicit env =>
    import env.*

    fixture = fixtureWithDefaults()

    // Activity before LSU
    participant1.health.ping(participant2)
    alice = participant1.parties.enable("Alice")
    bank = participant2.parties.enable("Bank")

    iou1 = IouSyntax.createIou(participant2)(bank, alice, 1.0)
    IouSyntax.archive(participant2)(iou1, bank)
    iou2 = IouSyntax.createIou(participant2)(bank, alice, 2.0)

    clue("assert that prunable stores are not empty") {
      getPhysicalState(participant2, fixture.currentPsid).sequencedEventStore
        .sequencedEvents()
        .futureValueUS should not be empty

      getPhysicalState(participant2, fixture.currentPsid).requestJournalStore
        .size()
        .futureValueUS should be > 0

      // Ensure an ACS commitment is computed
      environment.simClock.value.advance(Duration.ofSeconds(5))
      participant1.health.ping(participant2)

      eventually() {
        getLogicalState(participant2, fixture.lsid).acsCommitmentStore
          .searchComputedBetween(CantonTimestamp.Epoch, fixture.upgradeTime)
          .futureValueUS should not be empty
      }
    }

    clue("perform lsu") {
      performSynchronizerNodesLsu(fixture)
      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
      transferTraffic()
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
      }

      waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)
      oldSynchronizerNodes.all.stop()
    }
  }

  "And activity happens after LSU" in { implicit env =>
    import env.*

    // ACS commitments are exchanged and upgrade time is clean (no outstanding ACS commitments)
    participant1.health.ping(participant2)
    eventually() {
      noOutstandingCommitments(participant1, upgradeTime) shouldBe upgradeTime
      noOutstandingCommitments(participant1, upgradeTime) shouldBe upgradeTime
    }

    clue("archive Iou 2.0") {
      val aliceIou =
        participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(alice)

      val bob = participant1.parties.enable("Bob")

      participant1.ledger_api.javaapi.commands
        .submit(Seq(alice), aliceIou.id.exerciseTransfer(bob.toLf).commands().asScala.toSeq)

      val bobIou = participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(bob)

      participant2.ledger_api.javaapi.commands
        .submit(Seq(bank), bobIou.id.exerciseArchive().commands().asScala.toSeq)
    }

    IouSyntax.createIou(participant2)(bank, alice, 3.0)
  }

  "Pruning" should {
    "work correctly" in { implicit env =>
      import env.*

      val allSynchronizerParameters = participant1.underlying.value.sync
        .lookupTopologyClient(fixture.newPsid)
        .value
        .awaitSnapshot(fixture.upgradeTime)
        .futureValueUS
        .listDynamicSynchronizerParametersChanges()
        .futureValueUS

      // sequencing time of the tick that triggers LSU
      val oldSynchronizerLastSequencingTime =
        fixture.upgradeTime + SequencerUtils.timeOffsetPastSynchronizerUpgrade(
          fixture.upgradeTime,
          allSynchronizerParameters,
        )

      /*
        We want to wait until the safe to prune offset corresponds to a sequencing time
        that is at least oldSynchronizerLastSequencingTime. This allows to completely prune the old
        sequenced event store.
       */
      val (safeOffset, safeTimestamp) = waitP2SafeTsReaches(oldSynchronizerLastSequencingTime)

      // Check that there is data that is prunable on the new sequencedEventStore
      clue("prunable stores on the new synchronizer have prunable data") {
        getPhysicalState(participant2, fixture.newPsid).sequencedEventStore
          .sequencedEvents()
          .futureValueUS
          .filter(_.timestamp < safeTimestamp) should not be empty

        getPhysicalState(participant2, fixture.newPsid).requestJournalStore
          .lastRequestTimestampBeforeOrAt(safeTimestamp)
          .futureValueUS shouldBe defined

        getLogicalState(participant2, fixture.lsid).acsCommitmentStore
          .searchComputedBetween(fixture.upgradeTime, safeTimestamp)
          .futureValueUS
          // periods completely before safeTimestamp
          .filter { case (period, _, _) =>
            period.toInclusive.forgetRefinement < safeTimestamp
          } should not be empty
      }

      participant2.pruning.prune(safeOffset)

      clue("old physical stores are pruned") {
        getPhysicalState(participant2, fixture.currentPsid).sequencedEventStore
          .sequencedEvents()
          .futureValueUS shouldBe empty

        getPhysicalState(participant2, fixture.currentPsid).requestJournalStore
          .size()
          .futureValueUS shouldBe 0
      }

      clue("logical data on the old synchronizer is pruned") {
        getLogicalState(participant2, fixture.lsid).acsCommitmentStore
          .searchComputedBetween(CantonTimestamp.Epoch, fixture.upgradeTime)
          .futureValueUS shouldBe empty
      }

      clue("new physical stores are pruned") {
        getPhysicalState(participant2, fixture.newPsid).sequencedEventStore
          .sequencedEvents()
          .futureValueUS
          .filter(_.timestamp < safeTimestamp) shouldBe empty

        getPhysicalState(participant2, fixture.newPsid).requestJournalStore
          .lastRequestTimestampBeforeOrAt(safeTimestamp)
          .futureValueUS shouldBe empty
      }

      clue("logical data on the new synchronizer is prune") {
        getLogicalState(participant2, fixture.lsid).acsCommitmentStore
          .searchComputedBetween(fixture.upgradeTime, safeTimestamp)
          .futureValueUS
          // periods completely before safeTimestamp
          .filter { case (period, _, _) =>
            period.toInclusive.forgetRefinement < safeTimestamp
          } shouldBe empty
      }
    }
  }

  "Journal garbage collection" should {
    "work correctly" in { implicit env =>
      import env.*

      val pcs2 = participant2.underlying.value.sync.syncPersistentStateManager.getAllLogical
        .get(daId)
        .value
        .activeContractStore

      val cid1 = LfContractId.assertFromString(iou1.id.contractId)
      val cid2 = LfContractId.assertFromString(iou2.id.contractId)

      // Entries are in the store
      pcs2.fetchStates(Seq(cid1, cid2)).futureValueUS.fmap(_.status) shouldBe
        Map(cid1 -> ActiveContractStore.Archived, cid2 -> ActiveContractStore.Archived)

      /*
        Time needs to advance by at least the recordTime+journalGarbageCollectionDelay.
        Timestamp needs to be become clean.
       */
      val targetTs = CantonTimestamp.Epoch.add(journalGarbageCollectionDelay.asJava).plusSeconds(15)
      environment.simClock.value.advanceTo(targetTs)
      waitForTargetTimeOnSequencer(sequencer2, targetTs, logger)
      waitP2SafeTsReaches(targetTs)

      eventually() {
        pcs2.fetchStates(Seq(cid1, cid2)).futureValueUS shouldBe empty
      }

      pcs2.fetchStates(Seq(cid1, cid2)).futureValueUS.foreach(println)
    }
  }
}
