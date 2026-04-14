// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.syntax.option.*
import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  CommandFailure,
  InstanceReference,
  LocalParticipantReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.{
  Fixture,
  getLsuSequencingTestMetricValues,
}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.integration.{ConfigTransforms, *}
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.synchronizer.sequencer.config.{
  LsuSequencingBoundsOverride,
  SequencerNodeConfig,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.GrpcConnection
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{HasExecutionContext, UniquePortGenerator}
import io.scalaland.chimney.dsl.*
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.annotation.nowarn
import scala.util.chaining.*

/*
The goal is to test the following scenario:
  - Synchronizer S1 has sequencers sequencer1 and sequencer2
  - LSU to synchronizer S2 with sequencers3 and sequencer4 is done
  - Some activity happens (Daml and topology transactions)
  - The synchronizer is considered broken
  - Roll forward to S3 with sequencer5 and sequencer6 is done

Topology:
  - P1 connected to sequencer1 and sequencer2 (threshold=1)
  - P2 connected to sequencer1 and sequencer2 (threshold=2)
  - P3 connected to sequencer1 and sequencer2 (threshold=2)

Important notes:
  - State retrieval from the sequencers on S2:
    - SequencerLsuStateRequest.timestamp has to be set when exporting topology
      Note: The sequencer will wait until this time has elapsed on the synchronizer using the time tracker.
      In particular, it means that globalMaxSequencingTimeExclusive cannot be used.
      Suggestion is to use the sequencing time of the latest sequenced topology transaction.

    - GetLsuTrafficControlStateRequest.timestamp has to be set when exporting the traffic state
      Note: the timestamp must correspond to a fully processed message on the sequencer.
      Suggestion is to use the sequencing time of the latest sequenced topology transaction (and potentially
      giving a bit of free traffic to users).

  - Initialization of the sequencers on S3:
    - InitializeSequencerFromLsuPredecessorRequest.ignore_psid_check should be set to true
    - lsu-sequencing-bounds-override config has to be set

  - Participant manual upgrade
    - upgrade time:
      Should either be empty or set to sequencing time of the latest message that should be processed by the participant.
      If defined and the perform_manual_lsu returns error
         Synchronizer index is past upgrade time
      then a second call should be done with empty upgrade time.

Flow to guarantee time monotonicity:
  - Agree on a max sequencing time Tmax for the broken synchronizer S2
  - Have all the sequencer nodes of S2 set Tmax in the config and restart:
      canton.sequencers.sequencer.parameters.lsu-repair.global-max-sequencing-time-exclusive=Tmax
    Note: all sequencers must apply this config before this time is reached on the synchronizer
  - Agree on values for the LSU sequencing time override for the sequencers of S3
  - Have all the sequencer nodes of S3 set these values
      canton.sequencers.sequencer.parameters.lsu-repair.lsu-sequencing-bounds-override
 */
@nowarn("msg=dead code")
abstract class LsuRollForwardIntegrationTest extends LsuBase with HasExecutionContext {

  override protected def testName: String =
    s"lsu-roll-forward-" + (if (isOnline) "online" else "offline")

  /** Whether the upgradability tests are done online (connected to the synchronizer) or offline
    * (disconnected to the synchronizer). In practice, this depends on whether the upgrade time is
    * specified or note. See documentation of
    * [[com.digitalasset.canton.participant.sync.OfflineManualLogicalSynchronizerUpgrade]] and
    * [[com.digitalasset.canton.participant.sync.OnlineManualLogicalSynchronizerUpgrade]]
    */
  def isOnline: Boolean

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(
        Set("sequencer1", "sequencer2"),
        Set("sequencer3", "sequencer4"),
        Set("sequencer5", "sequencer6"),
      ),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val newOldMediators: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")

  private lazy val upgradeTime1: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  /*
  Ideally, we should compute the values during the test. However, changing the config of a running node
  in our integration test is difficult. Hence, we pick values that are sufficiently in the future.
   */
  private lazy val maxSequencingTime = CantonTimestamp.Epoch.plusSeconds(599)
  private lazy val lsuSequencingBoundsOverride =
    LsuSequencingBoundsOverride(
      lowerBoundSequencingTimeExclusive = maxSequencingTime.plusSeconds(1),
      upgradeTime = maxSequencingTime.plusSeconds(1).plusSeconds(30),
    )

  // Sequencing time of the latest topology message on the broken synchronizer
  private var lastTopologyActivityS2: CantonTimestamp = _

  override protected lazy val upgradeTime: CantonTimestamp = throw new IllegalAccessException(
    "Use upgradeTime1 and upgradeTime2 instead"
  )

  override protected def configTransforms: List[ConfigTransform] = {
    val allNewNodes = Set(3, 4, 5, 6).flatMap(idx => Seq(s"sequencer$idx", s"mediator$idx"))

    val setLsuSequencingBoundsOverride: SequencerNodeConfig => SequencerNodeConfig =
      _.focus(_.parameters.lsuRepair.lsuSequencingBoundsOverride)
        .replace(lsuSequencingBoundsOverride.some)

    List(
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
      ConfigTransforms.updateSequencerConfig("sequencer5")(setLsuSequencingBoundsOverride),
      ConfigTransforms.updateSequencerConfig("sequencer6")(setLsuSequencingBoundsOverride),
      ConfigTransforms.updateSequencerConfig("sequencer3")(
        _.focus(_.parameters.lsuRepair.globalMaxSequencingTimeExclusive)
          .replace(Some(maxSequencingTime))
      ),
      ConfigTransforms.updateSequencerConfig("sequencer4")(
        _.focus(_.parameters.lsuRepair.globalMaxSequencingTimeExclusive)
          .replace(Some(maxSequencingTime))
      ),
    ) ++ ConfigTransforms.enableAlphaVersionSupport
  }

  private var fixture1: Fixture = _
  private var fixture2: Fixture = _

  private var bank: PartyId = _
  private var alice: PartyId = _
  private var bob: PartyId = _
  private var wally: PartyId = _

  private var upgradeTime3: CantonTimestamp = _ // when P3 performs manual LSU

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S6M6_Config
      /*
      The test is made slightly more robust by controlling explicitly which nodes are running.
      This allows to ensure that correct synchronizer nodes are used for each LSU.
       */
      .withManualStart
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S2M2)
      }
      .addConfigTransforms(configTransforms*)
      .addConfigTransforms(
        _.focus(_.monitoring.metrics)
          .replace(
            MetricsConfig(
              qualifiers = Seq[MetricQualification](MetricQualification.Debug),
              reporters = Seq(MetricsReporterConfig.Prometheus(port = UniquePortGenerator.next)),
            )
          )
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.start()

        participant1.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), 1)
        )
        participant2.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), 2)
        )
        participant3.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), 2)
        )

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)

        participants.all.dars.upload(CantonExamplesPath)
        participant1.health.ping(participant2)
        participant1.health.ping(participant3)
      }

  private def cleanSynchronizerIndex(
      p: LocalParticipantReference
  )(implicit env: TestConsoleEnvironment): SynchronizerIndex =
    p.underlying.value.sync.ledgerApiIndexer.asEval.value.ledgerApiStore.value
      .cleanSynchronizerIndex(env.daId.logical)
      .futureValueUS
      .value

  "LSU should allow roll forward" should {
    "perform first LSU" in { implicit env =>
      import env.*

      fixture1 = Fixture(
        currentPsid = daId,
        upgradeTime = upgradeTime1,
        oldSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer1, sequencer2), Seq(mediator1, mediator2)),
        newSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer3, sequencer4), Seq(mediator3, mediator4)),
        newOldNodesResolution = Map(
          "sequencer3" -> "sequencer1",
          "sequencer4" -> "sequencer2",
          "mediator3" -> "mediator1",
          "mediator4" -> "mediator2",
        ),
        oldSynchronizerOwners = synchronizerOwners1,
        newPV = ProtocolVersion.dev,
        newSerial = daId.serial.increment.toNonNegative,
      )

      bank = participant1.parties.enable("Bank")
      alice = participant2.parties.enable("Alice")
      bob = participant3.parties.enable("Bob")
      IouSyntax.createIou(participant1)(bank, alice, 1.0).discard
      IouSyntax.createIou(participant1)(bank, bob, 2.0).discard

      fixture1.newSynchronizerNodes.all.start()
      performSynchronizerNodesLsu(fixture1)

      environment.simClock.value.advanceTo(upgradeTime1.immediateSuccessor)
      transferTraffic(Some(fixture1))

      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture1.newPsid)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture1.currentPsid)) shouldBe false
      }

      waitForTargetTimeOnSequencer(sequencer3, environment.clock.now, logger)
      waitForTargetTimeOnSequencer(sequencer4, environment.clock.now, logger)
    }

    "some activity happens on S2" in { implicit env =>
      import env.*

      IouSyntax.createIou(participant1)(bank, alice, 3.0)
      wally = participant2.parties.enable("Wally")

      lastTopologyActivityS2 = participant2.topology.party_to_participant_mappings
        .list(fixture1.newPsid, filterParty = wally.toProtoPrimitive)
        .loneElement
        .context
        .sequenced
        .pipe(CantonTimestamp.assertFromInstant)
    }

    "prepare nodes of the recovery synchronizer (S3)" in { implicit env =>
      import env.*

      fixture2 = Fixture(
        currentPsid = fixture1.newPsid,
        upgradeTime = null, // it should not be used
        oldSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer3, sequencer4), Seq(mediator3, mediator4)),
        newSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer5, sequencer6), Seq(mediator5, mediator6)),
        newOldNodesResolution = Map(
          "sequencer5" -> "sequencer3",
          "sequencer6" -> "sequencer4",
          "mediator5" -> "mediator3",
          "mediator6" -> "mediator4",
        ),
        oldSynchronizerOwners = null, // it should not be used
        newPV = testedProtocolVersion, // potentially a downgrade
        newSerial = fixture1.newPsid.serial.increment.toNonNegative,
      )

      fixture2.newSynchronizerNodes.all.start()

      environment.simClock.value.advanceTo(maxSequencingTime)

      /*
      We cannot rely on the standard waitForTargetTimeOnSequencer: as soon as the sequencing time is past
      the max sequencing time, nothing gets sequenced (and so waitForTargetTimeOnSequencer does not complete)
       */
      eventually() {
        val ts = sequencer3.underlying.value.sequencer.sequencer.sequencingTime.futureValueUS.value
        ts should be >= maxSequencingTime
      }

      /*
      What we really want is restarting the sequencer (see comment below).
      Stopping the mediators and participants removes mediators and participants warning due to the sequencers being down.
       */
      mediator3.stop()
      mediator4.stop()
      participants.local.stop()

      /*
      When exporting the topology, the sequencer waits for the sequencerLsuStateTsOverride timestamp to be reached.
      The goal of the restart is to ensure that the waiting works even after a crash/restart.
       */
      sequencer4.stop()
      sequencer4.start()

      mediator3.start()
      mediator4.start()
      participants.local.start()

      migrateSynchronizerNodes(
        fixture2,
        ignorePsidCheck = true,
        sequencerLsuStateTsOverride = Some(lastTopologyActivityS2),
      )
    }

    "LSU sequencing test can be performed on the new synchronizer" in { implicit env =>
      import env.*

      environment.simClock.value.advanceTo(
        lsuSequencingBoundsOverride.lowerBoundSequencingTimeExclusive.immediateSuccessor
      )

      eventually() {
        environment.simClock.value.advance(Duration.ofMillis(10))
        sequencer5.setup.test_lsu_sequencing(NonNegativeInt.zero)
        val m = getLsuSequencingTestMetricValues(mediator5)
        m.get(sequencer5.id).value should be > 0L
      }
    }

    "manual upgrade fails if synchronizer index is lower than upgrade time" in { implicit env =>
      assume(isOnline)

      import env.*

      val upgradeTime = cleanSynchronizerIndex(participant1).recordTime.minusSeconds(1)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers.perform_manual_lsu(
          currentPsid = fixture2.currentPsid,
          successorPsid = fixture2.newPsid,
          upgradeTime = upgradeTime.some, // is smaller than the clean synchronizer index
          sequencerSuccessors =
            Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
        ),
        _.errorMessage should include("Synchronizer index is past upgrade time"),
      )
    }

    "Traffic can bet set on the new synchronizer" in { _ =>
      transferTraffic(
        Some(fixture2),
        trafficTsOverride = Some(lastTopologyActivityS2),
      )
    }

    "manual upgrade fails if the list of successors is empty" in { implicit env =>
      import env.*

      val upgradeTime1 = cleanSynchronizerIndex(participant1).recordTime
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers.perform_manual_lsu(
          currentPsid = fixture2.currentPsid,
          successorPsid = fixture2.newPsid,
          upgradeTime = Option.when(isOnline)(upgradeTime1),
          sequencerSuccessors = Map(), // should not be empty
        ),
        _.errorMessage should include("No sequencer successor was found"),
      )
    }

    "manual upgrade fails if the list of successors is smaller than the threshold" in {
      implicit env =>
        import env.*

        val upgradeTime2 = cleanSynchronizerIndex(participant2).recordTime

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant2.synchronizers.perform_manual_lsu(
            currentPsid = fixture2.currentPsid,
            successorPsid = fixture2.newPsid,
            upgradeTime = Option.when(isOnline)(upgradeTime2),
            sequencerSuccessors =
              Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
          ),
          _.errorMessage should include(
            "Not enough successors sequencers (1) to meet the sequencer threshold (2)"
          ),
        )
    }

    "(P1, P2) manual upgrade succeeds if the list of successors contains at least threshold elements" in {
      implicit env =>
        import env.*

        val upgradeTime1 = cleanSynchronizerIndex(participant1).recordTime

        loggerFactory.assertLogs(
          participant1.synchronizers.perform_manual_lsu(
            currentPsid = fixture2.currentPsid,
            successorPsid = fixture2.newPsid,
            upgradeTime = Option.when(isOnline)(upgradeTime1),
            sequencerSuccessors =
              Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
          ),
          _.warningMessage should include(
            s"Missing successor information for the following sequencers: Set($sequencer2). They will be removed from the pool of sequencers."
          ),
        )

        val upgradeTime2 = cleanSynchronizerIndex(participant2).recordTime

        participant2.synchronizers.perform_manual_lsu(
          currentPsid = fixture2.currentPsid,
          successorPsid = fixture2.newPsid,
          upgradeTime = Option.when(isOnline)(upgradeTime2),
          sequencerSuccessors = Map(
            sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection],
            sequencer2.id -> sequencer6.sequencerConnection.transformInto[GrpcConnection],
          ),
        )
    }

    "(P3) Threshold can be decreased to work around missing connections" in { implicit env =>
      import env.*

      upgradeTime3 = cleanSynchronizerIndex(participant3).recordTime

      def performManualLsu() = participant3.synchronizers.perform_manual_lsu(
        currentPsid = fixture2.currentPsid,
        successorPsid = fixture2.newPsid,
        upgradeTime = Option.when(isOnline)(upgradeTime3),
        sequencerSuccessors =
          Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        performManualLsu(),
        _.errorMessage should include(
          "Not enough successors sequencers (1) to meet the sequencer threshold (2)"
        ),
      )

      // Decrease the threshold from 2 to 1...
      participant3.synchronizers.modify(
        daName,
        _.focus(_.sequencerConnections.sequencerTrustThreshold).replace(PositiveInt.one),
      )

      // ... so that manual LSU succeeds
      loggerFactory.assertLogs(
        performManualLsu(),
        _.warningMessage should include(
          s"Missing successor information for the following sequencers: Set($sequencer2). They will be removed from the pool of sequencers."
        ),
      )

      // idempotency test
      participant3.synchronizers.perform_manual_lsu(
        currentPsid = fixture2.currentPsid,
        successorPsid = fixture2.newPsid,
        upgradeTime = Option.when(isOnline)(upgradeTime3),
        sequencerSuccessors =
          Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
      )
    }

    "activity on the new recovery synchronizer and sanity checks" in { implicit env =>
      import env.*

      environment.simClock.value.advanceTo(
        lsuSequencingBoundsOverride.upgradeTime.immediateSuccessor
      )

      waitForTargetTimeOnSequencer(sequencer5, environment.clock.now, logger)

      participants.all.forall(_.synchronizers.is_connected(fixture2.newPsid)) shouldBe true

      withClue("topology activity on S2 is not lost") {
        def ensureWallyCanBeFound(n: InstanceReference): Unit =
          n.topology.party_to_participant_mappings
            .list(fixture2.newPsid, filterParty = wally.toProtoPrimitive)
            .loneElement
            .discard

        (participants.all ++ fixture2.newSynchronizerNodes.all).foreach(ensureWallyCanBeFound)

        val expectedParties =
          (Seq(alice, bob, bank, wally) ++ participants.all.map(_.adminParty))
            .map(_.toProtoPrimitive)

        forAll(participants.all) {
          _.ledger_api.updates
            .topology_transactions(PositiveInt.tryCreate(7))
            .flatMap(
              _.participantAuthorizationAdded.map(_.partyId)
            ) should contain theSameElementsAs expectedParties
        }
      }

      val aliceIou = participant2.ledger_api.javaapi.state.acs.await(Iou.COMPANION)(alice)
      val bobIou = participant3.ledger_api.javaapi.state.acs.await(Iou.COMPANION)(bob)

      participant2.ledger_api.javaapi.commands
        .submit(
          Seq(alice),
          Seq(aliceIou.id.exerciseTransfer(bob.toProtoPrimitive).commands().loneElement),
        )

      participant3.ledger_api.javaapi.commands
        .submit(
          Seq(bob),
          Seq(bobIou.id.exerciseTransfer(alice.toProtoPrimitive).commands().loneElement),
        )

      // idempotency after some activity
      participant3.synchronizers.perform_manual_lsu(
        currentPsid = fixture2.currentPsid,
        successorPsid = fixture2.newPsid,
        upgradeTime = Option.when(isOnline)(upgradeTime3),
        sequencerSuccessors =
          Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
      )

      // idempotency using recent timestamp
      participant3.synchronizers.perform_manual_lsu(
        currentPsid = fixture2.currentPsid,
        successorPsid = fixture2.newPsid,
        upgradeTime = cleanSynchronizerIndex(participant3).recordTime.some,
        sequencerSuccessors =
          Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
      )
    }
  }
}

final class LsuOnlineRollForwardIntegrationTest extends LsuRollForwardIntegrationTest {
  override def isOnline: Boolean = true
}

final class LsuOfflineRollForwardIntegrationTest extends LsuRollForwardIntegrationTest {
  override def isOnline: Boolean = false
}
