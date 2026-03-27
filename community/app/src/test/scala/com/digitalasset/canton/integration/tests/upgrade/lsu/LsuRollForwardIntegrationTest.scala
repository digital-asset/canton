// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.syntax.option.*
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.topology.transaction.GrpcConnection
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import io.scalaland.chimney.dsl.*
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.annotation.nowarn

/*
The goal is to test the following scenario:
  - Synchronizer S1 has sequencers sequencer1 and sequencer2
  - LSU to synchronizer S2 with sequencers3 and sequencer4 is done
  - Some activity happens
  - The synchronizer is considered broken
  - Roll forward to S3 with sequencer5 and sequencer6 is done

Topology:
  - P1 connected to sequencer1 and sequencer2 (threshold=1)
  - P2 connected to sequencer1 and sequencer2 (threshold=2)
  - P3 connected to sequencer1 and sequencer2 (threshold=2)

Important notes:
  - InitializeSequencerFromLsuPredecessorRequest.ignore_psid_check should be set to true
  - upgrade time:
    Should either be empty or set to sequencing time of the latest message that should be processed by the participant.
    If defined and the perform_manual_lsu returns error
       Synchronizer index is past upgrade time
    then a second call should be done with empty upgrade time.

Limitations:
  - Genesis topology of S3 is the topology at upgrade time from S1 -> S2. TODO(#31526) Remove this limitation
  - S3 is currently broken. TODO(#31526) Remove this limitation
 */
@nowarn("msg=dead code")
final class LsuRollForwardIntegrationTest extends LsuBase with HasExecutionContext {

  override protected def testName: String = "lsu-roll-forward"

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

  override protected lazy val upgradeTime: CantonTimestamp = throw new IllegalAccessException(
    "Use upgradeTime1 and upgradeTime2 instead"
  )

  override protected def configTransforms: List[ConfigTransform] = {
    val allNewNodes = Set(3, 4, 5, 6).flatMap(idx => Seq(s"sequencer$idx", s"mediator$idx"))

    List(
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
    ) ++ ConfigTransforms.enableAlphaVersionSupport
  }

  private var fixture1: Fixture = _
  private var fixture2: Fixture = _

  private var bank: PartyId = _
  private var alice: PartyId = _
  private var bob: PartyId = _

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
      p: LocalParticipantReference,
      lsid: SynchronizerId,
  ): SynchronizerIndex =
    p.underlying.value.sync.ledgerApiIndexer.asEval.value.ledgerApiStore.value
      .cleanSynchronizerIndex(lsid)
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

      IouSyntax.createIou(participant1)(bank, alice, 3.0)
    }

    "prepare nodes of the recovery synchronizer (S3)" in { implicit env =>
      import env.*

      fixture2 = Fixture(
        currentPsid = fixture1.newPsid,
        upgradeTime = null, // it should not be used
        // TODO(#31526) Allow to grab topology from S2 instead of S1
        oldSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer1, sequencer2), Seq(mediator1, mediator2)),
        newSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer5, sequencer6), Seq(mediator5, mediator6)),
        newOldNodesResolution = Map(
          "sequencer5" -> "sequencer1",
          "sequencer6" -> "sequencer2",
          "mediator5" -> "mediator1",
          "mediator6" -> "mediator2",
        ),
        oldSynchronizerOwners = null, // it should not be used
        newPV = testedProtocolVersion, // potentially a downgrade
        newSerial = fixture1.newPsid.serial.increment.toNonNegative,
      )

      fixture2.newSynchronizerNodes.all.start()
      migrateSynchronizerNodes(fixture2, ignorePsidCheck = true)
    }

    "manual upgrade fails if synchronizer index is lower than upgrade time " in { implicit env =>
      import env.*

      val t1 = environment.clock.now

      IouSyntax.createIou(participant1)(bank, alice, 4.0)

      eventually() {
        cleanSynchronizerIndex(participant1, daId).recordTime should be > t1
      }

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers.perform_manual_lsu(
          currentPsid = fixture2.currentPsid,
          successorPsid = fixture2.newPsid,
          upgradeTime = t1.some, // is smaller than the clean synchronizer index
          sequencerSuccessors =
            Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
        ),
        _.errorMessage should include("Synchronizer index is past upgrade time"),
      )
    }

    "manual upgrade fails if the list of successors is empty" in { implicit env =>
      import env.*

      val upgradeTime1 = cleanSynchronizerIndex(participant1, daId).recordTime
      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.synchronizers.perform_manual_lsu(
          currentPsid = fixture2.currentPsid,
          successorPsid = fixture2.newPsid,
          upgradeTime = upgradeTime1.some,
          sequencerSuccessors = Map(), // should not be empty
        ),
        _.errorMessage should include("No sequencer successor was found"),
      )
    }

    "manual upgrade fails if the list of successors is smaller than the threshold" in {
      implicit env =>
        import env.*

        val upgradeTime2 = cleanSynchronizerIndex(participant2, daId).recordTime

        loggerFactory.assertThrowsAndLogs[CommandFailure](
          participant2.synchronizers.perform_manual_lsu(
            currentPsid = fixture2.currentPsid,
            successorPsid = fixture2.newPsid,
            upgradeTime = upgradeTime2.some,
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

        val upgradeTime1 = cleanSynchronizerIndex(participant1, daId).recordTime

        loggerFactory.assertLogs(
          participant1.synchronizers.perform_manual_lsu(
            currentPsid = fixture2.currentPsid,
            successorPsid = fixture2.newPsid,
            upgradeTime = upgradeTime1.some,
            sequencerSuccessors =
              Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
          ),
          _.warningMessage should include(
            s"Missing successor information for the following sequencers: Set($sequencer2). They will be removed from the pool of sequencers."
          ),
        )

        val upgradeTime2 = cleanSynchronizerIndex(participant2, daId).recordTime

        participant2.synchronizers.perform_manual_lsu(
          currentPsid = fixture2.currentPsid,
          successorPsid = fixture2.newPsid,
          upgradeTime = upgradeTime2.some,
          sequencerSuccessors = Map(
            sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection],
            sequencer2.id -> sequencer6.sequencerConnection.transformInto[GrpcConnection],
          ),
        )
    }

    "(P3) Threshold can be decreased to work around missing connections" in { implicit env =>
      import env.*

      val upgradeTime3 = cleanSynchronizerIndex(participant3, daId).recordTime

      def performManualLsu() = participant3.synchronizers.perform_manual_lsu(
        currentPsid = fixture2.currentPsid,
        successorPsid = fixture2.newPsid,
        upgradeTime = upgradeTime3.some,
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
        upgradeTime = upgradeTime3.some,
        sequencerSuccessors =
          Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
      )
    }

    /*
     TODO(#31526) enable this test case

     "activity on the new recovery synchronizer and sanity checks" in { implicit env =>
      import env.*

      val latestUpgradeTime = participants.local
        .map { p =>
          synchronizerConfig(p, fixture2.newPsid).predecessor.value.upgradeTime
        }
        .maxOption
        .value
      waitForTargetTimeOnSequencer(sequencer5, latestUpgradeTime, logger)

      participants.all.forall(_.synchronizers.is_connected(fixture2.newPsid)) shouldBe true

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

      participant3.synchronizers.perform_manual_lsu(
        currentPsid = fixture2.currentPsid,
        successorPsid = fixture2.newPsid,
        upgradeTime = upgradeTime3.some,
        sequencerSuccessors =
          Map(sequencer1.id -> sequencer5.sequencerConnection.transformInto[GrpcConnection]),
      )
    }
     */
  }
}
