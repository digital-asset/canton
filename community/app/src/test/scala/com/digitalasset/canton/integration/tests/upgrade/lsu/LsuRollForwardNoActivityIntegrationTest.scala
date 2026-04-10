// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import cats.syntax.option.*
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.synchronizer.sequencer.config.{
  LsuSequencingBoundsOverride,
  SequencerNodeConfig,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.GrpcConnection
import com.digitalasset.canton.version.ProtocolVersion
import io.scalaland.chimney.dsl.*
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.annotation.nowarn

/** The goal is to test the following scenario:
  *   - Synchronizer S1 has sequencers sequencer1 and sequencer2
  *   - LSU to synchronizer S2 with sequencers3 and sequencer4 is done
  *   - Some activity happens (Daml and topology transactions)
  *   - The synchronizer is considered broken
  *   - Roll forward to S3 with sequencer5 and sequencer6 is done
  *
  * This test is similar to [[LsuRollForwardIntegrationTest]] except there is no activity on the
  * broken synchronizer. As such, we export topology and traffic from S1.
  *
  * We also don't test as many edge cases on the participants.
  */
@nowarn("msg=dead code")
final class LsuRollForwardNoActivityIntegrationTest extends LsuBase with HasExecutionContext {

  override protected def testName: String = "lsu-roll-forward-no-activity"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(
        Set("sequencer1"),
        Set("sequencer2"),
        Set("sequencer3"),
      ),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val newOldMediators: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")

  private lazy val upgradeTime1: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private lazy val lsuSequencingBoundsOverride =
    LsuSequencingBoundsOverride(
      lowerBoundSequencingTimeExclusive = upgradeTime1,
      upgradeTime = upgradeTime1,
    )

  override protected lazy val upgradeTime: CantonTimestamp = throw new IllegalAccessException(
    "Use upgradeTime1 and upgradeTime2 instead"
  )

  override protected def configTransforms: List[ConfigTransform] = {
    val allNewNodes = Set(2, 3).flatMap(idx => Seq(s"sequencer$idx", s"mediator$idx"))

    val setLsuSequencingBoundsOverride: SequencerNodeConfig => SequencerNodeConfig =
      _.focus(_.parameters.lsuRepair.lsuSequencingBoundsOverride)
        .replace(lsuSequencingBoundsOverride.some)

    List(
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
      ConfigTransforms.updateSequencerConfig("sequencer3")(setLsuSequencingBoundsOverride),
    ) ++ ConfigTransforms.enableAlphaVersionSupport
  }

  private var fixture1: Fixture = _
  private var fixture2: Fixture = _

  private var bank: PartyId = _
  private var alice: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S3M3_Config
      /*
      The test is made slightly more robust by controlling explicitly which nodes are running.
      This allows to ensure that correct synchronizer nodes are used for each LSU.
       */
      .withManualStart
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        participants.local.start()

        participant1.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer1))

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)

        participants.all.dars.upload(CantonExamplesPath)
        participant1.health.ping(participant1)
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
        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1)),
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2)),
        newOldNodesResolution = Map(
          "sequencer2" -> "sequencer1",
          "mediator2" -> "mediator1",
        ),
        oldSynchronizerOwners = synchronizerOwners1,
        newPV = ProtocolVersion.dev,
        newSerial = daId.serial.increment.toNonNegative,
      )

      bank = participant1.parties.enable("Bank")
      alice = participant1.parties.enable("Alice")
      IouSyntax.createIou(participant1)(bank, alice, 1.0).discard

      fixture1.newSynchronizerNodes.all.start()
      performSynchronizerNodesLsu(fixture1)

      environment.simClock.value.advanceTo(upgradeTime1.immediateSuccessor)
      transferTraffic(Some(fixture1))

      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture1.newPsid)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture1.currentPsid)) shouldBe false
      }

      waitForTargetTimeOnSequencer(sequencer1, environment.clock.now, logger)
    }

    "prepare nodes of the recovery synchronizer (S3)" in { implicit env =>
      import env.*

      fixture2 = Fixture(
        currentPsid = fixture1.newPsid,
        upgradeTime = null, // it should not be used
        // read topology state from S1
        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1)),
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer3), Seq(mediator3)),
        newOldNodesResolution = Map(
          "sequencer3" -> "sequencer1",
          "mediator3" -> "mediator1",
        ),
        oldSynchronizerOwners = null, // it should not be used
        newPV = testedProtocolVersion, // potentially a downgrade
        newSerial = fixture1.newPsid.serial.increment.toNonNegative,
      )

      fixture2.newSynchronizerNodes.all.start()

      migrateSynchronizerNodes(fixture2, ignorePsidCheck = true)
      transferTraffic(Some(fixture2))
    }

    "P1 performs manual upgrade" in { implicit env =>
      import env.*

      val upgradeTime1 = cleanSynchronizerIndex(participant1).recordTime

      participant1.synchronizers.perform_manual_lsu(
        currentPsid = fixture2.currentPsid,
        successorPsid = fixture2.newPsid,
        upgradeTime = upgradeTime1.some,
        sequencerSuccessors =
          Map(sequencer1.id -> sequencer3.sequencerConnection.transformInto[GrpcConnection]),
      )

      participants.all.forall(_.synchronizers.is_connected(fixture2.newPsid)) shouldBe true
    }

    "activity on the new recovery synchronizer and sanity checks" in { implicit env =>
      import env.*

      waitForTargetTimeOnSequencer(sequencer3, environment.clock.now, logger)

      val aliceIou = participant1.ledger_api.javaapi.state.acs.await(Iou.COMPANION)(alice)

      participant1.ledger_api.javaapi.commands
        .submit(
          Seq(alice),
          Seq(
            aliceIou.id
              .exerciseTransfer(participant1.adminParty.toProtoPrimitive)
              .commands()
              .loneElement
          ),
        )
    }
  }
}
