// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.admin.api.client.data.RegisteredSynchronizer.Status
import com.digitalasset.canton.admin.api.client.data.{
  KnownPhysicalSynchronizerId,
  RegisteredSynchronizer,
  SequencerConnection,
  SynchronizerConnectionConfig,
  SynchronizerPredecessor,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.Fixture
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuEndToEndIntegrationTest.ExpectedRegisteredSynchronizers
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.jdk.CollectionConverters.*

/*
 * This test is used to test the logical synchronizer upgrade.
 * It uses 3 participants, 2 sequencers and 2 mediators.
 */
sealed trait LsuEndToEndIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-end-to-end"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = if (useStaticTime) {
    CantonTimestamp.Epoch.plusSeconds(30)
  } else {
    CantonTimestamp.now().plusSeconds(15)
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  "Logical synchronizer upgrade" should {
    "work end-to-end" in { implicit env =>
      import env.*

      participant1.health.ping(participant2)

      val alice = participant1.parties.enable("Alice")
      val bank = participant2.parties.enable("Bank")
      IouSyntax.createIou(participant2)(bank, alice).discard

      val fixture = fixtureWithDefaults()
      performSynchronizerNodesLsu(fixture)

      val expectedRegisteredSynchronizers = clue("Values returned by list registered before LSU") {
        val (currentConfig, currentConfiguredPsid, _) =
          participant1.synchronizers.list_registered().loneElement

        val expectedRegisteredSynchronizers =
          ExpectedRegisteredSynchronizers(currentConfig, fixture)

        currentConfiguredPsid.toOption.value shouldBe fixture.currentPsid

        eventually() {
          participant1.synchronizers.list_all_registered() should contain theSameElementsAs Seq(
            expectedRegisteredSynchronizers.currentRegisteredSynchronizerBeforeLsu,
            expectedRegisteredSynchronizers.newRegisteredSynchronizerBeforeLsu,
          )
        }

        expectedRegisteredSynchronizers
      }

      environment.simClock.foreach(_.advanceTo(upgradeTime.immediateSuccessor))
      transferTraffic()

      eventually() {
        environment.simClock.foreach(_.advance(Duration.ofSeconds(1)))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture.currentPsid)) shouldBe false
      }

      clue("Values returned by list registered after LSU") {
        val (_, currentConfiguredPsid, _) =
          participant1.synchronizers.list_registered().loneElement

        currentConfiguredPsid.toOption.value shouldBe fixture.newPsid

        eventually() {
          participant1.synchronizers.list_all_registered() should contain theSameElementsAs Seq(
            expectedRegisteredSynchronizers.currentRegisteredSynchronizerBeforeLsu
              .copy(isConnected = false, status = Status.LsuSource),
            expectedRegisteredSynchronizers.newRegisteredSynchronizerBeforeLsu
              .copy(isConnected = true, status = Status.Active),
          )
        }
      }

      oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer2, upgradeTime.immediateSuccessor, logger)

      /*
      We do several ping, disconnect, reconnect because reconnect comes with crash-recovery
      and acknowledgements to the sequencers.
       */
      (0 to 2).foreach { i =>
        logger.debug(s"Round $i of ping")
        participant1.health.ping(participant2)
        participants.all.synchronizers.disconnect_all()
        participants.all.synchronizers.reconnect_all()
      }

      val aliceIou =
        participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(alice)
      val bob = participant1.parties.enable("Bob")

      participant1.ledger_api.javaapi.commands
        .submit(Seq(alice), aliceIou.id.exerciseTransfer(bob.toLf).commands().asScala.toSeq)

      val bobIou = participant1.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(bob)

      // Ensure that everything still works, even when a participant (3) had done no activity prior to LSU
      participant3.health.ping(participant1)
      val charlie = participant3.parties.enable("Charlie")
      participant1.ledger_api.javaapi.commands
        .submit(Seq(bob), bobIou.id.exerciseTransfer(charlie.toLf).commands().asScala.toSeq)
      val charlieIou =
        participant3.ledger_api.javaapi.state.acs.await(IouSyntax.modelCompanion)(charlie)

      participant2.ledger_api.javaapi.commands
        .submit(Seq(bank), charlieIou.id.exerciseArchive().commands().asScala.toSeq)

      // Subsequent call should be successful
      participant1.underlying.value.sync
        .performLsu(daId, fixture.synchronizerSuccessor)
        .futureValueUS
        .value shouldBe ()
    }
  }
}

private object LsuEndToEndIntegrationTest {
  import org.scalatest.OptionValues.*

  final case class ExpectedRegisteredSynchronizers(
      initialSynchronizerConnectionConfig: SynchronizerConnectionConfig,
      fixture: Fixture,
  )(implicit env: TestConsoleEnvironment) {
    import env.*

    val currentRegisteredSynchronizerBeforeLsu = RegisteredSynchronizer(
      config = initialSynchronizerConnectionConfig,
      status = Status.Active,
      psid = KnownPhysicalSynchronizerId(fixture.currentPsid),
      predecessor = None,
      isConnected = true,
    )

    val newSequencerConnections: NonEmpty[Map[SequencerAlias, SequencerConnection]] = NonEmpty
      .from(
        Map(
          sequencer1.sequencerAlias -> sequencer2.sequencerConnection
            .copy(sequencerAlias = sequencer1.sequencerAlias, sequencerId = Some(sequencer1.id))
        )
      )
      .value

    val newRegisteredSynchronizerBeforeLsu = RegisteredSynchronizer(
      config = initialSynchronizerConnectionConfig
        .focus(_.sequencerConnections.aliasToConnection)
        .replace(newSequencerConnections)
        .focus(_.synchronizerId)
        .replace(Some(fixture.newPsid)),
      status = Status.LsuTarget,
      psid = KnownPhysicalSynchronizerId(fixture.newPsid),
      predecessor = Some(
        SynchronizerPredecessor(
          psid = fixture.currentPsid,
          upgradeTime = fixture.upgradeTime,
          isLateUpgrade = false,
        )
      ),
      isConnected = false,
    )
  }
}

final class LsuEndToEndSimClockIntegrationTest extends LsuEndToEndIntegrationTest

final class LsuEndToEndWallClockIntegrationTest extends LsuEndToEndIntegrationTest {
  override protected val useStaticTime: Boolean = false
}
