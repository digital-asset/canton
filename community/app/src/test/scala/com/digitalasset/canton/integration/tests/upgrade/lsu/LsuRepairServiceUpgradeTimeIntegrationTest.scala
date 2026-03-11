// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import better.files.File
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import monocle.macros.syntax.lens.*

import java.time.Duration

/** This test ensures that the repair service can be used at upgrade time. We test an ACS import:
  * replicate Alice from p1 to p2.
  */
@UnstableTest // TODO(#31077)
final class LsuRepairServiceUpgradeTimeIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-repair-service-upgrade-time"

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
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.sequencerInfo)
          /*
          The first connect with the new synchronizer will timeout because the new sequencer is stopped and
          we want the failure to be fast. However, a value that is too low could make the test flaky (if the
          subsequent/successful connect is too slow).
           */
          .replace(NonNegativeDuration.ofSeconds(2))
      )
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        defaultEnvironmentSetup()

        participants.all.dars.upload(CantonExamplesPath)
      }

  "Repair service" should {
    "can be used at LSU/upgrade time" in { implicit env =>
      import env.*

      val aliceAcs = File.newTemporaryFile(prefix = s"$testName-alice-acs")
      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)

      val alice = participant1.parties.enable("Alice")
      val bank = participant1.parties.enable("Bank")
      IouSyntax.createIou(participant1)(bank, alice, amount = 1.0)

      val ledgerEndP1 = participant1.ledger_api.state.end()

      Seq(participant1, participant2).foreach(
        _.topology.party_to_participant_mappings
          .propose_delta(
            alice,
            adds = Seq((participant2.id, ParticipantPermission.Observation)),
            store = daId,
            requiresPartyToBeOnboarded = true,
          )
      )

      performSynchronizerNodesLsu(fixture)
      participant1.parties.export_party_acs(
        alice,
        daId,
        participant2.id,
        ledgerEndP1,
        aliceAcs.canonicalPath,
      )

      sequencer2.stop() // to prevent reconnect to the synchronizer
      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      // Ensure LSU is done, which ensures that record time of ACS import will be upgrade time
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))

        participant2.underlying.value.sync.synchronizerConnectionConfigStore
          .getActive(daId)
          .value
          .configuredPSId
          .toOption
          .value shouldBe fixture.newPSId
      }

      val ledgerEndP2 = participant2.ledger_api.state.end()

      participant2.synchronizers.disconnect_all()
      participant2.parties.import_party_acsV2(aliceAcs.canonicalPath, daId)

      sequencer2.start()
      transferTraffic()

      // P1 should eventually connect
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participant1.synchronizers.is_connected(fixture.newPSId) shouldBe true
      }

      participant2.synchronizers.reconnect_all()
      participant2.synchronizers
        .list_connected()
        .loneElement
        .physicalSynchronizerId shouldBe fixture.newPSId

      participant2.ledger_api.state.acs.active_contracts_of_party(alice) should have size 1

      val createEvent = participant2.ledger_api.updates
        .transactions(
          Set(alice),
          completeAfter = PositiveInt.one,
          beginOffsetExclusive = ledgerEndP2,
        )
        .loneElement
        .transaction

      CantonTimestamp.fromProtoTimestamp(createEvent.recordTime.value).value shouldBe upgradeTime

      IouSyntax.createIou(participant1)(bank, alice, amount = 2.0)
      participant2.ledger_api.state.acs.active_contracts_of_party(alice) should have size 2
    }
  }
}
