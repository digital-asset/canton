// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.upgrade.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.{ProtocolVersionChecksFixtureAnyWordSpec, config}
import monocle.macros.syntax.lens.*

import java.time.Duration

/*
Topology:
- P1 and P2 connected to the synchronizer
- One sequencer, one mediator

Scenario:
- LSU to pv=dev
- P2 cannot connect to the successor after the automatic upgrade because it does not support alpha protocol versions
- P1 submits a transactions that requires P2 confirmation
- P2 is restarted with alpha protocol versions support
- P2 reconnects to the synchronizer and confirm the transaction
- Transaction is successful

Notes:
- This test *cannot* run in-memory
- Since changing the config of a participant after it is bootstrapped is difficult in integration tests,
  we do the following trick:
  - Use P3 that has alpha protocol versions support enabled
  - Have P2 and P3 share the DB
  - Ensure P2 and P3 do not run at the same
 */
final class LsuBinaryUpgradeAfterUpgradeTimeIntegrationTest
    extends LsuBase
    with ProtocolVersionChecksFixtureAnyWordSpec {

  override protected def testName: String = "lsu-binary-upgrade-after-upgrade-time"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(
    new UsePostgres(
      loggerFactory,
      nodeDbMapping = nodeName => if (nodeName == "participant3") "participant2" else nodeName,
    )
  )

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer2" -> "sequencer1")
  override protected lazy val newOldMediators: Map[String, String] = Map("mediator2" -> "mediator1")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S2M2_Config.withManualStart
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
      // all nodes but P2 support alpha pv
      .addConfigTransform(
        ConfigTransforms.updateParticipantConfig("participant2")(
          _.focus(_.parameters.alphaVersionSupport).replace(false)
        )
      )
      .withSetup { implicit env =>
        import env.*

        // start all nodes but P3
        participant1.start()
        participant2.start()
        sequencers.local.start()
        mediators.local.start()

        participant1.synchronizers.connect_by_config(defaultSynchronizerConnectionConfig())
        participant2.synchronizers.connect_by_config(defaultSynchronizerConnectionConfig())

        participant1.dars.upload(CantonExamplesPath)
        participant2.dars.upload(CantonExamplesPath)

        synchronizerOwners1.foreach(
          _.topology.synchronizer_parameters.propose_update(
            daId,
            _.copy(reconciliationInterval = config.PositiveDurationSeconds.ofSeconds(1)),
          )
        )

        oldSynchronizerNodes = SynchronizerNodes(Seq(sequencer1), Seq(mediator1))
        newSynchronizerNodes = SynchronizerNodes(Seq(sequencer2), Seq(mediator2))
      }

  "Logical synchronizer upgrade" should {
    "work if the binary upgrade is done after upgrade time" onlyRunWhen (testedProtocolVersion.isStable) in {
      implicit env =>
        import env.*

        val fixture = fixtureWithDefaults()
        fixture.newPSId.protocolVersion.isDev shouldBe true

        val iouSignedByP2 = IouSyntax.createIou(participant2)(
          payer = participant2.adminParty,
          owner = participant1.adminParty,
        )
        val newOwner = participant1.parties.enable(
          "NewOwner",
          synchronizeParticipants = Seq(participant1, participant2), // p3 is offline
        )

        loggerFactory.assertLogsUnordered(
          {
            performSynchronizerNodesLSU(fixture)
            environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
            eventually() {
              participant1.synchronizers.is_connected(fixture.newPSId) shouldBe true
              // no dev support
              participant2.synchronizers.is_connected(fixture.newPSId) shouldBe false

              participant1.synchronizers.is_connected(fixture.currentPSId) shouldBe false
              participant2.synchronizers.is_connected(fixture.currentPSId) shouldBe false
            }

            oldSynchronizerNodes.all.stop()
            environment.simClock.value.advance(Duration.ofSeconds(1))
            waitForTargetTimeOnSequencer(sequencer2, environment.clock.now)

            participant1.ledger_api.javaapi.commands.submit_async(
              Seq(participant1.adminParty),
              Seq(
                iouSignedByP2.id
                  .exerciseTransfer(newOwner.toProtoPrimitive)
                  .commands()
                  .loneElement
              ),
            )

            eventually() {
              participant1.underlying.value.sync
                .connectedSynchronizerForAlias(daName)
                .value
                .numberOfDirtyRequests() shouldBe 1
            }

            participant2.stop()
            participant3.db.migrate() // dev migration
            participant3.start()
            participant3.synchronizers.reconnect_all()

            eventually() {
              participant3.synchronizers.is_connected(fixture.newPSId) shouldBe true
            }
          },
          // Logged twice: once during handshake and once during connect attempt
          _.warningMessage should include(
            "Validation failure: Failed handshake: The protocol version required by the server (dev) is not among the supported protocol versions by the client"
          ),
          _.warningMessage should include(
            "Validation failure: Failed handshake: The protocol version required by the server (dev) is not among the supported protocol versions by the client"
          ),
          // TODO(#30534) This message can be made more explicit (also include resolution) when individual errors bubble up
          _.errorMessage should (include(
            s"Unable to perform handshake with ${fixture.newPSId}"
          ) and include("Trust threshold of 1 is no longer reachable")),
          _.errorMessage should (include(s"Upgrade to ${fixture.newPSId} failed") and include(
            "Trust threshold of 1 is no longer reachable"
          )),
        )

        participant1.ledger_api.javaapi.state.acs.await(Iou.COMPANION)(
          newOwner,
          predicate = _.data.owner == newOwner.toProtoPrimitive,
        )
    }
  }
}
