// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.TestPredicateFiltersFixtureAnyWordSpec
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.synchronizer.PendingHandshakeWithLsuSuccessor
import com.digitalasset.canton.store.PendingOperationStore
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

/** The goal is to check persistence of
  * [[com.digitalasset.canton.participant.synchronizer.PendingHandshakeWithLsuSuccessor]].
  *
  * Scenario:
  *   - LSU to pv=dev
  *   - P1 handshake to the successor fails because it does not support alpha protocol versions
  *   - P2 handshake to successor fails because the sequencer is not up. LSU cancellation
  *     subsequently removed the pending handshake from the store.
  *
  * Notes:
  *   - This test *cannot* run in-memory
  *   - Since changing the config of a participant after it is bootstrapped is difficult in
  *     integration tests, we do the following trick:
  *     - Use P3 that has alpha protocol versions support enabled
  *     - Have P1 and P3 share the DB
  *     - Ensure P1 and P3 do not run at the same
  */
final class LsuPersistentHandshakeSuccessorIntegrationTest
    extends LsuBase
    with TestPredicateFiltersFixtureAnyWordSpec {

  override protected def testName: String = "lsu-binary-upgrade-after-upgrade-time"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )
  registerPlugin(
    new UsePostgres(
      loggerFactory,
      nodeDbMapping = nodeName => if (nodeName == "participant3") "participant1" else nodeName,
    )
  )

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1", "sequencer4" -> "sequencer2")
  override protected lazy val newOldMediators: Map[String, String] =
    Map("mediator3" -> "mediator1", "mediator4" -> "mediator2")

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private var fixture: LsuBase.Fixture = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S4M4_Config.withManualStart
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S2M2)
      }
      .addConfigTransforms(configTransforms*)
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.sequencerInfo)
          /*
          The handshakes fail initial because sequencers are not up.
          The default timeout (before giving up) is 30 seconds and during that time, the simple execution queue
          for the synchronizer connect/disconnect/handshakes is blocked.
          A lower value makes the test faster. A value that is too low would make the test flaky.
           */
          .replace(NonNegativeDuration.ofSeconds(3))
      )
      // all nodes but P2 support alpha pv
      .addConfigTransform(
        ConfigTransforms.updateParticipantConfig("participant1")(
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

        defaultEnvironmentSetup(connectParticipants = false)

        participant1.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer1))
        participant2.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer2))

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)

        oldSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer1, sequencer2), Seq(mediator1, mediator2))
        newSynchronizerNodes =
          SynchronizerNodes(Seq(sequencer3, sequencer4), Seq(mediator3, mediator4))
      }

  private def getPendingHandshakesStore(p: LocalParticipantReference)(implicit
      executionContext: ExecutionContext
  ): PendingOperationStore[PendingHandshakeWithLsuSuccessor, PhysicalSynchronizerId] =
    PendingOperationStore(
      p.underlying.value.storage,
      timeouts,
      loggerFactory,
      PendingHandshakeWithLsuSuccessor,
      PhysicalSynchronizerId.fromString,
    )

  private def hasPendingHandshake(
      p: LocalParticipantReference
  )(implicit env: TestConsoleEnvironment, executionContext: ExecutionContext): Boolean =
    getPendingHandshakesStore(p)
      .get(
        env.daId,
        PendingHandshakeWithLsuSuccessor.operationKey,
        PendingHandshakeWithLsuSuccessor.operationName,
      )
      .value
      .futureValueUS
      .isDefined

  "Logical synchronizer upgrade" should {
    "persist the pending handshake with the successor" onlyRunWhen (testedProtocolVersion.isStable) in {
      implicit env =>
        import env.*

        fixture = fixtureWithDefaults()
        fixture.newPsid.protocolVersion.isDev shouldBe true

        performSynchronizerNodesLsu(fixture, announceSequencerSuccessors = false)

        // So that first handshake fails.
        // Note: since the error is transient, there is not WARN/ERROR in the logs (only INFO)
        sequencer3.stop()
        sequencer4.stop()

        // This triggers the handshakes attempts
        fixture.oldSynchronizerNodes.sequencers
          .zip(fixture.newSynchronizerNodes.sequencers)
          .foreach { case (oldSequencer, newSequencer) =>
            oldSequencer.topology.lsu.sequencer_successors.propose_successor(
              sequencerId = oldSequencer.id,
              endpoints = newSequencer.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
              successorSynchronizerId = fixture.newPsid,
            )
          }

        // Wait until the pending handshake is inserted in the store
        eventually() {
          hasPendingHandshake(participant1) shouldBe true
          hasPendingHandshake(participant2) shouldBe true
        }

        // Should lead to a new attempt because of the persistence
        participant1.stop()
        sequencer3.start()

        val failedHandshakeError = "Validation failure: Failed handshake: "
          + "The protocol version required by the server (dev) is not among the supported protocol versions by the client"

        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          participant1.start(),
          logs => {
            forExactly(1, logs)(_.warningMessage should include(failedHandshakeError))

            forExactly(1, logs)(
              _.errorMessage should (include(
                s"Failed to perform the synchronizer handshake with ${fixture.newPsid}"
              ) and include(failedHandshakeError))
            )
          },
        )
    }

    "pending handshake with successor should be cleaned upon successful handshake" onlyRunWhen (testedProtocolVersion.isStable) in {
      implicit env =>
        import env.*

        hasPendingHandshake(participant1) shouldBe true
        participant1.stop()
        participant3.db.migrate() // dev migration
        participant3.start()
        eventually() {
          hasPendingHandshake(participant3) shouldBe false
        }
    }

    "pending handshake with successor should be cleaned upon LSU cancellation" onlyRunWhen (testedProtocolVersion.isStable) in {
      implicit env =>
        import env.*

        hasPendingHandshake(participant2) shouldBe true
        fixture.oldSynchronizerOwners.foreach(
          _.topology.lsu.announcement.revoke(fixture.newPsid, fixture.upgradeTime)
        )
        eventually() {
          hasPendingHandshake(participant2) shouldBe false
        }
    }
  }
}
