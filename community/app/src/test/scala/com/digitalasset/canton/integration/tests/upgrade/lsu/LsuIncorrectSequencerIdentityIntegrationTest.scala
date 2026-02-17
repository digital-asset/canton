// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.admin.api.client.data.SynchronizerConnectionConfig
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.SynchronizerTimeTrackerConfig
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import org.scalatest.Assertion

import java.time.Duration

/** Ensures that LSU fails if sequencers report incorrect identity.
  */
final class LsuIncorrectSequencerIdentityIntegrationTest extends LsuBase {
  override protected def testName: String = "lsu-incorrect-sequencer-identity"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1", "sequencer4" -> "sequencer2")
  override protected lazy val newOldMediators: Map[String, String] =
    Map("mediator3" -> "mediator1", "mediator4" -> "mediator2")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S4M4_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S2M2)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        defaultEnvironmentSetup(connectParticipants = false)

        participant1.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 2)
        )
      }

  "Logical synchronizer upgrade" should {
    "detect incorrect sequencer identities" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant1)

      fixture.oldSynchronizerOwners.foreach(
        _.topology.lsu.announcement.propose(fixture.newPSId, fixture.upgradeTime)
      )

      eventually() {
        forAll(fixture.oldSynchronizerNodes.all)(
          _.topology.lsu.announcement
            .list(store = Some(fixture.currentPSId))
            .filter(_.item.successorSynchronizerId == fixture.newPSId)
            .loneElement
        )
      }

      migrateSynchronizerNodes(fixture)

      loggerFactory.assertLogsUnorderedOptional(
        {
          sequencer1.topology.lsu.sequencer_successors.propose_successor(
            sequencerId = sequencer1.id,
            // sequencer4 has id of sequencer2 instead of sequencer1
            endpoints = sequencer4.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
            synchronizerId = fixture.currentPSId,
          )
          sequencer2.topology.lsu.sequencer_successors.propose_successor(
            sequencerId = sequencer2.id,
            // sequencer3 has id of sequencer1 instead of sequencer2
            endpoints = sequencer3.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
            synchronizerId = fixture.currentPSId,
          )

          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

          eventually() {
            participants.all.forall(_.synchronizers.is_connected(fixture.newPSId)) shouldBe false
            participants.all.forall(
              _.synchronizers.is_connected(fixture.currentPSId)
            ) shouldBe false
          }

          oldSynchronizerNodes.all.stop()
        },
        (
          LogEntryOptionality.OptionalMany,
          _.warningMessage should include(
            s"Connection is not on expected sequencer: expected Some(${sequencer1.id}), got ${sequencer2.id}"
          ),
        ),
        (
          LogEntryOptionality.OptionalMany,
          _.warningMessage should include(
            s"Connection is not on expected sequencer: expected Some(${sequencer2.id}), got ${sequencer1.id}"
          ),
        ),
        // TODO(#30534) This message can be made more explicit (also include resolution) when individual errors bubble up
        (
          LogEntryOptionality.Required,
          _.errorMessage should (include(
            s"Unable to perform handshake with ${fixture.newPSId}"
          ) and include("Trust threshold of 2 is no longer reachable")),
        ),
        // TODO(#30534) This message can be made more explicit (also include resolution) when individual errors bubble up
        (
          LogEntryOptionality.Required,
          _.errorMessage should include(s"Upgrade to ${fixture.newPSId} failed"),
        ),
      )
    }
  }
}

/** Ensures that LSU fails if one sequencer announces itself as the successor. Ensures that a wrong
  * announcement of the sequencer successor can be fixed.
  */
final class LsuSuccessorSequencerIsPredecessorIntegrationTest extends LsuBase {
  override protected def testName: String = "lsu-sequencer-successor-is-predecessor"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map("sequencer3" -> "sequencer1", "sequencer4" -> "sequencer2")
  override protected lazy val newOldMediators: Map[String, String] =
    Map("mediator3" -> "mediator1", "mediator4" -> "mediator2")
  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M4_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S2M2)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        defaultEnvironmentSetup(connectParticipants = false)

        participant1.synchronizers.connect_by_config(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = sequencer1,
            timeTracker = SynchronizerTimeTrackerConfig(observationLatency =
              config.NonNegativeFiniteDuration.Zero
            ),
          )
        )
        participant2.synchronizers.connect_by_config(
          SynchronizerConnectionConfig(
            synchronizerAlias = daName,
            sequencerConnections = sequencer2,
            timeTracker = SynchronizerTimeTrackerConfig(observationLatency =
              config.NonNegativeFiniteDuration.Zero
            ),
          )
        )
      }

  "Logical synchronizer upgrade" should {
    "detect incorrect PSId" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)

      fixture.oldSynchronizerOwners.foreach(
        _.topology.lsu.announcement.propose(fixture.newPSId, fixture.upgradeTime)
      )

      eventually() {
        forAll(fixture.oldSynchronizerNodes.all)(
          _.topology.lsu.announcement
            .list(store = Some(fixture.currentPSId))
            .filter(_.item.successorSynchronizerId == fixture.newPSId)
            .loneElement
        )
      }

      migrateSynchronizerNodes(fixture)

      // TODO(#30534) This message can be made more explicit (also include resolution) when individual errors bubble up
      def failedHandshakeLogLine(entry: LogEntry, p: ParticipantReference): Assertion = {
        entry.errorMessage should (include(
          s"Unable to perform handshake with ${fixture.newPSId}"
        ) and include("Trust threshold of 1 is no longer reachable"))
        entry.loggerName should include(s"participant=${p.name}")
      }

      def incorrectSequencerPSId(entry: LogEntry, p: ParticipantReference): Assertion = {
        entry.warningMessage should (include("connection") and include(
          s"is not on expected synchronizer: expected Some(${fixture.newPSId}), got ${fixture.currentPSId}"
        ))

        entry.loggerName should include(s"participant=${p.name}")
      }

      loggerFactory.assertLogsUnordered(
        {
          sequencer1.topology.lsu.sequencer_successors.propose_successor(
            sequencerId = sequencer1.id,
            // It should be sequencer3 here
            endpoints = sequencer1.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
            synchronizerId = fixture.currentPSId,
          )
          sequencer2.topology.lsu.sequencer_successors.propose_successor(
            sequencerId = sequencer2.id,
            // It should be sequencer4 here
            endpoints = sequencer2.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
            synchronizerId = fixture.currentPSId,
          )

          // fix the successor announcement
          sequencer2.topology.lsu.sequencer_successors.propose_successor(
            sequencerId = sequencer2.id,
            endpoints = sequencer4.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
            synchronizerId = fixture.currentPSId,
          )

          // Ensure P2 sees the correct announcement before progressing the time
          eventually() {
            participant2.topology.lsu.sequencer_successors
              .list(daId)
              .filter(_.item.sequencerId == sequencer2.id)
              .filter(_.context.serial == PositiveInt.two)
              .loneElement
          }

          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

          eventually() {
            participant1.synchronizers.is_connected(fixture.newPSId) shouldBe false
            participant2.synchronizers.is_connected(fixture.newPSId) shouldBe true

            participants.all.forall(
              _.synchronizers.is_connected(fixture.currentPSId)
            ) shouldBe false
          }

          environment.simClock.value.advance(Duration.ofSeconds(1))
          waitForTargetTimeOnSequencer(sequencer3, environment.clock.now)

          participant2.health.ping(participant2)
        },
        // initial handshake
        incorrectSequencerPSId(_, participant1),
        incorrectSequencerPSId(_, participant2),
        failedHandshakeLogLine(_, participant1),
        failedHandshakeLogLine(_, participant2),
        // connect
        incorrectSequencerPSId(_, participant1),
        // TODO(#30534) This message can be made more explicit (also include resolution) when individual errors bubble up
        _.errorMessage should include(s"Upgrade to ${fixture.newPSId} failed"),
      )
    }
  }
}
