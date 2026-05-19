// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.{
  Fixture,
  getLsuStatusMetricValues,
  getParticipantHandshakesMetricValues,
}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.topology.SequencerId
import monocle.macros.syntax.lens.*

import java.time.Duration

/*
Check that handshake is done as soon as threshold many successors are known.
The check relies on metrics.

Topology:
- P1 connected to S1 and S2 with threshold 1
- P2 connected to S1 and S2 with threshold 2
 */
final class LsuEarlyHandshakeIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-early-handshake"

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
      .addConfigTransforms(
        _.focus(_.monitoring.metrics)
          .replace(
            MetricsConfig(
              qualifiers = Seq[MetricQualification](MetricQualification.Debug),
              reporters = Seq(
                MetricsReporterConfig.Prometheus(
                  port = UniquePortGenerator.next
                )
              ),
            )
          )
      )
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        defaultEnvironmentSetup(connectParticipants = false)

        participant1.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 1)
        )
        participant2.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 2)
        )

        participant1.health.ping(participant2)

        fixture = fixtureWithDefaults()
      }

  private var fixture: Fixture = _

  private def getKnownSuccessors(p: LocalParticipantReference): Set[SequencerId] =
    p.underlying.value.sync.synchronizerConnectionConfigStore
      .get(fixture.newPsid)
      .value
      .config
      .sequencerConnections
      .aliasToConnection
      .forgetNE
      .values
      .map(_.sequencerId.value)
      .toSet

  "Logical synchronizer upgrade" should {
    "update metics" when {
      "when LSU is announced" in { implicit env =>
        import env.*

        // Nothing is set yet
        getLsuStatusMetricValues(participant1) shouldBe empty

        fixture.oldSynchronizerOwners.foreach(
          _.topology.lsu.announcement.propose(fixture.newPsid, fixture.upgradeTime)
        )

        // Ensure all nodes see the announcement
        eventually() {
          forAll(fixture.oldSynchronizerNodes.all ++ participants.local)(
            _.topology.lsu.announcement
              .list(store = Some(fixture.currentPsid))
              .filter(_.item.successorSynchronizerId == fixture.newPsid)
              .loneElement
          )
        }

        migrateSynchronizerNodes(fixture)
      }

      "when sequencer1 successor is announced" in { implicit env =>
        import env.*

        // No handshake was done yet
        forAll(fixture.newSynchronizerNodes.sequencers)(
          getParticipantHandshakesMetricValues(_) shouldBe empty
        )

        sequencer1.topology.lsu.sequencer_successors.propose_successor(
          sequencerId = sequencer1.id,
          endpoints = sequencer3.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
          successorSynchronizerId = fixture.newPsid,
        )

        // participants see the announcement
        eventually() {
          forAll(participants.local)(
            _.topology.lsu.sequencer_successors
              .list()
              .filter(_.item.sequencerId == sequencer1.id)
              .loneElement
          )
        }

        // Sequencer trust threshold for P1 is one
        eventually() {
          getLsuStatusMetricValues(participant1)(
            fixture.newPsid
          ).value should be >= ParticipantMetrics.LsuStatus.LocalCopyDone.value
        }

        // Check number of handshakes on the new sequencers
        // Only P1 performs a handshake, with s3
        getParticipantHandshakesMetricValues(sequencer3)
          .get((participant1.id, "success"))
          .value shouldBe 1L
        getParticipantHandshakesMetricValues(sequencer3)
          .get((participant2.id, "success")) shouldBe empty
        getParticipantHandshakesMetricValues(sequencer4)
          .get((participant1.id, "success")) shouldBe empty
        getParticipantHandshakesMetricValues(sequencer4)
          .get((participant2.id, "success")) shouldBe empty

        getKnownSuccessors(participant1) shouldBe Set(sequencer1.id)
      }

      "when sequencer2 successor is announced" in { implicit env =>
        import env.*

        sequencer2.topology.lsu.sequencer_successors.propose_successor(
          sequencerId = sequencer2.id,
          endpoints = sequencer4.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
          successorSynchronizerId = fixture.newPsid,
        )

        // participants see the announcement
        eventually() {
          forAll(participants.local)(
            _.topology.lsu.sequencer_successors
              .list()
              .filter(_.item.sequencerId == sequencer1.id)
              .loneElement
          )
        }

        eventually() {
          getLsuStatusMetricValues(participant2)(
            fixture.newPsid
          ).value should be >= ParticipantMetrics.LsuStatus.LocalCopyDone.value
        }

        getParticipantHandshakesMetricValues(sequencer4)
          .get((participant2.id, "success"))
          .value shouldBe 1L

        // configs are updated
        getKnownSuccessors(participant1) shouldBe Set(sequencer1.id, sequencer2.id)
        getKnownSuccessors(participant2) shouldBe Set(sequencer1.id, sequencer2.id)
      }
    }

    "and then perform LSU" in { implicit env =>
      import env.*

      environment.simClock.foreach(_.advanceTo(upgradeTime.immediateSuccessor))
      transferTraffic()

      eventually() {
        environment.simClock.foreach(_.advance(Duration.ofSeconds(1)))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture.currentPsid)) shouldBe false
      }

      oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer3, upgradeTime.immediateSuccessor, logger)

      participant1.health.ping(participant2)
    }
  }
}
