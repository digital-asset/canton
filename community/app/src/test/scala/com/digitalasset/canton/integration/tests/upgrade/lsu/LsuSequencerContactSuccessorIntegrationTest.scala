// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.{
  Fixture,
  getLsuSuccessorContactStatusMetricValues,
}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import monocle.macros.syntax.lens.*

import java.time.Duration

final class LsuSequencerContactSuccessorIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu_sequencer_contact_successor"

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

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(configTransforms*)
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
      .withSetup { implicit env =>
        defaultEnvironmentSetup()
      }

  private var fixture: Fixture = _

  "Sequencers" should {
    "contact their successor and update metrics to reflect status" in { implicit env =>
      import env.*

      fixture = fixtureWithDefaults()

      // LSU announced, sequencer not known yet
      performSynchronizerNodesLsu(fixture, announceSequencerSuccessors = false)
      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer1) shouldBe Map(fixture.newPsid -> 0)
      }

      // Value of the metric is updated upon start up
      sequencer1.stop()
      sequencer1.start()
      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer1) shouldBe Map(fixture.newPsid -> 0)
      }

      // Announcement of the successor
      sequencer1.topology.lsu.sequencer_successors.propose_successor(
        sequencerId = sequencer1.id,
        endpoints = sequencer2.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
        successorSynchronizerId = fixture.newPsid,
      )

      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer1) shouldBe Map(fixture.newPsid -> 1)
      }

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
      transferTraffic()
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
      }

      waitForTargetTimeOnSequencer(sequencer2, environment.clock.now, logger)
      oldSynchronizerNodes.all.stop()

      participant1.health.ping(participant2)
    }

    "metric is updated in case of replacement and cancellation" in { implicit env =>
      import env.*
      val psid2 = fixture.newPsid.incrementSerial
      val psid3 = psid2.incrementSerial
      val upgradeTime2 = fixture.upgradeTime.plusSeconds(30)
      val upgradeTime3 = upgradeTime2.plusSeconds(30)

      sequencer2.topology.lsu.announcement.propose(psid2, upgradeTime2)

      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer2) shouldBe Map(psid2 -> 0)
      }

      sequencer2.topology.lsu.announcement.propose(psid3, upgradeTime3)

      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer2) shouldBe Map(psid2 -> 0, psid3 -> 0)
      }

      sequencer2.topology.lsu.announcement.revoke(psid3, upgradeTime3)

      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer2) shouldBe Map(psid2 -> 0, psid3 -> -1)
      }
    }
  }
}
