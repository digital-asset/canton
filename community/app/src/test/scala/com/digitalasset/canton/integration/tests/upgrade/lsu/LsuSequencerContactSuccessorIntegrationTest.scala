// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.{
  Fixture,
  getLsuSuccessorContactStatusMetricValues,
}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import java.time.Duration

/** Each sequencer attempts to contact its successor when it processes its own successor
  * announcement. Upon successful contact, a metric should be changed.
  */
final class LsuSequencerContactSuccessorIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu_sequencer_contact_successor"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )

  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] = Map(
    "sequencer3" -> "sequencer1",
    "sequencer4" -> "sequencer2",
  )
  override protected lazy val newOldMediators: Map[String, String] = Map(
    "mediator3" -> "mediator1",
    "mediator4" -> "mediator2",
  )

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M4_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S2M2)
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

  private lazy val handshakeFailureWarn =
    s"Unable to perform handshake with ${fixture.newPsid}"

  "Sequencers" should {
    "have contact metrics set to 0 initially" in { implicit env =>
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
    }

    "not update the metric if psid is incorrect" in { implicit env =>
      import env.*

      // Incorrect announcement of the successor: wrong psid
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
        sequencer1.topology.lsu.sequencer_successors.propose_successor(
          sequencerId = sequencer1.id,
          // announced successor is itself -> wrong psid
          endpoints = sequencer1.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
          successorSynchronizerId = fixture.newPsid,
        ),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should include(
                s"Error when contacting successor: expecting psid to be ${fixture.newPsid} but found ${fixture.currentPsid}"
              ),
              "warning on sequencer",
            ),
            (
              _.warningMessage should include(
                "Connection internal-sequencer-connection-sequencer1-0: Invalid synchronizer"
              ),
              "connection pool warn on p1",
            ),
            (
              _.warningMessage should include(
                "Connection internal-sequencer-connection-sequencer1-0: Invalid synchronizer"
              ),
              "connection pool warn on p2",
            ),
            (
              _.warningMessage should include(handshakeFailureWarn),
              "handshake failure on p1",
            ),
            (
              _.warningMessage should include(handshakeFailureWarn),
              "handshake failure on p2",
            ),
          )
        ),
      )

      // metric is not updated
      getLsuSuccessorContactStatusMetricValues(sequencer1) shouldBe Map(fixture.newPsid -> 0)
    }

    "not update the metric if sequencer id is incorrect" in { implicit env =>
      import env.*

      // Incorrect announcement of the successor: wrong sequencer id
      loggerFactory.assertEventuallyLogsSeq(SuppressionRule.Level(Level.WARN))(
        sequencer1.topology.lsu.sequencer_successors.propose_successor(
          sequencerId = sequencer1.id,
          // announced successor is another sequencer -> wrong sequencer id
          endpoints = sequencer4.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
          successorSynchronizerId = fixture.newPsid,
        ),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should include(
                s"Error when contacting successor: expecting sequencer id to be ${sequencer1.id} but found ${sequencer2.id}"
              ),
              "warning on sequencer",
            ),
            (
              _.warningMessage should include(
                "Validation failure: Connection is not on expected sequencer"
              ),
              "connection pool warn on p1",
            ),
            (
              _.warningMessage should include(
                "Validation failure: Connection is not on expected sequencer"
              ),
              "connection pool warn on p2",
            ),
            (
              _.warningMessage should include(handshakeFailureWarn),
              "handshake failure on p1",
            ),
            (
              _.warningMessage should include(handshakeFailureWarn),
              "handshake failure on p2",
            ),
          )
        ),
      )

      // metric is not updated
      getLsuSuccessorContactStatusMetricValues(sequencer1) shouldBe Map(fixture.newPsid -> 0)
    }

    "update the metric when the successor is correct" in { implicit env =>
      import env.*

      sequencer1.topology.lsu.sequencer_successors.propose_successor(
        sequencerId = sequencer1.id,
        endpoints = sequencer3.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
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

      waitForTargetTimeOnSequencer(sequencer3, environment.clock.now, logger)
      oldSynchronizerNodes.all.stop()

      participant1.health.ping(participant2)
    }

    "metric is updated in case of replacement and cancellation" in { implicit env =>
      import env.*
      val psid2 = fixture.newPsid.incrementSerial
      val psid3 = psid2.incrementSerial
      val upgradeTime2 = fixture.upgradeTime.plusSeconds(30)
      val upgradeTime3 = upgradeTime2.plusSeconds(30)

      sequencer3.topology.lsu.announcement.propose(psid2, upgradeTime2)

      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer3) shouldBe Map(psid2 -> 0)
      }

      sequencer3.topology.lsu.announcement.propose(psid3, upgradeTime3)

      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer3) shouldBe Map(psid2 -> 0, psid3 -> 0)
      }

      sequencer3.topology.lsu.announcement.revoke(psid3, upgradeTime3)

      eventually() {
        getLsuSuccessorContactStatusMetricValues(sequencer3) shouldBe Map(psid2 -> 0, psid3 -> -1)
      }
    }
  }
}
