// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.{
  getLsuStatusMetricValues,
  getParticipantHandshakesMetricValues,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.participant.synchronizer.grpc.GrpcSynchronizerRegistry
import com.digitalasset.canton.{UniquePortGenerator, config}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

/** The goal of this test is to ensure that participants do the handshake with as many sequencers as
  * possible and that this "waiting" is interrupted in case of shutdown or when minimumDuration has
  * elapsed.
  *
  * Topology:
  *   - P1, P2, P3 connected to S1 and S2 with threshold=1
  *     - The handshake is done as soon as S1 successor is known
  *     - Announcement of S2 successor triggers another handshake on which we do checks/assertions
  *
  *   - Two sequencers:
  *     - S1 has S3 as successor
  *     - S2 has S4 as successor
  *
  * Tests:
  *   - P1 waits until all sequencers are up
  *   - P2 stops waiting because of shutdown
  *   - P3 stops waiting because of minimumDuration has elapsed
  */
final class LsuExhaustiveHandshakeIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-exhaustive-handshake"

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
    EnvironmentDefinition.P3S4M4_Config
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
      .addConfigTransform(
        ConfigTransforms.updateAllParticipantConfigs {

          case ("participant1" | "participant2", pConfig) =>
            pConfig
              .focus(_.parameters.lsu.handshake.minimumDuration)
              // big value to ensure that the minimumDuration does not interrupt the wait
              .replace(Some(config.NonNegativeFiniteDuration.ofDays(1)))

          case ("participant3", pConfig) =>
            pConfig
              .focus(_.parameters.lsu.handshake.minimumDuration)
              // small value to ensure that the minimumDuration interrupts the wait
              .replace(Some(config.NonNegativeFiniteDuration.ofSeconds(3)))

          case (_other, pConfig) => pConfig
        }
      )
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        defaultEnvironmentSetup(connectParticipants = false)

        participants.local.foreach(
          _.synchronizers.connect_by_config(
            synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 1)
          )
        )
      }

  "Participants" should {
    "perform the handshake with as many sequencers as possible" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()
      val p2Id = participant2.id

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

      /*
        Stopping S4 so that handshake with S4 fails.
        Stopping the mediators first to avoid connectivity warnings in the logs.
       */

      // Ensure that handshake with S4 fails
      fixture.newSynchronizerNodes.mediators.stop()
      sequencer4.stop()

      sequencer1.topology.lsu.sequencer_successors.propose_successor(
        sequencerId = sequencer1.id,
        endpoints = sequencer3.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
        successorSynchronizerId = fixture.newPsid,
      )

      // Initial handshake succeed because threshold=1
      eventually() {
        forAll(participants.local) { p =>
          getLsuStatusMetricValues(p)
            .get(fixture.newPsid)
            .value should be >= ParticipantMetrics.LsuStatus.LocalCopyDone
        }
      }

      loggerFactory.assertEventuallyLogsSeq(
        SuppressionRule.Level(Level.DEBUG) && SuppressionRule.forLogger[GrpcSynchronizerRegistry]
      )(
        sequencer2.topology.lsu.sequencer_successors.propose_successor(
          sequencerId = sequencer2.id,
          endpoints = sequencer4.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
          successorSynchronizerId = fixture.newPsid,
        ),
        entries => {
          // All participants start the waiting
          forExactly(3, entries)(
            _.debugMessage should include("Handshake was successful. Starting to wait until")
          )
          // P3 eventually stops waiting
          forExactly(1, entries) { entry =>
            entry.debugMessage should include(
              "Stopping the wait because max waiting time is reached."
            )
            entry.loggerName should include("participant3")
          }
        },
      )

      loggerFactory.assertLogsSeq(
        SuppressionRule.Level(Level.DEBUG) && SuppressionRule.forLogger[GrpcSynchronizerRegistry]
      )(
        participant2.stop(),
        forExactly(1, _) { entry =>
          entry.debugMessage should include("Stopping the wait because of shutdown.")
          entry.loggerName should include("participant2")
        },
      )

      // Starting S4 should eventually allow P1 to handshake with S4
      sequencer4.start()

      eventually() {
        getParticipantHandshakesMetricValues(sequencer4)
          .get((participant1.id, "success"))
          .value should be >= 1L
      }

      getParticipantHandshakesMetricValues(sequencer4).get((p2Id, "success")) shouldBe empty

      getParticipantHandshakesMetricValues(sequencer4).get(
        (participant3.id, "success")
      ) shouldBe empty
    }
  }
}
