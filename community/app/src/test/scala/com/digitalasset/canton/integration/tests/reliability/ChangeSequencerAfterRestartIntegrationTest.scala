// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.reliability

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.admin.api.client.data.SequencerConnections
import com.digitalasset.canton.config
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, LocalInstanceReference}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import monocle.macros.syntax.lens.*
import org.scalactic.source.Position
import org.slf4j.event.Level

import scala.concurrent.Future
import scala.util.Try

/** Test that we can switch to a new sequencer if the old one is gone
  *
  * This test originated from 2.x deployments where a client modified the DNS of the sequencer and
  * then could not bring the PN and MED back up enough in order to update the sequencer connection.
  */
class ChangeSequencerAfterRestartIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S2M1_Manual
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.sequencerInfo)
          .replace(config.NonNegativeDuration.ofSeconds(3))
      )
      .withSetup { implicit env =>
        import env.*
        Seq[LocalInstanceReference](mediator1, sequencer1, sequencer2, participant1)
          .start()

        bootstrap.synchronizer(
          synchronizerName = daName.toProtoPrimitive,
          sequencers = Seq(sequencer1, sequencer2),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1, sequencer2),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )
        // we only keep the connection to sequencer1 so we can test the switch to sequencer2
        mediator1.sequencer_connection.set(
          SequencerConnections.single(sequencer1.sequencerConnection)
        )
        mediator1.sequencer_connection.get() shouldBe Some(
          SequencerConnections.single(sequencer1.sequencerConnection)
        )

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        // verify it works
        clue("participant can ping itself") {
          participant1.health.maybe_ping(participant1) shouldBe defined
        }

        // shutdown nodes
        participant1.stop()
        mediator1.stop()
        sequencer1.stop()
      }

  "Check that we can switch to a new sequencer" should {
    "mediator should be able to switch to new sequencer".taggedAs(mkTag("mediator")) in {
      implicit env =>
        import env.*
        // start up nodes
        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            // To prevent racy tests we need to block until the mediator is up.
            val startupF = Future {
              mediator1.start()
            }

            clue(s"${mediator1.name} startup begins") {
              eventually() {
                mediator1.health.status.isRunning shouldBe true
              }
            }

            clue(s"adjusting ${mediator1.name} sequencer connection") {
              eventually() {
                logger.debug("attempting to set the connection")
                Try(
                  mediator1.sequencer_connection.set(
                    SequencerConnections.single(sequencer2.sequencerConnection)
                  )
                ).isSuccess shouldBe true
              }
            }
            clue(s"${mediator1.name} startup eventually completes") {
              val patience = defaultPatience.copy(timeout = defaultPatience.timeout.scaledBy(2))
              startupF.futureValue(patience, Position.here)
            }
          },
          expectedWarnings,
        )

        clue(s"${mediator1.name} doesn't struggle with invalid connections") {
          loggerFactory.assertThrowsAndLogsUnorderedOptional[CommandFailure](
            mediator1.sequencer_connection.set(
              SequencerConnections.single(sequencer1.sequencerConnection)
            ),
            (
              LogEntryOptionality.Optional,
              _.warningMessage should include("Connection has failed validation"),
            ),
            (
              LogEntryOptionality.Required,
              _.errorMessage should include("Connection pool failed to initialize"),
            ),
          )
        }
    }
    "participant should be able to switch to new sequencer".taggedAs(mkTag("participant")) in {
      implicit env =>
        import env.*

        // start up nodes
        clue("starting up participant") {
          participant1.start()
        }
        clue("adjusting participant sequencer connection") {
          participant1.synchronizers.modify(
            daName,
            _.copy(sequencerConnections =
              SequencerConnections.single(sequencer2.sequencerConnection)
            ),
          )
          participant1.synchronizers.reconnect_all()
        }

        eventually() {
          participant1.health.maybe_ping(participant1) shouldBe defined
        }

    }
  }

  private def mkTag(component: String) = ReliabilityTest(
    Component(component, "connected to sequencer"),
    AdverseScenario(
      dependency = "sequencer",
      details = s"changes connection while $component is shut down",
    ),
    Remediation(
      remediator = "mutable sequencer connection",
      action = s"$component is manually reconfigured",
    ),
    outcome = s"$component can reconnect and resume",
  )

  private val expectedWarnings = LogEntry.assertLogSeq(
    Seq.empty,
    Seq(
      _.message should (include("Is the server running")),
      _.message should (include("Is the server initialized")),
      _.message should (include("Unable to connect to sequencer")),
    ),
  ) _

}
