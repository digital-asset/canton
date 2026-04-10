// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.UniquePortGenerator
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.getLsuSequencingTestMetricValues
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule
import monocle.macros.syntax.lens.*
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.slf4j.event.Level

import java.time.Duration

/*
In this test, we check that the successor can be tested before the upgrade time.

Topology:
- 2 sequencers, 2 mediators
- s1 is migrated to s3, and s2 is migrated to s4 (same for the mediators)
- LSU is announced
- Synchronizer nodes of the successors are bootstrapped
- LsuSequencingTest message is sequenced and processed, which has the side effect of bumping metrics
- LSU is finally performed
 */
sealed abstract class LsuSanityCheckSuccessorSynchronizerIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-sanity-check-successor-synchronizer"

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

  "LSU successor" should {
    "be testable before upgrade time" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant1)

      performSynchronizerNodesLsu(fixture)

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        sequencer3.setup.test_lsu_sequencing(NonNegativeInt.one),
        _.errorMessage should include("Mediator group 1 does not exist at timestamp"),
      )

      /*
      What can happen is that the LsuSequencingTest message has a sequencing time (on the successor) that
      is before the effective time of the LSU announcement. Hence, this message is not delivered.
      To bypass that, we do a few iterations.
       */
      eventually() {
        environment.simClock.value.advance(Duration.ofMillis(10))
        sequencer3.setup.test_lsu_sequencing(NonNegativeInt.zero)
        val m = getLsuSequencingTestMetricValues(mediator3)
        m.get(sequencer3.id).value should be > 0L
      }

      sequencer4.setup.test_lsu_sequencing(NonNegativeInt.zero)
      sequencer4.setup.test_lsu_sequencing(NonNegativeInt.zero)
      eventually() {
        getLsuSequencingTestMetricValues(mediator3).get(sequencer4.id).value shouldBe 2
        getLsuSequencingTestMetricValues(mediator4).get(sequencer4.id).value shouldBe 2
      }

      // Check whether the command behaves well with a restart
      sequencer4.stop()
      sequencer4.start()
      sequencer4.setup.test_lsu_sequencing(NonNegativeInt.zero)
      eventually() {
        getLsuSequencingTestMetricValues(mediator3).get(sequencer4.id).value shouldBe 3
        getLsuSequencingTestMetricValues(mediator4).get(sequencer4.id).value shouldBe 3
      }

      environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)

      clue(
        "We get an LSU sequencing test message sequenced > upgradeTime to check that sequencer can handle that"
      ) {
        loggerFactory.assertEventuallyLogsSeq(
          SuppressionRule.Level(Level.DEBUG)
            && SuppressionRule.forLogger[OutputModule[?]] && SuppressionRule.LoggerNameContains(
              "sequencer=sequencer4"
            )
        )(
          sequencer4.setup.test_lsu_sequencing(NonNegativeInt.zero),
          logs => {
            sequencer4.setup.test_lsu_sequencing(
              NonNegativeInt.zero
            ) // we keep sending test messages until we match below

            if (!this.isInstanceOf[ReferenceLsuSanityCheckSuccessorSynchronizerIntegrationTest]) {
              forAtLeast(1, logs) { log =>
                val logPattern =
                  """Sending block \d+ \(current epoch = \d+, block's BFT time = (.+), block size = (\d+).+""".r
                inside(log.message) {
                  case logPattern(ts, blockSize) if blockSize.toInt > 0 =>
                    CantonTimestamp.fromString(ts).value should be > upgradeTime.immediateSuccessor
                }
              }
            } else {
              succeed // for reference sequencer it is hard to detect the block timestamps + contents from the logs
            }
          },
          timeUntilSuccess = 60.seconds,
          maxPollInterval = 1.second,
        )
      }

      transferTraffic()
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture.newPsid)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture.currentPsid)) shouldBe false
      }

      participant1.health.ping(participant1)

      // Check it also works after upgrade time
      sequencer4.setup.test_lsu_sequencing(NonNegativeInt.zero)
      eventually() {
        getLsuSequencingTestMetricValues(mediator3).get(sequencer4.id).value should be >= 5L
        getLsuSequencingTestMetricValues(mediator4).get(sequencer4.id).value should be >= 5L
      }
    }
  }
}

final class BftLsuSanityCheckSuccessorSynchronizerIntegrationTest
    extends LsuSanityCheckSuccessorSynchronizerIntegrationTest {
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )
}

final class ReferenceLsuSanityCheckSuccessorSynchronizerIntegrationTest
    extends LsuSanityCheckSuccessorSynchronizerIntegrationTest {
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1", "sequencer2"), Set("sequencer3", "sequencer4")),
    )
  )
}
