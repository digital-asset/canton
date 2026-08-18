// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.daml.metrics.api.MetricQualification
import com.digitalasset.canton.config.{ExponentialBackoffConfig, NonNegativeDuration}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S2M2
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.tests.upgrade.lsu.LsuBase.{
  Fixture,
  getLsuStatusMetricValues,
  getParticipantHandshakesMetricValues,
}
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.metrics.{MetricsConfig, MetricsReporterConfig}
import com.digitalasset.canton.participant.metrics.ParticipantMetrics
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{UniquePortGenerator, config}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.Duration
import scala.annotation.nowarn

/*
The goal of this test is to ensure that metrics representing the status of LSU are correctly updated:
- On the participants (progress of the LSU).
- On the sequencers (number of handshakes).

Topology:
- Two sequencers (s1, s2)
- Two participants:
  - p1 connected to s1
  - p2 connected to s1 and s2 with threshold 2
 */
@nowarn("msg=dead code following this construct")
final class LsuMetricsIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-metrics"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(
        Set("sequencer1", "sequencer2"),
        Set("sequencer3", "sequencer4"),
        Set("sequencer5", "sequencer6"),
      ),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val newOldMediators: Map[String, String] =
    throw new IllegalAccessException("Use fixtures instead")
  override protected lazy val upgradeTime: CantonTimestamp = throw new IllegalAccessException(
    "Use fixtures instead"
  )

  override protected def configTransforms: List[ConfigTransform] = {
    val allNewNodes = Set(
      "sequencer3",
      "sequencer4",
      "sequencer5",
      "sequencer6",
      "mediator3",
      "mediator4",
      "mediator5",
      "mediator6",
    )

    List(
      ConfigTransforms.disableAutoInit(allNewNodes),
      ConfigTransforms.useStaticTime,
    ) ++ ConfigTransforms.enableAlphaVersionSupport
  }

  private lazy val upgradeTime1: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)
  private lazy val upgradeTime2: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(60)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 2,
        numSequencers = 6,
        numMediators = 6,
      )
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
        _.focus(_.parameters.timeouts.processing.sequencerInfo)
          /*
          The first handshake with the new takes too long for P2 because s2 successor is stopped.
          The default timeout (before giving up) is 30 seconds and during that time, the simple execution queue
          for the synchronizer connect/disconnect/handshakes is blocked.
          A lower value makes the test faster. A value that is too low would make the test flaky.
           */
          .replace(NonNegativeDuration.ofSeconds(3))
      )
      .addConfigTransform(
        // We want to retry more aggressively in the test
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.lsu.handshake.retry)
            .replace(
              ExponentialBackoffConfig(
                initialDelay = config.NonNegativeFiniteDuration.ofMillis(100),
                maxDelay = config.NonNegativeDuration.ofMillis(200),
                maxRetries = Int.MaxValue,
              )
            )
        )
      )
      .addConfigTransforms(configTransforms*)
      .addConfigTransforms(ConfigTransforms.enableDevVersionSupport*)
      .withSetup { implicit env =>
        import env.*

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)

        participant1.synchronizers.connect_by_config(defaultSynchronizerConnectionConfig())

        /*
        Using threshold 2 ensures that the handshake with the successor is considered successful
        only when handshake is done with both sequencers. It makes testing the sequencer metrics easier.
         */
        participant2.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 2)
        )

        participant1.health.ping(participant2)

        fixture1 = Fixture(
          currentPsid = daId,
          upgradeTime = upgradeTime1,
          oldSynchronizerNodes =
            SynchronizerNodes(Seq(sequencer1, sequencer2), Seq(mediator1, mediator2)),
          newSynchronizerNodes =
            SynchronizerNodes(Seq(sequencer3, sequencer4), Seq(mediator3, mediator4)),
          newOldNodesResolution = Map(
            "sequencer3" -> "sequencer1",
            "sequencer4" -> "sequencer2",
            "mediator3" -> "mediator1",
            "mediator4" -> "mediator2",
          ),
          oldSynchronizerOwners = synchronizerOwners1,
          newPV = ProtocolVersion.dev,
          newSerial = daId.serial.increment.toNonNegative,
        )

        fixture2 = Fixture(
          currentPsid = fixture1.newPsid,
          upgradeTime = upgradeTime2,
          oldSynchronizerNodes = fixture1.newSynchronizerNodes,
          newSynchronizerNodes =
            SynchronizerNodes(Seq(sequencer5, sequencer6), Seq(mediator5, mediator6)),
          newOldNodesResolution = Map(
            "sequencer5" -> "sequencer3",
            "sequencer6" -> "sequencer4",
            "mediator5" -> "mediator3",
            "mediator6" -> "mediator4",
          ),
          oldSynchronizerOwners = Set(sequencer3, sequencer4),
          newPV = ProtocolVersion.dev,
          newSerial = fixture1.newPsid.serial.increment.toNonNegative,
        )
      }

  private var fixture1: Fixture = _
  private var fixture2: Fixture = _

  /** Performs the check restart participants, performs check again. The goal is to ensure the
    * metric value is correctly set after a restart.
    */
  private def checkLsuStatusMetrics(
      check: Unit => Assertion
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    logger.info("Checking metrics before restarting the nodes")
    eventually()(check(()))
    participants.local.stop()
    participants.local.start()
    participants.local.synchronizers.reconnect_all()

    logger.info("Checking metrics after restarting the nodes")
    eventually()(check(()))
  }

  "Logical synchronizer upgrade update metrics" should {
    "when LSU is announced" in { implicit env =>
      import env.*

      // Nothing is set yet
      getLsuStatusMetricValues(participant1) shouldBe empty

      fixture1.oldSynchronizerOwners.foreach(
        _.topology.lsu.announcement.propose(fixture1.newPsid, fixture1.upgradeTime)
      )

      // Ensure all nodes see the announcement
      eventually() {
        forAll(fixture1.oldSynchronizerNodes.all ++ participants.local)(
          _.topology.lsu.announcement
            .list(store = Some(fixture1.currentPsid))
            .filter(_.item.successorSynchronizerId == fixture1.newPsid)
            .loneElement
        )
      }

      checkLsuStatusMetrics(_ =>
        forAll(participants.local)(
          getLsuStatusMetricValues(_) shouldBe Map(
            fixture1.newPsid -> ParticipantMetrics.LsuStatus.LsuAnnounced
          )
        )
      )
    }

    "when sequencer successors are announced" in { implicit env =>
      import env.*

      migrateSynchronizerNodes(fixture1)

      // No handshake was done yet
      forAll(fixture1.newSynchronizerNodes.sequencers)(
        getParticipantHandshakesMetricValues(_) shouldBe empty
      )

      // prevent the handshake from succeeding so that we really control the steps
      fixture1.newSynchronizerNodes.all.stop()

      sequencer1.topology.lsu.sequencer_successors.propose_successor(
        sequencerId = sequencer1.id,
        endpoints = sequencer3.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
        successorSynchronizerId = fixture1.newPsid,
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

      checkLsuStatusMetrics(_ =>
        getLsuStatusMetricValues(participant1) shouldBe Map(
          fixture1.newPsid -> ParticipantMetrics.LsuStatus.SequencerSuccessorsKnown
        )
      )

      // P2 still needs to see the successor of sequencer2
      getLsuStatusMetricValues(participant2) shouldBe Map(
        fixture1.newPsid -> ParticipantMetrics.LsuStatus.LsuAnnounced
      )

      sequencer2.topology.lsu.sequencer_successors.propose_successor(
        sequencerId = sequencer2.id,
        endpoints = sequencer4.sequencerConnection.endpoints.map(_.toURI(useTls = false)),
        successorSynchronizerId = fixture1.newPsid,
      )

      // participants see the announcement
      eventually() {
        forAll(participants.local)(
          _.topology.lsu.sequencer_successors
            .list()
            .filter(_.item.sequencerId == sequencer2.id)
            .loneElement
        )

        getLsuStatusMetricValues(participant1) shouldBe Map(
          fixture1.newPsid -> ParticipantMetrics.LsuStatus.SequencerSuccessorsKnown
        )
        getLsuStatusMetricValues(participant2) shouldBe Map(
          fixture1.newPsid -> ParticipantMetrics.LsuStatus.SequencerSuccessorsKnown
        )
      }
    }

    "when handshake/topology copy is done" in { implicit env =>
      import env.*

      fixture1.newSynchronizerNodes.all.start()

      // Checking handshake and local copy independently is too difficult
      checkLsuStatusMetrics(_ =>
        forAll(participants.local) { p =>
          getLsuStatusMetricValues(p)
            .get(fixture1.newPsid)
            .value should be >= ParticipantMetrics.LsuStatus.LocalCopyDone
        }
      )

      /*
      Check the number of handshakes on the sequencers
      Because of retries, the values can be strictly greater than 1
       */
      eventually() {
        // P1 and P2 connected to sequencer3
        val s3Handshakes = getParticipantHandshakesMetricValues(sequencer3)
        s3Handshakes((participant1.id, "success")) should be >= 1L
        s3Handshakes((participant2.id, "success")) should be >= 1L

        // P2 only connected to sequencer4
        getParticipantHandshakesMetricValues(sequencer4)(
          (participant2.id, "success")
        ) should be >= 1L
      }
    }

    "when LSU is done" in { implicit env =>
      import env.*

      environment.simClock.foreach(_.advanceTo(upgradeTime1.immediateSuccessor))
      transferTraffic(Some(fixture1))

      eventually() {
        environment.simClock.foreach(_.advance(Duration.ofSeconds(1)))
        participants.all.forall(_.synchronizers.is_connected(fixture1.newPsid)) shouldBe true
        participants.all.forall(_.synchronizers.is_connected(fixture1.currentPsid)) shouldBe false
      }

      checkLsuStatusMetrics(_ =>
        forAll(participants.local)(
          getLsuStatusMetricValues(_)
            .get(fixture1.newPsid)
            .value shouldBe ParticipantMetrics.LsuStatus.LsuDone
        )
      )

      fixture1.oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer3, upgradeTime1.immediateSuccessor, logger)

      participant1.health.ping(participant2)
    }

    "Only the last successful LSU is reported" in { implicit env =>
      import env.*

      fixture2.newSynchronizerNodes.all.start()

      performSynchronizerNodesLsu(fixture2)

      eventually() {
        participant1.underlying.value.sync.getLsuStatusMetrics().futureValueUS.value shouldBe Set(
          fixture1.newPsid -> ParticipantMetrics.LsuStatus.LsuDone,
          fixture2.newPsid -> ParticipantMetrics.LsuStatus.LocalCopyDone,
        )
      }

      environment.simClock.value.advanceTo(fixture2.upgradeTime.immediateSuccessor)
      transferTraffic(Some(fixture2))
      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        participants.all.forall(_.synchronizers.is_connected(fixture2.newPsid)) shouldBe true
      }

      fixture2.oldSynchronizerNodes.all.stop()
      waitForTargetTimeOnSequencer(sequencer5, upgradeTime2.immediateSuccessor, logger)

      /*
       Only the last successful LSU is reported.
       Note we cannot directly inspect metrics because it takes a while for the old metric to be garbage collected
       by prometheus.
       */
      participant1.underlying.value.sync.getLsuStatusMetrics().futureValueUS.value shouldBe Set(
        fixture2.newPsid -> ParticipantMetrics.LsuStatus.LsuDone
      )

      checkLsuStatusMetrics(_ =>
        forAll(participants.local)(
          getLsuStatusMetricValues(_) shouldBe Map(
            fixture1.newPsid -> ParticipantMetrics.LsuStatus.LsuDone,
            fixture2.newPsid -> ParticipantMetrics.LsuStatus.LsuDone,
          )
        )
      )
    }

    "Third LSU is announced" in { implicit env =>
      import env.*

      val psid4 = fixture2.newPsid.incrementSerial
      fixture2.newSynchronizerNodes.sequencers.foreach(
        _.topology.lsu.announcement
          .propose(psid4, fixture2.upgradeTime.plusSeconds(86400))
      )

      // Ensure participants see the LSU announcement
      eventually() {
        forAll(participants.local)(
          _.topology.lsu.announcement
            .list(store = Some(fixture2.newPsid))
            .filter(_.item.successorSynchronizerId == psid4)
            .loneElement
        )
      }

      checkLsuStatusMetrics(_ =>
        forAll(participants.local)(
          getLsuStatusMetricValues(_) shouldBe Map(
            fixture1.newPsid -> ParticipantMetrics.LsuStatus.LsuDone,
            fixture2.newPsid -> ParticipantMetrics.LsuStatus.LsuDone,
            psid4 -> ParticipantMetrics.LsuStatus.LsuAnnounced,
          )
        )
      )
    }

    "Third LSU is cancelled" in { implicit env =>
      import env.*

      val psid4 = fixture2.newPsid.incrementSerial
      fixture2.newSynchronizerNodes.sequencers.foreach(
        _.topology.lsu.announcement
          .revoke(psid4, fixture2.upgradeTime.plusSeconds(86400))
      )

      // Ensure participants see the LSU cancellation
      eventually() {
        forAll(participants.local)(
          _.topology.lsu.announcement
            .list(store = Some(fixture2.newPsid))
            .filter(_.item.successorSynchronizerId == psid4)
            shouldBe empty
        )
      }

      checkLsuStatusMetrics(_ =>
        forAll(participants.local)(
          getLsuStatusMetricValues(_) shouldBe Map(
            fixture1.newPsid -> ParticipantMetrics.LsuStatus.LsuDone,
            fixture2.newPsid -> ParticipantMetrics.LsuStatus.LsuDone,
            psid4 -> ParticipantMetrics.LsuStatus.NoLsu,
          )
        )
      )
    }
  }
}
