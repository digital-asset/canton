// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.EnvironmentDefinition.S4M4
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.upgrade.lsu.LogicalUpgradeUtils.SynchronizerNodes
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTimeOnSequencer
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import org.scalatest.Assertion

import java.time.Duration
import scala.concurrent.Future

/*
Check what happens if some of the synchronizer nodes upgrade late or don't upgrade.

Initial topology: 4 sequencers (for BFT fault tolerance, F = 1) and corresponding mediators (SV-like setup)
- p1 connected to s1
- p2 connected to s2
- p3 connected to s1, s2 with trust threshold=1
- p4 connected to s1, s2 with trust threshold=2

LSU
- s2 and m2 don't upgrade
- s1 and m1 upgrade to s5 and m5, s3 and m3 to s6 and m6, and s4 and m4 to s7 and m7; mediators upgrade is done late
  (i.e., after the upgrade time)
  - We check that a request submitted to s5 after upgrade time, but before m5 upgrades, succeeds eventually

What happens
- p1 automatically upgrade
- p2 needs repair
- p3 automatically upgrade (s2 removed from the list of successors)
- p4 needs repair
 */
@UnstableTest // TODO(#30893)
final class LsuFaultySynchronizerNodesIntegrationTest extends LsuBase {

  override protected def testName: String = "lsu-faulty-synchronizer-nodes"

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer.tryCreate(
        Set("sequencer1", "sequencer2", "sequencer3", "sequencer4"),
        Set("sequencer5", "sequencer6", "sequencer7"),
      ),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  override protected lazy val newOldSequencers: Map[String, String] =
    Map(
      "sequencer5" -> "sequencer1",
      "sequencer6" -> "sequencer3",
      "sequencer7" -> "sequencer4",
    )
  override protected lazy val newOldMediators: Map[String, String] =
    Map(
      "mediator5" -> "mediator1",
      "mediator6" -> "mediator3",
      "mediator7" -> "mediator4",
    )

  override protected lazy val upgradeTime: CantonTimestamp = CantonTimestamp.Epoch.plusSeconds(30)

  private val newMediatorsAndSequencers: Map[String, String] =
    Map(
      "mediator5" -> "sequencer5",
      "mediator6" -> "sequencer6",
      "mediator7" -> "sequencer7",
    )

  private var automaticallyUpgraded: Seq[ParticipantReference] = _
  private var manuallyUpgraded: Seq[LocalParticipantReference] = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4S7M7_Config
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S4M4)
      }
      .addConfigTransforms(configTransforms*)
      .withSetup { implicit env =>
        import env.*

        participant1.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer1))
        participant2.synchronizers.connect_by_config(synchronizerConnectionConfig(sequencer2))
        participant3.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 1)
        )
        participant4.synchronizers.connect_by_config(
          synchronizerConnectionConfig(Seq(sequencer1, sequencer2), threshold = 2)
        )

        automaticallyUpgraded = Seq(participant1, participant3)
        manuallyUpgraded = Seq(participant2, participant4)

        participants.all.dars.upload(CantonExamplesPath)

        setDefaultsDynamicSynchronizerParameters(daId, synchronizerOwners1)

        oldSynchronizerNodes = SynchronizerNodes(
          Seq(sequencer1, sequencer3, sequencer4),
          Seq(mediator1, mediator3, mediator4),
        )
        newSynchronizerNodes = SynchronizerNodes(
          Seq(sequencer5, sequencer6, sequencer7),
          Seq(mediator5, mediator6, mediator7),
        )
      }

  "Logical synchronizer upgrade" should {
    "work when there are faulty synchronizer nodes" in { implicit env =>
      import env.*

      val fixture = fixtureWithDefaults()

      participant1.health.ping(participant2)
      participant1.health.ping(participant3)
      // no activity for P4 on purpose: LSU should also work that way

      val upgradeFailureError = s"Upgrade to ${fixture.newPSId} failed"

      val logAssertions: Seq[(LogEntryOptionality, LogEntry => Assertion)] =
        Seq(
          (
            LogEntryOptionality.Required,
            _.toString should (include("participant2") and include(upgradeFailureError) and include(
              "No sequencer successor was found"
            )),
          ),
          (
            LogEntryOptionality.Required,
            _.toString should (include("participant4") and include(upgradeFailureError) and include(
              "Not enough successors sequencers (1) to meet the sequencer threshold (2)"
            )),
          ),
          (
            LogEntryOptionality.Required,
            entry => {
              entry.toString should include("participant3")
              // p3 will be connected to only a sequencer after the lsu
              entry.warningMessage should include(
                s"Missing successor information for the following sequencers: Set($sequencer2). They will be removed from the pool of sequencers."
              )
            },
          ),
          (
            LogEntryOptionality.OptionalMany,
            _.shouldBeCantonErrorCode(SequencerError.NotAtUpgradeTimeOrBeyond),
          ),
        )

      val exportDirectory = loggerFactory.assertLogsUnorderedOptional(
        clue("Migrate") {
          fixture.oldSynchronizerOwners.foreach(
            _.topology.lsu.announcement
              .propose(fixture.newPSId, fixture.upgradeTime)
          )

          val exportDirectory = exportNodesData(
            SynchronizerNodes(
              sequencers = fixture.oldSynchronizerNodes.sequencers,
              mediators = fixture.oldSynchronizerNodes.mediators,
            ),
            successorPSId = fixture.newPSId,
          )

          // Note that mediators are not migrated yet
          newOldSequencers.foreach { case (newSequencerName, oldSequencerName) =>
            migrateSequencer(
              migratedSequencer = s(newSequencerName),
              newStaticSynchronizerParameters = fixture.newStaticSynchronizerParameters,
              exportDirectory = exportDirectory,
              oldNodeName = oldSequencerName,
            )

            s(oldSequencerName).topology.lsu.sequencer_successors.propose_successor(
              sequencerId = s(oldSequencerName).id,
              endpoints =
                s(newSequencerName).sequencerConnection.endpoints.map(_.toURI(useTls = false)),
              synchronizerId = fixture.currentPSId,
            )
          }

          environment.simClock.value.advanceTo(upgradeTime.immediateSuccessor)
          transferTraffic(suppressLogs = false)

          exportDirectory
        },
        logAssertions*
      )

      eventually() {
        environment.simClock.value.advance(Duration.ofSeconds(1))
        forEvery(automaticallyUpgraded)(_.synchronizers.is_connected(fixture.newPSId) shouldBe true)
        forEvery(automaticallyUpgraded)(
          _.synchronizers.is_connected(fixture.currentPSId) shouldBe false
        )
      }
      for (newSequencer <- newOldSequencers.keys) {
        waitForTargetTimeOnSequencer(ls(newSequencer), environment.clock.now, logger)
      }

      participant1.underlying.value.sync
        .connectedSynchronizerForAlias(daName)
        .value
        .numberOfDirtyRequests() shouldBe 0

      /*
         P1 and P2 are automatically upgraded but the ping does not succeed yet because
         the mediator did not migrate yet.
       */
      val pingF = Future(participant1.health.ping(participant3))

      eventually() {
        participant1.underlying.value.sync
          .connectedSynchronizerForAlias(daName)
          .value
          .numberOfDirtyRequests() shouldBe 1 // ping is in-flight

        pingF.isCompleted shouldBe false // ping is not completed
      }

      newOldMediators.foreach { case (newMediatorName, oldMediatorName) =>
        migrateMediator(
          migratedMediator = m(newMediatorName),
          newPSId = fixture.newPSId,
          newSequencers = Seq(s(newMediatorsAndSequencers(newMediatorName))),
          exportDirectory = exportDirectory,
          oldNodeName = oldMediatorName,
        )
      }

      pingF.futureValue // ping should succeed

      manuallyUpgraded.foreach { p =>
        p.repair.perform_manual_lsu(
          currentPhysicalSynchronizerId = fixture.currentPSId,
          successorPhysicalSynchronizerId = fixture.newPSId,
          announcedUpgradeTime = fixture.upgradeTime,
          successorConfig = synchronizerConnectionConfig(sequencer5),
        )
        p.synchronizers.reconnect_all()
      }

      // Activity should be possible for all participants
      participant1.health.ping(participant2)
      participant3.health.ping(participant4)
    }
  }
}
