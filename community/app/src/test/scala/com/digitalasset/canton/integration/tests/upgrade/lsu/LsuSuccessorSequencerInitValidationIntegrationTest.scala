// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrade.lsu

import better.files.File
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseBftSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.store.StoredTopologyTransaction
import com.digitalasset.canton.topology.transaction.LsuAnnouncement
import com.digitalasset.canton.util.GrpcStreamingUtils
import monocle.macros.syntax.lens.*

import scala.util.Using

final class LsuSuccessorSequencerInitValidationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var predecessorId = Option.empty[com.digitalasset.canton.topology.PhysicalSynchronizerId]

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(numParticipants = 0, numSequencers = 2, numMediators = 1)
      .withManualStart
      .addConfigTransforms(
        ConfigTransforms.updateSequencerConfig("sequencer2")(
          _.focus(_.topology.checkInitialTopologySnapshotConsistency).replace(false)
        )
      )
      .withSetup { implicit env =>
        import env.*
        sequencer1.start()
        mediator1.start()
        predecessorId = Some(
          bootstrap.synchronizer(
            daName.unwrap,
            synchronizerOwners = Seq(sequencer1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
          )
        )
      }

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      MultiSynchronizer(
        Seq("sequencer1", "sequencer2")
          .map(name => Set(InstanceName.tryCreate(name)))
      ),
    )
  )

  private def assertNoLsuAnnouncement(file: File): Unit = {
    val snapshot = Using.resource(file.newInputStream)(input =>
      GrpcStreamingUtils
        .parseDelimitedFromTrusted(input, StoredTopologyTransaction)
        .valueOrFail("deserialize successor snapshot")
    )
    snapshot should not be empty
    snapshot.flatMap(_.selectMapping[LsuAnnouncement]) shouldBe empty
  }

  "LSU successor sequencer initialization from predecessor" should {
    "wire the mandatory announcement validation before persisting initialization" in {
      implicit env =>
        import env.*

        sequencer2.start()

        File.temporaryFile("lsu-successor-without-announcement", ".proto").foreach { file =>
          sequencer1.topology.transactions.export_genesis_state(file.canonicalPath)
          assertNoLsuAnnouncement(file)

          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            sequencer2.setup.initialize_from_lsu_predecessor(
              inputFile = file.canonicalPath,
              synchronizerId = predecessorId.value.logical,
              synchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
              ignorePsidCheck = false,
            ),
            entries =>
              forExactly(1, entries) { entry =>
                entry.errorMessage should include(TopologyManagerError.MissingTopologyMapping.id)
                entry.errorMessage should include("does not contain an LSU announcement")
              },
          )

          // A restart must not treat a rejected snapshot as persisted initialization.
          sequencer2.stop()
          sequencer2.start()
          sequencer2.health.wait_for_ready_for_initialization()
          sequencer2.health.initialized() shouldBe false
        }
    }
  }
}
