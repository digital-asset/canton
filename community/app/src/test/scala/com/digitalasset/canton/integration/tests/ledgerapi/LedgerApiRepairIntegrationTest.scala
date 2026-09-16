// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.digitalasset.canton.config.{NonNegativeFiniteDuration, TestSequencerClientFor}
import com.digitalasset.canton.console.{CommandFailure, FeatureFlag, LocalParticipantReference}
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.repair.RepairServiceIntegrationTest
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.sequencing.client.DelayedSequencerClient
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import monocle.syntax.all.*

import scala.concurrent.Promise

class LedgerApiRepairIntegrationTest extends RepairServiceIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory
    )
  )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Config
      .addConfigTransforms(
        ConfigTransforms.enableAdvancedCommands(FeatureFlag.Repair)
      )
      .withNetworkBootstrap { implicit env =>
        val daDesc = EnvironmentDefinition.S1M1
        new NetworkBootstrapper(
          // Make the topology transaction effective immediately
          daDesc.withTopologyChangeDelay(NonNegativeFiniteDuration.ofSeconds(0))
        )
      }
      // Set up the delayed sequencer client for participant1
      .updateTestingConfig(
        _.focus(_.testSequencerClientFor).replace(
          Set(TestSequencerClientFor(this.getClass.getSimpleName, "participant1", "synchronizer1"))
        )
      )

  override protected def cantonTestsPath: String = CantonExamplesPath

  "topology transaction at ledger end prevents repair transactions" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, alias = daName)
    participant2.synchronizers.connect_local(sequencer1, alias = daName)
    participant1.dars.upload(cantonTestsPath, synchronizerId = daId)
    participant2.dars.upload(cantonTestsPath, synchronizerId = daId)
    eventually()(assert(participant1.synchronizers.is_connected(daId)))
    eventually()(assert(participant2.synchronizers.is_connected(daId)))
    val alice = participant1.parties.testing.enable("alice")
    val bob = participant2.parties.testing.enable("bob")
    // ensure all participants have observed a point after the topology changes before disconnecting them
    participants.local.foreach(_.testing.fetch_synchronizer_times())
    participant1.synchronizers.disconnect(daName)

    val (repairContract, releaseClient1) = withSynchronizerConnected(daName) {
      val startOffset = participant1.ledger_api.state.end()
      IouSyntax.createIous(participant1, alice, alice, 1 to 1)

      // preparing repair contracts while synchronizer is still connected
      val repairContract = createContractInstance(participant1, daId, alice, bob)

      logger.debug("Creating new topology event")
      // Usually the sequencer generates a SequencerIndexMoved event when the topology transaction
      // becomes effective. `TopologyChangeDelay` here is set to 0, so the SequencerIndexMoved is generated
      // right after the topology transaction. However, this test needs a scenario where
      // the last event is the topology transaction, so we need to delay the SequencerIndexMoved event.
      // Setting up the sequencer client of participant1 to block on processing the specified event.
      // The counter is set to 1, meaning that it should block events after one processed successfully
      val releaseClient = setupDelayedSequencerClientForParticipant1(daId, participant1, 1)
      val newParty = participant1.parties.testing.enable(
        "NewParty",
        synchronizeParticipants = List(participant1, participant2),
      )

      val txs = participant1.ledger_api.updates
        .topology_transactions(1, Seq(newParty), startOffset)
        .map(_.topologyTransaction)
      txs.headOption.value.events.headOption.flatMap(_.event.participantAuthorizationAdded) match {
        case Some(event) =>
          event.participantId shouldBe participant1.id.toLf
          event.partyId shouldBe newParty.toProtoPrimitive
        case None =>
          fail("Expected a participant authorization added event in the topology transaction")
      }

      // no more events should be ingested after the topology transaction
      Some(participant1.ledger_api.state.end()) shouldBe txs.headOption.map(_.offset)
      (repairContract, releaseClient)
    }

    logger.debug("repair event must fail")
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.repair.add(daId, testedProtocolVersion, Seq(repairContract)),
      _.commandFailureMessage should include(
        "Cannot apply a repair command as the last event is a topology offset."
      ),
    )

    logger.debug("releasing sequencer client")
    releaseClient1.success(())

    logger.debug("reconnecting and running a transaction")
    withSynchronizerConnected(daName) {
      IouSyntax.createIous(participant1, alice, alice, 1 to 1)
    }

    logger.debug("retrying repair should succeed")
    participant1.repair.add(daId, testedProtocolVersion, Seq(repairContract))

    // same scenario again, but this time using the force flag
    val releaseClient2 = withSynchronizerConnected(daName) {
      val startOffset = participant1.ledger_api.state.end()
      logger.debug("Creating the second topology event")
      val releaseClient = setupDelayedSequencerClientForParticipant1(daId, participant1, 1)
      val newParty = participant1.parties.testing.enable(
        "NewParty2",
        synchronizeParticipants = List(participant1, participant2),
      )
      val txs = participant1.ledger_api.updates
        .topology_transactions(1, Seq(newParty), startOffset)
        .map(_.topologyTransaction)

      // no more events should be ingested after the topology transaction
      Some(participant1.ledger_api.state.end()) shouldBe txs.headOption.map(_.offset)
      releaseClient
    }

    logger.debug("resubmitting with force flag")
    participant1.repair.add(
      daId,
      testedProtocolVersion,
      Seq(repairContract),
      forceRepairWhenTopologyTransactionAtLedgerEnd = true,
    )
    releaseClient2.success(())

    logger.debug("reconnecting and running a transaction, showing that the repair was successful")
    withSynchronizerConnected(daName) {
      IouSyntax.createIous(participant1, alice, alice, 1 to 1)
    }
  }

  private def setupDelayedSequencerClientForParticipant1(
      sync: PhysicalSynchronizerId,
      participant: LocalParticipantReference,
      count: Int,
  ): Promise[Unit] = {
    @volatile var blockAfterSequencerEvents = count
    val releaseClient = Promise[Unit]()
    val participant2SequencerClientInterceptor = DelayedSequencerClient
      .delayedSequencerClient(this.getClass.getSimpleName, sync, participant.id.uid.toString)
      .value
    participant2SequencerClientInterceptor.setDelayPolicy { _ =>
      if (blockAfterSequencerEvents > 0) {
        blockAfterSequencerEvents = blockAfterSequencerEvents - 1
        DelayedSequencerClient.Immediate
      } else {
        logger.debug("Delaying sequencer batch")
        DelayedSequencerClient.DelayUntil(releaseClient.future)
      }
    }
    releaseClient
  }
}
