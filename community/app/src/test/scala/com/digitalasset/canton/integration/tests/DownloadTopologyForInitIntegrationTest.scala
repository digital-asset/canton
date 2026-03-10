// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{DbConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.{DbStorage, StorageSingleFactory}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.{MediatorId, NodeIdentity}

import java.time.Instant

abstract class DownloadTopologyForInitIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer {

  private var mediator2Id: MediatorId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S2M2_Config
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1, sequencer2),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1, sequencer2),
            mediators = Seq(mediator1, mediator2),
            overrideMediatorToSequencers = Some(
              Map(
                // A threshold of 2 ensures that the mediators connect to all sequencers.
                // TODO(#19911) Make this properly configurable
                mediator1 -> (Seq(sequencer1, sequencer2), PositiveInt.two, NonNegativeInt.zero),
                // Have this so that mediator2 gets registered in the topology state.
                mediator2 -> (Seq(sequencer2), PositiveInt.one, NonNegativeInt.zero),
              )
            ),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        // Shutdown mediator2, so that it does not emit warnings if we mess with its topology state
        mediator2Id = mediator2.id
        mediator2.stop()
      }

  "detect a hash disagreement among sequencers" in { implicit env =>
    import env.*

    // Double-check that OwnerToKeyMappings for mediator1 and mediator2 have the same timestamp,
    // so that the OTK for mediator2 shows up in the initial topology snapshot for mediator1
    def keyValidFrom(keyOwner: NodeIdentity): Instant = sequencer1.topology.owner_to_key_mappings
      .list(store = Some(daId), filterKeyOwnerUid = keyOwner.filterString)
      .loneElement
      .context
      .validFrom
    keyValidFrom(mediator1.id) shouldBe keyValidFrom(mediator2Id)

    // we make sequencer1 malicious by deleting the topology mappings for mediator2 from the db
    val factory = new StorageSingleFactory(sequencer1.config.storage)
    val storage = factory.tryCreate(
      connectionPoolForParticipant = false,
      None,
      new SimClock(CantonTimestamp.Epoch, loggerFactory),
      None,
      CommonMockMetrics.dbStorage,
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    ) match {
      case jdbc: DbStorage => jdbc
      case _ => fail("should be db storage")
    }
    import storage.api.*
    storage
      .update(
        sqlu"""delete from common_topology_transactions where identifier = ${mediator2Id.member.identifier};""",
        "test-delete-mediator-topology-mapping",
      )
      .futureValueUS should be > 0
    storage.close()

    val sequencerClient =
      mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.sequencerClient

    // Need to restart the sequencer due to potentially cached initial topology state hash
    // that will not pick up the removed db record above
    sequencer1.stop()
    sequencer1.start()

    // Wait until mediator1 has reconnected to the sequencer
    eventually() {
      sequencer1.health.status.successOption.value.connectedMediators should contain(mediator1.id)
    }

    val errorMessage = sequencerClient
      .downloadTopologyStateForInit(0, retryLogLevel = None)
      .futureValueUS
      .leftOrFail("Expected a hash disagreement")

    // Have we got the right kind of error?
    errorMessage should include("Failed to get initial topology state hash: FailedToReachThreshold")

    // Has it received hashes for both sequencer1 and sequencer2?
    errorMessage should include regex raw"TopologyStateForInitHashResponse\(SHA-256:\S+\.\.\.\) -> Set\(SEQ::sequencer1::\S+\.\.\.\)"
    errorMessage should include regex raw"TopologyStateForInitHashResponse\(SHA-256:\S+\.\.\.\) -> Set\(SEQ::sequencer2::\S+\.\.\.\)"

    // No failures occurred while getting the hash?
    errorMessage should endWith(",Map())")
  }

}

class DownloadTopologyForInitIntegrationTestPostgres
    extends DownloadTopologyForInitIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))
  // TODO(#29833): Graceful shutdown of BftBlockOrderer
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
