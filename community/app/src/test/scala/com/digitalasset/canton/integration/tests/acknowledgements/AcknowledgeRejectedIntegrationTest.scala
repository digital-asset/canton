// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.acknowledgements

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseConfigTransforms,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.bftsequencer.AwaitsBftSequencerAuthenticationDisseminationQuorum
import com.digitalasset.canton.integration.util.TestUtils.waitForTargetTime
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.SequencerConfig.BftSequencer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.WriteReadiness
import monocle.macros.syntax.lens.*

import scala.concurrent.duration.DurationInt

sealed trait AcknowledgeRejectedP2PNotReadyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with AwaitsBftSequencerAuthenticationDisseminationQuorum {

  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M1_Config
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(participant1, participant2),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1, sequencer2, sequencer3, sequencer4),
            mediators = Seq(mediator1),
          )
        )
      }

  "A BFT sequencer that is not P2P-ready" must {
    "reject an acknowledgement promptly" in { implicit env =>
      import env.*

      waitUntilAllBftSequencersAuthenticateDisseminationQuorum()

      val sequencerClient =
        mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator.sequencerClient

      val timestampToAck = environment.clock.now
      waitForTargetTime(mediator1, sequencerClient, timestampToAck, logger)

      clue("stop all but one sequencer, so that sequencer4 loses P2P readiness") {
        Seq(sequencer1, sequencer2, sequencer3).foreach(_.stop())
        // restarting helps sequencer4 notice that the others have stopped deterministically.
        sequencer4.stop()
        sequencer4.start()
        eventually(60.seconds) {
          sequencer4.bft.get_write_readiness() shouldBe a[WriteReadiness.P2PNotReady]
        }
      }

      clue("wait until we know that the acknowledge request will go to sequencer4") {
        eventually() {
          sequencerClient.connectionPool.nbSequencers.unwrap shouldBe 1
        }
      }

      clue("acknowledge via the mediator's sequencer client; only sequencer4 is reachable") {
        loggerFactory.suppressWarningsAndErrors {
          sequencerClient
            .acknowledgeSigned(timestampToAck)
            .value
            .failOnShutdown("acknowledging during test")
            // The acknowledgement should be immediately rejected.
            // 20 seconds is below the 2-minute GRPC timeout but still long
            // enough to account for flakiness.
            .futureValue(timeout(20.seconds))
            .left
            .value should include("P2P connectivity is not ready")
        }
      }
    }
  }
}

class AcknowledgeRejectedP2PNotReadyIntegrationTestPostgres
    extends AcknowledgeRejectedP2PNotReadyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}

sealed trait AcknowledgeRejectedOverloadedIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UseBftSequencer(loggerFactory))
  // Must run after UseBftSequencer, which replaces the BFT orderer config.
  registerPlugin(
    new UseConfigTransforms(
      Seq(
        ConfigTransforms.updateSequencerConfig("sequencer1")(
          _.focus(_.sequencer).modify {
            case bft: BftSequencer => bft.focus(_.config.maxMempoolQueueSize).replace(0)
            case other => other
          }
        )
      ),
      loggerFactory,
    )
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Config
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(participant1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.local,
            mediators = Seq(mediator1),
          )
        )
      }

  "A BFT sequencer whose mempool is full" must {
    "answer an acknowledgement promptly instead of hanging until the gRPC deadline" in {
      implicit env =>
        import env.*

        val mediator = mediator1.underlying.value.replicaManager.mediatorRuntime.value.mediator
        val sequencerClient = mediator.sequencerClient

        // Acknowledge a timestamp the mediator has already processed, we
        // can't use waitForTargetTime since that would use the (overloaded)
        // sequencer.
        val timestampToAck = mediator.topologyClient.approximateTimestamp

        loggerFactory.suppressWarningsAndErrors {
          sequencerClient
            .acknowledgeSigned(timestampToAck)
            .value
            .failOnShutdown("acknowledging during test")
            .futureValue(timeout(20.seconds))
            .left
            .value should include("SEQUENCER_OVERLOADED")
        }
    }
  }
}

class AcknowledgeRejectedOverloadedIntegrationTestPostgres
    extends AcknowledgeRejectedOverloadedIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
