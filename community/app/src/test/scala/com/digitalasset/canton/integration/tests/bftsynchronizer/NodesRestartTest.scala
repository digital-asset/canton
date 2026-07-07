// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.console.LocalInstanceReference
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.sequencing.client.SequencerSubscriptionError.LostSequencerSubscription

trait NodesRestartTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with NodeTestingUtils {

  def checkAllHealthy(refs: Seq[LocalInstanceReference]): Unit =
    eventually() {
      forEvery(refs) { ref =>
        // Check health via status to avoid cached results/flakiness
        ref.health.status.isInitialized shouldBe true
      }
    }

  override def environmentDefinition: EnvironmentDefinition =
    // not using withManualStart because we use Environment#startAndReconnect as part of the test
    EnvironmentDefinition.P1_S1M1

  "All nodes are healthy" in { implicit env =>
    import env.*
    checkAllHealthy(nodes.local)
  }

  "Restart participant nodes not connected to a synchronizer" in { implicit env =>
    import env.*
    stopAndWait(participant1)
    startAndWait(participant1)
    checkAllHealthy(nodes.local)
  }

  "Restart an onboarded mediator node" in { implicit env =>
    import env.*
    stopAndWait(mediator1)
    startAndWait(mediator1)
    checkAllHealthy(nodes.local)
  }

  "Restart an onboarded sequencer node" in { implicit env =>
    import env.*
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        stopAndWait(sequencer1)
        startAndWait(sequencer1)
      },
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq.empty,
        mayContain = Seq(
          _.warningMessage should include("Health-check service responded NOT_SERVING for"),
          _.warningMessage should include("Token refresh failed with Status{code=UNAVAILABLE"),
          _.shouldBeCantonErrorCode(LostSequencerSubscription), // mediator might squeak
        ),
      ),
    )
    checkAllHealthy(nodes.local)
  }

  "Restart a participant node and reconnect to a previously connected synchronizer" in {
    implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, daName)

      eventually() {
        val result = participant1.synchronizers.list_connected()
        result should have size (1)
      }

      stopAndWait(participant1)

      // This "simulates" a startup of the Canton application, which triggers and automatic reconnect
      env.environment.startAndReconnect()

      eventually() {
        val result = participant1.synchronizers.list_connected()
        result should have size (1)
      }
  }
}

class NodesRestartTestPostgres extends NodesRestartTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
