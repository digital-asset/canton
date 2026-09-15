// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}

import scala.concurrent.Future

trait NodesBootstrapTest extends CommunityIntegrationTest with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0_S2M1

  "submit conflicting topology transactions" in { implicit env =>
    import env.*

    val seq1Tx = Future(
      sequencer1.topology.sequencers.propose(
        daId,
        threshold = PositiveInt.one,
        active = Seq(sequencer1.id),
        // We need to mention the other sequencer here, otherwise it gets "deactivated" and is likely not recoverable anymore.
        passive = Seq(sequencer2.id),
        serial = Some(PositiveInt.two),
        // Skips the server-side awaitUS on the asyncResult.
        synchronize = None,
      )
    )

    val seq2Tx = Future(
      sequencer2.topology.sequencers.propose(
        daId,
        threshold = PositiveInt.one,
        active = Seq(sequencer2.id),
        // We need to mention the other sequencer here, otherwise it gets "deactivated" and is likely not recoverable anymore.
        passive = Seq(sequencer1.id),
        serial = Some(PositiveInt.two),
        // Skips the server-side awaitUS on the asyncResult.
        synchronize = None,
      )
    )

    seq1Tx.futureValue.serial.value shouldBe 2
    seq2Tx.futureValue.serial.value shouldBe 2

    // Wait for conflict resolution and state distribution.
    eventually() {
      val seq1 = sequencer1.topology.sequencers.list(store = daId)
      val seq2 = sequencer2.topology.sequencers.list(store = daId)

      seq1 should not be empty
      seq2 should not be empty
      seq1.head.context.serial.value shouldBe 2
      seq2.head.context.serial.value shouldBe 2

      // We don't know which transaction won the race, but both sequencers
      // must have accepted the exact same transaction.
      seq1.head.item.active shouldBe seq2.head.item.active
    }
  }
}

// Default meaning in-memory
//class NodesBootstrapTestDefault extends NodesBootstrapTest {
//  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))
//}

class NodesBootstrapTestPostgres extends NodesBootstrapTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
