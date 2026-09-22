// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.commitments

import com.digitalasset.canton.TestPredicateFiltersFixtureAnyWordSpec
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.store.AcsDigestStore
import com.digitalasset.canton.version.ProtocolVersion
import monocle.syntax.all.*

/** Integration test for the ACS commitment processing pipeline when running with PV=35, for example
  * to check that the sender watermark increases even though no commitments are exchanged.
  */
sealed trait AcsCommitmentsLocalPipelineOnlyIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with TestPredicateFiltersFixtureAnyWordSpec
    with HasCycleUtils {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1

  "the digest processor creates digests for counterparticipants" onlyRunWithOrLessThan (ProtocolVersion.v35) in {
    implicit env =>
      import env.*

      // trigger reconciliation checkpoints often to trigger sender watermark updates
      sequencer1.topology.synchronizer_parameters.propose_update(
        daId,
        _.update(reconciliationInterval = PositiveDurationSeconds.ofSeconds(1)),
      )

      participant1.synchronizers.connect_local(sequencer1, daName)
      participant2.synchronizers.connect_local(sequencer1, daName)
      participants.all.dars.upload(CantonExamplesPath)

      // running a ping exchanges contracts
      participant1.health.ping(participant2)

      // create parties and a contract using those parties
      val alice = participant1.parties.enable("alice")
      val bob = participant2.parties.enable("bob")

      IouSyntax.createIou(participant1)(alice, alice, observers = List(bob)).discard

      // ping one more time
      participant2.health.ping(participant1)

      participants.local.foreach { p =>
        clue(s"checking sender watermark for ${p.name}") {
          eventually() {
            val sync = p.underlying.value.sync.syncPersistentStateManager.get(daId).value
            val latestCheckpoint = sync.acsDigestStore
              .latestCheckpointUpTo(Offset.MaxValue, AcsDigestStore.checkpointReconciliationFilter)
              .futureValueUS
              .value
            val senderWatermark =
              sync.acsCommitmentSenderWatermarkStore.lookupWatermark().futureValueUS.value

            senderWatermark.offset shouldBe latestCheckpoint.offset
          }
        }
      }
  }
}

class AcsCommitmentsLocalPipelineOnlyIntegrationTestInMemory
    extends AcsCommitmentsLocalPipelineOnlyIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransform(ConfigTransforms.allInMemory)
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  registerPlugin(new UseBftSequencer(loggerFactory))
}

class AcsCommitmentsLocalPipelineOnlyIntegrationTestH2
    extends AcsCommitmentsLocalPipelineOnlyIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class AcsCommitmentsLocalPipelineOnlyIntegrationTestPostgres
    extends AcsCommitmentsLocalPipelineOnlyIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
