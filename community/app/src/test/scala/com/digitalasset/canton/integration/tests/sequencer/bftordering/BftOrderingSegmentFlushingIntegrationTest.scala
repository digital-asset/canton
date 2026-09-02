// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.digitalasset.canton.admin.api.client.data.SequencingParameters as ConsoleSequencingParameters
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.{
  HowLongToBlacklist,
  HowManyCanWeBlacklist,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  BlacklistLeaderSelectionPolicyConfig,
  SequencingParameters,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.slf4j.event

import scala.concurrent.duration.*
import scala.concurrent.{Future, blocking}

@SuppressWarnings(Array("com.digitalasset.canton.DiscardedFuture"))
class BftOrderingSegmentFlushingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 3,
        numSequencers = 4,
        numMediators = 1,
      )
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1, sequencer2, sequencer3, sequencer4),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1, sequencer2, sequencer3, sequencer4),
            mediators = Seq(mediator1),
          )
        )
      }

  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      availabilityMinProposalCreationDelay = 0.seconds,
      minRequestsInBatch = 1,
      maxBatchCreationInterval = 1.milli,
      // avoid view changes in this test, as we want to test flushing of segments, not view changes
      viewChangeTimeoutOverride = Some(10.hours),
    )
  )
  registerPlugin(new UsePostgres(loggerFactory))

  "be able to flush segments" onlyRunWithOrGreaterThan ProtocolVersion.v35 in { implicit env =>
    import env.*

    applySequencingParameters(
      SequencingParameters.create(
        blacklistLeaderSelectionPolicyConfig = BlacklistLeaderSelectionPolicyConfig(
          howLongToBlacklist = HowLongToBlacklist.NoBlacklisting,
          howManyCanWeBlacklist = HowManyCanWeBlacklist.NoBlacklisting,
        ),
        maxRequestsInBatch = 1,
        maxBatchesPerBlockProposal = 1,
      )(testedProtocolVersion)
    )

    participant1.synchronizers.connect_local(sequencer1, daName)
    participant2.synchronizers.connect_local(sequencer2, daName)
    participant3.synchronizers.connect_local(sequencer3, daName)

    sequencer4.stop()

    loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(event.Level.INFO))(
      {
        (0 until 10).foreach { _ =>
          Future(participant1.health.maybe_ping(participant2, timeout = 1.second))
          Future(participant2.health.maybe_ping(participant3, timeout = 1.second))
          Future(participant3.health.maybe_ping(participant1, timeout = 1.second))
        }
        blocking(Thread.sleep(2000))
        sequencer4.start()
        sequencer4.health.wait_for_running()
        sequencer4.health.wait_for_initialized()
      },
      logs => {
        forAtLeast(1, logs) { log =>
          log.infoMessage should include("blocks to flush segment")
        }
      },
      timeUntilSuccess = 1.minute,
    )
  }

  def applySequencingParameters(
      sequencingParams: topology.SequencingParameters
  )(implicit env: TestConsoleEnvironment): Assertion = {
    import env.*
    val newConsoleSequencingParameters = {
      val newSequencingParametersProto = sequencingParams.toByteString
      ConsoleSequencingParameters(Some(newSequencingParametersProto))
    }
    sequencer1.topology.sequencing_parameters.propose(
      synchronizer1Id.logical,
      newConsoleSequencingParameters,
    )
    eventually(timeUntilSuccess = 1.minute) {
      sequencer1.bft
        .get_ordering_topology()
        .sequencingParameters shouldBe sequencingParams
    }
  }
}
