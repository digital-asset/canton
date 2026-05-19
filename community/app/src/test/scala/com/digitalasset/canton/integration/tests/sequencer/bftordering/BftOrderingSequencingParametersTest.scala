// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.digitalasset.canton.admin.api.client.data.SequencingParameters as ConsoleSequencingParameters
import com.digitalasset.canton.admin.api.client.data.topology.ListSequencingParametersStateResult
import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.{
  HowLongToBlacklist,
  HowManyCanWeBlacklist,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters.{
  DefaultLeaderSelectionPolicyConfig,
  SegmentLength,
}
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.DurationInt

class BftOrderingSequencingParametersTest extends CommunityIntegrationTest with SharedEnvironment {

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0_S1M1

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  "using bft sequencer" should {

    "be able to change sequencing parameters" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
      implicit env =>
        import env.*

        val defaultSequencingParameters = SequencingParameters.Default(testedProtocolVersion)

        sequencer1.topology.sequencing_parameters
          .latest(store = synchronizer1Id) shouldBe None
        sequencer1.topology.sequencing_parameters.get_sequencing_parameters(
          synchronizerId = synchronizer1Id
        ) shouldBe None
        sequencer1.topology.sequencing_parameters.list(
          store = synchronizer1Id
        ) shouldBe empty
        sequencer1.bft
          .get_ordering_topology()
          .sequencingParameters shouldBe defaultSequencingParameters

        val newPbftViewChangeTimeout =
          SequencingParameters.DefaultPbftViewChangeTimeout.toConfig.plusSeconds(1).toInternal

        val newSegmentLength =
          SegmentLength(SequencingParameters.DefaultSegmentLength.length + PositiveLong.one)

        val newLeaderSelectionPolicyConfig =
          DefaultLeaderSelectionPolicyConfig.copy(
            howLongToBlacklist = HowLongToBlacklist.NoBlacklisting,
            howManyCanWeBlacklist = HowManyCanWeBlacklist.NoBlacklisting,
          )

        val newSequencingParameters =
          SequencingParameters.create(
            newPbftViewChangeTimeout,
            newSegmentLength,
            newLeaderSelectionPolicyConfig,
          )(
            testedProtocolVersion
          )
        val newSequencingParametersProto = newSequencingParameters.toByteString
        val newConsoleSequencingParameters =
          ConsoleSequencingParameters(Some(newSequencingParametersProto))

        sequencer1.topology.sequencing_parameters.propose(
          synchronizer1Id.logical,
          newConsoleSequencingParameters,
        )

        eventually() {
          sequencer1.topology.sequencing_parameters.latest(
            store = synchronizer1Id
          ) shouldBe Some(newConsoleSequencingParameters)
          sequencer1.topology.sequencing_parameters.get_sequencing_parameters(
            synchronizerId = synchronizer1Id
          ) shouldBe Some(newConsoleSequencingParameters)
          sequencer1.topology.sequencing_parameters.list(
            store = synchronizer1Id
          ) should matchPattern {
            case Seq(
                  ListSequencingParametersStateResult(
                    _,
                    `newConsoleSequencingParameters`,
                  )
                ) =>
          }
          sequencer1.bft
            .get_ordering_topology()
            .sequencingParameters shouldBe newSequencingParameters
        }

        val defaultSequencingParametersProto = defaultSequencingParameters.toByteString
        val defaultConsoleSequencingParameters =
          ConsoleSequencingParameters(Some(defaultSequencingParametersProto))

        sequencer1.topology.sequencing_parameters.propose_update(
          synchronizer1Id.logical,
          _ => defaultConsoleSequencingParameters,
        )

        eventually(timeUntilSuccess = 1.minute) {
          sequencer1.topology.sequencing_parameters.latest(
            store = synchronizer1Id
          ) shouldBe Some(defaultConsoleSequencingParameters)
          sequencer1.topology.sequencing_parameters.get_sequencing_parameters(
            synchronizerId = synchronizer1Id
          ) shouldBe Some(defaultConsoleSequencingParameters)
          sequencer1.topology.sequencing_parameters.list(
            store = synchronizer1Id,
            proposals = false,
            timeQuery = TimeQuery.HeadState,
            operation = Some(TopologyChangeOp.Replace),
            filterSynchronizer = synchronizer1Id.filterString,
          ) should matchPattern {
            case Seq(
                  ListSequencingParametersStateResult(
                    _,
                    `defaultConsoleSequencingParameters`,
                  )
                ) =>
          }
          sequencer1.bft
            .get_ordering_topology()
            .sequencingParameters shouldBe defaultSequencingParameters
        }
    }
  }
}
