// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.protocol.DynamicSequencingParameters
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters
import com.digitalasset.canton.topology.transaction.DynamicSequencingParametersState

import scala.concurrent.duration.DurationInt

class BftOrderingDynamicSequencingParametersTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override protected def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P0_S1M1

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory, epochLength = EpochLength(1)))

  "using bft sequencer" should {

    "be able to change sequencing parameters" in { implicit env =>
      import env.*

      sequencer1.bft
        .get_ordering_topology()
        .sequencingParameters
        .pbftViewChangeTimeout shouldBe PositiveFiniteDuration(1.second).toInternal

      val newSequencingParameters =
        SequencingParameters.create(PositiveFiniteDuration(5.seconds).toInternal)(
          testedProtocolVersion
        )
      val newSequencingParametersProto = newSequencingParameters.toProto.toByteString

      sequencer1.topology.transactions.propose(
        mapping = DynamicSequencingParametersState(
          synchronizer1Id.logical,
          DynamicSequencingParameters(Option(newSequencingParametersProto))(
            DynamicSequencingParameters.protocolVersionRepresentativeFor(testedProtocolVersion)
          ),
        ),
        store = synchronizer1Id,
      )

      eventually() {
        sequencer1.bft.get_ordering_topology().sequencingParameters shouldBe newSequencingParameters
      }
    }

  }
}
