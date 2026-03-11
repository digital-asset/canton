// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.IgnoringUnitTestEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  failingCryptoProvider,
  fakeIgnoringModule,
}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

class AvailabilityModuleUpdateTopologyTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with AvailabilityModuleTestUtils {

  "The availability module" should {

    "update the topology during state transfer if it's more recent" in {
      val initialMembershipOtherNodes = Set(Node1, Node2)
      val initialMembership = Membership.forTesting(Node0, initialMembershipOtherNodes)
      val initialCryptoProvider = failingCryptoProvider[IgnoringUnitTestEnv]
      val newMembershipOtherNodes = Set(Node1, Node2, Node3)
      val newMembership = Membership.forTesting(Node0, newMembershipOtherNodes)
      val newCryptoProvider = failingCryptoProvider[IgnoringUnitTestEnv]
      val disseminationProtocolState = new DisseminationProtocolState()

      val disseminationStatus =
        ABatchDisseminationProgressNode0And1WithNode0Vote._1 -> ABatchDisseminationProgressNode0And1WithNode0Vote._2
          .update()
          .asComplete
          .getOrElse(fail("test data should be complete"))
      disseminationProtocolState.disseminationProgress.addOne(disseminationStatus)

      val availability =
        createAndStartAvailability[IgnoringUnitTestEnv](
          otherNodes = initialMembershipOtherNodes,
          cryptoProvider = initialCryptoProvider,
          disseminationProtocolState = disseminationProtocolState,
          consensus = fakeIgnoringModule,
        )

      // double-check initial values
      availability.getActiveMembership shouldBe initialMembership
      availability.getActiveCryptoProvider shouldBe initialCryptoProvider
      availability.getMessageAuthorizer shouldBe initialMembership.orderingTopology

      availability.receive(
        Availability.Consensus
          .UpdateTopologyDuringStateTransfer(newMembership, newCryptoProvider)
      )

      // make sure new values are different
      availability.getActiveMembership shouldBe newMembership // we don't care about other fields
      availability.getActiveCryptoProvider shouldBe newCryptoProvider
      availability.getMessageAuthorizer shouldBe newMembership.orderingTopology

      // check that dissemination progress hasn't changed yet
      disseminationProtocolState.disseminationProgress should contain only disseminationStatus

      // send a regular proposal request without changing the topology and check that progress is reviewed
      availability.receive(
        Availability.Consensus.CreateProposal(
          BlockNumber.First,
          EpochNumber.First,
          newMembership,
          newCryptoProvider,
        )
      )

      val expectedDisseminationProgress =
        disseminationStatus._1 -> disseminationStatus._2
          .changeMembership(newMembership)
          .toEither
          .leftOrFail("dissemination should be in progress")
          // Stats gets reset after updating the dissemination progress
          .copy(disseminationRegressions = 0)
      disseminationProtocolState.disseminationProgress should contain only expectedDisseminationProgress

      // check that progress is not reviewed again when receiving the same topology
      disseminationProtocolState.disseminationProgress.addOne(disseminationStatus)
      availability.receive(
        Availability.Consensus.CreateProposal(
          BlockNumber(1),
          EpochNumber.First,
          newMembership,
          newCryptoProvider,
        )
      )

      disseminationProtocolState.disseminationProgress should contain only disseminationStatus
    }

    "not update the topology to an outdated one" in {
      val initialMembership = Membership
        .forTesting(Node0)
        .copy(orderingTopology =
          OrderingTopologyNode0
            .copy(activationTime = TopologyActivationTime(CantonTimestamp.MaxValue))
        )
      val initialOrderingTopology = initialMembership.orderingTopology
      val initialCryptoProvider = failingCryptoProvider[IgnoringUnitTestEnv]
      val newMembership = Membership.forTesting(Node0, Set(Node1))
      val newMembershipOutdated = newMembership.copy(orderingTopology =
        newMembership.orderingTopology.copy(activationTime =
          TopologyActivationTime(initialOrderingTopology.activationTime.value.minusSeconds(1))
        )
      )
      val newCryptoProvider = failingCryptoProvider[IgnoringUnitTestEnv]

      val disseminationProtocolState = new DisseminationProtocolState()

      val disseminationStatus = ABatchDisseminationProgressNode0And1WithNode0Vote
      disseminationProtocolState.disseminationProgress.addOne(disseminationStatus)

      val availability =
        createAndStartAvailability[IgnoringUnitTestEnv](
          cryptoProvider = initialCryptoProvider,
          customMembership = Some(initialMembership),
          disseminationProtocolState = disseminationProtocolState,
          consensus = fakeIgnoringModule,
        )

      suppressProblemLogs(
        availability.receive(
          Availability.Consensus
            .UpdateTopologyDuringStateTransfer(newMembershipOutdated, newCryptoProvider)
        )
      )

      availability.getActiveMembership.orderingTopology shouldBe initialOrderingTopology
      availability.getActiveCryptoProvider shouldBe initialCryptoProvider
      availability.getMessageAuthorizer shouldBe initialOrderingTopology

      // check that dissemination progress hasn't changed
      disseminationProtocolState.disseminationProgress should contain only disseminationStatus

      // send a regular proposal request without changing the topology and check that progress is not reviewed

      loggerFactory.assertLogs(
        availability.receive(
          Availability.Consensus.CreateProposal(
            BlockNumber.First,
            EpochNumber.First,
            newMembership,
            newCryptoProvider,
          )
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include("tried to overwrite topology")
        },
      )

      disseminationProtocolState.disseminationProgress should contain only disseminationStatus
    }
  }
}
