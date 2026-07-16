// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.{
  HowLongToBlacklist,
  HowManyCanWeBlacklist,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  BlacklistLeaderSelectionPolicyConfig,
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.AnyWordSpec

class BlacklistLeaderSelectionPolicyStateTest extends AnyWordSpec with BaseTest {

  private implicit val pv: ProtocolVersion = testedProtocolVersion

  private def n(i: Int): BftNodeId = BftNodeId(s"node$i")
  private val n0 = n(0)
  private val n1 = n(1)
  private val n2 = n(2)
  private val n3 = n(3)
  private val nodes = Set(n0, n1, n2, n3)
  private val orderingTopology = OrderingTopology.forTesting(
    nodes,
    epochLength = EpochLength(10),
  )

  private val blockToLeaderAll: Map[BlockNumber, BftNodeId] = Map(
    BlockNumber(0L) -> n0,
    BlockNumber(1L) -> n1,
    BlockNumber(2L) -> n2,
    BlockNumber(3L) -> n3,
  )

  private val blockToLeaderAllWithoutN0 = blockToLeaderAll.removed(BlockNumber(0L))

  private def initState(
      blacklist: (BftNodeId, BlacklistStatus.BlacklistStatusMark)*
  ): BlacklistLeaderSelectionPolicyState = BlacklistLeaderSelectionPolicyState.create(
    EpochNumber.First,
    BlockNumber.First,
    Map.from(blacklist),
  )(testedProtocolVersion)

  private def stateNextEpoch(
      blacklist: (BftNodeId, BlacklistStatus.BlacklistStatusMark)*
  ): BlacklistLeaderSelectionPolicyState = BlacklistLeaderSelectionPolicyState.create(
    EpochNumber(1L),
    BlockNumber(10L),
    Map.from(blacklist),
  )(testedProtocolVersion)

  private def makeConfig(
      howLongToBlacklist: HowLongToBlacklist = SequencingParameters.DefaultHowLongToBlackList,
      howManyCanWeBlacklist: HowManyCanWeBlacklist =
        SequencingParameters.DefaultHowManyCanWeBlacklist,
  ): SequencingParameters = SequencingParameters.create(
    SequencingParameters.DefaultPbftViewChangeTimeout,
    SequencingParameters.DefaultSegmentLength,
    BlacklistLeaderSelectionPolicyConfig(howLongToBlacklist, howManyCanWeBlacklist),
  )

  private def makeOrderingTopology(config: SequencingParameters): OrderingTopology =
    OrderingTopology.forTesting(
      nodes,
      epochLength = EpochLength(10),
      sequencingParameters = Some(config),
    )

  "BlacklistLeaderSelectionPolicyState" should {
    "a clean node" should {
      "stay clean if not punished" in {
        BlacklistLeaderSelectionPolicyStateWithTopology(
          initState(),
          orderingTopology,
          testedProtocolVersion,
        )
          .update(
            orderingTopology,
            blockToLeaderAll,
            Set.empty,
          )
          .state shouldBe stateNextEpoch()
      }

      "be blacklisted if punished" in {
        BlacklistLeaderSelectionPolicyStateWithTopology(
          initState(),
          orderingTopology,
          testedProtocolVersion,
        )
          .update(
            orderingTopology,
            blockToLeaderAll,
            Set(n0),
          )
          .state shouldBe stateNextEpoch(n0 -> BlacklistStatus.Blacklisted(1, 1))
      }
    }

    "a blacklisted node" should {
      "stay blacklisted if still time" in {
        BlacklistLeaderSelectionPolicyStateWithTopology(
          initState(
            n0 -> BlacklistStatus.Blacklisted(1, 2)
          ),
          orderingTopology,
          testedProtocolVersion,
        ).update(
          orderingTopology,
          blockToLeaderAllWithoutN0,
          Set.empty,
        ).state shouldBe stateNextEpoch(n0 -> BlacklistStatus.Blacklisted(1, 1))
      }

      "go on trial if waited long enough" in {
        BlacklistLeaderSelectionPolicyStateWithTopology(
          initState(
            n0 -> BlacklistStatus.Blacklisted(1, 1)
          ),
          orderingTopology,
          testedProtocolVersion,
        ).update(
          orderingTopology,
          blockToLeaderAllWithoutN0,
          Set.empty,
        ).state shouldBe stateNextEpoch(n0 -> BlacklistStatus.OnTrial(1))
      }
    }

    "a node on trial" should {
      "become clean if succeed" in {
        BlacklistLeaderSelectionPolicyStateWithTopology(
          initState(
            n0 -> BlacklistStatus.OnTrial(1)
          ),
          orderingTopology,
          testedProtocolVersion,
        ).update(
          orderingTopology,
          blockToLeaderAll,
          Set.empty,
        ).state shouldBe stateNextEpoch()
      }

      "become blacklisted if punished" in {
        BlacklistLeaderSelectionPolicyStateWithTopology(
          initState(
            n0 -> BlacklistStatus.OnTrial(1)
          ),
          orderingTopology,
          testedProtocolVersion,
        ).update(
          orderingTopology,
          blockToLeaderAll,
          Set(n0),
        ).state shouldBe stateNextEpoch(n0 -> BlacklistStatus.Blacklisted(2, 2))
      }

      "stay on trial if did not participate" in {
        BlacklistLeaderSelectionPolicyStateWithTopology(
          initState(
            n0 -> BlacklistStatus.OnTrial(1)
          ),
          orderingTopology,
          testedProtocolVersion,
        ).update(
          orderingTopology,
          blockToLeaderAllWithoutN0,
          Set.empty,
        ).state shouldBe stateNextEpoch(n0 -> BlacklistStatus.OnTrial(1))
      }
    }

    "should only select clean and nodes on trial" in {
      BlacklistLeaderSelectionPolicyStateWithTopology(
        initState(n1 -> BlacklistStatus.OnTrial(1), n2 -> BlacklistStatus.Blacklisted(1, 1)),
        orderingTopology,
        testedProtocolVersion,
      ).computeLeaders() shouldBe Seq(n0, n1, n3)
    }

    "should only drop up to f nodes" in {
      BlacklistLeaderSelectionPolicyStateWithTopology(
        initState(n1 -> BlacklistStatus.Blacklisted(2, 2), n2 -> BlacklistStatus.Blacklisted(1, 1)),
        orderingTopology,
        testedProtocolVersion,
      )
        .computeLeaders() shouldBe Seq(n0, n2, n3)
    }

    "should not drop any if config says so" in {
      BlacklistLeaderSelectionPolicyStateWithTopology(
        initState(n1 -> BlacklistStatus.Blacklisted(2, 1), n2 -> BlacklistStatus.Blacklisted(1, 1)),
        makeOrderingTopology(
          makeConfig(howManyCanWeBlacklist =
            BlacklistLeaderSelectionPolicyConfig.HowManyCanWeBlacklist.NoBlacklisting
          )
        ),
        testedProtocolVersion,
      ).computeLeaders() shouldBe Seq(n0, n1, n2, n3)
    }

    "maximum cap" should {
      "not blacklist further than cap" in {
        val limit = 10L
        val failedAttempts = 100L
        Table(
          "policy",
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist.Linear(Some(limit)),
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist
            .LinearWithParameters(Some(limit), 10, 10),
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist
            .Exponential(Some(limit), 10),
        )
          .forEvery { policy =>
            val topology = makeOrderingTopology(makeConfig(howLongToBlacklist = policy))
            BlacklistLeaderSelectionPolicyStateWithTopology(
              initState(n1 -> BlacklistStatus.OnTrial(failedAttempts)),
              topology,
              testedProtocolVersion,
            ).update(
              topology,
              blockToLeaderAllWithoutN0,
              Set(n1),
            ).state shouldBe stateNextEpoch(
              n1 -> BlacklistStatus.Blacklisted(failedAttempts + 1, limit)
            )
          }
      }

      "don't apply limit if you are below" in {
        val limit = 100L
        val failedAttempts = 5L
        Table(
          ("policy", "next value"),
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist.Linear(
            Some(limit)
          ) -> (failedAttempts + 1),
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist.LinearWithParameters(
            Some(limit),
            5,
            10,
          ) -> (5 * (failedAttempts + 1) + 10),
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist.Exponential(
            Some(limit),
            5,
          ) -> 69L, // 2 ^{5+1} + 5
        ).forEvery { case (policy, nextHowManyLeft) =>
          val topology = makeOrderingTopology(makeConfig(howLongToBlacklist = policy))
          BlacklistLeaderSelectionPolicyStateWithTopology(
            initState(n1 -> BlacklistStatus.OnTrial(failedAttempts)),
            topology,
            testedProtocolVersion,
          ).update(
            topology,
            blockToLeaderAllWithoutN0,
            Set(n1),
          ).state shouldBe stateNextEpoch(
            n1 -> BlacklistStatus.Blacklisted(failedAttempts + 1, nextHowManyLeft)
          )
        }
      }

      "update if config change" in {
        val oldValue = 100L
        val newLimit = 10L
        Table(
          "policy",
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist
            .Linear(Some(newLimit)),
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist
            .LinearWithParameters(Some(newLimit), 0L, 0L),
          BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist
            .Exponential(Some(newLimit), 0L),
        ).forEvery { policy =>
          BlacklistLeaderSelectionPolicyStateWithTopology(
            initState(n0 -> BlacklistStatus.Blacklisted(oldValue, oldValue)),
            orderingTopology,
            testedProtocolVersion,
          ).update(
            makeOrderingTopology(makeConfig(howLongToBlacklist = policy)),
            blockToLeaderAllWithoutN0,
            Set.empty,
          ).state shouldBe stateNextEpoch(
            n0 -> BlacklistStatus.Blacklisted(oldValue, newLimit - 1)
          )
        }
      }
    }
  }
}
