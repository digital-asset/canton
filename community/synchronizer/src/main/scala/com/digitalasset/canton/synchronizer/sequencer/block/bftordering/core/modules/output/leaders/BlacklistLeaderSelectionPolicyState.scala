// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencer.admin.v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.Status
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.BlacklistLeaderSelectionPolicy.Blacklist
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.LeaderSelectionPolicy.rotateLeaders
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

import scala.collection.immutable.SortedSet

final case class BlacklistLeaderSelectionPolicyStateWithTopology(
    state: BlacklistLeaderSelectionPolicyState,
    topology: OrderingTopology,
) {
  def epochNumber: EpochNumber = state.epochNumber
  def startBlock: BlockNumber = state.startBlock

  def computeBlockToLeader(
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig
  ): Map[BlockNumber, BftNodeId] =
    Epoch.blockToLeadersFromEpochInfo(
      computeLeaders(config),
      state.startBlock,
      topology.epochLength,
    )

  def update(
      newTopology: OrderingTopology,
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig,
      blockToLeader: Map[BlockNumber, BftNodeId],
      nodesToPunish: Set[BftNodeId],
  ): BlacklistLeaderSelectionPolicyStateWithTopology = {
    val newBlacklist = updateBlacklist(newTopology, config, blockToLeader, nodesToPunish)
    BlacklistLeaderSelectionPolicyStateWithTopology(
      BlacklistLeaderSelectionPolicyState.create(
        EpochNumber(epochNumber + 1),
        BlockNumber(startBlock + topology.epochLength),
        newBlacklist,
      )(state.representativeProtocolVersion.representative),
      newTopology,
    )
  }
  private def updateBlacklist(
      newTopology: OrderingTopology,
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig,
      blockToLeader: Map[BlockNumber, BftNodeId],
      nodesToPunish: Set[BftNodeId],
  ): Blacklist = {
    val nodesThatParticipated = blockToLeader.values.toSet
    val currentBlacklistWithoutRemovedNodes = state.blacklist.filter { case (node, _) =>
      newTopology.contains(node)
    }

    newTopology.nodes.foldLeft(currentBlacklistWithoutRemovedNodes) { case (blacklist, node) =>
      blacklist.updatedWith(node)(
        BlacklistStatus.transformToWorkOnBlacklistStatus { status =>
          val howEpochWent: BlacklistStatus.HowEpochWent = if (nodesToPunish.contains(node)) {
            BlacklistStatus.HowEpochWent.ShouldBePunished
          } else {
            if (nodesThatParticipated.contains(node)) {
              BlacklistStatus.HowEpochWent.Succeeded
            } else {
              BlacklistStatus.HowEpochWent.DidNotParticipate
            }
          }
          status.update(howEpochWent, config.howLongToBlackList)
        }
      )
    }
  }

  private def selectLeaders(
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig
  ): SortedSet[BftNodeId] = {
    val allBlacklistedNodes: Seq[BftNodeId] = state.blacklist
      .collect { case (node, BlacklistStatus.Blacklisted(_, epochLeftUntilNewTrial)) =>
        node -> epochLeftUntilNewTrial
      }
      .toSeq
      .sortBy(x => x._2 -> x._1)
      .reverse
      .map(_._1)

    val blacklistedNodes: Set[BftNodeId] = allBlacklistedNodes
      .take(config.howManyCanWeBlacklist.howManyCanWeBlacklist(topology))
      .toSet

    SortedSet.from(topology.nodes.removedAll(blacklistedNodes))
  }

  def computeLeaders(
      config: BftBlockOrdererConfig.BlacklistLeaderSelectionPolicyConfig
  ): Seq[BftNodeId] =
    rotateLeaders(selectLeaders(config), epochNumber)
}

final case class BlacklistLeaderSelectionPolicyState(
    epochNumber: EpochNumber,
    startBlock: BlockNumber,
    blacklist: Blacklist,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      BlacklistLeaderSelectionPolicyState.type
    ]
) extends HasProtocolVersionedWrapper[BlacklistLeaderSelectionPolicyState] {

  def toProto30: v30.BlacklistLeaderSelectionPolicyState =
    v30.BlacklistLeaderSelectionPolicyState.of(
      epochNumber,
      startBlock,
      blacklist.view.mapValues(_.toProto30).toMap,
    )

  override protected val companionObj: BlacklistLeaderSelectionPolicyState.type =
    BlacklistLeaderSelectionPolicyState
}

object BlacklistLeaderSelectionPolicyState
    extends VersioningCompanion[BlacklistLeaderSelectionPolicyState] {
  def create(
      epochNumber: EpochNumber,
      startBlockNumber: BlockNumber,
      blacklist: Blacklist,
  )(protocolVersion: ProtocolVersion): BlacklistLeaderSelectionPolicyState =
    BlacklistLeaderSelectionPolicyState(epochNumber, startBlockNumber, blacklist)(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  override def name: String = "BlacklistLeaderSelectionPolicyState"

  private def fromProto30(
      proto: v30.BlacklistLeaderSelectionPolicyState
  ): ParsingResult[BlacklistLeaderSelectionPolicyState] = for {
    rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    epochNumber = EpochNumber(proto.epochNumber)
    startBlockNumber = BlockNumber(proto.startBlockNumber)
  } yield BlacklistLeaderSelectionPolicyState(
    epochNumber,
    startBlockNumber,
    proto.blacklist.flatMap { case (node, protoStatus) =>
      val status: Option[BlacklistStatus.BlacklistStatusMark] = protoStatus.status match {
        case Status.Empty => None
        case v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.Status.OnTrial(value) =>
          Some(BlacklistStatus.OnTrial(value.failedAttemptsBeforeTrial))
        case v30.BlacklistLeaderSelectionPolicyState.BlacklistStatus.Status.Blacklisted(value) =>
          Some(
            BlacklistStatus.Blacklisted(
              failedAttemptsBefore = value.failedAttemptsBefore,
              epochsLeftUntilNewTrial = value.epochsLeftUntilNewTrial,
            )
          )
      }
      status.map(BftNodeId(node) -> _)
    },
  )(rpv)

  override val versioningTable: VersioningTable =
    VersioningTable(
      ProtoVersion(30) ->
        VersionedProtoCodec(
          SupportedVersions.CantonProtocol
        )(v30.BlacklistLeaderSelectionPolicyState)(
          supportedProtoVersion(_)(fromProto30),
          _.toProto30,
        )
    )

  def FirstBlacklistLeaderSelectionPolicyState(
      protocolVersion: ProtocolVersion
  ): BlacklistLeaderSelectionPolicyState =
    create(
      EpochNumber(0L),
      BlockNumber.First,
      Map.empty,
    )(
      protocolVersion
    )
}
