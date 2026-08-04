// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.{
  HowLongToBlacklist,
  HowManyCanWeBlacklist,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters.SegmentLength
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.{v30, v31}
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

import java.time.Duration

/** @param pbftViewChangeTimeout
  * @param segmentLength
  * @param blacklistLeaderSelectionPolicyConfig
  * @param maxRequestsInBatch
  *   The maximum number of requests in a batch. Needs to be the same across the network for the BFT
  *   time assumptions to hold.
  * @param maxBatchesPerBlockProposal
  *   The maximum number of batches per block proposal (pre-prepare). Needs to be the same across
  *   the network for the BFT time assumptions to hold.
  */
final case class SequencingParameters private (
    pbftViewChangeTimeout: PositiveFiniteDuration,
    segmentLength: SegmentLength,
    blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig,
    maxRequestsInBatch: Short,
    maxBatchesPerBlockProposal: Short,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      topology.SequencingParameters.type
    ]
) extends PrettyPrinting
    with HasProtocolVersionedWrapper[SequencingParameters] {

  private val maxRequestsPerBlock = maxBatchesPerBlockProposal * maxRequestsInBatch
  require(
    maxRequestsPerBlock < BftTime.MaxRequestsPerBlock,
    s"Maximum block size too big: $maxRequestsInBatch maximum requests per batch and " +
      s"$maxBatchesPerBlockProposal maximum batches per block proposal means " +
      s"$maxRequestsPerBlock maximum requests per block, " +
      s"but the maximum number allowed of requests per block is ${BftTime.MaxRequestsPerBlock}",
  )
  require(
    maxRequestsInBatch <= 32,
    s"Max request in batch too big: $maxRequestsInBatch exceeds maximum allowed of 32",
  )
  require(
    maxBatchesPerBlockProposal <= 31,
    s"Max batches per block proposal too big: $maxBatchesPerBlockProposal exceeds maximum allowed of 31",
  )

  override protected val companionObj: SequencingParameters.type = SequencingParameters

  override protected def pretty: Pretty[SequencingParameters.this.type] =
    prettyOfClass(
      param("pbftViewChangeTimeout", _.pbftViewChangeTimeout),
      param("segmentLength", _.segmentLength.length.value),
      param("blacklistConfig", _.blacklistLeaderSelectionPolicyConfig),
    )

  def update(
      pbftViewChangeTimeout: PositiveFiniteDuration = this.pbftViewChangeTimeout,
      segmentLength: SegmentLength = this.segmentLength,
      blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig =
        this.blacklistLeaderSelectionPolicyConfig,
      maxRequestsInBatch: Short = this.maxRequestsInBatch,
      maxBatchesPerBlockProposal: Short = this.maxBatchesPerBlockProposal,
  ): SequencingParameters =
    SequencingParameters(
      pbftViewChangeTimeout = pbftViewChangeTimeout,
      segmentLength = segmentLength,
      blacklistLeaderSelectionPolicyConfig = blacklistLeaderSelectionPolicyConfig,
      maxRequestsInBatch = maxRequestsInBatch,
      maxBatchesPerBlockProposal = maxBatchesPerBlockProposal,
    )(representativeProtocolVersion)

  def toProto30: v30.DynamicSequencingParametersPayload = v30.DynamicSequencingParametersPayload(
    Option(pbftViewChangeTimeout.toProtoPrimitive)
  )

  def toProto31: v31.DynamicSequencingParametersPayload = v31.DynamicSequencingParametersPayload(
    Option(pbftViewChangeTimeout.toProtoPrimitive),
    segmentLength.length.value,
    Option(blacklistLeaderSelectionPolicyConfig.toProto),
    maxRequestsInBatch.toInt,
    maxBatchesPerBlockProposal.toInt,
  )
}

object SequencingParameters extends VersioningCompanion[SequencingParameters] {

  val DefaultPbftViewChangeTimeout: PositiveFiniteDuration =
    PositiveFiniteDuration.tryCreate(Duration.ofSeconds(10))

  final case class SegmentLength(length: PositiveLong) {
    def epochLength(numberOfSequencers: Long): EpochLength = EpochLength(
      length.value * numberOfSequencers
    )
  }

  val DefaultHowLongToBlackList: BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist =
    BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist.Linear(maximumEpochBlacklisted =
      Some(250L)
    )
  val DefaultHowManyCanWeBlacklist
      : topology.BlacklistLeaderSelectionPolicyConfig.HowManyCanWeBlacklist =
    BlacklistLeaderSelectionPolicyConfig.HowManyCanWeBlacklist.NumFaultsTolerated
  val DefaultLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig =
    BlacklistLeaderSelectionPolicyConfig(
      DefaultHowLongToBlackList,
      DefaultHowManyCanWeBlacklist,
    )
  val NoBlacklistingLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig =
    BlacklistLeaderSelectionPolicyConfig(
      HowLongToBlacklist.NoBlacklisting,
      HowManyCanWeBlacklist.NoBlacklisting,
    )

  val DefaultSegmentLength: SegmentLength = SegmentLength(PositiveLong.tryCreate(10L))
  val DefaultMaxRequestsInBatch: Short = 32
  val DefaultMaxBatchesPerProposal: Short = 16
  def Default(implicit synchronizerProtocolVersion: ProtocolVersion): SequencingParameters =
    SequencingParameters(
      DefaultPbftViewChangeTimeout,
      DefaultSegmentLength,
      DefaultLeaderSelectionPolicyConfig,
      DefaultMaxRequestsInBatch,
      DefaultMaxBatchesPerProposal,
    )(
      protocolVersionRepresentativeFor(synchronizerProtocolVersion)
    )
  def NoBlacklisting(implicit synchronizerProtocolVersion: ProtocolVersion): SequencingParameters =
    SequencingParameters(
      DefaultPbftViewChangeTimeout,
      DefaultSegmentLength,
      NoBlacklistingLeaderSelectionPolicyConfig,
      DefaultMaxRequestsInBatch,
      DefaultMaxBatchesPerProposal,
    )(
      protocolVersionRepresentativeFor(synchronizerProtocolVersion)
    )

  def fromProto30(
      proto: v30.DynamicSequencingParametersPayload
  ): ParsingResult[SequencingParameters] =
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      pbftViewChangeTimeout <- PositiveFiniteDuration.fromProtoPrimitiveO("pbftViewChangeTimeout")(
        proto.pbftViewChangeTimeout
      )
    } yield SequencingParameters(
      pbftViewChangeTimeout,
      DefaultSegmentLength,
      DefaultLeaderSelectionPolicyConfig,
      DefaultMaxRequestsInBatch,
      DefaultMaxBatchesPerProposal,
    )(rpv)

  def fromProto31(
      proto: v31.DynamicSequencingParametersPayload
  ): ParsingResult[SequencingParameters] =
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
      pbftViewChangeTimeout <- PositiveFiniteDuration.fromProtoPrimitiveO("pbftViewChangeTimeout")(
        proto.pbftViewChangeTimeout
      )
      segmentLength <- PositiveLong
        .create(proto.segmentLength)
        .left
        .map(err => ValueConversionError("segmentLength", err.message))
        .map(SegmentLength(_))
      blacklistLeaderSelectionPolicyConfig <-
        ProtoConverter
          .required("blacklistLeaderSelectionPolicy", proto.blacklistLeaderSelectionPolicy)
          .flatMap(
            BlacklistLeaderSelectionPolicyConfig.fromProto
          )
      maxRequestsInBatch = {
        if (proto.maxRequestsInBatch == 0L) DefaultMaxRequestsInBatch
        else proto.maxRequestsInBatch.toShort
      }
      maxBatchesPerProposal = {
        if (proto.maxBatchesPerProposal == 0L) DefaultMaxBatchesPerProposal
        else proto.maxBatchesPerProposal.toShort
      }
    } yield SequencingParameters(
      pbftViewChangeTimeout,
      segmentLength,
      blacklistLeaderSelectionPolicyConfig,
      maxRequestsInBatch,
      maxBatchesPerProposal,
    )(rpv)

  override def name: String = "SequencingParameters"

  override def versioningTable: data.topology.SequencingParameters.VersioningTable =
    VersioningTable(
      ProtoVersion(30) ->
        VersionedProtoCodec(ProtocolVersion.v34)(
          v30.DynamicSequencingParametersPayload
        )(
          supportedProtoVersion(_)(SequencingParameters.fromProto30),
          _.toProto30,
        ),
      ProtoVersion(31) ->
        VersionedProtoCodec(ProtocolVersion.v35)(
          v31.DynamicSequencingParametersPayload
        )(
          supportedProtoVersion(_)(SequencingParameters.fromProto31),
          _.toProto31,
        ),
    )

  def create(
      pbftViewChangeTimeout: PositiveFiniteDuration = DefaultPbftViewChangeTimeout,
      segmentLength: SegmentLength = DefaultSegmentLength,
      blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig =
        DefaultLeaderSelectionPolicyConfig,
      maxRequestsInBatch: Short = DefaultMaxRequestsInBatch,
      maxBatchesPerBlockProposal: Short = DefaultMaxBatchesPerProposal,
  )(implicit synchronizerProtocolVersion: ProtocolVersion): SequencingParameters =
    SequencingParameters(
      pbftViewChangeTimeout,
      segmentLength,
      blacklistLeaderSelectionPolicyConfig,
      maxRequestsInBatch,
      maxBatchesPerBlockProposal,
    )(
      protocolVersionRepresentativeFor(synchronizerProtocolVersion)
    )

}
