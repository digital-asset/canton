// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
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

final case class SequencingParameters private (
    pbftViewChangeTimeout: PositiveFiniteDuration,
    segmentLength: SegmentLength,
    blacklistLeaderSelectionPolicyConfig: BlacklistLeaderSelectionPolicyConfig,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      topology.SequencingParameters.type
    ]
) extends PrettyPrinting
    with HasProtocolVersionedWrapper[SequencingParameters] {
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
  ): SequencingParameters =
    SequencingParameters(
      pbftViewChangeTimeout = pbftViewChangeTimeout,
      segmentLength = segmentLength,
      blacklistLeaderSelectionPolicyConfig = blacklistLeaderSelectionPolicyConfig,
    )(representativeProtocolVersion)

  def toProto30: v30.DynamicSequencingParametersPayload = v30.DynamicSequencingParametersPayload(
    Option(pbftViewChangeTimeout.toProtoPrimitive)
  )

  def toProto31: v31.DynamicSequencingParametersPayload = v31.DynamicSequencingParametersPayload(
    Option(pbftViewChangeTimeout.toProtoPrimitive),
    segmentLength.length.value,
    Option(blacklistLeaderSelectionPolicyConfig.toProto),
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
  def Default(implicit synchronizerProtocolVersion: ProtocolVersion): SequencingParameters =
    SequencingParameters(
      DefaultPbftViewChangeTimeout,
      DefaultSegmentLength,
      DefaultLeaderSelectionPolicyConfig,
    )(
      protocolVersionRepresentativeFor(synchronizerProtocolVersion)
    )
  def NoBlacklisting(implicit synchronizerProtocolVersion: ProtocolVersion): SequencingParameters =
    SequencingParameters(
      DefaultPbftViewChangeTimeout,
      DefaultSegmentLength,
      NoBlacklistingLeaderSelectionPolicyConfig,
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
    } yield SequencingParameters(
      pbftViewChangeTimeout,
      segmentLength,
      blacklistLeaderSelectionPolicyConfig,
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
  )(implicit synchronizerProtocolVersion: ProtocolVersion): SequencingParameters =
    SequencingParameters(
      pbftViewChangeTimeout,
      segmentLength,
      blacklistLeaderSelectionPolicyConfig,
    )(
      protocolVersionRepresentativeFor(synchronizerProtocolVersion)
    )

}
