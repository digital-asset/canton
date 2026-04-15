// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology

import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.DynamicSequencingParametersPayload
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.digitalasset.canton.version.{
  HasRepresentativeProtocolVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}
import com.google.protobuf.ByteString

import java.time.Duration

final case class SequencingParameters private (pbftViewChangeTimeout: PositiveFiniteDuration)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      topology.SequencingParameters.type
    ]
) extends PrettyPrinting
    with HasRepresentativeProtocolVersion {
  override protected val companionObj: SequencingParameters.type = SequencingParameters

  override protected def pretty: Pretty[SequencingParameters.this.type] =
    prettyOfClass(
      param("pbftViewChangeTimeout", _.pbftViewChangeTimeout)
    )

  def toProto: DynamicSequencingParametersPayload = DynamicSequencingParametersPayload(
    Option(pbftViewChangeTimeout.toProtoPrimitive)
  )
}

object SequencingParameters extends VersioningCompanion[SequencingParameters] {

  private val DefaultPbftViewChangeTimeout =
    PositiveFiniteDuration.tryCreate(Duration.ofSeconds(1))

  def Default(implicit synchronizerProtocolVersion: ProtocolVersion): SequencingParameters =
    SequencingParameters(DefaultPbftViewChangeTimeout)(
      protocolVersionRepresentativeFor(synchronizerProtocolVersion)
    )

  def fromProto(proto: DynamicSequencingParametersPayload): ParsingResult[SequencingParameters] =
    for {
      rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData)
      pbftViewChangeTimeout <- PositiveFiniteDuration.fromProtoPrimitiveO("pbftViewChangeTimeout")(
        proto.pbftViewChangeTimeout
      )
    } yield SequencingParameters(pbftViewChangeTimeout)(rpv)

  def fromPayload(payload: ByteString): ParsingResult[SequencingParameters] =
    for {
      payload <- ProtoConverter.protoParser(DynamicSequencingParametersPayload.parseFrom)(payload)
      sequencingParameters <- fromProto(payload)
    } yield sequencingParameters

  override def name: String = "SequencingParameters"

  override def versioningTable: data.topology.SequencingParameters.VersioningTable =
    VersioningTable(
      SupportedVersions.ProtoData ->
        VersionedProtoCodec(SupportedVersions.CantonProtocol)(
          v30.DynamicSequencingParametersPayload
        )(
          supportedProtoVersion(_)(SequencingParameters.fromProto),
          _.toProto,
        )
    )

  def create(
      pbftViewChangeTimeout: PositiveFiniteDuration
  )(implicit synchronizerProtocolVersion: ProtocolVersion): SequencingParameters =
    SequencingParameters(pbftViewChangeTimeout)(
      protocolVersionRepresentativeFor(synchronizerProtocolVersion)
    )
}
