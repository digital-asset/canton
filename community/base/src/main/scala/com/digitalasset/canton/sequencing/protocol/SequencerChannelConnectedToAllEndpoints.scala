// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*

final case class SequencerChannelConnectedToAllEndpoints()(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      SequencerChannelConnectedToAllEndpoints.type
    ]
) extends HasProtocolVersionedWrapper[SequencerChannelConnectedToAllEndpoints] {
  @transient override protected lazy val companionObj
      : SequencerChannelConnectedToAllEndpoints.type =
    SequencerChannelConnectedToAllEndpoints

  def toProtoV30: v30.SequencerChannelConnectedToAllEndpoints =
    v30.SequencerChannelConnectedToAllEndpoints()
}

object SequencerChannelConnectedToAllEndpoints
    extends HasProtocolVersionedCompanion[SequencerChannelConnectedToAllEndpoints] {
  override val name: String = "SequencerChannelConnectedToAllEndpoints"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(-1) -> UnsupportedProtoCodec(),
    ProtoVersion(30) -> VersionedProtoConverter(ProtocolVersion.dev)(
      v30.SequencerChannelConnectedToAllEndpoints
    )(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30.toByteString,
    ),
  )

  def apply(
      protocolVersion: ProtocolVersion
  ): SequencerChannelConnectedToAllEndpoints =
    SequencerChannelConnectedToAllEndpoints()(
      protocolVersionRepresentativeFor(protocolVersion)
    )

  def fromProtoV30(
      sequencerChannelConnectedToAllEndpointsP: v30.SequencerChannelConnectedToAllEndpoints
  ): ParsingResult[SequencerChannelConnectedToAllEndpoints] = {
    val v30.SequencerChannelConnectedToAllEndpoints() = sequencerChannelConnectedToAllEndpointsP
    for {
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield SequencerChannelConnectedToAllEndpoints()(rpv)
  }
}