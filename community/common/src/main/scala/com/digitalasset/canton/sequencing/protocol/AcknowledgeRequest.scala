// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.option.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasMemoizedProtocolVersionedWrapperCompanion,
  HasProtocolVersionedWrapper,
  HasProtocolVersionedWrapperCompanion,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
}
import com.google.protobuf.ByteString

case class AcknowledgeRequest private (member: Member, timestamp: CantonTimestamp)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[AcknowledgeRequest],
    override val deserializedFrom: Option[ByteString] = None,
) extends HasProtocolVersionedWrapper[AcknowledgeRequest]
    with ProtocolVersionedMemoizedEvidence {
  def toProtoV0: v0.AcknowledgeRequest =
    v0.AcknowledgeRequest(member.toProtoPrimitive, timestamp.toProtoPrimitive.some)

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override protected def companionObj: HasProtocolVersionedWrapperCompanion[AcknowledgeRequest] =
    AcknowledgeRequest
}

object AcknowledgeRequest extends HasMemoizedProtocolVersionedWrapperCompanion[AcknowledgeRequest] {
  def apply(
      member: Member,
      timestamp: CantonTimestamp,
      protocolVersion: ProtocolVersion,
  ): AcknowledgeRequest =
    AcknowledgeRequest(member, timestamp)(protocolVersionRepresentativeFor(protocolVersion))

  override protected def name: String = "AcknowledgeRequest"

  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(0) -> VersionedProtoConverter(
        ProtocolVersion.v2,
        supportedProtoVersionMemoized(v0.AcknowledgeRequest) { req => bytes =>
          fromProtoV0(req)(Some(bytes))
        },
        _.toProtoV0.toByteString,
      )
    )

  def fromProtoV0Unmemoized(
      reqP: v0.AcknowledgeRequest
  ): ParsingResult[AcknowledgeRequest] = fromProtoV0(reqP)(None)

  private def fromProtoV0(
      reqP: v0.AcknowledgeRequest
  )(deserializedFrom: Option[ByteString]): ParsingResult[AcknowledgeRequest] =
    for {
      member <- Member.fromProtoPrimitive(reqP.member, "member")
      timestamp <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "timestamp",
        reqP.timestamp,
      )
    } yield {
      AcknowledgeRequest(member, timestamp)(
        protocolVersionRepresentativeFor(ProtoVersion(0)),
        deserializedFrom,
      )
    }
}
