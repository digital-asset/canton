// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.traverse._
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasProtoV0,
  HasVersionedMessageCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.google.protobuf.ByteString

case class SequencerSnapshot(
    lastTs: CantonTimestamp,
    heads: Map[Member, SequencerCounter],
    status: SequencerPruningStatus,
    additional: Option[SequencerSnapshot.ImplementationSpecificInfo],
) extends HasProtoV0[v0.SequencerSnapshot]
    with HasVersionedWrapper[VersionedMessage[SequencerSnapshot]] {

  override def toProtoV0: v0.SequencerSnapshot = v0.SequencerSnapshot(
    Some(lastTs.toProtoPrimitive),
    heads.map { case (member, counter) =>
      member.toProtoPrimitive -> counter
    },
    Some(status.toProtoV0),
    additional.map(a => v0.ImplementationSpecificInfo(a.implementationName, a.info)),
  )

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[SequencerSnapshot] =
    VersionedMessage(toProtoV0.toByteString, 0)

}
object SequencerSnapshot extends HasVersionedMessageCompanion[SequencerSnapshot] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.SequencerSnapshot)(fromProtoV0)
  )

  override protected def name: String = "sequencer snapshot"

  lazy val Unimplemented = SequencerSnapshot(
    CantonTimestamp.MinValue,
    Map.empty,
    SequencerPruningStatus.Unimplemented,
    None,
  )

  case class ImplementationSpecificInfo(implementationName: String, info: ByteString)

  def fromProtoV0(
      request: v0.SequencerSnapshot
  ): ParsingResult[SequencerSnapshot] =
    for {
      lastTs <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "latestTimestamp",
        request.latestTimestamp,
      )
      heads <- request.headMemberCounters.toList
        .traverse { case (member, counter) =>
          Member.fromProtoPrimitive(member, "registeredMembers").map(m => m -> counter)
        }
        .map(_.toMap)
      status <- ProtoConverter.parseRequired(
        SequencerPruningStatus.fromProtoV0,
        "status",
        request.status,
      )
    } yield SequencerSnapshot(
      lastTs,
      heads,
      status,
      request.additional.map(a => ImplementationSpecificInfo(a.implementationName, a.info)),
    )
}
