// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.FieldNotSet
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.{HasProtoV0, HasVersionedWrapper, HasVersionedWrapperCompanion}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.domain.admin.version.VersionedSequencerSnapshot
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import scalapb.GeneratedMessageCompanion

case class SequencerSnapshot(
    lastTs: CantonTimestamp,
    heads: Map[Member, SequencerCounter],
    status: SequencerPruningStatus,
    additional: Option[SequencerSnapshot.ImplementationSpecificInfo],
) extends HasProtoV0[v0.SequencerSnapshot]
    with HasVersionedWrapper[VersionedSequencerSnapshot] {

  override def toProtoV0: v0.SequencerSnapshot = v0.SequencerSnapshot(
    Some(lastTs.toProtoPrimitive),
    heads.map { case (member, counter) =>
      member.toProtoPrimitive -> counter
    },
    Some(status.toProtoV0),
    additional.map(a => v0.ImplementationSpecificInfo(a.implementationName, a.info)),
  )

  override protected def toProtoVersioned(version: ProtocolVersion): VersionedSequencerSnapshot =
    VersionedSequencerSnapshot(VersionedSequencerSnapshot.Version.V0(toProtoV0))

}
object SequencerSnapshot
    extends HasVersionedWrapperCompanion[VersionedSequencerSnapshot, SequencerSnapshot] {

  override protected def ProtoClassCompanion
      : GeneratedMessageCompanion[VersionedSequencerSnapshot] = VersionedSequencerSnapshot

  override protected def name: String = "sequencer snapshot"

  override protected def fromProtoVersioned(
      proto: VersionedSequencerSnapshot
  ): ParsingResult[SequencerSnapshot] = proto.version match {
    case VersionedSequencerSnapshot.Version.Empty =>
      Left(FieldNotSet("VersionedSequencerSnapshot.version"))
    case VersionedSequencerSnapshot.Version.V0(metadata) => fromProtoV0(metadata)
  }

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
