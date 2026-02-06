// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.traffic

import cats.implicits.toTraverseOps
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  ProtoVersion,
  ProtocolVersion,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanion,
}

final case class LsuTrafficState(membersTraffic: Map[Member, TrafficState])(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[LsuTrafficState.type]
) extends HasProtocolVersionedWrapper[LsuTrafficState] {

  @transient override protected lazy val companionObj: LsuTrafficState.type = LsuTrafficState

  def toProtoV30: v30.LsuTrafficState = {
    val membersTrafficProto = membersTraffic.map { case (member, state) =>
      member.toProtoPrimitive -> state.toProtoV30
    }
    v30.LsuTrafficState(membersTrafficProto)
  }
}

object LsuTrafficState extends VersioningCompanion[LsuTrafficState] {
  override def name: String = "lsu traffic state"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.LsuTrafficState)(
      supportedProtoVersion(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  def fromProtoV30(
      request: v30.LsuTrafficState
  ): ParsingResult[LsuTrafficState] =
    request.lsuTrafficStates.toList
      .traverse { case (memberP, stateP) =>
        for {
          member <- Member.fromProtoPrimitive(memberP, "member")
          state <- TrafficState.fromProtoV30(stateP)
        } yield member -> state
      }
      .map(trafficState =>
        LsuTrafficState(trafficState.toMap)(versioningTable.table(ProtoVersion(30)))
      )
}
