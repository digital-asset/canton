// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.SequencedEventTrafficState
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.traffic.v0.MemberTrafficStatus as MemberTrafficStatusP

final case class MemberTrafficStatus(
    member: Member,
    timestamp: CantonTimestamp,
    trafficState: SequencedEventTrafficState,
    currentAndFutureTopUps: List[TopUpEvent],
) {
  def toProtoV0: MemberTrafficStatusP = {
    MemberTrafficStatusP(
      member.toProtoPrimitive,
      Some(trafficState.toProtoV0),
      currentAndFutureTopUps.map(_.toProtoV0),
      Some(timestamp.toProtoPrimitive),
    )
  }
}

object MemberTrafficStatus {
  def fromProtoV0(
      trafficStatusP: MemberTrafficStatusP
  ): Either[ProtoDeserializationError, MemberTrafficStatus] = {
    for {
      member <- Member.fromProtoPrimitive(
        trafficStatusP.member,
        "member",
      )
      trafficState <- ProtoConverter.parseRequired(
        SequencedEventTrafficState.fromProtoV0,
        "traffic_state",
        trafficStatusP.trafficState,
      )
      topUps <- trafficStatusP.topUpEvents.toList.traverse(TopUpEvent.fromProtoV0)
      ts <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "ts",
        trafficStatusP.ts,
      )
    } yield MemberTrafficStatus(
      member,
      ts,
      trafficState,
      topUps,
    )
  }
}
