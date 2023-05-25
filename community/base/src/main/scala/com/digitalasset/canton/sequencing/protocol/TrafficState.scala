// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

/** The traffic state communicated to members on the sequencer API */
final case class TrafficState(
    extraTrafficRemainder: Long,
    timestamp: CantonTimestamp,
) {
  def toProtoV0: v0.TrafficState = v0.TrafficState(
    extraTrafficRemainder = extraTrafficRemainder,
    timestamp = Some(timestamp.toProtoPrimitive),
  )
}

object TrafficState {

  def fromProtoV0(
      trafficStateP: v0.TrafficState
  ): ParsingResult[TrafficState] = {
    for {
      timestamp <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "timestamp",
        trafficStateP.timestamp,
      )
    } yield TrafficState(
      extraTrafficRemainder = trafficStateP.extraTrafficRemainder,
      timestamp = timestamp,
    )
  }

}
