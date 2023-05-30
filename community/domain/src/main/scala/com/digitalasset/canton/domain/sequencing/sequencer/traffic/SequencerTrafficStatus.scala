// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.Member

// TODO(i12857): Persist traffic consumption and remaining base rate
final case class MemberTrafficStatus private[sequencing] (
    member: Member,
    lastAcceptedTraffic: CantonTimestamp,
    totalExtraTrafficLimit: NonNegativeLong = NonNegativeLong.tryCreate(0L),
    totalExtraTrafficConsumption: NonNegativeLong = NonNegativeLong.tryCreate(0L),
    remainingBaseTraffic: NonNegativeLong = NonNegativeLong.tryCreate(0L),
) {
  require(
    totalExtraTrafficLimit >= totalExtraTrafficConsumption,
    s"Consumed traffic ($totalExtraTrafficConsumption) can't be greater than topped-up traffic ($totalExtraTrafficLimit)",
  )

  def extraTrafficRemainder: NonNegativeLong = {
    // The require above guarantees that this will always work
    NonNegativeLong.tryCreate(totalExtraTrafficLimit.value - totalExtraTrafficConsumption.value)
  }

  def toTrafficState(timestamp: CantonTimestamp): TrafficState =
    TrafficState(extraTrafficRemainder, timestamp)
}

object MemberTrafficStatus {

  def empty(member: Member, lastAcceptedTraffic: CantonTimestamp): MemberTrafficStatus = {
    MemberTrafficStatus(member, lastAcceptedTraffic)
  }

  def create(
      member: Member,
      lastAcceptedTraffic: CantonTimestamp,
      totalExtraTrafficLimit: NonNegativeLong = NonNegativeLong.tryCreate(0L),
      totalExtraTrafficConsumption: NonNegativeLong = NonNegativeLong.tryCreate(0L),
      remainingBaseTraffic: NonNegativeLong = NonNegativeLong.tryCreate(0L),
  ): Either[String, MemberTrafficStatus] =
    Either.cond(
      totalExtraTrafficLimit >= totalExtraTrafficConsumption,
      MemberTrafficStatus(
        member = member,
        lastAcceptedTraffic = lastAcceptedTraffic,
        totalExtraTrafficLimit = totalExtraTrafficLimit,
        totalExtraTrafficConsumption = totalExtraTrafficConsumption,
        remainingBaseTraffic = remainingBaseTraffic,
      ),
      s"Total extra traffic consumption $totalExtraTrafficConsumption must not exceed total extra traffic limit $totalExtraTrafficLimit",
    )

}

final case class SequencerTrafficStatus(members: Seq[MemberTrafficStatus])
