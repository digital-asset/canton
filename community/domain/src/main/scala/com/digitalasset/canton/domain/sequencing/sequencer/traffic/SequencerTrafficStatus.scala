// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.Member

// TODO(i12857): Persist traffic consumption and remaining base rate
final case class MemberTrafficStatus private[traffic] (
    member: Member,
    lastAcceptedTraffic: CantonTimestamp,
    totalExtraTrafficLimit: Long = 0L,
    totalExtraTrafficConsumption: Long = 0L,
    remainingBaseTraffic: Long = 0L,
) {
  require(totalExtraTrafficLimit >= totalExtraTrafficConsumption)

  def extraTrafficRemainder: Long = totalExtraTrafficLimit - totalExtraTrafficConsumption
}

final case class SequencerTrafficStatus(members: Seq[MemberTrafficStatus])
