// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import cats.Order.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PhysicalSynchronizerId

final case class SynchronizerRank(
    reassignments: Map[
      LfContractId,
      (LfPartyId, PhysicalSynchronizerId),
    ], // (cid, (submitter, current synchronizer))
    priority: Int,
    synchronizerId: PhysicalSynchronizerId, // synchronizer for submission
)

object SynchronizerRank {
  // The highest priority synchronizer should be picked first, so negate the priority
  implicit val synchronizerRanking: Ordering[SynchronizerRank] =
    Ordering.by(x => (-x.priority, x.reassignments.size, x.synchronizerId))

  def single(synchronizerId: PhysicalSynchronizerId): SynchronizerRank =
    SynchronizerRank(Map.empty, 0, synchronizerId)
}
