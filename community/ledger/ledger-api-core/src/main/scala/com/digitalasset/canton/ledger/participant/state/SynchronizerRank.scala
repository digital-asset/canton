// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import cats.Order.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.{LfContractId, Stakeholders}
import com.digitalasset.canton.topology.PhysicalSynchronizerId

/** @param reassignments
  *   contracts to reassign to `synchronizerId`, batched by (submitter, current synchronizer,
  *   stakeholders); the contracts of a batch are unassigned and assigned together
  */
final case class SynchronizerRank(
    reassignments: Map[
      (LfPartyId, PhysicalSynchronizerId, Stakeholders),
      Set[LfContractId],
    ],
    priority: Int,
    synchronizerId: PhysicalSynchronizerId, // synchronizer for submission
) {
  lazy val contractCount: Int = reassignments.values.map(_.size).sum
}

object SynchronizerRank {
  // The highest priority synchronizer should be picked first, so negate the priority
  implicit val synchronizerRanking: Ordering[SynchronizerRank] =
    Ordering.by(x => (-x.priority, x.reassignments.size, x.contractCount, x.synchronizerId))

  def single(synchronizerId: PhysicalSynchronizerId): SynchronizerRank =
    SynchronizerRank(Map.empty, 0, synchronizerId)
}
