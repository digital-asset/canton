// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref

/** Selects which hosting relationships are included in a snapshot.
  *
  * @param parties
  *   Only these parties are considered, all parties if empty.
  * @param allParticipants
  *   If false, only the hosting on the local participant is considered. If true, the hosting on all
  *   participants is considered.
  * @param synchronizerIds
  *   Only the hosting on these synchronizers is considered, all synchronizers if empty.
  */
final case class IndexerHostedPartyFilter(
    parties: Set[Ref.Party] = Set.empty,
    allParticipants: Boolean = false,
    synchronizerIds: Set[SynchronizerId] = Set.empty,
)
