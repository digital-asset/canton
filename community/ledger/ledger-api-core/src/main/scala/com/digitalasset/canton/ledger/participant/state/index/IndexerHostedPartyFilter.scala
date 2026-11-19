// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.nonempty.NonEmpty

/** Selects which hosting relationships are included in a snapshot.
  *
  * @param parties
  *   Only these parties are considered, all parties if None.
  * @param allParticipants
  *   If false, only the hosting on the local participant is considered. If true, the hosting on all
  *   participants is considered.
  * @param synchronizerIds
  *   Only the hosting on these synchronizers is considered, all synchronizers if None.
  */
final case class IndexerHostedPartyFilter(
    parties: Option[NonEmpty[Set[Ref.Party]]] = None,
    allParticipants: Boolean = false,
    synchronizerIds: Option[NonEmpty[Set[SynchronizerId]]] = None,
)
