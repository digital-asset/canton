// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  ViewNumber,
}

/** The class allows preserving some contextual information during the roundtrip from output to
  * local availability to retrieve the batches.
  *
  * @param isLastInEpoch
  *   From consensus: whether a block is the last in an epoch. Since the output module processes
  *   blocks in order, this boolean information is enough to determine when an epoch ends and the
  *   ordering topology for the next epoch may thus need to be sent to the consensus module.
  */
final case class OrderedBlockForOutput(
    orderedBlock: OrderedBlock,
    viewNumber: ViewNumber,
    originalLeader: BftNodeId,
    isLastInEpoch: Boolean,
    orderingMode: OrderingMode,
)

object OrderedBlockForOutput {}
