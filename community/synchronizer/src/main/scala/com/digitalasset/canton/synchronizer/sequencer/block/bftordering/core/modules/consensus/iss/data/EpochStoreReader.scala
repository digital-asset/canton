// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.tracing.TraceContext

trait EpochStoreReader[E <: Env[E]] {

  def loadEpochInfo(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[EpochInfo]]

  protected def loadEpochInfoActionName(epochNumber: EpochNumber): String =
    s"Load epoch $epochNumber info"

  def loadOrderedBlocks(
      initialEpochNumber: EpochNumber,
      limit: Int,
  )(implicit traceContext: TraceContext): E#FutureUnlessShutdownT[Seq[OrderedBlockForOutput]]
  protected def loadOrderedBlocksActionName(initialEpochNumber: EpochNumber, limit: Int): String =
    s"Load ordered blocks starting from epoch $initialEpochNumber (limit $limit)"

  def lastEpochWithCompletedBlock(lowerBound: EpochNumber)(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[EpochNumber]]
  protected def lastEpochWithCompletedBlockActionName: String =
    "Load epoch which has a completed block"
}
