// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo

import EpochStore.Epoch

object Bootstrap {

  private val BootstrapStartBlockNumber = BlockNumber.First
  private val BootstrapEpochLength = EpochLength(0)

  val BootstrapEpochNumber: EpochNumber = EpochNumber(-1L)

  def bootstrapEpochInfo(activationTime: TopologyActivationTime): EpochInfo =
    EpochInfo(
      BootstrapEpochNumber,
      BootstrapStartBlockNumber,
      BootstrapEpochLength,
      activationTime,
    )

  // Note that a bootstrap epoch does not contain commits, which results in using empty canonical commit sets for
  //  the first blocks proposed by each leader. These blocks are required to be empty so that actual transactions
  //  are not assigned inaccurate timestamps.
  def bootstrapEpoch(
      activationTime: TopologyActivationTime
  ): Epoch =
    Epoch(bootstrapEpochInfo(activationTime), lastBlockCommits = Seq.empty)
}
