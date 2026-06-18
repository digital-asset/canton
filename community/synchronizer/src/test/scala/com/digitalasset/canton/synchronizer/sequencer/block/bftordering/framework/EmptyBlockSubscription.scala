// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.BlockSubscription
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches}

class EmptyBlockSubscription extends BlockSubscription {

  @volatile private var _sequencerCoreIsSlow: Boolean = false
  @volatile private var _bufferSize: Int = 0

  override def subscription(): Source[Traced[BlockFormat.Block], KillSwitch] =
    Source.empty.viaMat(KillSwitches.single)(Keep.right)

  override def receiveBlock(
      block: BlockFormat.Block
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
    ()

  override def isSequencerCoreSlow: Boolean = _sequencerCoreIsSlow

  override def bufferSize: Int = _bufferSize

  def setSequencerCoreIsSlow(slow: Boolean, bufferSize: Int): Unit = {
    _sequencerCoreIsSlow = slow
    _bufferSize = bufferSize
  }
}
