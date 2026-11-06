// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.metrics

/** Bundle of the Speedy interpreter metric plugins that are enabled when metrics are on.
  *
  * @param iterationsBetweenInterruptions
  *   batch size used by [[stepCount]] to rebuild the total step count from completed batches plus
  *   the steps in the current batch
  */
final class MetricPlugins(iterationsBetweenInterruptions: Long) {
  val fetchNodeCount = new FetchNodeCount
  val stepCount = new StepCount(iterationsBetweenInterruptions)
  val txNodeCount = new TxNodeCount

  def reset(): Unit = {
    txNodeCount.reset()
    fetchNodeCount.reset()
    stepCount.reset()
  }

  def asSeq(): Seq[MetricPlugin] =
    Seq(fetchNodeCount, stepCount, txNodeCount)
}
