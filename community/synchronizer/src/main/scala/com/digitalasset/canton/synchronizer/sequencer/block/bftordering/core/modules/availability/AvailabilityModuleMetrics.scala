// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

private[availability] object AvailabilityModuleMetrics {

  def emitInvalidMessage(metrics: BftOrderingMetrics, from: BftNodeId)(implicit
      mc: MetricsContext
  ): Unit = {
    import metrics.security.noncompliant.*
    behavior.mark()(
      mc.withExtraLabels(
        labels.Sequencer -> from,
        labels.violationType.Key ->
          labels.violationType.values.DisseminationInvalidMessage,
      )
    )
  }

  def emitDisseminationStateStats(
      metrics: BftOrderingMetrics,
      state: DisseminationProtocolState,
  )(implicit mc: MetricsContext): Unit = {
    import metrics.availability.*

    val (requestsCount, requestedBatches) =
      state.nextToBeProvidedToConsensus.maxBatchesPerProposal
        .map(maxBatchesPerProposal => (1, maxBatchesPerProposal.toInt))
        .getOrElse((0, 0))

    requested.proposals.updateValue(requestsCount)
    requested.batches.updateValue(requestedBatches)

    val readyForConsensusMc = mc.withExtraLabels(dissemination.labels.ReadyForConsensus -> "true")
    val disseminationComplete = state.disseminationCompleteView
    locally {
      implicit val mc: MetricsContext = readyForConsensusMc
      dissemination.bytes.updateValue(disseminationComplete.map(_._2.stats.bytes).sum)
      dissemination.requests.updateValue(disseminationComplete.map(_._2.stats.requests).sum)
      dissemination.batches.updateValue(disseminationComplete.size)
    }
    val inProgressMc = mc.withExtraLabels(dissemination.labels.ReadyForConsensus -> "false")
    locally {
      implicit val mc: MetricsContext = inProgressMc
      dissemination.bytes.updateValue(
        state.disseminationProgress.values.map(_.stats.bytes).sum
      )
      dissemination.requests.updateValue(
        state.disseminationProgress.values.map(_.stats.requests).sum
      )
      dissemination.batches.updateValue(state.disseminationProgress.size)
    }

    val regressionsToSigning =
      state.disseminationProgress.values.map(_.regressionsToSigning).sum +
        disseminationComplete.map(_._2.regressionsToSigning).sum
    val regressedDisseminations =
      state.disseminationProgress.values.map(_.disseminationRegressions).sum +
        disseminationComplete.map(_._2.disseminationRegressions).sum
    regression.batch.inc(regressionsToSigning.toLong)(
      mc.withExtraLabels(
        regression.labels.stage.Key -> regression.labels.stage.values.Signing.toString
      )
    )
    regression.batch.inc(regressedDisseminations.toLong)(
      mc.withExtraLabels(
        regression.labels.stage.Key -> regression.labels.stage.values.Dissemination.toString
      )
    )
    // Reset regressions counts to avoid double counting
    state.disseminationProgress.addAll(state.disseminationProgress.map { case (key, status) =>
      key -> status.resetRegressions()
    })
  }
}
