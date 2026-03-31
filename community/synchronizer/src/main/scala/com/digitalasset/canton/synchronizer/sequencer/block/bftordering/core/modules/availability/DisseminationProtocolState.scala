// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  DisseminationStatus,
}

import java.time.Instant
import scala.collection.{View, mutable}

final case class InitialSaveInProgress(
    // The following fields are only used for metrics
    availabilityEnterInstant: Option[Instant] = None
)

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class DisseminationProtocolState(
    val beingFirstSaved: mutable.Map[BatchId, InitialSaveInProgress] = mutable.Map.empty,
    var disseminationProgress: mutable.Map[BatchId, DisseminationStatus] =
      mutable.LinkedHashMap.empty,
    var nextToBeProvidedToConsensus: NextToBeProvidedToConsensus =
      NextToBeProvidedToConsensus.First,
    var lastProposalTime: Option[CantonTimestamp] = None,
    var lastProposalRequestTime: Option[CantonTimestamp] = None,
    val disseminationQuotas: BatchDisseminationNodeQuotaTracker =
      new BatchDisseminationNodeQuotaTracker,
) {

  def disseminationCompleteView: View[(BatchId, DisseminationStatus.Complete)] =
    disseminationStatusView(_.asComplete)

  def disseminationInProgressView: View[(BatchId, DisseminationStatus.InProgress)] =
    disseminationStatusView(_.asInProgress)

  private[availability] def disseminationStatusView[X](
      f: DisseminationStatus => Option[X]
  ): View[(BatchId, X)] =
    disseminationProgress.view.flatMap { case (batchId, s: DisseminationStatus) =>
      f(s).map(batchId -> _)
    }
}

final case class NextToBeProvidedToConsensus(
    forBlock: BlockNumber,
    maxBatchesPerProposal: Option[
      Short
    ], // `None` means no actual proposal request from consensus yet
)
object NextToBeProvidedToConsensus {
  val First: NextToBeProvidedToConsensus = NextToBeProvidedToConsensus(BlockNumber.First, None)
}
