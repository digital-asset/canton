// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlockForOutput,
  OrderingMode,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.Jitter

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

/** A stream of jittered delays for retrying a request, based on the number of attempts. The delay
  * is calculated using the provided `Jitter` implementation, and is at least `minimumDelay`. The
  * `initialDelay` is used as the starting point for the first attempt.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
final case class JitterStream(
    jitter: Jitter,
    initialDelay: FiniteDuration,
    minimumDelay: FiniteDuration,
) {
  private var lastDelay: FiniteDuration = initialDelay
  private var lastAttempt: Int = 1

  def next(attempt: Int): FiniteDuration = {
    require(attempt >= lastAttempt)
    if (attempt >= lastAttempt) {
      lastDelay = jitter(initialDelay, lastDelay, attempt)
      lastAttempt = attempt
    }
    lastDelay.plus(minimumDelay)
  }
}

object JitterStream {

  /** Create a JitterStream from the given configuration and random source. It uses the
    * `Jitter.full` implementation to calculate the delays, with the provided
    * `outputFetchTimeoutCap`, `outputFetchTimeout`, and `outputFetchMinimumDelay` values.
    * `Jitter.full.apply` produces a timeout value between 0 and the exponential (we use base 2) as
    * `initialValue*math.pow(base.toDouble, attempt.toDouble)`, the unit of the initial delay is
    * important because the exp is on the non-converted value, the cap is converted to the same unit
    * of the initial delay with ceiling, and what guarantees that the jitter does not yield 0 is the
    * minimum delay.
    */
  def create(config: BftBlockOrdererConfig, random: Random): JitterStream =
    JitterStream(
      Jitter.full(config.outputFetchTimeoutCap, Jitter.randomSource(random.self)),
      config.outputFetchTimeout,
      config.outputFetchMinimumDelay,
    )
}

final case class MissingBatchStatus(
    batchId: BatchId,
    originalProof: ProofOfAvailability,
    numberOfAttempts: Int,
    jitterStream: JitterStream,
    orderingMode: OrderingMode,
) {
  def calculateTimeout(): FiniteDuration = jitterStream.next(numberOfAttempts)
}

final class MainOutputFetchProtocolState {
  // tracks retrieval of a single batch, including across retry attempts
  val localOutputMissingBatches: mutable.SortedMap[BatchId, MissingBatchStatus] =
    mutable.SortedMap.empty
  val incomingBatchRequests: mutable.Map[BatchId, Set[BftNodeId]] = mutable.SortedMap.empty
  // tracks all batches from one specific block that the output module has requested
  val pendingBatchesRequests: mutable.ArrayDeque[BatchesRequest] = mutable.ArrayDeque.empty
  // tracks remote batches that have been received but not completed database storage,
  // in order to avoid re-requesting it when batch is needed
  val pendingRemoteBatchIdsToStore: mutable.SortedSet[BatchId] =
    mutable.SortedSet[BatchId]()

  def findProofOfAvailabilityForMissingBatchId(
      missingBatchId: BatchId
  ): Option[ProofOfAvailability] = for {
    batchesRequest <- pendingBatchesRequests.find(_.missingBatches.contains(missingBatchId))
    proof <- batchesRequest.blockForOutput.orderedBlock.batchRefs.find(_.batchId == missingBatchId)
  } yield proof

  def removeRequestsWithNoMissingBatches(): Unit = {
    val _ = pendingBatchesRequests.removeAll(_.missingBatches.isEmpty)
  }
}

final class BatchesRequest(
    val blockForOutput: OrderedBlockForOutput,
    val missingBatches: mutable.SortedSet[BatchId],
    val traceContext: TraceContext,
)
