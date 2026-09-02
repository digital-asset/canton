// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlockForOutput,
  OrderingMode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.JitterGenerator
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.Jitter

import java.time.Instant
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.Random

object OutputFetchProtocolState {

  /** Creates a jitter generator from the given configuration and random source. It uses the
    * `Jitter.full` implementation to calculate the delays, with the provided
    * `outputFetchTimeoutCap`, `outputFetchTimeout`, and `outputFetchMinimumDelay` values.
    *
    * Note that `Jitter.full.apply` produces a timeout value between 0 and the exponential (we use
    * base 2) as `initialValue*math.pow(base.toDouble, attempt.toDouble)`, the unit of the initial
    * delay is important because the exp is on the non-converted value, the cap is converted to the
    * same unit of the initial delay with ceiling, and what guarantees that the jitter does not
    * yield 0 is the minimum delay.
    */
  def createJitterGenerator(config: BftBlockOrdererConfig, random: Random): JitterGenerator =
    JitterGenerator(
      Jitter.full(cap = config.outputFetchTimeoutCap, Jitter.randomSource(random.self)),
      initialDelay = config.outputFetchTimeout,
      minimumDelay = config.outputFetchMinimumDelay,
    )
}

final case class MissingBatchStatus(
    batchId: BatchId,
    originalProof: ProofOfAvailability,
    numberOfAttempts: Int,
    jitterStream: JitterGenerator,
    orderingMode: OrderingMode,
    firstTimeWeMadeRequest: Map[BftNodeId, Instant],
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
    proof <- batchesRequest.proofs.find(_.batchId == missingBatchId)
  } yield proof

  def removeRequestsWithNoMissingBatches(): Unit = {
    val _ = pendingBatchesRequests.removeAll(_.missingBatches.isEmpty)
  }
}

sealed trait BatchesRequest {
  def traceContext: TraceContext
  def proofs: Seq[ProofOfAvailability]
  def orderingMode: OrderingMode
  def originalLeader: BftNodeId

  lazy val missingBatches: mutable.SortedSet[BatchId] =
    mutable.SortedSet.from(proofs.map(_.batchId))
}

final class OrderedBlockBatchesRequest(
    val blockForOutput: OrderedBlockForOutput,
    val traceContext: TraceContext,
) extends BatchesRequest {
  override val proofs: Seq[ProofOfAvailability] = blockForOutput.orderedBlock.batchRefs
  override val orderingMode: OrderingMode = blockForOutput.orderingMode
  override val originalLeader: BftNodeId = blockForOutput.originalLeader
}

final class UnorderedBlockBatchesRequest(
    val blockNumber: BlockNumber,
    val originalLeader: BftNodeId,
    val proofs: Seq[ProofOfAvailability],
    val traceContext: TraceContext,
) extends BatchesRequest {
  override val orderingMode: OrderingMode = OrderingMode.Consensus
}
