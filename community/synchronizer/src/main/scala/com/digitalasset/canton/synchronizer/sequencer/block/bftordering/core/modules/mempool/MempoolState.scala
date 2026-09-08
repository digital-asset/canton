// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.mempool.MempoolState.QueuedOrderRequest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Mempool
import io.opentelemetry.api.trace.Span

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class MempoolState(var weakQuorum: Int, var isBlacklisted: Boolean = false) {

  val receivedOrderRequests: mutable.Queue[QueuedOrderRequest] = mutable.Queue()
  var toBeProvidedToAvailability: Int = 0
  var authenticatedCount: Int = 1

  /** The latest known sequencing time, as reported by the local output module once a block is
    * assembled; used to lazily discard requests whose max sequencing time has passed as they are
    * dequeued during batch creation, without scanning [[receivedOrderRequests]]. Only ever moves
    * forward: block storage can complete out of order, so updates can arrive with a lower time than
    * one already seen, and those must not move the cutoff backward.
    */
  private var latestKnownSequencingTime: CantonTimestamp = CantonTimestamp.MinValue

  def canDisseminate: Boolean = authenticatedCount >= weakQuorum

  def enqueueRequest(orderRequest: Mempool.OrderRequest, span: Span): Unit =
    receivedOrderRequests.enqueue(new QueuedOrderRequest(orderRequest, span))

  def updateLatestKnownSequencingTime(latestKnownSequencingTime: CantonTimestamp): Unit =
    if (latestKnownSequencingTime > this.latestKnownSequencingTime)
      this.latestKnownSequencingTime = latestKnownSequencingTime

  /** Dequeues up to `n` non-expired requests from the front of the queue for batching, bounded by
    * `maxCombinedBytes`. Expired requests are always discarded (span ended) without counting
    * against `n` or the weight budget. Oversized requests, whose weight alone exceeds
    * `maxCombinedBytes` also discarded, regardless of position, to avoid blocking the queue.
    * Oversized requests should be unreachable in practice, since the mempool already rejects them
    * upstream, it only matters if the budget shrinks after oversized request enqueued.
    */
  @SuppressWarnings(Array("org.wartremover.warts.While"))
  def dequeueForBatch(n: Int, maxCombinedBytes: NonNegativeInt): Seq[QueuedOrderRequest] = {
    val result = mutable.ArrayBuffer.empty[QueuedOrderRequest]
    var remainingSize = maxCombinedBytes.value
    var done = false
    while (!done && result.sizeIs < n) {
      receivedOrderRequests.headOption match {
        case None => done = true
        case Some(head) =>
          val expired = head.orderRequest.maxSequencingTime.exists(_ <= latestKnownSequencingTime)
          lazy val size =
            head.orderRequest.tx.value.payload.size() // not evaluated when expired is true
          if (expired || size > maxCombinedBytes.value) {
            receivedOrderRequests.dequeue().discard
            head.span.end()
          } else if (size > remainingSize) {
            done = true
          } else {
            receivedOrderRequests.dequeue().discard
            remainingSize -= size
            result += head
          }
      }
    }
    result.toSeq
  }
}

object MempoolState {
  final class QueuedOrderRequest(
      val orderRequest: Mempool.OrderRequest,
      val span: Span,
  )
}
