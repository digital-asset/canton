// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.client.SequencerClientSend.SendRequestTimestamps
import com.digitalasset.canton.sequencing.protocol.{AggregationRule, Batch, MessageId}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext

/** Natural batching for sequencer submissions.
  *
  * N queued submissions are merged into ONE Batch with ONE sendAsync call.
  * This collapses N sign + serialize + gRPC flush operations into 1.
  *
  * Under low load: 1 item, 1 sendAsync (no overhead).
  * Under high load: N items → 1 merged Batch → 1 sendAsync.
  * N operations become 1 operation, not N operations fired quickly.
  */
class BatchingSendQueue(
    delegate: SequencerClientSend,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private case class QueuedSend(
      batch: Batch[DefaultOpenEnvelope],
      timestamps: SendRequestTimestamps,
      messageId: MessageId,
      aggregationRule: Option[AggregationRule],
      callback: SendCallback,
      amplify: Boolean,
      useConfirmationResponseAmplificationParameters: Boolean,
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  )

  private val queue = new ConcurrentLinkedQueue[QueuedSend]()
  private val flushScheduled = new AtomicBoolean(false)

  def sendAsync(
      batch: Batch[DefaultOpenEnvelope],
      timestamps: SendRequestTimestamps,
      messageId: MessageId,
      aggregationRule: Option[AggregationRule],
      callback: SendCallback,
      amplify: Boolean,
      useConfirmationResponseAmplificationParameters: Boolean,
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit = {
    queue.add(QueuedSend(
      batch, timestamps, messageId, aggregationRule, callback,
      amplify, useConfirmationResponseAmplificationParameters,
      traceContext, metricsContext,
    ))
    scheduleFlush()
  }

  private def scheduleFlush(): Unit =
    if (flushScheduled.compareAndSet(false, true))
      ec.execute(() => flush())

  private def flush(): Unit = {
    flushScheduled.set(false)

    // Drain everything into a list
    var items = List.newBuilder[QueuedSend]
    var count = 0
    var item = queue.poll()
    while (item != null) {
      items += item
      count += 1
      item = queue.poll()
    }

    val drained = items.result()
    if (drained.isEmpty) {
      if (!queue.isEmpty) scheduleFlush()
      return
    }

    if (drained.sizeIs == 1) {
      // Single item — send directly, no merging overhead
      val s = drained.head
      implicit val tc: TraceContext = s.traceContext
      implicit val mc: MetricsContext = s.metricsContext
      delegate.sendAsync(
        s.batch, s.timestamps, s.messageId, s.aggregationRule,
        s.callback, s.amplify, s.useConfirmationResponseAmplificationParameters,
      )
    } else {
      // Multiple items — merge into ONE batch, ONE sendAsync call.
      // Each original batch's envelopes are concatenated, ordered by
      // original timestamp to preserve causal ordering within the batch.
      // Each envelope carries its own Recipients, so the sequencer
      // delivers correctly.
      val sorted = drained.sortBy(_.timestamps.maxSequencingTime)
      val mergedEnvelopes = sorted.flatMap(_.batch.envelopes)
      val first = drained.head
      implicit val tc: TraceContext = first.traceContext
      implicit val mc: MetricsContext = first.metricsContext

      val mergedBatch = Batch(mergedEnvelopes, delegate.protocolVersion)

      // Use the earliest maxSequencingTime (most conservative deadline)
      val earliestMaxSeqTime = drained.map(_.timestamps.maxSequencingTime).min
      val mergedTimestamps = SendRequestTimestamps(
        topologyTimestamp = first.timestamps.topologyTimestamp,
        approximateTimestampForSigning = first.timestamps.approximateTimestampForSigning,
        maxSequencingTime = earliestMaxSeqTime,
      )

      // Fan out the callback: when the merged send completes, notify all original callers
      val allCallbacks = drained.map(_.callback)
      val mergedCallback: SendCallback = { result =>
        allCallbacks.foreach(cb => cb(result))
      }

      logger.debug(s"Natural batch: merged $count submissions into 1 sendAsync")(TraceContext.empty)

      delegate.sendAsync(
        mergedBatch,
        mergedTimestamps,
        first.messageId, // use first item's messageId for tracking
        first.aggregationRule,
        mergedCallback,
        amplify = first.amplify,
        useConfirmationResponseAmplificationParameters =
          first.useConfirmationResponseAmplificationParameters,
      )
    }

    if (!queue.isEmpty) scheduleFlush()
  }

  def pendingCount: Int = queue.size()
}
