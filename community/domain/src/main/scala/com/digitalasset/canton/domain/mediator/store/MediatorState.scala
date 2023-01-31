// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.data.OptionT
import cats.instances.future.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.ResponseAggregation
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentSkipListMap}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Provides state management for messages received by the mediator.
  * Non-finalized response aggregations are kept in memory, such that in case of the node shutting down,
  * they are lost but the participants waiting for a transaction result will simply timeout.
  * The finalized response aggregations are stored in the provided [[FinalizedResponseStore]].
  * It is expected that `fetchPendingRequestIdsBefore` operation is not called concurrently with operations
  * to modify the pending requests.
  */
private[mediator] class MediatorState(
    val finalizedResponseStore: FinalizedResponseStore,
    val deduplicationStore: MediatorDeduplicationStore,
    val clock: Clock,
    metrics: MediatorMetrics,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  // outstanding requests are kept in memory while finalized requests will be stored
  // a skip list is used to optimise for when we fetch all keys below a given timestamp
  private val pendingRequests =
    new ConcurrentSkipListMap[RequestId, ResponseAggregation](implicitly[Ordering[RequestId]])

  // once requests are finished, we keep em around for a few minutes so we can deal with any
  // late request, avoiding a database lookup. otherwise, we'll be doing a db lookup in our
  // main processing stage, slowing things down quite a bit
  private val finishedRequests = TrieMap[RequestId, ResponseAggregation]()
  private val evictionQueue = new ConcurrentLinkedQueue[RequestId]()
  private val evictionQueueCount = new AtomicInteger(0)
  private val MAX_FINISHED_REQUESTS = 1000

  private def updateNumRequests(num: Int): Unit =
    metrics.outstanding.updateValue(_ + num)

  /** Adds an incoming ResponseAggregation */
  def add(
      responseAggregation: ResponseAggregation
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val requestId = responseAggregation.requestId
    if (!responseAggregation.isFinalized) {
      val existingValue = Option(pendingRequests.putIfAbsent(requestId, responseAggregation))
      ErrorUtil.requireState(
        existingValue.isEmpty,
        s"Unexpected pre-existing request for $requestId",
      )

      metrics.requests.mark()
      updateNumRequests(1)
      Future.unit
    } else
      finalizedResponseStore.store(responseAggregation)
  }

  def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, ResponseAggregation] = {
    // TODO(#10025): in an overload situation, when participants start to reply late, the fetching
    //   from the store became punitive in the semi-optimal mediator response processing
    Option(pendingRequests.get(requestId))
      .orElse(finishedRequests.get(requestId)) match {
      case Some(cp) => OptionT.some[Future](cp)
      case None => finalizedResponseStore.fetch(requestId)
    }
  }

  /** Replaces a [[ResponseAggregation]] for the `requestId` if the stored version matches `currentVersion`.
    * You can only use this to update non-finalized aggregations
    */
  def replace(oldValue: ResponseAggregation, newValue: ResponseAggregation)(implicit
      traceContext: TraceContext
  ): OptionT[Future, Unit] = {
    ErrorUtil.requireArgument(
      oldValue.requestId == newValue.requestId,
      s"RequestId ${oldValue.requestId} cannot be replaced with ${newValue.requestId}",
    )
    ErrorUtil.requireArgument(!oldValue.isFinalized, s"Already finalized ${oldValue.requestId}")

    val requestId = oldValue.requestId

    def storeFinalized: OptionT[Future, Unit] = OptionT.liftF {
      finalizedResponseStore.store(newValue) map { _ =>
        // keep the request around for a while to avoid a database lookup under contention
        finishedRequests.put(requestId, newValue) match {
          // request was not yet present, ensure we schedule the eviction
          case None =>
            // request was not yet present, remember it for some time and remove an older request
            evictionQueue.offer(requestId)
            // if this addition makes the cache exceeding the max size, don't increase it, but remove the head
            val count = evictionQueueCount.getAndUpdate { x =>
              if (x >= MAX_FINISHED_REQUESTS) x else x + 1
            }
            if (count >= MAX_FINISHED_REQUESTS) {
              Option(evictionQueue.poll()).fold(
                logger.error("Removing finished request failed with empty poll!")
              )(finishedRequests.remove(_).discard)
            }
          case Some(_) =>
        }
        Option(pendingRequests.remove(requestId)) foreach { _ =>
          updateNumRequests(-1)
        }
      }
    }

    def updatePending(): Unit = {
      val _ = pendingRequests.put(requestId, newValue)
    }

    for {
      // I'm not really sure about these validations or errors...
      currentValue <- OptionT.fromOption(Option(pendingRequests.get(requestId)).orElse {
        MediatorError.InternalError
          .Reject(
            s"Request $requestId has unexpectedly disappeared (expected version: ${oldValue.version}, new version: ${newValue.version})."
          )
          .log()
        None
      })
      _ <-
        if (currentValue.version == oldValue.version) OptionT.some[Future](())
        else {
          MediatorError.InternalError
            .Reject(
              s"Request $requestId has an unexpected version ${currentValue.version} (expected version: ${oldValue.version}, new version: ${newValue.version})."
            )
            .log()
          OptionT.none[Future, Unit]
        }
      _ <-
        if (newValue.isFinalized) storeFinalized
        else OptionT.some[Future](updatePending())
    } yield ()
  }

  /** Fetch pending requests that have a timestamp below the provided `cutoff` */
  def pendingRequestIdsBefore(cutoff: CantonTimestamp): List[RequestId] =
    pendingRequests.keySet().headSet(RequestId(cutoff)).asScala.toList

  /** Fetch a response aggregation from the pending requests collection. */
  def getPending(requestId: RequestId): Option[ResponseAggregation] = Option(
    pendingRequests.get(requestId)
  )

  /** Prune unnecessary data from before and including the given timestamp which should be calculated by the [[Mediator]]
    * based on the participant response timeout domain parameters. In practice a much larger retention period
    * may be kept.
    *
    * Also updates the current age of the oldest finalized response after pruning.
    */
  def prune(pruneRequestsBeforeAndIncludingTs: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = finalizedResponseStore.prune(pruneRequestsBeforeAndIncludingTs)

  /** Locate the timestamp of the finalized response at or, if skip > 0, near the beginning of the sequence of finalized responses.
    *
    * If skip == 0, returns the timestamp of the oldest, unpruned finalized response.
    */
  def locatePruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = for {
    ts <- finalizedResponseStore.locatePruningTimestamp(skip.value)
    _ = if (skip.value == 0) MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, ts)
  } yield ts

  /** Report the max-event-age metric based on the oldest response timestamp and the current clock time or
    * zero if no oldest timestamp exists (e.g. mediator fully pruned).
    */
  def reportMaxResponseAgeMetric(oldestResponseTimestamp: Option[CantonTimestamp]): Unit =
    MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, oldestResponseTimestamp)

  override def onClosed(): Unit = Lifecycle.close(finalizedResponseStore)(logger)
}
