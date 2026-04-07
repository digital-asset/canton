// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator.store

import cats.data.OptionT
import cats.syntax.functor.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsHelper
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.synchronizer.mediator.{
  FinalizedResponse,
  ResponseAggregation,
  ResponseAggregator,
}
import com.digitalasset.canton.synchronizer.metrics.MediatorMetrics
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext

/** Provides state management for messages received by the mediator. Non-finalized response
  * aggregations are kept in memory, such that in case of the node shutting down, they are lost but
  * the participants waiting for a transaction result will simply timeout. The finalized response
  * aggregations are stored in the provided [[FinalizedResponseStore]]. It is expected that
  * `fetchPendingRequestIdsBefore` operation is not called concurrently with operations to modify
  * the pending requests.
  */
private[mediator] class MediatorState(
    val finalizedResponseStore: FinalizedResponseStore,
    val deduplicationStore: MediatorDeduplicationStore,
    val clock: Clock,
    val metrics: MediatorMetrics,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  // outstanding requests are kept in memory while finalized requests will be stored
  // a skip list is used to optimise for when we fetch all keys below a given timestamp
  private val pendingRequests =
    new ConcurrentSkipListMap[RequestId, ResponseAggregation[?]](implicitly[Ordering[RequestId]])

  // The participant response timeout for requests. This extra data structure facilitates fully asynchronous
  // request processing. MultiDict, because multiple requests could have the same response timeout.
  private val participantResponseTimeouts = mutable.SortedMultiDict[CantonTimestamp, RequestId]()

  private def updateNumRequests(num: Int): Unit =
    metrics.outstanding.updateValue(_ + num)

  def registerTimeoutForRequest(requestId: RequestId, participantResponseTimeout: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Unit = {
    participantResponseTimeouts.addOne(participantResponseTimeout -> requestId)
    ErrorUtil.requireState(
      !pendingRequests.containsKey(requestId),
      s"Unexpectedly setting the participant response timeout for the already pending request $requestId",
    )
  }

  /** Registers an incoming ResponseAggregation as pending request */
  def registerPendingRequest(
      responseAggregation: ResponseAggregation[?]
  )(implicit traceContext: TraceContext): Unit = {
    val requestId = responseAggregation.requestId
    ErrorUtil.requireState(
      pendingRequests.putIfAbsent(requestId, responseAggregation) eq null,
      s"Unexpected pre-existing request for $requestId",
    )

    metrics.requests.mark()(MediatorMetrics.nonduplicateRejectContext)
    updateNumRequests(1)
  }

  def add(
      finalizedResponse: FinalizedResponse
  )(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] =
    storeFinalized(finalizedResponse)

  def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): OptionT[FutureUnlessShutdown, ResponseAggregator] =
    OptionT
      .fromOption[FutureUnlessShutdown][ResponseAggregator](getPending(requestId))
      .orElse(finalizedResponseStore.fetch(requestId).widen[ResponseAggregator])

  /** Stores the finalized response, performs housekeeping of the internal state, and publishes a
    * new finalized record time if appropriate.
    *
    * Completes the [[ResponseAggregation.finalizedPromise]] of the pending aggregation (if any)
    * only AFTER the DB write succeeds. This is the mechanism that ensures the clean sequencer
    * counter for the confirmation request event only advances once the verdict is persisted,
    * enabling correct crash recovery.
    */
  private def storeFinalized(finalizedResponse: FinalizedResponse)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = {
    val requestId = finalizedResponse.requestId
    finalizedResponseStore.store(finalizedResponse) map { _ =>
      Option(pendingRequests.remove(requestId)) foreach { pendingAggregation =>
        updateNumRequests(-1)
        // Complete the finalized promise AFTER persistence so crash recovery replays correctly.
        pendingAggregation.completeFinalizedPromise()
      }
    }
  }

  /** Replaces a [[ResponseAggregation]] for the `requestId` if the stored version matches
    * `currentVersion`. You can only use this to update non-finalized aggregations
    *
    * @return
    *   Whether the replacement was successful
    */
  def replace(oldValue: ResponseAggregator, newValue: ResponseAggregation[?])(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Boolean] = {
    ErrorUtil.requireArgument(
      oldValue.requestId == newValue.requestId,
      s"RequestId ${oldValue.requestId} cannot be replaced with ${newValue.requestId}",
    )
    ErrorUtil.requireArgument(!oldValue.isFinalized, s"Already finalized ${oldValue.requestId}")

    val requestId = oldValue.requestId

    (for {
      // I'm not really sure about these validations or errors...
      currentValue <- OptionT.fromOption[FutureUnlessShutdown](
        Option(pendingRequests.get(requestId)).orElse {
          MediatorError.InternalError
            .Reject(
              s"Request $requestId has unexpectedly disappeared (expected version: ${oldValue.version}, new version: ${newValue.version})."
            )
            .log()
          None
        }
      )
      _ <-
        if (currentValue.version == oldValue.version) OptionT.some[FutureUnlessShutdown](())
        else {
          MediatorError.InternalError
            .Reject(
              s"Request $requestId has an unexpected version ${currentValue.version} (expected version: ${oldValue.version}, new version: ${newValue.version})."
            )
            .log()
          OptionT.none[FutureUnlessShutdown, Unit]
        }
      _ <- OptionT.liftF[FutureUnlessShutdown, Unit] {
        newValue.asFinalized(protocolVersion) match {
          case None =>
            pendingRequests.put(requestId, newValue)
            FutureUnlessShutdown.unit
          case Some(finalizedResponse) => storeFinalized(finalizedResponse)
        }
      }
    } yield true).getOrElse(false)
  }

  /** Fetch pending requests that have a timeout below the provided `cutoff` */
  def pendingTimedoutRequest(cutoff: CantonTimestamp): List[RequestId] = {
    val range = participantResponseTimeouts.rangeUntil(cutoff)
    // `range` is "connected" to `participantResponseTimeouts. Therefore extract
    // the request ids first, before removing the range from the original map.
    val requestsToTimeOut = range.values.toList
    range.keySet.foreach(participantResponseTimeouts.removeKey)
    requestsToTimeOut
  }

  /** Fetch a response aggregation from the pending requests collection. */
  def getPending(requestId: RequestId): Option[ResponseAggregation[?]] = Option(
    pendingRequests.get(requestId)
  )

  @VisibleForTesting
  def getPendingRequestsSize: Int = pendingRequests.size()

  /** Prune unnecessary data from before and including the given timestamp which should be
    * calculated by the [[Mediator]] based on the confirmation response timeout synchronizer
    * parameters. In practice a much larger retention period may be kept.
    *
    * Also updates the current age of the oldest finalized response after pruning.
    */
  def prune(pruneRequestsBeforeAndIncludingTs: CantonTimestamp)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Unit] = finalizedResponseStore.prune(pruneRequestsBeforeAndIncludingTs)

  /** Locate the timestamp of the finalized response at or, if skip > 0, near the beginning of the
    * sequence of finalized responses.
    *
    * If skip == 0, returns the timestamp of the oldest, unpruned finalized response.
    */
  def findPruningTimestamp(skip: NonNegativeInt)(implicit
      traceContext: TraceContext,
      callerCloseContext: CloseContext,
  ): FutureUnlessShutdown[Option[CantonTimestamp]] = for {
    ts <- finalizedResponseStore.findPruningTimestamp(skip.value)
    _ = if (skip.value == 0) MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, ts)
  } yield ts

  /** Report the max-event-age metric based on the oldest response timestamp and the current clock
    * time or zero if no oldest timestamp exists (e.g. mediator fully pruned).
    */
  def reportMaxResponseAgeMetric(oldestResponseTimestamp: Option[CantonTimestamp]): Unit =
    MetricsHelper.updateAgeInHoursGauge(clock, metrics.maxEventAge, oldestResponseTimestamp)

  override def onClosed(): Unit =
    LifeCycle.close(
      // deduplicationStore, Not closed on purpose, because the deduplication store methods all receive their close context directly from the caller
      finalizedResponseStore
    )(logger)
}
