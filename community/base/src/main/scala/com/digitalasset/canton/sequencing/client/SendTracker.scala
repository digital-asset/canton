// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.option.*
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricsContext.{withEmptyMetricsContext, withExtraMetricLabels}
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.SequencerClientMetrics
import com.digitalasset.canton.sequencing.client.SendResult.{Error, Success, Timeout}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  DecompressedSequencedEvent,
  Deliver,
  DeliverError,
  Envelope,
  MessageId,
}
import com.digitalasset.canton.sequencing.traffic.TrafficStateController
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.store.{SavePendingSendError, SendTrackerStore}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.common.annotations.VisibleForTesting

import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.TreeSet

/** When we make a send request to the sequencer it will not be sequenced until some point in the
  * future and may not be sequenced at all. To track a request call `send` with the messageId and
  * max-sequencing-time of the request, the tracker then observes sequenced events and will notify
  * the provided handler whether the send times out. For aggregatable submission requests, the send
  * tracker notifies the handler of successful sequencing of the submission request, not of
  * successful delivery of the envelopes when the threshold encoded in the
  * [[com.digitalasset.canton.sequencing.protocol.AggregationRule]] has been reached. In fact, there
  * is no notification of whether the threshold was reached before the max sequencing time.
  */
class SendTracker(
    initialPendingSends: Map[MessageId, CantonTimestamp],
    store: SendTrackerStore,
    metrics: SequencerClientMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    trafficStateController: Option[TrafficStateController],
) extends NamedLogging
    with FlagCloseableAsync
    with AutoCloseable {

  import SendTracker.LatestAttemptRef

  private implicit val directExecutionContext: DirectExecutionContext = DirectExecutionContext(
    noTracingLogger
  )

  /** Details of sends in-flight
    * @param startedAtNanoO
    *   The time the request was made for calculating the elapsed duration for metrics. We use the
    *   host clock time for this value and it is only tracked ephemerally as the elapsed value will
    *   not be useful if the local process restarts during sequencing.
    */
  private case class PendingSend(
      maxSequencingTime: CantonTimestamp,
      callback: SendCallback,
      startedAtNanoO: Option[Long],
      latestAttemptRef: LatestAttemptRef,
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  )

  private case class PendingSends(
      byId: Map[MessageId, PendingSend],
      byExpiry: TreeSet[(CantonTimestamp, MessageId)],
  )

  // track the timeouts of the pending sends in a sorted set so we can
  // efficiently find the ones that are expired without traversing the entire map.
  private implicit val pendingSendExpiryOrdering: Ordering[(CantonTimestamp, MessageId)] =
    Ordering.by { case (maxSequencingTime, messageId) => (maxSequencingTime, messageId.unwrap) }

  private val pendingSends = new AtomicReference[PendingSends](
    PendingSends(
      byId = Map.from(initialPendingSends.iterator.map {
        // callbacks and startedAt times will be lost between restarts of the sequencer client
        case (messageId, maxSequencingTime) =>
          messageId -> PendingSend(
            maxSequencingTime,
            SendCallback.empty,
            startedAtNanoO = None,
            latestAttemptRef = new LatestAttemptRef(None),
            TraceContext.empty,
            MetricsContext.Empty,
          )
      }),
      byExpiry = TreeSet.from(initialPendingSends.iterator.map {
        case (messageId, maxSequencingTime) =>
          (maxSequencingTime, messageId)
      }),
    )
  )

  def track(
      messageId: MessageId,
      maxSequencingTime: CantonTimestamp,
      callback: SendCallback = SendCallback.empty,
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): Either[SavePendingSendError, LatestAttemptRef] =
    for {
      _ <- store.savePendingSend(messageId, maxSequencingTime)
    } yield {
      val latestAttempt = new LatestAttemptRef(None)
      val pendingSend = PendingSend(
        maxSequencingTime,
        callback,
        startedAtNanoO = Some(System.nanoTime),
        latestAttemptRef = latestAttempt,
        traceContext,
        metricsContext,
      )
      val previous = pendingSends.getAndUpdate { current =>
        current.copy(
          byId = current.byId.updated(messageId, pendingSend),
          byExpiry = current.byExpiry + ((maxSequencingTime, messageId)),
        )
      }
      previous.byId.get(messageId) match {
        case Some(previousPendingSend) =>
          // if we were able to persist the new message id without issue but found the message id in our in-memory
          // pending set it suggests either:
          //  - the database has been modified by a writer other than this sequencer client (so its pending set is not in sync)
          //  - there is a bug :-|
          sys.error(
            s"""The SequencerClient pending set of sends is out of sync from the database.
                 |The database reported no send for $messageId but our pending set includes a prior send with mst of ${previousPendingSend.maxSequencingTime}.""".stripMargin
          )
        case _none => // we're good
      }
      metrics.submissions.inFlight.inc()
      latestAttempt
    }

  /** Cancels a pending send without notifying any callers of the result. Should only be used if the
    * send operation itself fails and the transport returns an error indicating that the send will
    * never be sequenced. The SequencerClient should then call cancel to immediately allow retries
    * with the same message-id and then propagate the send error to the caller.
    */
  def cancelPendingSend(messageId: MessageId)(implicit
      traceContext: TraceContext
  ): Unit =
    removePendingSendUnlessTimeout(messageId, resultO = None, sequencedTimeO = None)

  /** Provide the latest sequenced events to update the send tracker
    *
    * Callers must not call this concurrently and it is assumed that it is called with sequenced
    * events in order of sequencing. On receiving an event it will perform the following steps in
    * order:
    *   1. If the event is a Deliver or DeliverError from a send that is being tracked it will stop
    *      tracking this message id. This allows using the message-id for new sends.
    *   1. Checks for any pending sends that have a max-sequencing-time that is less than the
    *      timestamp of this event. These events have timed out and a correct sequencer
    *      implementation will no longer sequence any events for this send. The callback of the
    *      pending event will be called with the outcome result.
    *
    * The operations performed by update are not atomic, if an error is encountered midway through
    * processing an event then a subsequent replay will cause operations that still have pending
    * sends stored to be retried.
    */
  def update(
      events: Seq[SequencedEventWithTraceContext[Batch[Envelope[?]]]]
  ): Unit = if (events.isEmpty) ()
  else {
    val maxTimestamp = events.foldLeft(CantonTimestamp.MinValue) { case (maxTs, event) =>
      removePendingSend(event.signedEvent.content)(event.traceContext)
      maxTs.max(event.timestamp)
    }
    processTimeouts(maxTimestamp)
  }

  private def processTimeouts(
      timestamp: CantonTimestamp
  ): Unit = {
    val snapshot = pendingSends.get()
    val timedOut = snapshot.byExpiry.iterator
      .takeWhile { case (maxSequencingTime, _) => maxSequencingTime < timestamp }
      .flatMap { case (_, messageId) =>
        snapshot.byId.get(messageId).map(pending => Traced(messageId)(pending.traceContext))
      }
      .toList
    // note: race condition on reused message-ids is fine as we assume that message ids
    // generated by the node are unique (see exception thrown in track)
    timedOut.foreach(_.withTraceContext { implicit traceContext =>
      handleTimeout(timestamp)
    })
  }

  @VisibleForTesting
  protected def handleTimeout(timestamp: CantonTimestamp)(
      messageId: MessageId
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Sequencer send [$messageId] has timed out at $timestamp")
    removePendingSendUnlessTimeout(
      messageId,
      UnlessShutdown.Outcome(SendResult.Timeout(timestamp)).some,
      None, // none because the message really timed out
    )
  }

  private def removePendingSend(
      event: DecompressedSequencedEvent[Envelope[?]]
  )(implicit traceContext: TraceContext): Unit =
    extractSendResult(event).foreach { case (messageId, sendResult) =>
      removePendingSendUnlessTimeout(
        messageId,
        UnlessShutdown.Outcome(sendResult).some,
        Some(event.timestamp),
      )
    }

  private def updateSequencedMetrics(pendingSend: PendingSend, result: SendResult): Unit = {
    def recordSequencingTime(success: Boolean): Unit = {
      val now = System.nanoTime
      withEmptyMetricsContext { implicit metricsContext =>
        pendingSend.startedAtNanoO foreach { startedAtNano =>
          val elapsed = java.time.Duration.of(now - startedAtNano, ChronoUnit.NANOS)
          metrics.submissions.sequencingTime.update(elapsed)
        }
      }

      pendingSend.latestAttemptRef.get.foreach { attempt =>
        val elapsed = java.time.Duration.of(now - attempt.startedAtNano, ChronoUnit.NANOS)
        withExtraMetricLabels(
          "sequencerAlias" -> attempt.sequencerAlias.toString,
          "success" -> success.toString,
        ) { metricsContext =>
          metrics.submissions.attemptSequencingTime.update(elapsed)(metricsContext)
        }(pendingSend.metricsContext)
      }
    }

    result match {
      case SendResult.Success(_) => recordSequencingTime(success = true)
      case SendResult.Error(_) =>
        // even though it's an error the sequencer still sequenced our request
        recordSequencingTime(success = false)
      case SendResult.Timeout(_) =>
        // intentionally not updating sequencing time as this implies no event was sequenced from our request
        metrics.submissions.dropped.inc()
    }
  }

  /** Removes the pending send. If a send result is supplied the callback will be called with it. If
    * the sequencedTime is supplied and it is more recent than the max-sequencing time of the event,
    * then we will not remove the pending send.
    */
  private def removePendingSendUnlessTimeout(
      messageId: MessageId,
      resultO: Option[UnlessShutdown[SendResult]],
      sequencedTimeO: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    def removePendingSendUpdate(current: PendingSends): PendingSends =
      current.byId.get(messageId) match {
        case Some(pending) if sequencedTimeO.exists(_ > pending.maxSequencingTime) => current
        case Some(pending) =>
          current.copy(
            byId = current.byId - messageId,
            byExpiry = current.byExpiry - ((pending.maxSequencingTime, messageId)),
          )
        case None => current
      }

    val current = pendingSends.getAndUpdate(removePendingSendUpdate)
    val updated = removePendingSendUpdate(current)
    val currentPending = current.byId.get(messageId)
    val updatedPending = updated.byId.get(messageId)

    // Metrics context extracted from the pending send
    // This allows to get labels such as the request type and application ID back and use them to update
    // event specific metrics
    val eventSpecificMetricsContext = currentPending
      .map(_.metricsContext)
      .getOrElse(
        // If we there's no pending send, set the application id and type labels to unknown to get consistent
        // labelling even during crash recovery (when we may not have corresponding pending sends for the receipts)
        MetricsContext(
          "application-id" -> "unknown",
          "type" -> "unknown",
        )
      )
    // Update the traffic controller with the traffic consumed in the receipt
    (trafficStateController, resultO) match {
      case (Some(tsc), Some(UnlessShutdown.Outcome(Success(deliver)))) =>
        deliver.trafficReceipt.foreach(
          tsc.updateWithReceipt(_, deliver.timestamp, None, eventSpecificMetricsContext)
        )
      case (Some(tsc), Some(UnlessShutdown.Outcome(Error(deliverError)))) =>
        deliverError.trafficReceipt.foreach(
          tsc.updateWithReceipt(
            _,
            deliverError.timestamp,
            CantonBaseError
              .statusErrorCodes(deliverError.reason)
              .headOption
              .orElse(Some("unknown")),
            eventSpecificMetricsContext,
          )
        )
      case (Some(tsc), Some(UnlessShutdown.Outcome(Timeout(timestamp)))) =>
        // Event was not sequenced but we can still advance the base rate at the timestamp
        tsc.tickStateAt(timestamp)
      case _ =>
    }

    (updatedPending, currentPending) match {
      // pending command was removed
      case (None, Some(pending)) =>
        resultO.foreach { result =>
          result.foreach(updateSequencedMetrics(pending, _))
          pending.callback(result)
        }
        store.removePendingSend(messageId)
        metrics.submissions.inFlight.dec()(eventSpecificMetricsContext)
      // if the sequencedTime is passed and it is more recent than the max-sequencing time of the
      // event, then we will not remove the pending send (it will be picked up later by the handleTimeout method)
      case (Some(_), _) =>
        // We observed the command being sequenced but it arrived too late to be processed.
        ()
      case _ =>
        logger.debug(s"Removing unknown pending command $messageId")
        store.removePendingSend(messageId)
    }
  }

  private def extractSendResult(
      event: DecompressedSequencedEvent[Envelope[?]]
  )(implicit traceContext: TraceContext): Option[(MessageId, SendResult)] =
    Option(event) collect {
      case deliver @ Deliver(_, _, _, Some(messageId), _, _, _) =>
        logger.trace(s"Send [$messageId] was successful")
        (messageId, SendResult.Success(deliver))

      case error @ DeliverError(_, _, _, messageId, reason, _) =>
        logger.debug(s"Send [$messageId] failed: $reason")
        (messageId, SendResult.Error(error))
    }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.emptyTraceContext
    Seq(
      SyncCloseable(
        "complete-pending-sends",
        pendingSends
          .get()
          .byId
          .keys
          .foreach(
            removePendingSendUnlessTimeout(_, Some(UnlessShutdown.AbortedDueToShutdown), None)
          ),
      ),
      SyncCloseable("send-tracker-store", store.close()),
    )
  }
}

object SendTracker {
  private[sequencing] final case class LatestAttempt(
      sequencerAlias: SequencerAlias,
      startedAtNano: Long,
  )

  type LatestAttemptRef = AtomicReference[Option[LatestAttempt]]
}
