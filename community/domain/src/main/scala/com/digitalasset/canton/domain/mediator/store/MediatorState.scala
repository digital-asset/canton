// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.data.EitherT
import cats.instances.future._
import cats.syntax.either._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.{
  MediatorRequestNotFound,
  ResponseAggregation,
  StaleVersion,
}
import com.digitalasset.canton.domain.metrics.MediatorMetrics
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import java.util.concurrent.ConcurrentSkipListMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/** Provides state management for messages received by the mediator.
  * Non-finalized response aggregations are kept in memory, such that in case of the node shutting down,
  * they are lost but the participants waiting for a transaction result will simply timeout.
  * The finalized response aggregations are stored in the provided [[FinalizedResponseStore]].
  * It is expected that `fetchPendingRequestIdsBefore` operation is not called concurrently with operations
  * to modify the pending requests.
  */
class MediatorState(
    val finalizedResponseStore: FinalizedResponseStore,
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

  private def updateNumRequests(num: Int): Unit = metrics.outstanding.metric.updateValue(_ + num)

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

      metrics.requests.metric.mark()
      updateNumRequests(1)
      Future.unit
    } else
      finalizedResponseStore.store(responseAggregation)
  }

  def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorRequestNotFound, ResponseAggregation] =
    Option(pendingRequests.get(requestId)) match {
      case Some(cp) => EitherT.rightT[Future, MediatorRequestNotFound](cp)
      case None => finalizedResponseStore.fetch(requestId)
    }

  /** Replaces a [[ResponseAggregation]] for the `requestId` if the stored version matches `currentVersion`.
    * You can only use this to update non-finalized aggregations
    */
  def replace(oldValue: ResponseAggregation, newValue: ResponseAggregation)(implicit
      traceContext: TraceContext
  ): EitherT[Future, StaleVersion, Unit] = {
    ErrorUtil.requireArgument(
      oldValue.requestId == newValue.requestId,
      s"RequestId ${oldValue.requestId} cannot be replaced with ${newValue.requestId}",
    )
    ErrorUtil.requireArgument(!oldValue.isFinalized, s"Already finalized ${oldValue.requestId}")

    val requestId = oldValue.requestId

    def storeFinalized: EitherT[Future, StaleVersion, Unit] = EitherT.right {
      finalizedResponseStore.store(newValue) map { _ =>
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
      currentValue <- Option(pendingRequests.get(requestId))
        .toRight(StaleVersion(requestId, newValue.version, oldValue.version))
        .toEitherT
      _ <- EitherT.cond(
        currentValue.version == oldValue.version,
        (),
        StaleVersion(requestId, newValue.version, currentValue.version),
      )
      _ <-
        if (newValue.isFinalized) storeFinalized
        else EitherT.pure[Future, StaleVersion](updatePending())
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
    */
  def prune(pruneRequestsBeforeAndIncludingTs: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    finalizedResponseStore.prune(pruneRequestsBeforeAndIncludingTs)

  override def onClosed(): Unit = Lifecycle.close(finalizedResponseStore)(logger)
}
