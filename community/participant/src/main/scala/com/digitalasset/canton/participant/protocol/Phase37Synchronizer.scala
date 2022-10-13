// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.data.{CantonTimestamp, PeanoQueue, SynchronizedPeanoTreeQueue}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.Phase37Synchronizer._
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{RequestCounter, RequestCounterDiscriminator}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise, blocking}

/** Synchronizes the request processing of phases 3 and 7.
  * At the end of phase 3, every request must signal that it has reached
  * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]] via [[markConfirmed]].
  * At the beginning of phase 7, requests can wait on the completion of phase 3 via [[awaitConfirmed]].
  *
  * Eventually, all requests above `initRc` should signal via [[markConfirmed]] or [[skipRequestCounter]].
  * Otherwise, the synchronizer becomes a memory leak.
  * This class assumes that the timestamps in request IDs grow strictly with the non-skipped request counters.
  *
  * @param initRc The request counter of the first request to synchronize.
  */
class Phase37Synchronizer(initRc: RequestCounter, override val loggerFactory: NamedLoggerFactory)
    extends NamedLogging {

  /** Maps request timestamps to promises that are completed once
    * the corresponding request has reached [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    *
    * This map is changed only via inserts and deletes, but never via updates.
    */
  private[this] val byTimestamp: concurrent.Map[CantonTimestamp, Promise[Unit]] =
    new TrieMap[CantonTimestamp, Promise[Unit]]()

  /** Lower bound on timestamps of requests that all have reached
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    * The lower bound need not be tight.
    * In fact, it lags behind the [[queue]]'s [[com.digitalasset.canton.data.PeanoQueue.head]].
    */
  private[this] val confirmedLowerBound: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](CantonTimestamp.MinValue)

  /** Keeps track of the timestamps in [[byTimestamp]] by requests that have reached
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    * Timestamps of requests below the head are removed from [[byTimestamp]].
    * Skipped [[com.digitalasset.canton.RequestCounter]]s are associated with [[scala.None$]].
    */
  private[this] val queue: PeanoQueue[RequestCounter, Option[CantonTimestamp]] =
    new SynchronizedPeanoTreeQueue[RequestCounterDiscriminator, Option[CantonTimestamp]](initRc)

  /** The returned future completes after the given request has reached
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    */
  def awaitConfirmed(requestId: RequestId)(implicit traceContext: TraceContext): Future[Unit] =
    blocking(synchronized {
      val timestamp = requestId.unwrap
      logger.debug(
        s"Request ${requestId.unwrap}: Awaiting confirmed state; lower bound is ${confirmedLowerBound.get}"
      )
      if (timestamp <= confirmedLowerBound.get) {
        Future.unit
      } else {
        byTimestamp.getOrElseUpdate(timestamp, Promise[Unit]()).future
      }
    })

  /** Marks the given request as having reached
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    *
    * @throws java.lang.IllegalArgumentException
    *     <ul>
    *       <li>If the maximum request counter Long.MaxValue is used</li>
    *       <li>If the same request counter was marked with a different timestamp or skipped
    *           and not all requests since the initial request counter `initRc` have been marked as confirmed or skipped.</li>
    *     </ul>
    */
  def markConfirmed(requestCounter: RequestCounter, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Request $requestCounter: Marking request as confirmed; head is ${queue.head}")
    insert(requestCounter, Some(requestId.unwrap))
  }

  /** Skips the given request counter without specifying a timestamp.
    *
    * @throws java.lang.IllegalArgumentException
    *     <ul>
    *       <li>If the maximum request counter Long.MaxValue is used</li>
    *       <li>If the same request counter was marked as confirmed
    *           and not all requests since the initial request counter `initRc` have been marked as confirmed or skipped.</li>
    *     </ul>
    */
  def skipRequestCounter(
      requestCounter: RequestCounter
  )(implicit traceContext: TraceContext): Unit = blocking(synchronized {
    logger.debug(s"Request $requestCounter: Skipping; head is ${queue.head}")
    insert(requestCounter, None)
  })

  private[this] def insert(requestCounter: RequestCounter, timestampO: Option[CantonTimestamp])(
      implicit traceContext: TraceContext
  ): Unit = blocking(synchronized {
    queue.insert(requestCounter, timestampO)
    timestampO.foreach { timestamp =>
      byTimestamp.get(timestamp).foreach(_.trySuccess(()))
      if (timestamp > confirmedLowerBound.get()) {
        val _ = byTimestamp.putIfAbsent(timestamp, KeptUnitPromise)
      }
    }
    collectGarbage()
  })

  private[this] def collectGarbage()(implicit traceContext: TraceContext): Unit = {
    @tailrec
    def go(): Unit = queue.poll() match {
      case None =>
      case Some((requestCounter, timestampO)) =>
        timestampO.foreach { timestamp =>
          val oldTimestamp = confirmedLowerBound.getAndSet(timestamp)
          if (oldTimestamp > timestamp) {
            ErrorUtil.internalError(
              new IllegalStateException(s"Decreasing timestamps $oldTimestamp and $timestamp")
            )
          }

          val promiseO = byTimestamp.remove(timestamp)
          promiseO.filter(!_.isCompleted).foreach { _ =>
            ErrorUtil.internalError(
              new IllegalStateException(
                s"Request $requestCounter at $timestamp has not yet completed the promise in the synchronizer."
              )
            )
          }
        }

        go()
    }

    go()
  }

  @VisibleForTesting
  private[protocol] def head: RequestCounter = queue.head

  @VisibleForTesting
  private[protocol] def lowerBound: CantonTimestamp = confirmedLowerBound.get
}

object Phase37Synchronizer {
  private val KeptUnitPromise: Promise[Unit] = Promise.successful(())
}
