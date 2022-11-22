// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.data.{
  CantonTimestamp,
  ConcurrentHMap,
  PeanoQueue,
  SynchronizedPeanoTreeQueue,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.Phase37Synchronizer.*
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  PendingRequestData,
  RequestType,
}
import com.digitalasset.canton.participant.protocol.ProtocolProcessor.PendingRequestDataOrReplayData
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.{DiscardOps, RequestCounter, RequestCounterDiscriminator}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.{Future, Promise, blocking}

/** Synchronizes the request processing of phases 3 and 7.
  * At the end of phase 3, every request must signal that it has reached
  * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]] via [[markConfirmed]]
  * or [[markTimeout]].
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

  /** Maps requests timestamps to pending request data.
    *
    * More precisely, the keys and values are typed to encode the kind of
    * [[com.digitalasset.canton.participant.protocol.ProcessingSteps.RequestType#PendingRequestData]]:
    * for any `T <: PendingRequestData` the following types are valid for a key-value pair:
    *    TRequestId[T] -> PendingRequestDataOrReplayData[T]
    */
  private[this] val pendingRequests = ConcurrentHMap.empty[HMapRequestRelation]

  /** Maps request timestamps to promises that are completed once
    * the corresponding request has reached [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    *
    * This map is changed only via inserts and deletes, but never via updates.
    */
  private[this] val byTimestamp = ConcurrentHMap.empty[HMapPromiseRelation]

  /** Lower bound on timestamps of requests that all have reached
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    *
    * The lower bound is tight because each insert into [[queue]] appears in a synchronized
    * block that also contains a call to [[collectGarbage]], which updates [[confirmedLowerBound]].
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
  def awaitConfirmed(requestId: RequestId, requestType: RequestType)(implicit
      traceContext: TraceContext
  ): Future[Option[PendingRequestDataOrReplayData[requestType.PendingRequestData]]] = {

    implicit val evPromise =
      Phase37Synchronizer.pendingRequestPromiseRelationFor[requestType.PendingRequestData]
    implicit val evRequest =
      Phase37Synchronizer.pendingRequestRelationFor[requestType.PendingRequestData]

    val ts = TCantonTimestamp[requestType.PendingRequestData](requestId.unwrap)

    blocking(synchronized {
      val timestamp = TCantonTimestamp[requestType.PendingRequestData](requestId.unwrap)
      logger.debug(
        s"Request ${requestId.unwrap}: Awaiting confirmed state; lower bound is ${confirmedLowerBound.get}"
      )

      pendingRequests.remove(ts) match {
        case Some(pendingRequestDataO) =>
          logger.debug(
            s"Request ${requestId.unwrap}: Returning pending request data to the caller"
          )

          // We know that the request was confirmed
          Future.successful(pendingRequestDataO)

        case None =>
          /*
            We don't know initially whether the request was not confirmed yet or
            whether it was already signaled to a caller of `awaitConfirmed`.
            This can be inferred using `confirmedLowerBound`.
           */

          if (requestId.unwrap <= confirmedLowerBound.get()) {
            logger.debug(
              s"Request ${requestId.unwrap}: Request data was already returned to another caller"
            )

            Future.successful(None)
          } else {
            val promise =
              Promise[Option[PendingRequestDataOrReplayData[requestType.PendingRequestData]]]()

            byTimestamp.putIfAbsent(timestamp, promise) match {
              case Some(_p) =>
                logger.debug(
                  s"Request ${requestId.unwrap}: another caller already subscribed to the update"
                )
                Future.successful(None)

              case None =>
                logger.debug(
                  s"Request ${requestId.unwrap}: Returning new promise to the caller"
                )

                promise.future
            }
          }
      }
    })
  }

  /** Clean internal data after the request has been inserted with [[markTimeout]].
    */
  def cleanOnTimeout(requestId: RequestId)(implicit traceContext: TraceContext): Unit = {
    implicit val evRequest = Phase37Synchronizer.pendingRequestRelationFor[PendingRequestData]
    val ts = TCantonTimestamp[PendingRequestData](requestId.unwrap)

    // Check that we don't have inconsistency with previously stored data
    blocking(synchronized {
      pendingRequests.remove(ts).flatten.foreach { existingRequestData =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Request with id `$requestId` was not marked as timed out. Found request data: ${existingRequestData.requestCounter}"
          )
        )
      }
    })

  }

  /** Marks the given request as having reached
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    *
    * @throws java.lang.IllegalArgumentException
    *     <ul>
    *       <li>If the maximum request counter Long.MaxValue is used</li>
    *       <li>If the same request counter was marked with a different timestamp or skipped
    *           and not all requests since the initial request counter `initRc` have been marked as confirmed or skipped.</li>
    *       <li> If the same request was marked with different requestData or indicated as timed out.
    *     </ul>
    */
  def markConfirmed(requestType: RequestType)(
      requestCounter: RequestCounter,
      requestId: RequestId,
      requestData: PendingRequestDataOrReplayData[requestType.PendingRequestData],
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Request $requestCounter: Marking request as confirmed; head is ${queue.head}")

    implicit val ev1 =
      Phase37Synchronizer.pendingRequestPromiseRelationFor[requestType.PendingRequestData]
    implicit val ev2 = Phase37Synchronizer.pendingRequestRelationFor[requestType.PendingRequestData]

    insert[requestType.PendingRequestData](requestCounter, requestId, Some(requestData))
  }

  /** Marks the given request as having timeout
    *
    * @throws java.lang.IllegalArgumentException
    * <ul>
    * <li>If the maximum request counter Long.MaxValue is used</li>
    * <li>If the same request counter was marked with a different timestamp or skipped
    * and not all requests since the initial request counter `initRc` have been marked as confirmed or skipped.</li>
    * <li>If the same request was marked as confirmed.</li>
    * </ul>
    */
  def markTimeout(
      requestCounter: RequestCounter,
      requestId: RequestId,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Request $requestCounter: Marking request as timeout; head is ${queue.head}")

    implicit val evPromise =
      Phase37Synchronizer.pendingRequestPromiseRelationFor[PendingRequestData]
    implicit val evRequest = Phase37Synchronizer.pendingRequestRelationFor[PendingRequestData]

    insert(requestCounter, requestId, None)
  }

  /** Common method called by [[markConfirmed]] and [[markTimeout]]
    * @param requestDataO Pending request data for a confirmed request, None for a timeout request
    */
  private[this] def insert[T <: PendingRequestData](
      requestCounter: RequestCounter,
      requestId: RequestId,
      requestDataO: Option[PendingRequestDataOrReplayData[T]],
  )(implicit
      evPromise: HMapPromiseRelation[TCantonTimestamp[T], Promise[
        Option[PendingRequestDataOrReplayData[T]]
      ]],
      evRequest: HMapRequestRelation[TCantonTimestamp[T], Option[
        PendingRequestDataOrReplayData[T]
      ]],
      traceContext: TraceContext,
  ): Unit = {
    val ts = TCantonTimestamp[T](requestId.unwrap)

    blocking(synchronized {
      queue.insert(requestCounter, Some(requestId.unwrap)).discard

      // Check that we don't have inconsistency with previously stored data
      pendingRequests.get(ts).filter(_ != requestDataO).foreach { existingRequestData =>
        def toString(requestDataO: Option[PendingRequestDataOrReplayData[T]]) =
          requestDataO.fold("timeout")(_.toString)

        ErrorUtil.internalError(
          new IllegalStateException(
            s"Request $requestCounter has different request data. Existing: ${toString(existingRequestData)}. New: ${toString(requestDataO)}"
          )
        )
      }

      byTimestamp.get(TCantonTimestamp[T](requestId.unwrap)) match {
        case Some(promise) =>
          logger.debug(s"Request $requestCounter: Completing promise with pending request data")
          promise.trySuccess(requestDataO).discard

        case None =>
          logger.debug(
            s"Request $requestCounter: Storing promise for future call to `awaitConfirmed`"
          )
          pendingRequests.putIfAbsent(ts, requestDataO).discard
      }

      collectGarbage()
    })
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

    queue.insert(requestCounter, None).discard
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

          implicit val evPromise =
            Phase37Synchronizer.pendingRequestPromiseRelationFor[PendingRequestData]

          val promiseO =
            byTimestamp.remove(TCantonTimestamp[PendingRequestData](timestamp))(evPromise)
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

  /** The following two relations don't satisfy the second requirement for type safety of the
    * [[ConcurrentHMap]]. However, since different requests have differents `ts` and `id`, we
    * can live we that.
    */
  private case class TCantonTimestamp[T <: PendingRequestData](
      ts: CantonTimestamp
  )

  private class HMapPromiseRelation[K, V]

  private def pendingRequestPromiseRelationFor[T <: PendingRequestData]
      : HMapPromiseRelation[TCantonTimestamp[T], Promise[
        Option[PendingRequestDataOrReplayData[T]]
      ]] =
    new HMapPromiseRelation[TCantonTimestamp[T], Promise[Option[PendingRequestDataOrReplayData[T]]]]

  private class HMapRequestRelation[K, V]

  /** We use an optional [[PendingRequestDataOrReplayData]]`[T]` here to allow to differentiate between
    * a missing entry in the map and a None coming from a timed out request.
    */
  private def pendingRequestRelationFor[T <: PendingRequestData]
      : HMapRequestRelation[TCantonTimestamp[T], Option[PendingRequestDataOrReplayData[T]]] =
    new HMapRequestRelation[TCantonTimestamp[T], Option[PendingRequestDataOrReplayData[T]]]
}
