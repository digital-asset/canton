// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.{FutureSupervisor, SupervisedPromise}
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

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

/** Synchronizes the request processing of phases 3 and 7.
  * At the end of phase 3, every request must signal that it has reached
  * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]] via [[markConfirmed]]
  * or [[markTimeout]].
  * At the beginning of phase 7, requests can wait on the completion of phase 3 via [[awaitConfirmed]].
  *
  * Eventually, all requests should either end with a `RequestState.ValidatedAndCompleted`.
  * After this point the request is cleaned from memory.
  * Otherwise, the synchronizer becomes a memory leak.
  * This class assumes that the timestamps in request IDs grow strictly with the non-skipped request counters.
  */
class Phase37Synchronizer(
    initRc: RequestCounter,
    override val loggerFactory: NamedLoggerFactory,
    futureSupervisor: FutureSupervisor,
) extends NamedLogging {

  /** Maps requests timestamps to pending request data.
    *
    * More precisely, the keys and values are typed to encode the kind of
    * [[com.digitalasset.canton.participant.protocol.ProcessingSteps.RequestType#PendingRequestData]]:
    * for any `T <: PendingRequestData` the key-value type is equivalent to
    * TCantonTimestamp[T] -> Option[PendingRequestDataOrReplayData[T], Future[Boolean]].
    *
    * The key is the underlying timestamp of the request id.
    * A value of None encodes a timeout request.
    */
  private[this] val pendingRequests = ConcurrentHMap.empty[HMapRequestRelation]

  /** Maps request timestamps to promises, and their respective filters.
    * The predicate future completes with either the pending request data, if it's the first valid call,
    * or None otherwise.
    */
  private[this] val byTimestamp = ConcurrentHMap.empty[HMapPromiseRelation]

  /** Maps request timestamps to its current state that can either be:
    * (1) [[ValidatedAndCompleted]] -> a request has completed successfully and
    * the corresponding data has been returned
    * (2) [[ConfirmedWithPendingValidation]] -> a request has not been completed yet, only confirmed via
    * [[markConfirmed]] or [[markTimeout]]
    * This map is ordered by timestamp (aka request id).
    */
  private[this] val requestState: ConcurrentSkipListMap[CantonTimestamp, RequestState] =
    new ConcurrentSkipListMap[CantonTimestamp, RequestState]()

  /** Lower bound on timestamps of requests that all have reached
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]],
    * in other words, a [[markConfirmed]] or [[markTimeout]] has already been called for that requestId.
    * The lower bound is tight because each insert into [[queue]] appears in a synchronized
    * block that also contains a call to [[updateBoundAndCleanMemory]],
    * which updates [[confirmedLowerBound]].
    */
  private[this] val confirmedLowerBound: AtomicReference[CantonTimestamp] =
    new AtomicReference[CantonTimestamp](CantonTimestamp.MinValue)

  /** Keeps track of the timestamps and decision time of requests that have reached
    * [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Confirmed]].
    * Timestamps of previous unfinished requests are checked via [[requestState]]. If valid or
    * the decision time has elapsed they will trigger a cleanup.
    * Skipped [[com.digitalasset.canton.RequestCounter]]s are associated with [[scala.None]].
    */
  private[this] val queue: PeanoQueue[RequestCounter, Option[(CantonTimestamp, CantonTimestamp)]] =
    new SynchronizedPeanoTreeQueue[RequestCounterDiscriminator, Option[
      (CantonTimestamp, CantonTimestamp)
    ]](initRc)

  /** The returned future completes with either the pending request data,
    * if it's the first valid call, or None otherwise. Please note that for each request only the first
    * awaitConfirmed, where filter == true, completes with the pending request data even if the
    * filters are different.
    *
    * @param requestId    The request id (timestamp) of the request to synchronize.
    * @param filter       A function that returns if a request is either valid or not (e.g. contains a valid signature).
    *                     This filter can be different for each call of awaitConfirmed, but only the first valid filter
    *                     will complete with the pending request data.
    */
  def awaitConfirmed[T <: PendingRequestData](
      requestId: RequestId,
      filter: PendingRequestDataOrReplayData[T] => Future[Boolean] =
        (_: PendingRequestDataOrReplayData[T]) => Future.successful(true),
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Option[PendingRequestDataOrReplayData[T]]] = {
    implicit val evPromise = Phase37Synchronizer.pendingRequestPromiseRelationFor[T]
    implicit val evRequest = Phase37Synchronizer.pendingRequestRelationFor[T]

    val ts = TCantonTimestamp[T](requestId.unwrap)

    blocking(synchronized {
      pendingRequests.get(ts) match {
        // Request is already marked confirmed
        case Some(Some(rr @ RequestRelation(requestData, _))) =>
          logger.debug(s"Request $ts: Evaluating pending request data")

          // Triggers async computation
          val newPredicateF =
            rr.evaluateRequest(requestState, requestId.unwrap, filter)

          pendingRequests.replace_(ts, Option(RequestRelation(requestData, newPredicateF)))
          newPredicateF.map(Option.when(_)(requestData))

        // Request is already marked timeout
        case Some(None) =>
          logger.debug(s"Request $ts: Marked as timeout")
          Future.successful(None)

        // Request awaiting confirmation
        case None =>
          def updateStateForNotCompleted(): Promise[Option[PendingRequestDataOrReplayData[T]]] = {
            val promise: Promise[Option[
              PendingRequestDataOrReplayData[T]
            ]] =
              SupervisedPromise[Option[
                PendingRequestDataOrReplayData[T]
              ]]("phase37sync-pending-request-data", futureSupervisor)

            val newPromiseRelation = byTimestamp.get(ts) match {
              // Some call to awaitConfirmed was made before
              case Some(pr) =>
                logger.debug(s"Request $ts: Adding a new promise")
                pr.updateWithNewPromise(requestState, requestId.unwrap, promise, filter)

              // First call to awaitConfirmed
              case None =>
                logger.debug(
                  s"Request $ts, first call to awaitConfirmed: returning promise"
                )
                PromiseRelation.create(promise, filter)
            }

            byTimestamp.put_(ts, newPromiseRelation)
            promise
          }

          /*
           From the `confirmedLowerBound` we can infer if a request has already been confirmed
           or not (i.e. markConfirmed or markTimeout has already been called). If so, then an empty [[requestState]]
           means that the request was processed and its memory cleaned.
           */
          if (requestId.unwrap <= confirmedLowerBound.get()) {
            Option(requestState.get(requestId.unwrap)) match {
              // another call to awaitConfirmed has already received and successfully validated the data
              case None | Some(RequestState.ValidatedAndCompleted()) =>
                logger.debug(
                  s"Request ${requestId.unwrap}: Request data was already returned to another caller"
                )
                Future.successful(None)
              /* request is still not yet validated although a markConfirmed has already been called and, therefore,
                 we chain a new validation to the predicateFuture (i.e. flatmap) */
              case Some(RequestState.ConfirmedWithPendingValidation()) =>
                updateStateForNotCompleted().future
            }
          } else {
            updateStateForNotCompleted().future
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
      pendingRequests.remove(ts).flatten.foreach { case RequestRelation(existingRequestData, _) =>
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
    * @param requestCounter The request counter of the request to synchronize.
    * @param requestId    The request id (timestamp) of the request to mark as confirmed.
    * @param decisionTime The decision time for the request (timestamp + participant timeout + mediator timeout).
    * @param requestData  The data to return to the caller if the request if valid.
    * @throws java.lang.IllegalArgumentException
    * <ul>
    * <li> If the maximum request counter Long.MaxValue is used.</li>
    * <li> If the same request counter was marked with a different timestamp or skipped
    * and not all requests since the initial request counter `initRc` have been marked as confirmed or skipped.</li>
    * <li> If the same request was marked with different requestData or indicated as timed out.</li>
    * </ul>
    */
  def markConfirmed(requestType: RequestType)(
      requestCounter: RequestCounter,
      requestId: RequestId,
      decisionTime: CantonTimestamp,
      requestData: PendingRequestDataOrReplayData[requestType.PendingRequestData],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Unit = {
    logger.debug(
      s"Request ${requestId.unwrap}: Marking request as confirmed; head is ${queue.head}"
    )

    implicit val evPromise =
      Phase37Synchronizer.pendingRequestPromiseRelationFor[requestType.PendingRequestData]
    implicit val evRequest =
      Phase37Synchronizer.pendingRequestRelationFor[requestType.PendingRequestData]

    insert[requestType.PendingRequestData](
      requestCounter,
      requestId,
      decisionTime,
      Some(requestData),
    )
  }

  /** Marks the given request as having timeout.
    *
    * @param requestCounter The request counter of the request to synchronize.
    * @param requestId    The request id (timestamp) of the request to mark as timeout.
    * @param decisionTime The decision time for the request (timestamp + participant timeout + mediator timeout).
    * @throws java.lang.IllegalArgumentException
    * <ul>
    * <li> If the maximum request counter Long.MaxValue is used</li>
    * <li> If the same request counter was marked with a different timestamp or skipped
    * and not all requests since the initial request counter `initRc` have been marked as confirmed or skipped.</li>
    * <li> If the same request was marked as confirmed.</li>
    * </ul>
    */
  def markTimeout(
      requestCounter: RequestCounter,
      requestId: RequestId,
      decisionTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Unit = {
    logger.debug(
      s"Request ${requestId.unwrap}: Marking request as timeout; head is ${queue.head}"
    )

    implicit val evPromise =
      Phase37Synchronizer.pendingRequestPromiseRelationFor[PendingRequestData]
    implicit val evRequest = Phase37Synchronizer.pendingRequestRelationFor[PendingRequestData]

    insert(requestCounter, requestId, decisionTime, None)
  }

  /** Common method called by [[markConfirmed]] and [[markTimeout]]
    * @param requestDataO Pending request data for a confirmed request, None for a timeout request.
    */
  private[this] def insert[T <: PendingRequestData](
      requestCounter: RequestCounter,
      requestId: RequestId,
      decisionTime: CantonTimestamp,
      requestDataO: Option[PendingRequestDataOrReplayData[T]],
  )(implicit
      evPromise: HMapPromiseRelation[TCantonTimestamp[T], PromiseRelation[T]],
      evRequest: HMapRequestRelation[TCantonTimestamp[T], Option[RequestRelation[T]]],
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Unit = {

    val ts = TCantonTimestamp[T](requestId.unwrap)

    blocking(synchronized {
      if (!queue.insert(requestCounter, Some((requestId.unwrap, decisionTime))))
        updateBoundAndCleanMemory(requestId)
      else {
        // Check that we don't have inconsistency with previously stored data
        pendingRequests
          .get(ts)
          .map(_.map(_.requestData))
          .filter(_ != requestDataO)
          .foreach { existingRequestData =>
            def toString(requestDataO: Option[PendingRequestDataOrReplayData[T]]) =
              requestDataO.fold("timeout")(_.toString)

            ErrorUtil.internalError(
              new IllegalStateException(
                s"Request has different request data. Existing: ${toString(existingRequestData)}. New: ${toString(requestDataO)}"
              )
            )
          }

        byTimestamp.get(ts) match {
          // awaitConfirmed has already been called
          case Some(pr: PromiseRelation[_]) =>
            logger.debug(s"Request $ts: Completing promises with pending request data")
            val newPr = pr.evaluateAllPromises(requestState, requestDataO, requestId)
            byTimestamp.replace_(ts, newPr)

          // awaitConfirmed was not called before
          case None =>
            logger.debug(
              s"Request $ts: Storing promise for future calls to `awaitConfirmed`"
            )
            val pendingRequestData =
              requestDataO.map(requestData =>
                RequestRelation(requestData, Future.successful(false))
              )
            pendingRequests.putIfAbsent(ts, pendingRequestData).discard
        }
      }
      updateBoundAndCleanMemory(requestId)
    })
  }

  /** Skips the given request counter without specifying a timestamp.
    *
    * @throws java.lang.IllegalArgumentException
    * <ul>
    * <li>If the maximum request counter Long.MaxValue is used</li>
    * <li>If the same request counter was marked as confirmed
    * and not all requests since the initial request counter `initRc` have been marked as confirmed or skipped.</li>
    * </ul>
    */
  def skipRequestCounter(
      requestCounter: RequestCounter,
      requestId: RequestId,
  )(implicit traceContext: TraceContext): Unit = blocking(synchronized {
    logger.debug(s"Request $requestCounter: Skipping; head is ${queue.head}")

    queue.insert(requestCounter, None).discard
    updateBoundAndCleanMemory(requestId)
  })

  private[this] def updateBoundAndCleanMemory(
      requestId: RequestId
  )(implicit traceContext: TraceContext): Unit = {

    implicit val evPromise =
      Phase37Synchronizer.pendingRequestPromiseRelationFor[PendingRequestData]
    implicit val evRequest =
      Phase37Synchronizer.pendingRequestRelationFor[PendingRequestData]

    @tailrec
    def go(): Unit = queue.poll() match {
      case None =>
      case Some((_, timestampsO)) =>
        timestampsO match {
          case Some((timestamp, _)) =>
            val oldTimestamp = confirmedLowerBound.getAndSet(timestamp)
            if (oldTimestamp > timestamp) {
              ErrorUtil.internalError(
                new IllegalStateException(s"Decreasing timestamps $oldTimestamp and $timestamp")
              )
            }

            // tag the current request has confirmed but still not validated
            requestState
              .putIfAbsent(
                requestId.unwrap,
                RequestState.ConfirmedWithPendingValidation(),
              )
              .discard

            // remove a particular request given by timestamp
            def clean(requestToRemoveTs: CantonTimestamp): Unit = {
              pendingRequests
                .remove(TCantonTimestamp[PendingRequestData](requestToRemoveTs))
                .discard
              byTimestamp.remove(TCantonTimestamp[PendingRequestData](requestToRemoveTs)).discard
              requestState.remove(requestToRemoveTs).discard
            }

            // recursively checks the first entry of the map (oldest timestamp) and cleans it
            // if the decision time has been reached
            def cleanRoutine(): Unit = {
              // Using option because `.get` can return null, which is converted to None
              val firstEntryO = Option(requestState.firstEntry())
              firstEntryO.foreach { firstEntry =>
                val requestToCheckTs = firstEntry.getKey
                val requestStateToCheck = firstEntry.getValue

                requestStateToCheck match {
                  case RequestState.ValidatedAndCompleted() =>
                    clean(requestToCheckTs)
                    cleanRoutine()
                  case RequestState.ConfirmedWithPendingValidation() =>
                  // todo i11718 - when and how do we cleanup in this case
                  /*if (requestTimestamp > requestToCheckDt) {
                      clean(requestToCheckTs)
                      cleanRoutine()
                    }*/
                }
              }
            }

            cleanRoutine()

          case None =>

        }

        go()
    }

    go()
  }

  @VisibleForTesting
  private[protocol] def head: RequestCounter = queue.head

  @VisibleForTesting
  private[protocol] def lowerBound: CantonTimestamp = confirmedLowerBound.get

  @VisibleForTesting
  private[protocol] def memoryIsCleaned(requestId: RequestId): Boolean = {
    implicit val evPromise =
      Phase37Synchronizer.pendingRequestPromiseRelationFor[PendingRequestData]
    implicit val evRequest =
      Phase37Synchronizer.pendingRequestRelationFor[PendingRequestData]

    pendingRequests.get(TCantonTimestamp[PendingRequestData](requestId.unwrap)).isEmpty &&
    byTimestamp.get(TCantonTimestamp[PendingRequestData](requestId.unwrap)).isEmpty &&
    Option(requestState.get(requestId.unwrap)).isEmpty
  }
}

object Phase37Synchronizer {

  /** The following two relations don't satisfy the second requirement for type safety of the
    * [[ConcurrentHMap]]. However, since different requests have different `ts` and `id`, we
    * can live we that.
    */
  private case class TCantonTimestamp[T <: PendingRequestData](ts: CantonTimestamp)

  private class HMapPromiseRelation[K, V]

  /** Contains a list of promises to fulfil.
    *
    * @param promises until [[predicateFuture]] is set it will aggregate all promises and filters spanned by
    *                 each call to awaitConfirmed.
    * @param predicateFuture we use it to chain futures stemming from calls to awaitConfirmed.
    *                        This is first set by the first call to markConfirmed
    *                        that will create a chain of futures based on the current list [[promises]].
    *                        Subsequently, future call to awaitConfirmed will 'flatMap'/chain on this future,
    *                        no longer using the list [[promises]].
    */
  private case class PromiseRelation[T <: PendingRequestData](
      promises: Seq[
        (
            Promise[Option[PendingRequestDataOrReplayData[T]]],
            PendingRequestDataOrReplayData[T] => Future[Boolean],
        )
      ],
      predicateFuture: Option[(Future[Boolean], Option[PendingRequestDataOrReplayData[T]])],
  ) {

    /** This will either (1) append a new promise to [[promises]] if [[predicateFuture]] IS NOT set,
      * meaning that no markConfirmed has been called yet, or; (2) if [[predicateFuture]] IS set,
      * will chain (i.e. flatMap) a new future to [[predicateFuture]] to
      * evaluate the result of the current promise based on the previous evaluations and the current [[filter]].
      */
    def updateWithNewPromise(
        requestState: ConcurrentSkipListMap[CantonTimestamp, RequestState],
        ts: CantonTimestamp,
        promise: Promise[Option[PendingRequestDataOrReplayData[T]]],
        filter: PendingRequestDataOrReplayData[T] => Future[Boolean],
    )(implicit ec: ExecutionContext): PromiseRelation[T] =
      predicateFuture match {
        case Some((_, rdO)) =>
          val newPredicateFuture = evaluatePromise(
            requestState,
            ts,
            promise,
            filter,
          )
          // Fine here to overwrite the list with Nil because it will not be read anymore
          PromiseRelation(Nil, Some((newPredicateFuture, rdO)))
        case None =>
          this.copy(promises = promises :+ (promise, filter))

      }

    /** For each promise in [[promises]] creates a future 'chain' using flatmap
      * to be saved as the [[predicateFuture]].
      */
    def evaluateAllPromises(
        requestState: ConcurrentSkipListMap[CantonTimestamp, RequestState],
        requestDataO: Option[PendingRequestDataOrReplayData[T]],
        requestId: RequestId,
    )(implicit ec: ExecutionContext): PromiseRelation[T] = {
      NonEmpty.from(promises) match {
        case Some(promisesNe) =>
          // sequentially filters the promises until the first that is valid
          val initialState =
            PromiseRelation(Nil, Some((Future.successful(false), requestDataO)))

          promisesNe.foldLeft(initialState) { case (state, (promise, filter)) =>
            // chain futures using flatmap
            PromiseRelation(
              Nil,
              Some(
                (
                  state.evaluatePromise(
                    requestState,
                    requestId.unwrap,
                    promise,
                    filter,
                  ),
                  requestDataO,
                )
              ),
            )
          }

        case None => this
      }

    }

    /** Evaluates an input promise based on the result of the previous chained futures and the [[filter]].
      */
    def evaluatePromise(
        requestState: ConcurrentSkipListMap[CantonTimestamp, RequestState],
        ts: CantonTimestamp,
        promise: Promise[Option[PendingRequestDataOrReplayData[T]]],
        filter: PendingRequestDataOrReplayData[T] => Future[Boolean],
    )(implicit ec: ExecutionContext): Future[Boolean] = {
      def returnEmptyPromise(
          promise: Promise[Option[PendingRequestDataOrReplayData[T]]],
          isValid: Boolean,
      ): Future[Boolean] = {
        promise.trySuccess(None).discard
        Future.successful(isValid)
      }

      predicateFuture match {
        case Some((predicateFuture, requestDataO)) =>
          requestDataO match {
            case Some(rD) =>
              predicateFuture
                .flatMap { isValid =>
                  // check that no previous promise has already returned a valid request
                  if (isValid) {
                    returnEmptyPromise(promise, isValid)
                  } else {
                    filter(rD)
                      .map {
                        case true =>
                          /*
                            If the [[requestState]] was already cleaned by markConfirmed we still add
                            a new 'ValidatedAndCompleted' state. This is not a memory leak because a future call
                            to markConfirmed will eventually clean this entry.
                           */
                          requestState
                            .put(ts, RequestState.ValidatedAndCompleted())
                            .discard
                          promise.trySuccess(Some(rD)).discard
                          true
                        case false =>
                          promise.trySuccess(None).discard
                          false
                      }
                  }
                }
            case None =>
              predicateFuture.map { isValid =>
                promise.trySuccess(None).discard
                isValid
              }
          }
        case None => returnEmptyPromise(promise, isValid = false)
      }
    }

  }

  private object PromiseRelation {
    def create[T <: PendingRequestData](
        promise: Promise[Option[PendingRequestDataOrReplayData[T]]],
        filter: PendingRequestDataOrReplayData[T] => Future[Boolean],
    ): PromiseRelation[T] =
      new PromiseRelation(Seq((promise, filter)), None)

  }

  private def pendingRequestPromiseRelationFor[T <: PendingRequestData]
      : HMapPromiseRelation[TCantonTimestamp[T], PromiseRelation[T]] =
    new HMapPromiseRelation[TCantonTimestamp[T], PromiseRelation[T]]

  private class HMapRequestRelation[K, V]

  /** Similar to [[PromiseRelation]], but we use it when a markConfirmed is called before any other awaitConfirmed.
    * This first call to markConfirmed will set the [[requestData]] and
    * initialize the future chain [[predicateFuture]] that is then used by subsequent calls to awaitConfirmed.
    *
    * @param requestData     the data associated with a given request that is set first time a
    *                        markConfirmed is called (and no awaitConfirmed as been called yet).
    * @param predicateFuture we use it to chain futures stemming from calls to awaitConfirmed.
    *                        This is first set by the first call to markConfirmed
    *                        that will initialize this future chain.
    *                        Subsequently, future calls to awaitConfirmed will 'flatMap'/chain on this future.
    */
  private case class RequestRelation[T <: PendingRequestData](
      requestData: PendingRequestDataOrReplayData[T],
      // to chain the futures generated from the filters
      predicateFuture: Future[Boolean],
  ) {

    /** Evaluates the current request data based on the result of the previous chained futures and the [[filter]].
      */
    def evaluateRequest(
        requestState: ConcurrentSkipListMap[CantonTimestamp, RequestState],
        ts: CantonTimestamp,
        filter: PendingRequestDataOrReplayData[T] => Future[Boolean],
    )(implicit
        ec: ExecutionContext
    ): Future[Boolean] =
      predicateFuture
        .flatMap { isValid =>
          // check that no previous promise has already returned a valid request
          if (isValid) {
            Future.successful(isValid)
          } else {
            filter(requestData)
              .flatMap {
                case true =>
                  requestState.put(ts, RequestState.ValidatedAndCompleted()).discard
                  Future.successful(true)
                case false =>
                  Future.successful(false)
              }
          }
        }

  }

  /** We use an optional [[RequestRelation]]`[T]` here to allow to differentiate between
    * a missing entry in the map and a None coming from a timed out request.
    */
  private def pendingRequestRelationFor[T <: PendingRequestData]
      : HMapRequestRelation[TCantonTimestamp[T], Option[RequestRelation[T]]] =
    new HMapRequestRelation[TCantonTimestamp[T], Option[RequestRelation[T]]]
}

private sealed trait RequestState

private object RequestState {

  /** Marks a given request as completed and valid.
    */
  case class ValidatedAndCompleted() extends RequestState

  /** Marks a given request as being confirmed but still not validated.
    */
  case class ConfirmedWithPendingValidation() extends RequestState

}
