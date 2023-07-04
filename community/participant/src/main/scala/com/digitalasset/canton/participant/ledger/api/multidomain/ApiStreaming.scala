// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api.multidomain

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.util.DelayUtil

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining.scalaUtilChainingOps

class ApiStreaming(
    override protected val loggerFactory: NamedLoggerFactory
)(implicit
    executionContext: ExecutionContext
) extends NamedLogging {

  import ApiStreaming.*

  private val directEc = DirectExecutionContext(loggerFactory.getTracedLogger(getClass))

  // Uses an asynchronous polling callback to populate a distinct stream of new indexes.
  // If polling leads to the same result, it applies the specified back-off.
  // On backpressure no polling.
  // TODO(#11002) This per client stream polling approach meant for PoC purposes. If for some reason this should go to production: ensure that this is safe with graceful-shutdown.
  def asyncIndexPoller[T](
      minDelayForSameIndex: FiniteDuration = FiniteDuration(25, "millis"),
      maxDelayForSameIndex: FiniteDuration = FiniteDuration(1000, "millis"),
      increaseDelayForSameIndex: FiniteDuration => FiniteDuration = _ * 2, // exponential back-off
  )(
      poll: () => Future[T]
  ): Source[T, NotUsed] = {
    val streamTerminated = new AtomicBoolean(false)
    def pollUntilNextDistinctElem(
        previous: Option[T],
        delay: FiniteDuration,
    ): Future[T] = {
      if (streamTerminated.get())
        Future.failed(new RuntimeException("Polling should stop after stream completes."))
      else
        poll()
          .flatMap(next =>
            if (previous.contains(next)) {
              DelayUtil
                .delay(delay)
                .flatMap(_ =>
                  pollUntilNextDistinctElem(
                    previous = previous,
                    delay = increaseDelayForSameIndex(delay).min(maxDelayForSameIndex),
                  )
                )
            } else
              Future.successful(next)
          )
    }

    Source
      .unfoldAsync(None: Option[T])(previous =>
        pollUntilNextDistinctElem(
          previous = previous,
          delay = minDelayForSameIndex,
        ).map(next => Some(Some(next) -> next))
      )
      .watchTermination() { (_, completionFuture) =>
        completionFuture.onComplete(_ => streamTerminated.set(true))
        NotUsed
      }
  }

  /** Renders a stream of inclusive index from-to pairs, which are mutually exclusive, and covering the
    * complete range of (fromInclusive, toInclusive).
    * The stream starts only after the indexSource passes by fromInclusive.
    *
    * @param indexRange the inclusive complete range.
    * @param indexSource Source of strictly monotonically increasing indexes
    * @return a sequence of ranges following precisely each other monotonically. The first range will start with
    *         fromInclusive and the last range will end with toInclusive.
    */
  def tailingInclusiveIndexRanges[T](
      indexRange: IndexRange
  )(indexSource: Source[Long, T]): Source[IndexRange, T] =
    indexSource
      .takeWhile(_ < indexRange.toInclusive, inclusive = true)
      .map(_.min(indexRange.toInclusive))
      .statefulMapConcat { () =>
        val from = new AtomicLong(indexRange.fromInclusive)
        newIndex =>
          if (from.get > newIndex) Nil
          else {
            List(IndexRange(from.get, newIndex))
              .tap(_ => from.set(newIndex + 1))
          }
      }

  /** Queries a full range by issuing subsequent limited queries.
    *
    * @param rangeQueryWithLimit Function to issue a range query with limit. Results are expected to be returned in index order.
    * @param indexExtractor to get back the index of one result.
    * @param indexRange the complete range.
    * @return Results for the complete range in index order.
    */
  def pagedRangeQueryStream[T](
      rangeQueryWithLimit: IndexRange => Future[Vector[T]]
  )(indexExtractor: T => Long)(indexRange: IndexRange): Source[T, NotUsed] =
    Source
      .unfoldAsync(indexRange.fromInclusive)(fromInclusive =>
        if (indexRange.toInclusive < fromInclusive) Future.successful(None)
        else
          rangeQueryWithLimit(IndexRange(fromInclusive, indexRange.toInclusive))
            .map(results =>
              results.lastOption
                .map(last => indexExtractor(last) + 1 -> results)
            )(directEc)
      )
      .mapConcat(identity)
}

object ApiStreaming {

  final case class IndexRange(fromInclusive: Long, toInclusive: Long)
}
