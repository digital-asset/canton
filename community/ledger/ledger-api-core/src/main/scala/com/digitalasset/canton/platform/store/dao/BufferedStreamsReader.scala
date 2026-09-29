// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.metrics.Timed
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.dao.events.OffsetRange
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

/** Generic class that helps serving Ledger API streams (e.g. transactions, completions) from either
  * the in-memory fan-out buffer or from persistence depending on the requested offset range.
  *
  * @param inMemoryFanoutBuffer
  *   The in-memory fan-out buffer.
  * @param fetchFromPersistence
  *   Fetch stream events from persistence.
  * @param bufferedStreamEventsProcessingParallelism
  *   The processing parallelism for buffered elements payloads to API responses.
  * @param metrics
  *   Daml metrics.
  * @param streamName
  *   The name of a Ledger API stream. Used as a discriminator in metric registry names
  *   construction.
  * @param executionContext
  *   The execution context
  * @tparam PersistenceFetchArgs
  *   The Ledger API streams filter type of fetches from persistence.
  * @tparam ApiResponse
  *   The API stream response type.
  */
class BufferedStreamsReader[PersistenceFetchArgs, ApiResponse](
    inMemoryFanoutBuffer: InMemoryFanoutBuffer,
    fetchFromPersistence: FetchFromPersistence[PersistenceFetchArgs, ApiResponse],
    bufferedStreamEventsProcessingParallelism: Int,
    metrics: LedgerApiServerMetrics,
    streamName: String,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)

  private val bufferReaderMetrics = metrics.services.index.BufferedReader(streamName)

  /** Serves processed and filtered events from the buffer, with fallback to persistence fetches if
    * the bounds are not within the buffer range bounds.
    *
    * @param offsetRange
    *   The inclusive offset range of the search.
    * @param persistenceFetchArgs
    *   The filter used for fetching the Ledger API stream responses from persistence.
    * @param bufferFilter
    *   The filter used for filtering when searching within the buffer.
    * @param toApiResponse
    *   To Ledger API stream response converter.
    * @param loggingContext
    *   The logging context.
    * @param descendingOrder
    *   If true then events will be streamed from the most recent ones to the oldest.
    * @param limit
    *   If Some(x), stream at most x elements, used by paging
    * @tparam BufferOut
    *   The output type of elements retrieved from the buffer.
    * @return
    *   The Ledger API stream source.
    */
  def stream[BufferOut](
      offsetRange: OffsetRange,
      persistenceFetchArgs: PersistenceFetchArgs,
      bufferFilter: TransactionLogUpdate => Option[BufferOut],
      toApiResponse: BufferOut => Future[ApiResponse],
      descendingOrder: Boolean,
      skipPruningChecks: Boolean,
      limit: Option[Int],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, ApiResponse), NotUsed] = {
    def continue(
        offsetRange: OffsetRange,
        limit: Option[Int],
    ): Source[(Offset, ApiResponse), NotUsed] =
      Source
        .futureSource(Future {
          val bufferSlice = Timed.value(
            bufferReaderMetrics.slice,
            inMemoryFanoutBuffer.slice(
              range = offsetRange,
              filter = bufferFilter,
              limit = limit,
              reverseOrder = descendingOrder,
            ),
          )
          val (fromPersistenceBeforeImfo, remaining) = bufferSlice match {
            case Some(slice) =>
              bufferReaderMetrics.sliceSize.update(slice.fromImfo.size)(MetricsContext.Empty)
              val rangeBeforeSlice = offsetRange.before(slice.offsetRange)
              val rangeAfterSlice = offsetRange.after(slice.offsetRange)
              // The result from this function requires that the three offset ranges to follow each other in emission order
              // therefore for descending order we have to swap the ranges.
              if (descendingOrder) rangeAfterSlice -> rangeBeforeSlice
              else rangeBeforeSlice -> rangeAfterSlice

            case None =>
              Some(offsetRange) -> None
          }
          def fromPersistenceBeforeImfoSource(limit: Option[Int]) =
            fromPersistenceBeforeImfo
              .map(range =>
                fetchFromPersistence(
                  offsetRange = range,
                  filter = persistenceFetchArgs,
                  descendingOrder = descendingOrder,
                  skipPruningChecks = skipPruningChecks,
                  limit = limit,
                )
              )
              .getOrElse(Source.empty)
          def continuationSource(limit: Option[Int]) =
            remaining
              .map(continue(_, limit))
              .getOrElse(Source.empty)
          concatLimitedSource(
            fromPersistenceBeforeImfoSource,
            concatLimitedSource(
              bufferResponseStream(
                slice = bufferSlice.map(_.fromImfo).getOrElse(Vector.empty),
                toApiResponse = toApiResponse,
              ),
              continuationSource,
            ),
          )(limit)
        })
        .mapMaterializedValue(_ => NotUsed)

    Timed
      .source(bufferReaderMetrics.fetchTimer, continue(offsetRange, limit))
      .map { tx =>
        bufferReaderMetrics.fetchedTotal.inc()
        tx
      }
  }

  private def bufferResponseStream[BufferOut](
      slice: Vector[(Offset, BufferOut)],
      toApiResponse: BufferOut => Future[ApiResponse],
  )(limit: Option[Int]): Source[(Offset, ApiResponse), NotUsed] =
    if (slice.isEmpty) Source.empty
    else
      Source(limit match {
        case Some(limit) => slice.take(limit)
        case None => slice
      })
        .mapAsync(bufferedStreamEventsProcessingParallelism) { case (offset, payload) =>
          bufferReaderMetrics.fetchedBuffered.inc()
          Timed.future(
            bufferReaderMetrics.conversion,
            Future.delegate {
              toApiResponse(payload).map(offset -> _)(directEc)
            },
          )
        }

  private type LimitedSource = Option[Int] => Source[(Offset, ApiResponse), NotUsed]
  private def concatLimitedSource(
      a: LimitedSource,
      b: LimitedSource,
  ): LimitedSource = {
    case Some(limit) =>
      a(Some(limit)).foldConcat(0)((count, _) => count + 1) { count =>
        val newLimit = limit - count
        if (newLimit > 0) b(Some(newLimit))
        else Source.empty
      }

    case None =>
      a(None).concatLazy(b(None))
  }
}

private[platform] object BufferedStreamsReader {
  trait FetchFromPersistence[FILTER, ApiResponse] {
    def apply(
        offsetRange: OffsetRange,
        descendingOrder: Boolean,
        filter: FILTER,
        skipPruningChecks: Boolean,
        limit: Option[Int],
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[(Offset, ApiResponse), NotUsed]
  }
}
