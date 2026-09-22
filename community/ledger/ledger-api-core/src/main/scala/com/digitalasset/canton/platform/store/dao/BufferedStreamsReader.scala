// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import cats.syntax.option.*
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
    def toApiResponseStream(
        slice: Vector[(Offset, BufferOut)]
    ): Source[(Offset, ApiResponse), NotUsed] =
      if (slice.isEmpty) Source.empty
      else
        Source(slice)
          .mapAsync(bufferedStreamEventsProcessingParallelism) { case (offset, payload) =>
            bufferReaderMetrics.fetchedBuffered.inc()
            Timed.future(
              bufferReaderMetrics.conversion,
              Future.delegate {
                toApiResponse(payload).map(offset -> _)(directEc)
              },
            )
          }

    val source = Source
      .unfoldAsync(offsetRange.some) {
        case Some(currentRange) =>
          Future {
            val bufferSlice = Timed.value(
              bufferReaderMetrics.slice,
              inMemoryFanoutBuffer.slice(
                range = currentRange,
                filter = bufferFilter,
                limit = limit,
                reverseOrder = descendingOrder,
              ),
            )
            bufferSlice match {
              case None =>
                Some(
                  (
                    None,
                    fetchFromPersistence(
                      offsetRange = currentRange,
                      filter = persistenceFetchArgs,
                      descendingOrder = descendingOrder,
                      skipPruningChecks = skipPruningChecks,
                      limit = limit,
                    ),
                  )
                )
              case Some(slice) =>
                bufferReaderMetrics.sliceSize.update(slice.fromImfo.size)(MetricsContext.Empty)

                if (descendingOrder) {
                  limit match {
                    case Some(l) =>
                      val remainingRange = currentRange.before(slice.offsetRange)
                      Some(
                        (
                          None,
                          remainingRange match {
                            case Some(persistenceRange) if slice.fromImfo.sizeIs < l =>
                              toApiResponseStream(slice.fromImfo).concat(
                                fetchFromPersistence(
                                  offsetRange = persistenceRange,
                                  filter = persistenceFetchArgs,
                                  descendingOrder = descendingOrder,
                                  skipPruningChecks = skipPruningChecks,
                                  limit = Some(l - slice.fromImfo.size),
                                )
                              )
                            case _ => toApiResponseStream(slice.fromImfo)
                          },
                        )
                      )
                    case _ =>
                      Some(
                        (
                          currentRange.before(slice.offsetRange),
                          toApiResponseStream(slice.fromImfo),
                        )
                      )
                  }
                } else {
                  val sourceFromPersistence = currentRange.before(slice.offsetRange) match {
                    case None =>
                      Source.empty
                    case Some(persistenceRange) =>
                      fetchFromPersistence(
                        offsetRange = persistenceRange,
                        filter = persistenceFetchArgs,
                        descendingOrder = descendingOrder,
                        skipPruningChecks = skipPruningChecks,
                        limit = limit,
                      )
                  }

                  limit match {
                    case None =>
                      Some(
                        (
                          currentRange.after(slice.offsetRange),
                          sourceFromPersistence.concat(toApiResponseStream(slice.fromImfo)),
                        )
                      )
                    case Some(l) =>
                      Some(
                        (
                          None,
                          sourceFromPersistence.foldConcat(0)((count, _) => count + 1)(count =>
                            toApiResponseStream(slice.fromImfo.take(l - count))
                          ),
                        )
                      )
                  }
                }
            }
          }
        case _ => Future.successful(None)
      }
      .flatten

    Timed
      .source(bufferReaderMetrics.fetchTimer, source)
      .map { tx =>
        bufferReaderMetrics.fetchedTotal.inc()
        tx
      }
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
