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
import com.digitalasset.canton.platform.store.cache.InMemoryFanoutBuffer.{
  BackwardBufferSlice,
  BufferSlice,
}
import com.digitalasset.canton.platform.store.dao.BufferedStreamsReader.FetchFromPersistence
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
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
    * @param startInclusive
    *   The start inclusive offset of the search range.
    * @param endInclusive
    *   The end inclusive offset of the search range.
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
    * @tparam BufferOut
    *   The output type of elements retrieved from the buffer.
    * @return
    *   The Ledger API stream source.
    */
  def stream[BufferOut](
      startInclusive: Offset,
      endInclusive: Offset,
      persistenceFetchArgs: PersistenceFetchArgs,
      bufferFilter: TransactionLogUpdate => Option[BufferOut],
      toApiResponse: BufferOut => Future[ApiResponse],
      descendingOrder: Boolean,
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

    val source = if (descendingOrder) {
      Source
        .unfoldAsync(endInclusive.some) {
          case Some(end) if startInclusive <= end =>
            Future {
              val bufferSlice = Timed.value(
                bufferReaderMetrics.slice,
                inMemoryFanoutBuffer.sliceBackwards(
                  startInclusive = startInclusive,
                  endInclusive = end,
                  filter = bufferFilter,
                ),
              )

              Some(bufferSlice match {
                case BackwardBufferSlice.FinalSlice(slice) =>
                  (None, toApiResponseStream(slice))
                case BackwardBufferSlice.PartialSlice(slice @ _ :+ last) =>
                  (last._1.decrement, toApiResponseStream(slice))
                case BackwardBufferSlice.PartialSlice(_) => // empty vector
                  (
                    None,
                    fetchFromPersistence(
                      startInclusive = startInclusive,
                      endInclusive = end,
                      filter = persistenceFetchArgs,
                      descendingOrder = true,
                    ),
                  )
              })
            }
          case _ => Future.successful(None)
        }
        .flatten
    } else {
      Source
        .unfoldAsync(startInclusive) {
          case scanFrom if scanFrom <= endInclusive =>
            Future {
              val bufferSlice = Timed.value(
                bufferReaderMetrics.slice,
                inMemoryFanoutBuffer.slice(
                  startInclusive = scanFrom,
                  endInclusive = endInclusive,
                  filter = bufferFilter,
                ),
              )

              bufferReaderMetrics.sliceSize.update(bufferSlice.slice.size)(MetricsContext.Empty)
              bufferSlice match {
                case BufferSlice.Inclusive(slice) =>
                  val apiResponseSource = toApiResponseStream(slice)
                  val nextSliceStart =
                    slice.lastOption.map(_._1).getOrElse(endInclusive).increment
                  Some(nextSliceStart -> apiResponseSource)

                case BufferSlice.LastBufferChunkSuffix(bufferedStartExclusive, slice) =>
                  val sourceFromBuffer =
                    fetchFromPersistence(
                      startInclusive = scanFrom,
                      endInclusive = bufferedStartExclusive,
                      filter = persistenceFetchArgs,
                      descendingOrder = false,
                    )(loggingContext)
                      .concat(toApiResponseStream(slice))
                  Some(endInclusive.increment -> sourceFromBuffer)
              }
            }
          case _ => Future.successful(None)
        }
        .flatMapConcat(identity)
    }

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
        startInclusive: Offset,
        endInclusive: Offset,
        descendingOrder: Boolean,
        filter: FILTER,
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[(Offset, ApiResponse), NotUsed]
  }
}
