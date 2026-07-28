// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.EventSeqIdRange
import com.digitalasset.canton.platform.store.dao.events.IdPageSizing
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.OverflowStrategy
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.Future

private[platform] class PaginatingAsyncStream(
    override protected val loggerFactory: NamedLoggerFactory
) extends NamedLogging {

  import PaginatingAsyncStream.*

  private val directEc = DirectExecutionContext(noTracingLogger)

  /** Concatenates the results of multiple asynchronous calls into a single [[Source]], passing the
    * last seen event's offset to the next iteration query, so it can continue reading events from
    * this point.
    *
    * This is to implement pagination based on generic offset. The main purpose of the pagination is
    * to break down large queries into smaller batches. The reason for this is that we are currently
    * using simple blocking JDBC APIs and a long-running stream would end up occupying a thread in
    * the DB pool, severely limiting the ability of keeping multiple, concurrent, long-running
    * streams while serving lookup calls.
    *
    * @param startFromOffset
    *   initial offset
    * @param getOffset
    *   function that returns the offset to continue from given the last element of a page, or
    *   `None` when the stream should terminate (e.g. the page ended at the requested upper bound)
    * @param query
    *   a function that fetches results starting from provided offset
    * @tparam Off
    *   the type of the offset
    * @tparam T
    *   the type of the items returned in each call
    */
  def streamFromSeekPagination[Off, T](startFromOffset: Off, getOffset: T => Option[Off])(
      query: Off => Future[Vector[T]]
  ): Source[T, NotUsed] =
    Source
      .unfoldAsync(Option(startFromOffset)) {
        case None =>
          Future.successful(None) // finished reading the whole thing
        case Some(offset) =>
          query(offset).map { result =>
            val nextPageOffset: Option[Off] = result.lastOption.flatMap(getOffset)
            Some((nextPageOffset, result))
          }(directEc)
      }
      .flatMapConcat(Source(_))

  def streamIdsFromSeekPaginationWithoutIdFilter(
      idStreamName: String,
      idPageSizing: IdPageSizing,
      idPageBufferSize: Int,
      initialEventSeqIdRange: EventSeqIdRange,
      descendingOrder: Boolean,
  )(
      fetchPageDbQuery: IdPageQuery
  )(
      executeIdQuery: (Connection => IdPage) => Future[IdPage]
  )(implicit
      traceContext: TraceContext
  ): Source[Long, NotUsed] = {
    assert(idPageBufferSize > 0)
    def fetchPageQuery(paginationInput: PaginationInput): Connection => IdPage =
      c =>
        wrapIdDbQuery(
          in = paginationInput,
          f = fetchPageDbQuery.fetchPage(c),
        )(result =>
          s"[$idStreamName] for next ID page returned: limit:${paginationInput.limit} range:${paginationInput.fromTo.eventSeqIdRange}  #IDs:${result.ids.size}"
        )
    val initialFromTo = PaginationFromTo.of(
      eventSeqIdRange = initialEventSeqIdRange,
      descending = descendingOrder,
    )
    val initialState = IdPaginationState(
      fromIdInclusive = initialFromTo.eventSeqIdRange.startInclusive,
      pageSize = idPageSizing.minPageSize,
      last = false,
    )
    Source
      .unfoldAsync[IdPaginationState, Vector[Long]](initialState) { state =>
        if (state.last) Future.successful(None)
        else
          executeIdQuery(
            fetchPageQuery(
              PaginationInput(
                fromTo = initialFromTo.withStartInclusive(state.fromIdInclusive),
                limit = state.pageSize,
              )
            )
          ).map(page =>
            page.ids.lastOption.map(last =>
              IdPaginationState(
                fromIdInclusive = nextFromInclusive(last, descendingOrder),
                pageSize = Math.min(state.pageSize * 4, idPageSizing.maxPageSize),
                last = page.lastPage,
              ) -> page.ids
            )
          )(directEc)
      }
      .buffer(idPageBufferSize, OverflowStrategy.backpressure)
      .mapConcat(identity)
  }

  def streamIdsFromSeekPaginationWithIdFilter(
      idStreamName: String,
      idPageSizing: IdPageSizing,
      idPageBufferSize: Int,
      initialEventSeqIdRange: EventSeqIdRange,
      descendingOrder: Boolean,
  )(
      fetchPageDbQuery: IdFilterPageQuery
  )(
      executeFetchBounds: (Connection => Option[IdPageBounds]) => Future[Option[IdPageBounds]],
      idFilterQueryParallelism: Int,
      executeFetchPage: (Connection => Vector[Long]) => Future[Vector[Long]],
  )(implicit
      traceContext: TraceContext
  ): Source[Long, NotUsed] = {
    assert(idPageBufferSize > 0)
    streamIdPagesFromSeekPaginationWithIdFilter(
      idStreamName = idStreamName,
      idPageSizing = idPageSizing,
      initialEventSeqIdRange = initialEventSeqIdRange,
      descendingOrder = descendingOrder,
    )(fetchPageDbQuery)(
      executeFetchBounds = executeFetchBounds,
      idFilterQueryParallelism = idFilterQueryParallelism,
      executeFetchPage = executeFetchPage,
    )
      .buffer(idPageBufferSize, OverflowStrategy.backpressure)
      .mapConcat(_._2)
  }

  def streamIdPagesFromSeekPaginationWithIdFilter(
      idStreamName: String,
      idPageSizing: IdPageSizing,
      initialEventSeqIdRange: EventSeqIdRange,
      descendingOrder: Boolean,
  )(
      fetchPageDbQuery: IdFilterPageQuery
  )(
      executeFetchBounds: (Connection => Option[IdPageBounds]) => Future[Option[IdPageBounds]],
      idFilterQueryParallelism: Int,
      executeFetchPage: (Connection => Vector[Long]) => Future[Vector[Long]],
  )(implicit
      traceContext: TraceContext
  ): Source[(PaginationInput, Vector[Long]), NotUsed] = {
    def fetchBoundsQuery(
        paginationInput: PaginationInput
    ): Connection => Option[IdPageBounds] =
      c =>
        wrapIdDbQuery(
          in = paginationInput,
          f = fetchPageDbQuery.fetchPageBounds(c),
        )(result =>
          s"[$idStreamName] for next ID page bounds returned: limit:${paginationInput.limit} from:${paginationInput.fromTo.eventSeqIdRange.startInclusive} to:${result
              .map(_.fromTo.eventSeqIdRange.endInclusive)}"
        )
    def fetchPageQuery(
        paginationFromTo: PaginationFromTo
    ): Connection => Vector[Long] =
      c =>
        wrapIdDbQuery(
          in = paginationFromTo,
          f = fetchPageDbQuery.fetchPage(c),
        )(result =>
          s"[$idStreamName] for next ID page returned: ${paginationFromTo.eventSeqIdRange} #IDs:${result.size}"
        )
    val initialFromTo = PaginationFromTo.of(
      eventSeqIdRange = initialEventSeqIdRange,
      descending = descendingOrder,
    )
    val initialState = IdPaginationState(
      fromIdInclusive = initialFromTo.eventSeqIdRange.startInclusive,
      pageSize = idPageSizing.minPageSize,
      last = false,
    )
    Source
      .unfoldAsync[IdPaginationState, PaginationInput](initialState) { state =>
        if (state.last) Future.successful(None)
        else {
          val fromTo = initialFromTo.withStartInclusive(state.fromIdInclusive)
          executeFetchBounds(
            fetchBoundsQuery(
              PaginationInput(
                fromTo = fromTo,
                limit = state.pageSize,
              )
            )
          ).map(
            _.map(pageBounds =>
              IdPaginationState(
                fromIdInclusive = nextFromInclusive(
                  pageBounds.fromTo.eventSeqIdRange.endInclusive,
                  descendingOrder,
                ),
                pageSize = Math.min(state.pageSize * 4, idPageSizing.maxPageSize),
                last = pageBounds.lastPage,
              ) -> PaginationInput(
                fromTo = pageBounds.fromTo,
                limit = state.pageSize,
              )
            )
          )(directEc)
        }
      }
      .mapAsync(idFilterQueryParallelism)(paginationInput =>
        executeFetchPage(
          fetchPageQuery(paginationInput.fromTo)
        ).map(paginationInput -> _)(directEc)
      )
  }

  def wrapIdDbQuery[In, Out](
      in: In,
      f: In => Out,
  )(
      log: Out => String
  )(implicit traceContext: TraceContext): Out = {
    val started = System.nanoTime()
    val result = f(in)
    def elapsedMillis: Long = (System.nanoTime() - started) / 1000000
    logger.debug(
      s"ID query for ${log(result)} DB query took: ${elapsedMillis}ms"
    )
    result
  }
}

object PaginatingAsyncStream {

  final case class IdPaginationState(fromIdInclusive: Long, pageSize: Int, last: Boolean)

  private def nextFromInclusive(lastConsumedInclusive: Long, descending: Boolean): Long =
    if (descending) lastConsumedInclusive - 1 else lastConsumedInclusive + 1

  /** Describes the bounds for generating a paginated stream. The stream can be either ascending or
    * descending.
    *
    * @param eventSeqIdRange
    *   the event sequential id range to stream, oriented in the direction of traversal:
    *   `startInclusive` is always the first id to read and `endInclusive` the last. For an
    *   ascending stream `startInclusive <= endInclusive`; for a descending stream the range is
    *   flipped, so `startInclusive >= endInclusive`.
    * @param descending
    *   whether the stream is traversed from the highest to the lowest event sequential id.
    */
  final case class PaginationFromTo(
      eventSeqIdRange: EventSeqIdRange,
      descending: Boolean,
  ) {

    def withStartInclusive(id: Long): PaginationFromTo =
      copy(eventSeqIdRange = eventSeqIdRange.copy(startInclusive = id))

    def withEndInclusive(id: Long): PaginationFromTo =
      copy(eventSeqIdRange = eventSeqIdRange.copy(endInclusive = id))
  }

  object PaginationFromTo {
    def ascending(
        eventSeqIdRange: EventSeqIdRange
    ): PaginationFromTo = {
      assert(eventSeqIdRange.startInclusive <= eventSeqIdRange.endInclusive)
      PaginationFromTo(
        eventSeqIdRange = eventSeqIdRange,
        descending = false,
      )
    }

    def descending(
        eventSeqIdRange: EventSeqIdRange
    ): PaginationFromTo = {
      assert(eventSeqIdRange.startInclusive <= eventSeqIdRange.endInclusive)
      PaginationFromTo(
        eventSeqIdRange.flipped,
        descending = true,
      )
    }

    def of(
        eventSeqIdRange: EventSeqIdRange,
        descending: Boolean,
    ): PaginationFromTo =
      if (descending)
        PaginationFromTo.descending(eventSeqIdRange)
      else
        ascending(eventSeqIdRange)
  }

  trait IdFilterPageQuery {
    def fetchPageBounds(connection: Connection)(input: PaginationInput): Option[IdPageBounds]
    def fetchPage(connection: Connection)(fromTo: PaginationFromTo): Vector[Long]
  }

  final case class IdPageBounds(
      fromTo: PaginationFromTo,
      lastPage: Boolean,
  )

  trait IdPageQuery {
    def fetchPage(connection: Connection)(input: PaginationInput): IdPage
  }

  final case class IdPage(
      ids: Vector[Long],
      lastPage: Boolean,
  )

  final case class PaginationInput(
      fromTo: PaginationFromTo,
      limit: Int,
  )
}
