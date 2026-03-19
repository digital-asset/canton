// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
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
    *   function that returns a position/offset from the element of type [[T]]
    * @param query
    *   a function that fetches results starting from provided offset
    * @tparam Off
    *   the type of the offset
    * @tparam T
    *   the type of the items returned in each call
    */
  def streamFromSeekPagination[Off, T](startFromOffset: Off, getOffset: T => Off)(
      query: Off => Future[Vector[T]]
  ): Source[T, NotUsed] =
    Source
      .unfoldAsync(Option(startFromOffset)) {
        case None =>
          Future.successful(None) // finished reading the whole thing
        case Some(offset) =>
          query(offset).map { result =>
            val nextPageOffset: Option[Off] = result.lastOption.map(getOffset)
            Some((nextPageOffset, result))
          }(directEc)
      }
      .flatMapConcat(Source(_))

  def streamIdsFromSeekPaginationWithoutIdFilter(
      idStreamName: String,
      idPageSizing: IdPageSizing,
      idPageBufferSize: Int,
      initialFromIdExclusive: Long,
      initialEndInclusive: Long,
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
          s"[$idStreamName] for next ID page returned: limit:${paginationInput.limit} from:${paginationInput.fromTo.fromExclusive} to:${paginationInput.fromTo.toInclusive}  #IDs:${result.ids.size}"
        )
    val initialFromTo = PaginationFromTo.of(
      startExclusive = initialFromIdExclusive,
      endInclusive = initialEndInclusive,
      descending = descendingOrder,
    )
    val initialState = IdPaginationState(
      fromIdExclusive = initialFromTo.fromExclusive,
      pageSize = idPageSizing.minPageSize,
      last = false,
    )
    Source
      .unfoldAsync[IdPaginationState, Vector[Long]](initialState) { state =>
        executeIdQuery(
          fetchPageQuery(
            PaginationInput(
              fromTo = initialFromTo.copy(
                fromExclusive = state.fromIdExclusive
              ),
              limit = state.pageSize,
            )
          )
        ).map(page =>
          page.ids.lastOption.map(last =>
            IdPaginationState(
              fromIdExclusive = last,
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
      initialFromIdExclusive: Long,
      initialEndInclusive: Long,
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
      initialFromIdExclusive = initialFromIdExclusive,
      initialEndInclusive = initialEndInclusive,
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
      initialFromIdExclusive: Long,
      initialEndInclusive: Long,
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
          s"[$idStreamName] for next ID page bounds returned: limit:${paginationInput.limit} from:${paginationInput.fromTo.fromExclusive} to:${result
              .map(_.fromTo.toInclusive)}"
        )
    def fetchPageQuery(
        paginationFromTo: PaginationFromTo
    ): Connection => Vector[Long] =
      c =>
        wrapIdDbQuery(
          in = paginationFromTo,
          f = fetchPageDbQuery.fetchPage(c),
        )(result =>
          s"[$idStreamName] for next ID page returned: from:${paginationFromTo.fromExclusive} to:${paginationFromTo.toInclusive} #IDs:${result.size}"
        )
    val initialFromTo = PaginationFromTo.of(
      startExclusive = initialFromIdExclusive,
      endInclusive = initialEndInclusive,
      descending = descendingOrder,
    )
    val initialState = IdPaginationState(
      fromIdExclusive = initialFromTo.fromExclusive,
      pageSize = idPageSizing.minPageSize,
      last = false,
    )
    Source
      .unfoldAsync[IdPaginationState, PaginationInput](initialState) { state =>
        if (state.last) Future.successful(None)
        else {
          val fromTo = initialFromTo.copy(
            fromExclusive = state.fromIdExclusive
          )
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
                fromIdExclusive = pageBounds.fromTo.toInclusive,
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

  final case class IdPaginationState(fromIdExclusive: Long, pageSize: Int, last: Boolean)

  /** Describes bounds for generating paginated stream. The stream can be either descending or
    * ascending.
    * @param fromExclusive
    *   a starting bound for the stream (a sequential id from which to look for a first element in a
    *   direction of stream order). In case of ascending stream it's a lower bound in case of the
    *   descending stream the upper bound of the range. In a descending stream [[fromExclusive]]
    *   must be greather than or equal to [[toInclusive]], in an ascending one, the inequality sign
    *   is flipped.
    */
  final case class PaginationFromTo(
      fromExclusive: Long,
      toInclusive: Long,
      descending: Boolean,
  )

  object PaginationFromTo {
    def ascending(
        startExclusive: Long,
        endInclusive: Long,
    ): PaginationFromTo = {
      assert(startExclusive <= endInclusive)
      PaginationFromTo(
        fromExclusive = startExclusive,
        toInclusive = endInclusive,
        descending = false,
      )
    }

    def descending(
        startExclusive: Long,
        endInclusive: Long,
    ): PaginationFromTo = {
      assert(startExclusive <= endInclusive)
      PaginationFromTo(
        fromExclusive = endInclusive + 1, // Adjust bounds to flip inclusive/exclusive meaning
        toInclusive = startExclusive + 1,
        descending = true,
      )
    }

    def of(
        startExclusive: Long,
        endInclusive: Long,
        descending: Boolean,
    ): PaginationFromTo =
      if (descending)
        PaginationFromTo.descending(startExclusive, endInclusive)
      else
        ascending(startExclusive, endInclusive)
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
