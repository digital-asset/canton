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
      fetchPageDbQuery: Connection => PaginationInput => Vector[Long]
  )(
      executeIdQuery: (Connection => Vector[Long]) => Future[Vector[Long]]
  )(implicit
      traceContext: TraceContext
  ): Source[Long, NotUsed] = {
    assert(idPageBufferSize > 0)
    def wrapIdDbQuery(paginationInput: PaginationInput): Connection => Vector[Long] = { c =>
      val started = System.nanoTime()
      val result = fetchPageDbQuery(c)(paginationInput)
      def elapsedMillis: Long = (System.nanoTime() - started) / 1000000
      logger.debug(
        s"ID query for $idStreamName for IDs returned: limit:${paginationInput.limit} from:${paginationInput.paginationFromTo.fromExclusive} ${paginationInput.paginationFromTo.directionAsString} #IDs:${result.size} lastID:${result.lastOption} DB query took: ${elapsedMillis}ms"
      )
      result
    }
    val initialFromTo = PaginationFromTo.of(
      startExclusive = initialFromIdExclusive,
      endInclusive = initialEndInclusive,
      descending = descendingOrder,
    )
    val initialState = IdPaginationState(
      fromIdExclusive = initialFromTo.fromExclusive,
      pageSize = idPageSizing.minPageSize,
    )
    Source
      .unfoldAsync[IdPaginationState, Vector[Long]](initialState) { state =>
        executeIdQuery(
          wrapIdDbQuery(
            PaginationInput(
              paginationFromTo = initialFromTo.copy(
                fromExclusive = state.fromIdExclusive
              ),
              limit = state.pageSize,
            )
          )
        ).map { ids =>
          ids.lastOption.map { last =>
            val nextState = IdPaginationState(
              fromIdExclusive = last,
              pageSize = Math.min(state.pageSize * 4, idPageSizing.maxPageSize),
            )
            nextState -> ids
          }
        }(directEc)
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
      fetchPageDbQuery: Connection => IdFilterPaginationInput => Vector[Long]
  )(
      executeLastIdQuery: (Connection => Vector[Long]) => Future[Vector[Long]],
      idFilterQueryParallelism: Int,
      executeIdFilterQuery: (Connection => Vector[Long]) => Future[Vector[Long]],
  )(implicit
      traceContext: TraceContext
  ): Source[Long, NotUsed] = {
    assert(idPageBufferSize > 0)
    def wrapIdDbQuery(
        idFilterPaginationInput: IdFilterPaginationInput
    )(debugLogMiddle: Vector[Long] => String): Connection => Vector[Long] = { c =>
      val started = System.nanoTime()
      val result = fetchPageDbQuery(c)(idFilterPaginationInput)
      def elapsedMillis: Long = (System.nanoTime() - started) / 1000000
      logger.debug(
        s"ID query for $idStreamName ${debugLogMiddle(result)} DB query took: ${elapsedMillis}ms"
      )
      result
    }
    def lastIdDbQuery(
        paginationLastOnlyInput: PaginationLastOnlyInput
    ): Connection => Vector[Long] =
      wrapIdDbQuery(paginationLastOnlyInput)(result =>
        s"for next ID window returned: limit:${paginationLastOnlyInput.limit} from:${paginationLastOnlyInput.paginationFromTo.fromExclusive} ${paginationLastOnlyInput.paginationFromTo.directionAsString} to:$result"
      )
    def idFilterDbQuery(idFilterInput: IdFilterInput): Connection => Vector[Long] =
      wrapIdDbQuery(idFilterInput)(result =>
        s"for filtered IDs returned: from:${idFilterInput.paginationFromTo.fromExclusive} to:${idFilterInput.paginationFromTo.toInclusive} ${idFilterInput.paginationFromTo.directionAsString} #IDs:${result.size}"
      )
    val initialFromTo = PaginationFromTo.of(
      startExclusive = initialFromIdExclusive,
      endInclusive = initialEndInclusive,
      descending = descendingOrder,
    )
    val initialState = IdPaginationState(
      fromIdExclusive = initialFromTo.fromExclusive,
      pageSize = idPageSizing.minPageSize,
    )
    Source
      .unfoldAsync[IdPaginationState, PaginationFromTo](initialState) { state =>
        val fromTo = initialFromTo.copy(
          fromExclusive = state.fromIdExclusive
        )
        executeLastIdQuery(
          lastIdDbQuery(
            PaginationLastOnlyInput(
              paginationFromTo = fromTo,
              limit = state.pageSize,
            )
          )
        ).map { ids =>
          ids.lastOption.map { last =>
            val nextState = IdPaginationState(
              fromIdExclusive = last,
              pageSize = Math.min(state.pageSize * 4, idPageSizing.maxPageSize),
            )
            nextState -> fromTo.copy(
              toInclusive = last
            )
          }
        }(directEc)
      }
      .mapAsync(idFilterQueryParallelism)(paginationFromTo =>
        executeIdFilterQuery(
          idFilterDbQuery(
            IdFilterInput(paginationFromTo)
          )
        )
      )
      .buffer(idPageBufferSize, OverflowStrategy.backpressure)
      .mapConcat(identity)
  }
}

object PaginatingAsyncStream {

  final case class IdPaginationState(fromIdExclusive: Long, pageSize: Int)

  /** Describes bounds for generating paginated stream. The stream can be either descending or
    * ascending.
    * @param fromExclusive
    *   a starting bound for the stream (a sequential id from which to look for a first element in a
    *   direction of stream order). In case of ascending stream it's a lower bound in case of the
    *   descending stream the upper bound of the range. In a descending stream [[fromExclusive]]
    *   must be greather than or equal to [[toInclusive]], in an ascending one, the inequality sign
    *   is flipped.
    * @param toInclusive
    * @param descending
    */
  final case class PaginationFromTo(
      fromExclusive: Long,
      toInclusive: Long,
      descending: Boolean,
  ) {

    /** Return either "ASC" or "DESC" depending on [[descending]] value. Intended to be used in
      * logging.
      */
    def directionAsString: String = if (descending) "DESC" else "ASC"
  }

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

  sealed trait IdFilterPaginationInput {
    val paginationFromTo: PaginationFromTo
  }

  final case class PaginationInput(
      paginationFromTo: PaginationFromTo,
      limit: Int,
  ) extends IdFilterPaginationInput

  final case class IdFilterInput(
      paginationFromTo: PaginationFromTo
  ) extends IdFilterPaginationInput

  final case class PaginationLastOnlyInput(
      paginationFromTo: PaginationFromTo,
      limit: Int,
  ) extends IdFilterPaginationInput
}
