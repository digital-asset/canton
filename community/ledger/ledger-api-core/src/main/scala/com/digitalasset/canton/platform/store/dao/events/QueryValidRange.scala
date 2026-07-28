// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.platform.store.PruningOffsetService
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*

import scala.concurrent.{ExecutionContext, Future}

trait QueryValidRange {
  def withRangeNotPruned[T](
      offsetRange: OffsetRange,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => Future[T])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[T]

  def withOffsetNotBeforePruning[T](
      offset: Offset,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => Future[T])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[T]

  def filterPrunedEvents[T](offset: T => Offset)(
      events: Vector[T]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
  ): Future[Vector[T]]
}

final case class QueryValidRangeImpl(
    ledgerEndCache: LedgerEndCache,
    pruningOffsetService: PruningOffsetService,
    loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends QueryValidRange
    with NamedLogging {

  /** Runs a query and throws an error if the query accesses an invalid offset range.
    *
    * @param query
    *   query to execute
    * @param offsetRange
    *   the inclusive offset range used by the query (i.e. all fetched offsets are within it)
    * @param errorPruning
    *   function that generates a context-specific error parameterized by participant pruning offset
    * @param errorLedgerEnd
    *   function that generates a context-specific error parameterized by ledger end offset
    * @tparam T
    *   type of result passed through
    * @return
    *   either an Error if offset range violates conditions or query result
    *
    * Note in order to prevent race condition on connections at READ_COMMITTED isolation levels (in
    * fact any level below SNAPSHOT isolation level), this check must be performed after fetching
    * the corresponding range of data. This way we avoid a race between pruning and the query
    * reading the offsets in which offsets are "silently skipped". First fetching the objects and
    * only afterwards checking that no pruning operation has interfered, avoids such a race
    * condition.
    */
  override def withRangeNotPruned[T](
      offsetRange: OffsetRange,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => Future[T])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[T] = {
    assert(offsetRange.endInclusive >= offsetRange.startInclusive)
    withBoundsNotViolated(
      upperBoundInclusive = offsetRange.endInclusive,
      prunedOffsetCheck = prunedOffset => offsetRange.startInclusive > prunedOffset,
      errorPruning = errorPruning,
      errorLedgerEnd = errorLedgerEnd,
    )(query)
  }

  override def withOffsetNotBeforePruning[T](
      offset: Offset,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => Future[T])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[T] =
    withBoundsNotViolated(
      upperBoundInclusive = offset,
      prunedOffsetCheck = prunedOffset => offset >= prunedOffset,
      errorPruning = errorPruning,
      errorLedgerEnd = errorLedgerEnd,
    )(query)

  /** Rejects if `upperBoundInclusive` is beyond the ledger end, otherwise runs `query` and, only
    * afterwards (to avoid a race with a concurrent pruning operation), rejects if the pruning
    * offset does not satisfy `prunedOffsetCheck`.
    *
    * @param upperBoundInclusive
    *   the highest offset the query may access; must not be beyond the ledger end
    * @param prunedOffsetCheck
    *   given the participant pruning offset, whether the query did not access pruned data
    */
  private def withBoundsNotViolated[T](
      upperBoundInclusive: Offset,
      prunedOffsetCheck: Offset => Boolean,
      errorPruning: Offset => String,
      errorLedgerEnd: Option[Offset] => String,
  )(query: => Future[T])(implicit loggingContext: LoggingContextWithTrace): Future[T] = {
    val ledgerEnd = ledgerEndCache().map(_.lastOffset)
    if (Option(upperBoundInclusive) > ledgerEnd)
      Future.failed(
        RequestValidationErrors.ParticipantDataAccessedAfterLedgerEnd
          .Reject(
            cause = errorLedgerEnd(ledgerEnd),
            latestOffset = ledgerEnd.fold(0L)(_.unwrap),
          )(
            ErrorLoggingContext(logger, loggingContext)
          )
          .asGrpcError
      )
    else
      query.thereafterF(_ =>
        pruningOffsetService.pruningOffset
          .map(pruningOffsetO =>
            pruningOffsetO
              .filterNot(prunedOffsetCheck)
              .foreach(pruningOffsetUpToInclusive =>
                throw RequestValidationErrors.ParticipantPrunedDataAccessed
                  .Reject(
                    cause = errorPruning(pruningOffsetUpToInclusive),
                    earliestOffset = pruningOffsetUpToInclusive.unwrap,
                  )(
                    ErrorLoggingContext(logger, loggingContext)
                  )
                  .asGrpcError
              )
          )
      )
  }

  /** Filters out events that are at or below the participant's pruning offset.
    *
    * @param offset
    *   function to extract the offset from an event
    * @param events
    *   the events to filter
    * @tparam T
    *   the type of the events
    * @return
    *   a future of the filtered events
    */
  def filterPrunedEvents[T](offset: T => Offset)(
      events: Vector[T]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
  ): Future[Vector[T]] = {
    val ledgerEnd = ledgerEndCache().map(_.lastOffset)
    val beyondLegerEndO = events.find(event => Option(offset(event)) > ledgerEnd)
    beyondLegerEndO match {
      case Some(event) =>
        Future.failed(
          RequestValidationErrors.ParticipantDataAccessedAfterLedgerEnd
            .Reject(
              cause =
                s"Offset of event to be filtered ${offset(event)} is beyond ledger end $ledgerEnd",
              latestOffset = ledgerEnd.fold(0L)(_.unwrap),
            )(errorLoggingContext)
            .asGrpcError
        )
      case None =>
        pruningOffsetService.pruningOffset
          .map(participantPrunedUpTo =>
            events.filter(event => Option(offset(event)) > participantPrunedUpTo)
          )
    }
  }

}
