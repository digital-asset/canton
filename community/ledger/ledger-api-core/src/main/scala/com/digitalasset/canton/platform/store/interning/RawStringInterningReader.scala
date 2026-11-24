// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.StringInterningStorageBackend
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait RawStringInterningReader {
  def batchLookupExternalStrings(
      ids: Iterable[Int]
  )(implicit traceContext: TraceContext): Future[Map[Int, String]]

  def batchLookupInternalIds(
      rawStrings: Iterable[String]
  )(implicit traceContext: TraceContext): Future[Map[String, Int]]

  def lastExternalStringInRange(
      prefix: String,
      fromExclusive: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Option[String]]

  def loadStringInterningEntriesRange(
      fromExclusive: String,
      toInclusive: String,
  )(implicit traceContext: TraceContext): Future[Vector[(Int, String)]]

  def loadStringInterningEntries(
      fromIdExclusive: Int,
      untilIdInclusive: Int,
  )(implicit traceContext: TraceContext): Future[Vector[(Int, String)]]
}

class PersistenceBasedRawStringInterningReader(
    dbDispatcher: DbDispatcher,
    stringInterningStorageBackend: StringInterningStorageBackend,
    metrics: LedgerApiServerMetrics,
    executionContext: ExecutionContext,
    override val loggerFactory: NamedLoggerFactory,
) extends RawStringInterningReader
    with NamedLogging {
  private implicit val ec: ExecutionContext = executionContext

  override def batchLookupInternalIds(
      rawStrings: Iterable[String]
  )(implicit traceContext: TraceContext): Future[Map[String, Int]] =
    if (rawStrings.isEmpty) Future.successful(Map.empty)
    else {
      dbDispatcher
        .executeSql(metrics.index.db.batchLookupInternalIdsStringInterning)(
          stringInterningStorageBackend.batchLookupInternalIds(rawStrings)
        )(LoggingContextWithTrace(loggerFactory))
        .map(_.view.map(_.swap).toMap)
    }

  override def batchLookupExternalStrings(
      ids: Iterable[Int]
  )(implicit traceContext: TraceContext): Future[Map[Int, String]] =
    if (ids.isEmpty) Future.successful(Map.empty)
    else {
      dbDispatcher
        .executeSql(metrics.index.db.batchLookupExternalStrings)(
          stringInterningStorageBackend.batchLookupExternalStrings(ids)
        )(LoggingContextWithTrace(loggerFactory))
        .map(_.toMap)
    }

  override def lastExternalStringInRange(
      prefix: String,
      fromExclusive: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Option[String]] =
    dbDispatcher
      .executeSql(metrics.index.db.lastExternalStringInRange)(
        stringInterningStorageBackend.lastExternalStringInRange(
          prefix = prefix,
          fromExclusive = fromExclusive,
          limit = limit,
        )
      )(LoggingContextWithTrace(loggerFactory))

  override def loadStringInterningEntriesRange(
      fromExclusive: String,
      toInclusive: String,
  )(implicit traceContext: TraceContext): Future[Vector[(Int, String)]] =
    dbDispatcher
      .executeSql(metrics.index.db.loadStringInterningEntriesRange)(
        stringInterningStorageBackend.loadStringInterningEntriesRange(
          fromExclusive = fromExclusive,
          toInclusive = toInclusive,
        )
      )(LoggingContextWithTrace(loggerFactory))

  override def loadStringInterningEntries(
      fromIdExclusive: Int,
      untilIdInclusive: Int,
  )(implicit traceContext: TraceContext): Future[Vector[(Int, String)]] =
    dbDispatcher
      .executeSql(metrics.index.db.loadStringInterningEntries)(
        stringInterningStorageBackend.loadStringInterningEntries(
          fromIdExclusive = fromIdExclusive,
          untilIdInclusive = untilIdInclusive,
        )
      )(LoggingContextWithTrace(loggerFactory))
}
