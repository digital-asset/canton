// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import anorm.SqlParser.{int, str}
import anorm.{RowParser, ~}
import com.digitalasset.canton.platform.store.backend.StringInterningStorageBackend
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlExtensions.*

import java.sql.Connection

class StringInterningStorageBackendImpl(queryStrategy: QueryStrategy)
    extends StringInterningStorageBackend {

  private val StringInterningEntriesParser: RowParser[(Int, String)] =
    int("internal_id") ~ str("external_string") map { case internalId ~ externalString =>
      (internalId, externalString)
    }

  override def loadStringInterningEntries(fromIdExclusive: Int, untilIdInclusive: Int)(
      connection: Connection
  ): Vector[(Int, String)] =
    SQL"""
         SELECT internal_id, external_string
         FROM lapi_string_interning
         WHERE
           internal_id > $fromIdExclusive
           AND internal_id <= $untilIdInclusive
       """.asVectorOf(StringInterningEntriesParser)(connection)

  override def loadStringInterningEntriesRange(
      fromExclusive: String,
      toInclusive: String,
  )(connection: Connection): Vector[(Int, String)] =
    SQL"""
         SELECT internal_id, external_string
         FROM lapi_string_interning
         WHERE
           external_string > $fromExclusive
           AND external_string <= $toInclusive
         ORDER BY external_string
       """.asVectorOf(StringInterningEntriesParser)(connection)

  override def lastExternalStringInRange(
      prefix: String,
      fromExclusive: String,
      limit: Int,
  )(connection: Connection): Option[String] =
    SQL"""
         WITH page AS (
           SELECT external_string
           FROM lapi_string_interning
           WHERE
             external_string like ${prefix + '%'}
             AND external_string > $fromExclusive
           ORDER BY external_string
           ${QueryStrategy.limitClause(Some(limit))}
         )
         SELECT max(external_string) last_external_string
         FROM page
       """.asSingle(str("last_external_string").?)(connection)

  override def batchLookupInternalIds(
      rawStrings: Iterable[String]
  )(connection: Connection): Vector[(Int, String)] =
    if (rawStrings.isEmpty) Vector.empty
    else {
      SQL"""
         SELECT internal_id, external_string
         FROM lapi_string_interning
         WHERE
           external_string ${queryStrategy.anyOfStrings(rawStrings)}
         ORDER BY external_string
       """.asVectorOf(StringInterningEntriesParser)(connection)
    }

  override def batchLookupExternalStrings(
      ids: Iterable[Int]
  )(connection: Connection): Vector[(Int, String)] =
    if (ids.isEmpty) Vector.empty
    else {
      SQL"""
         SELECT internal_id, external_string
         FROM lapi_string_interning
         WHERE
           internal_id ${queryStrategy.anyOfSmallInts(ids)}
         ORDER BY internal_id
       """.asVectorOf(StringInterningEntriesParser)(connection)
    }
}
