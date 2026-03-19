// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.platform.store.backend.DbDto
import com.digitalasset.canton.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.digitalasset.canton.platform.store.backend.common.{
  AppendOnlySchema,
  Field,
  Schema,
  Table,
}
import com.digitalasset.canton.platform.store.interning.StringInterning

private[postgresql] object PGSchema {
  private val PGFieldStrategy = new FieldStrategy {
    override def stringArray[From](
        extractor: StringInterning => From => Iterable[String]
    ): Field[From, Iterable[String], ?] =
      PGStringArray(extractor)

    override def smallint[From](
        extractor: StringInterning => From => Int
    ): Field[From, Int, ?] =
      PGSmallint(extractor)

    override def smallintOptional[From](
        extractor: StringInterning => From => Option[Int]
    ): Field[From, Option[Int], ?] =
      PGSmallintOptional(extractor)

    override def insert[From](tableName: String)(
        fields: (String, Field[From, ?, ?])*
    ): Table[From] =
      PGTable.transposedInsert(tableName)(fields*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(PGFieldStrategy)
}
