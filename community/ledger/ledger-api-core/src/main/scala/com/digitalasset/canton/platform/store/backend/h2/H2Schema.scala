// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.platform.store.backend.DbDto
import com.digitalasset.canton.platform.store.backend.common.AppendOnlySchema.FieldStrategy
import com.digitalasset.canton.platform.store.backend.common.{
  AppendOnlySchema,
  Field,
  Schema,
  Table,
}
import com.digitalasset.canton.platform.store.interning.StringInterning

private[h2] object H2Schema {
  private val H2FieldStrategy = new FieldStrategy {
    override def bytea[From](
        extractor: StringInterning => From => Array[Byte]
    ): Field[From, Array[Byte], ?] =
      H2Bytea(extractor)

    override def byteaOptional[From](
        extractor: StringInterning => From => Option[Array[Byte]]
    ): Field[From, Option[Array[Byte]], ?] =
      H2ByteaOptional(extractor)

    override def insert[From](tableName: String)(
        fields: (String, Field[From, ?, ?])*
    ): Table[From] =
      Table.batchedInsert(tableName)(fields*)
  }

  val schema: Schema[DbDto] = AppendOnlySchema(H2FieldStrategy)
}
