// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.platform.store.backend.common.{BaseTable, Field, Table}

import java.sql.Connection

private[postgresql] object PGTable {

  private def transposedInsertBase[From](
      insertStatement: String,
      ordering: Option[Ordering[From]] = None,
  )(fields: Seq[(String, Field[From, ?, ?])]): Table[From] =
    new BaseTable[From](fields, ordering) {
      override def executeUpdate: Array[Array[?]] => Connection => Unit =
        data =>
          connection =>
            Table.ifNonEmpty(data) {
              val preparedStatement = connection.prepareStatement(insertStatement)
              fields.indices.foreach { i =>
                preparedStatement.setObject(i + 1, data(i))
              }
              preparedStatement.execute()
              preparedStatement.close()
            }
    }

  private def transposedInsertStatement(
      tableName: String,
      fields: Seq[(String, Field[?, ?, ?])],
      statementSuffix: String = "",
  ): String = {
    def commaSeparatedOf(extractor: ((String, Field[?, ?, ?])) => String): String =
      fields.view
        .map(extractor)
        .mkString(",")
    def inputFieldName: String => String = fieldName => s"${fieldName}_in"
    val tableFields = commaSeparatedOf(_._1)
    val selectFields = commaSeparatedOf { case (fieldName, field) =>
      field.selectFieldExpression(inputFieldName(fieldName))
    }
    val unnestFields = commaSeparatedOf(_ => "?")
    val inputFields = commaSeparatedOf(fieldDef => inputFieldName(fieldDef._1))
    s"""
       |INSERT INTO $tableName
       |   ($tableFields)
       | SELECT
       |   $selectFields
       | From
       |   unnest($unnestFields)
       | as t($inputFields)
       | $statementSuffix
       |""".stripMargin
  }

  def transposedInsert[From](tableName: String)(
      fields: (String, Field[From, ?, ?])*
  ): Table[From] =
    transposedInsertBase(transposedInsertStatement(tableName, fields))(fields)
}
