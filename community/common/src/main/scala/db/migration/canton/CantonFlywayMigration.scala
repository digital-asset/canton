// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.canton

import com.digitalasset.canton.util.ResourceUtil
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import java.sql.{Connection, PreparedStatement, Statement}

/** Base class for Canton's Scala-based Flyway migrations. It unwraps the Flyway-provided connection
  * and offers small resource-management helpers, so that subclasses only express the actual
  * migration logic.
  */
abstract class CantonFlywayMigration extends BaseJavaMigration {

  override def migrate(context: Context): Unit = migrate(context.getConnection)

  /** Runs the migration against the Flyway-provided JDBC connection. */
  protected def migrate(connection: Connection): Unit

  protected def withStatement[A](connection: Connection)(f: Statement => A): A =
    ResourceUtil.withResource(connection.createStatement())(f)

  protected def withPreparedStatement[A](connection: Connection, sql: String)(
      f: PreparedStatement => A
  ): A =
    ResourceUtil.withResource(connection.prepareStatement(sql))(f)
}

object CantonFlywayMigration {

  /** Mirrors the mapping used by `DbParameterUtils` for the regular (Slick-based) storage code.
    */
  def blobArrayTypeName(connection: Connection): String =
    connection.getMetaData.getDatabaseProductName.toLowerCase match {
      case name if name.contains("postgresql") => "bytea"
      case name if name.contains("h2") => "binary large object"
      case other =>
        throw new IllegalStateException(s"Unsupported database for migration: $other")
    }
}
