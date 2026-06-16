// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.canton

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.protocol.ContractInstance
import com.digitalasset.canton.resource.DbParameterUtils
import com.google.protobuf.ByteString

import java.sql.{Connection, SQLNonTransientException}

/** This migration backfills the new stakeholders column by extracting the stakeholders from the
  * contracts column.
  */
trait ReassignmentStoreStakeholdersMigration extends CantonFlywayMigration {

  override protected def migrate(connection: Connection): Unit = {
    addStakeholdersColumn(connection)
    backfillStakeholders(connection)
  }

  private def addStakeholdersColumn(connection: Connection): Unit =
    withStatement(connection)(
      _.execute("alter table par_reassignments add column stakeholders varchar array").discard
    )

  private def backfillStakeholders(connection: Connection): Unit =
    withPreparedStatement(
      connection,
      "select target_synchronizer_idx, reassignment_id, contracts from par_reassignments",
    ) { select =>
      withPreparedStatement(
        connection,
        "update par_reassignments set stakeholders = ? " +
          "where target_synchronizer_idx = ? and reassignment_id = ?",
      ) { update =>
        val resultSet = select.executeQuery()
        Iterator
          .continually(resultSet)
          .takeWhile(_.next())
          .foreach { row =>
            val targetSynchronizerIdx = row.getInt(1)
            val reassignmentId = row.getString(2)
            val stakeholders = DbParameterUtils
              .byteArraysFromSqlArray(row.getArray(3))
              .toSet
              .flatMap(deserializeStakeholders(reassignmentId, _))

            val sqlArray = connection.createArrayOf("varchar", stakeholders.toArray[AnyRef])
            update.setArray(1, sqlArray)
            update.setInt(2, targetSynchronizerIdx)
            update.setString(3, reassignmentId)
            update.addBatch()
          }
        update.executeBatch().discard
      }
    }

  private def deserializeStakeholders(
      reassignmentId: String,
      contractBytes: Array[Byte],
  ): Seq[String] =
    ContractInstance
      .decodeWithCreatedAt(ByteString.copyFrom(contractBytes))
      .fold(
        err =>
          throw new SQLNonTransientException(
            s"Failed to deserialize reassigned contract: $err for reassignmentId $reassignmentId"
          ),
        _.metadata.stakeholders.toSeq,
      )
}
