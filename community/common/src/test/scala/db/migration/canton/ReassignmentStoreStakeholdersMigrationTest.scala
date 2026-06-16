// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.canton

import com.digitalasset.canton.protocol.ExampleContractFactory

/** Test for the V6_0__reassignment_store_stakeholders migration, which backfills the new
  * `stakeholders` column
  */
trait ReassignmentStoreStakeholdersMigrationTest extends BaseFlywayTest {

  "ReassignmentStoreStakeholdersMigration" should {
    "backfill the stakeholders column from the persisted contracts" in {
      // 1. Migrate up to the schema version that still has the 'contracts' blob column.
      flyway(Some("5.2")).migrate()

      // 2. Insert a reassignment row carrying a serialized contract.
      val contract = ExampleContractFactory.build()
      val expectedStakeholders = contract.metadata.stakeholders

      withConnection { connection =>
        val statement = connection.prepareStatement(
          "insert into par_reassignments(target_synchronizer_idx, source_synchronizer_idx, " +
            "reassignment_id, unassignment_timestamp, contracts) values (?, ?, ?, ?, ?)"
        )
        statement.setInt(1, 1)
        statement.setInt(2, 2)
        statement.setString(3, "reassignment-1")
        statement.setLong(4, 0L)
        val contractsArray = connection.createArrayOf(
          CantonFlywayMigration.blobArrayTypeName(connection),
          Array[Array[Byte]](contract.encoded.toByteArray).asInstanceOf[Array[AnyRef]],
        )
        statement.setArray(5, contractsArray)
        statement.executeUpdate()
        statement.close()
      }

      // 3. Run the remaining migrations: V6_0 backfills stakeholders, V6_1 drops 'contracts'.
      flyway(None).migrate()

      // 4. The stakeholders column is populated from the deserialized contract.
      val storedStakeholders = withConnection { connection =>
        val resultSet = connection
          .createStatement()
          .executeQuery(
            "select stakeholders from par_reassignments where reassignment_id = 'reassignment-1'"
          )
        resultSet.next() shouldBe true
        inside(resultSet.getArray(1).getArray) { case array: Array[AnyRef] =>
          array.toSeq.map(_.toString).toSet
        }
      }

      storedStakeholders shouldBe expectedStakeholders
    }
  }
}

final class ReassignmentStoreStakeholdersMigrationTestH2
    extends ReassignmentStoreStakeholdersMigrationTest
    with BaseFlywayTestH2

final class ReassignmentStoreStakeholdersMigrationTestPostgres
    extends ReassignmentStoreStakeholdersMigrationTest
    with BaseFlywayTestPostgres
