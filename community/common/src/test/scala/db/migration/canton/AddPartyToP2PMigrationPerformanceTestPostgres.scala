// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.canton

import com.digitalasset.canton.discard.Implicits.DiscardOps

import java.nio.charset.StandardCharsets
import java.sql.Connection
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Performance test for the V6_4__add_party_to_p2p migration, which adds the `party` column to
  * `lapi_events_party_to_participant` and backfills it by joining against `lapi_party_entries`.
  *
  * The test
  *   1. migrates the database to the last version before the migration under test,
  *   1. fills `lapi_events_party_to_participant` and `lapi_party_entries` with a large number of
  *      rows, and
  *   1. measures how long the migration under test takes on that data.
  *
  * The migration only exists for Postgres, hence there is no H2 variant.
  *
  * Data volumes can be overridden without touching the code, e.g. to reproduce a customer-sized
  * database locally:
  * {{{
  * P2P_MIGRATION_PERF_EVENTS=5000000 P2P_MIGRATION_PERF_PARTIES=100000 \
  *   sbt "community-common/testOnly db.migration.canton.AddPartyToP2PMigrationPerformanceTestPostgres"
  * }}}
  */
final class AddPartyToP2PMigrationPerformanceTestPostgres extends BaseFlywayTestPostgres {

  /** The last schema version before the migration under test. */
  private val versionBeforeMigrationUnderTest = "6.6"

  /** The migration whose runtime is measured. Targeted explicitly so that migrations added later
    * cannot creep into the measurement.
    */
  private val migrationUnderTest = "6.7"

  private def intFromEnv(name: String, default: Int): Int =
    sys.env.get(name).flatMap(_.toIntOption).getOrElse(default)

  /** Number of party-to-participant events present when the migration runs. */
  private val numberOfEvents: Int = intFromEnv("P2P_MIGRATION_PERF_EVENTS", 10_000)

  /** Number of parties that were successfully allocated, i.e. that the backfill can resolve. */
  private val numberOfAllocatedParties: Int = intFromEnv("P2P_MIGRATION_PERF_PARTIES", 1_000)

  /** Number of participants the events are spread over. */
  private val numberOfParticipants: Int = 5

  /** Lower bound on the backfill throughput. Deliberately generous: the point of the assertion is
    * to catch a migration that degenerates (e.g. into a per-row or cross-product plan), while the
    * actual throughput is reported in the log.
    */
  private val minimumRowsPerSecond: Int = 5_000

  /** A fake but realistically sized namespace fingerprint, so that the migration copies party names
    * of a representative length.
    */
  private val namespace: String = s"1220${"a5" * 32}"

  private def partyName(partyId: Int): String = s"party-$partyId::$namespace"
  private def internedPartyName(partyId: Int): String = s"p|${partyName(partyId)}"

  /** Parties are assigned to events round-robin, so events referring to rejected allocations are
    * interleaved with the ones that get backfilled.
    */
  private def partyIdOfEvent(event: Int): Int = (event % numberOfAllocatedParties) + 1

  private lazy val maximumMigrationDuration: FiniteDuration =
    (numberOfEvents / minimumRowsPerSecond).seconds.max(30.seconds)

  "V6_4__add_party_to_p2p" should {
    s"backfill $numberOfEvents party-to-participant events within $maximumMigrationDuration" in {
      // 1. Bring the database into the state the migration will encounter in the field.
      flyway(Some(versionBeforeMigrationUnderTest)).migrate().discard

      // 2. Fill the tables the migration reads and writes.
      val (_, insertDuration) = timed {
        withConnection { connection =>
          connection.setAutoCommit(false)
          insertStringInterning(connection)
          insertPartyToParticipantEvents(connection)
          // Give the query planner the statistics it would have on a real database.
          analyze(connection)
        }
      }
      logger.info(
        s"Inserted $numberOfAllocatedParties party entries and $numberOfEvents party-to-participant " +
          s"events in ${humanReadable(insertDuration)}"
      )

      // 3. Run the migration under test on that data.
      val (_, migrationDuration) = timed {
        flyway(Some(migrationUnderTest)).migrate().discard
      }
      val rowsPerSecond =
        if (migrationDuration.toMillis > 0) numberOfEvents * 1000L / migrationDuration.toMillis
        else numberOfEvents.toLong
      logger.info(
        s"Migration $migrationUnderTest backfilled $numberOfEvents events " +
          s"($numberOfAllocatedParties parties) in ${humanReadable(migrationDuration)} " +
          s"[~$rowsPerSecond rows/s]"
      )

      // 4. The migration is only fast if it is also correct.
      withConnection { connection =>
        countOf(connection, "select count(*) from lapi_events_party_to_participant") shouldBe
          numberOfEvents.toLong
        countOf(
          connection,
          "select count(party) from lapi_events_party_to_participant",
        ) shouldBe numberOfEvents.toLong
        countOf(
          connection,
          "select count(*) from lapi_events_party_to_participant e join lapi_party_entries pe " +
            "on pe.party_id = e.party_id where pe.party is not null and e.party <> pe.party",
        ) shouldBe 0L
      }

      withClue("migration duration: ") {
        migrationDuration should be <= maximumMigrationDuration
      }
    }
  }

  private def insertStringInterning(connection: Connection): Unit =
    insertBatched(
      connection,
      "insert into lapi_string_interning(internal_id, external_string) " +
        "values (?, ?)",
      numberOfAllocatedParties,
    ) { (statement, index) =>
      val partyId = index + 1
      statement.setLong(1, partyId.toLong)
      statement.setString(2, internedPartyName(partyId))
    }

  private def insertPartyToParticipantEvents(connection: Connection): Unit = {
    val emptyTraceContext = Array.emptyByteArray
    insertBatched(
      connection,
      "insert into lapi_events_party_to_participant(event_sequential_id, event_offset, update_id, " +
        "party_id, participant_id, participant_permission, participant_authorization_event, " +
        "synchronizer_id, record_time, trace_context) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      numberOfEvents,
    ) { (statement, index) =>
      val sequentialId = (index + 1).toLong
      statement.setLong(1, sequentialId)
      statement.setLong(2, sequentialId)
      statement.setBytes(3, s"update-$index".getBytes(StandardCharsets.UTF_8))
      statement.setInt(4, partyIdOfEvent(index))
      statement.setInt(5, (index % numberOfParticipants) + 1)
      statement.setInt(6, 1)
      statement.setInt(7, 1)
      statement.setInt(8, 1)
      statement.setLong(9, sequentialId)
      statement.setBytes(10, emptyTraceContext)
    }
  }
}
