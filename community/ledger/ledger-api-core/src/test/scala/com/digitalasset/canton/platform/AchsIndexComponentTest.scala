// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import anorm.SqlParser.long
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.indexer.IndexerConfig.AchsConfig
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt

class AchsIndexComponentTest extends AnyFlatSpec with IndexComponentTest with BeforeAndAfterEach {

  val achsConfig: AchsConfig = AchsConfig(
    validAtDistanceTarget = NonNegativeLong.tryCreate(60L),
    lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(40L),
  )

  val aggregationThreshold: Long = 5L

  override protected val indexerConfig: IndexerConfig = IndexerConfig(
    achsConfig = Some(achsConfig),
    achsAggregationThreshold = aggregationThreshold,
  )

  private var connection: java.sql.Connection = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    connection = DriverManager.getConnection(jdbcUrl)
  }

  override def afterEach(): Unit =
    try connection.close()
    finally super.afterEach()

  private def getLastEventSeqId: Long =
    SQL"SELECT ledger_end_sequential_id FROM lapi_parameters"
      .as(long("ledger_end_sequential_id").?.single)(connection)
      .getOrElse(0L)

  private val recordTimeRef = new AtomicReference(CantonTimestamp.now())
  private val nextRecordTime: () => CantonTimestamp =
    () => recordTimeRef.updateAndGet(_.immediateSuccessor)

  behavior of "ACHS maintenance"

  private def getAchsSize: Long =
    SQL"SELECT COUNT(DISTINCT event_sequential_id) AS count FROM lapi_filter_achs_stakeholder"
      .as(long("count").single)(connection)

  it should "result in minimal ACHS size with few survivors" in {
    val txsCreatedThenArchived = 10
    val txsCreatedNotArchived = 1
    val txSize = 1
    val repetitions = 20

    val allUpdates = (1 to repetitions).flatMap { _ =>
      createsAndArchives(
        nextRecordTime = nextRecordTime,
        txSize = txSize,
        txsCreatedThenArchived = txsCreatedThenArchived,
        txsCreatedNotArchived = txsCreatedNotArchived,
        createPayloadLength = 42,
        archiveArgumentPayloadLengthFromTo = (10, 20),
        archiveResultPayloadLengthFromTo = (10, 20),
      )
    }
    val achsSizeBefore = getAchsSize
    val lastEventSeqIdBefore = getLastEventSeqId

    ingestUpdates(allUpdates*)

    // The only survivors are the not-archived contracts, so ACHS increase should be small
    val survivors = txsCreatedNotArchived * repetitions.toLong
    eventually() {
      val achsSizeAfter = getAchsSize
      (achsSizeAfter - achsSizeBefore) should be <= survivors
    }

    val expectedLastEventSeqId =
      lastEventSeqIdBefore + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * repetitions

    verifyAchsConsistency(expectedLastEventSeqId)
  }

  it should "populate ACHS correctly for creates only" in {

    val txsCreatedNotArchived = 100
    val txSize = 3

    val allUpdates = createsAndArchives(
      nextRecordTime = nextRecordTime,
      txSize = txSize,
      txsCreatedThenArchived = 0,
      txsCreatedNotArchived = txsCreatedNotArchived,
      createPayloadLength = 42,
      archiveArgumentPayloadLengthFromTo = (10, 20),
      archiveResultPayloadLengthFromTo = (10, 20),
    )

    val lastEventSeqIdBefore = getLastEventSeqId
    ingestUpdates(allUpdates*)

    val expectedLastEventSeqId = lastEventSeqIdBefore + txSize * txsCreatedNotArchived

    verifyAchsConsistency(expectedLastEventSeqId)
  }

  it should "populate ACHS correctly for archives as well" in {
    val txsCreatedThenArchived = 100
    val txsCreatedNotArchived = 100
    val txSize = 3

    val allUpdates = createsAndArchives(
      nextRecordTime = nextRecordTime,
      txSize = txSize,
      txsCreatedThenArchived = txsCreatedThenArchived,
      txsCreatedNotArchived = txsCreatedNotArchived,
      createPayloadLength = 42,
      archiveArgumentPayloadLengthFromTo = (10, 20),
      archiveResultPayloadLengthFromTo = (10, 20),
    )

    val lastEventSeqIdBefore = getLastEventSeqId
    val targetLastEventSeqId =
      lastEventSeqIdBefore + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived)

    ingestUpdates(allUpdates*)
    verifyAchsConsistency(targetLastEventSeqId)
  }

  private def verifyAchsConsistency(targetLastEventSeqId: Long): Unit = {
    val expectedValidAt = targetLastEventSeqId - achsConfig.validAtDistanceTarget.unwrap
    val expectedLastPopulated = expectedValidAt - achsConfig.lastPopulatedDistanceTarget.unwrap
    eventually(60.seconds) {
      val validAt =
        SQL"SELECT valid_at FROM lapi_achs_state"
          .as(long("valid_at").single)(connection)

      validAt should be >= expectedValidAt - aggregationThreshold
      validAt should be <= expectedValidAt + aggregationThreshold

      val lastPopulated =
        SQL"SELECT last_populated FROM lapi_achs_state"
          .as(long("last_populated").single)(connection)
      lastPopulated should be >= expectedLastPopulated - aggregationThreshold
      lastPopulated should be <= expectedLastPopulated + aggregationThreshold

      val lastRemoved =
        SQL"SELECT last_removed FROM lapi_achs_state"
          .as(long("last_removed").single)(connection)
      lastRemoved shouldBe validAt

      val activeIds =
        SQL"""SELECT filters.event_sequential_id
              FROM lapi_filter_activate_stakeholder filters
              WHERE filters.event_sequential_id <= $lastPopulated
                AND NOT EXISTS (
                  SELECT 1
                  FROM lapi_events_deactivate_contract deactivate_evs
                  WHERE filters.event_sequential_id = deactivate_evs.deactivated_event_sequential_id
                    AND deactivate_evs.event_sequential_id <= $lastRemoved
                )
              ORDER BY filters.event_sequential_id"""
          .as(long("event_sequential_id").*)(connection)
          .toSet

      val achsIds =
        SQL"""SELECT event_sequential_id
              FROM lapi_filter_achs_stakeholder
              ORDER BY event_sequential_id"""
          .as(long("event_sequential_id").*)(connection)
          .toSet

      achsIds should not be empty
      achsIds shouldBe activeIds
    }
  }
}
