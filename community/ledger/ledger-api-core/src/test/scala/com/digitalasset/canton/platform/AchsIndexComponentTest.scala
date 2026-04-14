// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import anorm.SqlParser.long
import anorm.~
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.AcsRangeInfo
import com.digitalasset.canton.ledger.participant.state.Update.CommitRepair
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.platform.config.{
  ActiveContractsServiceStreamsConfig,
  IndexServiceConfig,
}
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.indexer.IndexerConfig.AchsConfig
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.dao.events.ACSReader
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.event.Level

import java.sql.Connection
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, Promise}

class AchsIndexComponentTest extends AnyFlatSpec with IndexComponentTest {

  val aggregationThreshold: Long = 5L

  val achsConfig: AchsConfig = AchsConfig(
    validAtDistanceTarget = NonNegativeLong.tryCreate(60L),
    lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(40L),
    aggregationThreshold = aggregationThreshold,
    initAggregationThreshold = aggregationThreshold,
  )

  override protected val indexerConfig: IndexerConfig = IndexerConfig(
    achsConfig = Some(achsConfig)
  )

  private val testDbMetrics = DatabaseMetrics.ForTesting("achs-index-component-test")

  /** Executes a sql query through the indexer's dbSupport connection pool (pool size 1 for H2).
    * This avoids creating a separate H2 connection that could see inconsistent data due to H2's
    * synchronization bug. See DbType.H2Database and JdbcIndexer for more.
    */
  private def withConnection[T](f: Connection => T): T =
    dbSupport.dbDispatcher
      .executeSql(testDbMetrics)(f)
      .futureValue

  private def getLastEventSeqId: Long =
    withConnection { implicit connection =>
      SQL"SELECT ledger_end_sequential_id FROM lapi_parameters"
        .as(long("ledger_end_sequential_id").?.single)
        .getOrElse(0L)
    }

  private val recordTimeRef = new AtomicReference(CantonTimestamp.now())
  private val nextRecordTime: () => CantonTimestamp =
    () => recordTimeRef.updateAndGet(_.immediateSuccessor)

  behavior of "ACHS maintenance"

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

  behavior of "ACHS reading"

  it should "return active contracts when activeAt is before ACHS' validAt (falls back to activate)" in {
    val txsCreatedNotArchived = 200
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

    val start = index.currentLedgerEnd().futureValue.fold(0L)(_.unwrap)

    ingestUpdates(allUpdates*)

    val validAt = getAchsValidAt
    val beforeValidAt = offsetForEventSeqId(validAt) - 1L

    val contractsBeforeValidAt = activeContractIds(beforeValidAt).filter(_._2 > start)

    contractsBeforeValidAt should not be empty
    contractsBeforeValidAt.size shouldBe (beforeValidAt - start) * txSize
  }

  it should "return active contracts when activeAt is after ACHS validAt (uses ACHS)" in {
    val txsCreatedThenArchived = 10
    val txsCreatedNotArchived = 1
    val txSize = 3
    val repetitions = 50

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

    val start = index.currentLedgerEnd().futureValue.fold(0L)(_.unwrap)

    ingestUpdates(allUpdates*)

    val ledgerEnd = index.currentLedgerEnd().futureValue

    val contractsAtLedgerEnd = activeContractIds(ledgerEnd.value.unwrap).filter(_._2 > start)

    contractsAtLedgerEnd.size shouldBe txsCreatedNotArchived * txSize * repetitions

    // offset at validAt so that ACHS is used
    val atValidAtOffset = offsetForEventSeqId(getAchsValidAt)
    // offset before validAt so that ACHS is not used
    val beforeValidAtOffset = atValidAtOffset - 1

    val achsContracts = activeContractIds(atValidAtOffset).filter(_._2 > start)
    val noAchsContracts = activeContractIds(beforeValidAtOffset).filter(_._2 > start)

    achsContracts should not be empty
    noAchsContracts should not be empty
    achsContracts.size shouldBe noAchsContracts.size + txSize

    noAchsContracts shouldBe achsContracts.filter(_._2 < atValidAtOffset)
  }

  it should "return correct active contracts when ACHS validAt overtakes the ACS query point mid-stream" in {
    val txsCreatedThenArchived = 0
    val txsCreatedNotArchived = 3
    val txSize = 3
    val initialRepetitions = 40

    // we need around LedgerApiStreamsBufferSize (128) events in ACHS to ensure we are back-pressuring, and we can pause the pipeline
    val initialUpdates = (1 to initialRepetitions).flatMap { _ =>
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
    val lastEventSeqIdBefore = getLastEventSeqId
    val lastEventSeqId =
      lastEventSeqIdBefore + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * initialRepetitions

    ingestUpdates(initialUpdates*)
    verifyAchsConsistency(lastEventSeqId)

    // Restart indexer with minimal buffering/parallelism to ensure backpressure propagates
    // all the way to the ACHS ID source when the stream is paused.
    val indexServiceConfig: IndexServiceConfig = IndexServiceConfig(
      activeContractsServiceStreams = ActiveContractsServiceStreamsConfig(
        maxIdsPerIdPage = 1,
        maxPayloadsPerPayloadsPage = 1,
        maxParallelActiveIdQueries = 1,
        idFilterQueryParallelism = 1,
        maxParallelPayloadCreateQueries = 1,
        contractProcessingParallelism = 1,
      )
    )
    restartIndexer(
      serviceConfig = indexServiceConfig
    )
    verifyAchsConsistency(lastEventSeqId)

    // activeAtEventSeqId should be >= validAt, so ACHS is initially used.
    val validAtBefore = getAchsValidAt
    val queryOffset = offsetForEventSeqId(validAtBefore) + 1
    val queryEventSeqId = eventSeqIdForOffset(queryOffset)

    // Start ACS retrieval and concurrently ingest more data to make validAt overtake queryEventSeqId.
    val concurrentRepetitions = 10
    val concurrentUpdates = (1 to concurrentRepetitions).flatMap { _ =>
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

    val numContracts = 10
    // Stream ACS while ingestion is happening, get the first numContracts from ACHS.
    // Then pause until ingestion completes to give ACHS time to advance past the query's activeAt.
    // Thus, mid-stream, ACHS' validAt will advance past query's activeAt sequential id.
    val fetchStarted = Promise[Unit]()
    val ingestionDone = Promise[Unit]()
    val acsF = index
      .getActiveContracts(
        eventFormat = allPartyEventFormat,
        activeAt = Some(Offset.tryFromLong(queryOffset)),
        rangeInfo = AcsRangeInfo.empty,
      )
      .zipWithIndex
      .mapAsync(1) { case (elem, idx) =>
        if (idx == numContracts) {
          fetchStarted.trySuccess(())
          ingestionDone.future.map(_ => elem)
        } else Future.successful(elem)
      }
      .runWith(Sink.seq)

    // wait until we have fetched some contracts from ACHS
    fetchStarted.future.futureValue

    val fallbackMessage = "falling back to activate filter tables"
    val acs = loggerFactory.assertLogsSeq(
      SuppressionRule.LevelAndAbove(Level.DEBUG) && SuppressionRule.forLogger[ACSReader]
    )(
      // signal that ingestion is done (ACHS' validAt should have advanced past query's activeAt)
      {
        ingestUpdates(concurrentUpdates*)
        // Wait for ACHS validAt to advance past the query point before resuming the ACS stream.
        eventually() {
          getAchsValidAt should be > queryEventSeqId
        }
        ingestionDone.trySuccess(())
        acsF.futureValue
      },
      logs =>
        withClue(
          s"Expected fallback log '$fallbackMessage' not found. Captured logs:\n${logs.map(_.message).mkString("\n")}"
        ) {
          logs.exists(
            _.message.contains(fallbackMessage)
          ) shouldBe true
        },
    )

    // validAt should have advanced past the query's event sequential id, if it fails it means that fallback was not triggered due to the validAt overtaking the query point
    val validAtAfter = getAchsValidAt
    validAtAfter should be > queryEventSeqId

    val achsContractIds = acs
      .flatMap(
        _.contractEntry.activeContract
          .flatMap(_.createdEvent.map(event => event.contractId -> event.offset))
      )

    achsContractIds should not be empty

    // The ACHS-enabled result should match the ACHS-disabled reference
    restartIndexer(
      config = indexerConfig.copy(achsConfig = None)
    )
    val referenceContractIds = activeContractIds(queryOffset)
    referenceContractIds should not be empty
    achsContractIds shouldBe referenceContractIds

    // Restart with original config for the next tests
    restartIndexer(config = this.indexerConfig)
  }

  it should "return same active contracts with ACHS enabled and disabled" in {
    val txsCreatedThenArchived = 5
    val txsCreatedNotArchived = 1
    val txSize = 3
    val repetitions = 50

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

    val lastEventSeqIdBefore = getLastEventSeqId
    ingestUpdates(allUpdates*)
    val expectedLastEventSeqId =
      lastEventSeqIdBefore + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * repetitions

    verifyAchsConsistency(expectedLastEventSeqId)

    val ledgerEnd = index.currentLedgerEnd().futureValue.value.unwrap

    // fetch ACS using the ACHS-enabled index service (ACHS should be used)
    val achsContracts = activeContractIds(ledgerEnd)
    achsContracts should not be empty

    // restart indexer with ACHS disabled and fetch ACS again (ACHS should not be used, but result should be the same)
    restartIndexer(
      config = indexerConfig.copy(
        achsConfig = None
      )
    )

    val noAchsContracts = activeContractIds(ledgerEnd)
    noAchsContracts should not be empty

    achsContracts shouldBe noAchsContracts

    // restart with original config for the next tests
    restartIndexer(config = indexerConfig)
  }

  behavior of "ACHS in repair mode"

  it should "leave ACHS unchanged when ingesting repair transactions" in {
    // Ensure we start from the original config
    restartIndexer(indexerConfig)

    val txsCreatedThenArchived = 1
    val txsCreatedNotArchived = 1
    val txSize = 3
    val repetitions = 20

    // Phase 1: ingest normal data with ACHS enabled so that ACHS is populated
    val initialUpdates = (1 to repetitions).flatMap { _ =>
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

    val lastEventSeqIdInit = getLastEventSeqId
    ingestUpdates(initialUpdates*)

    val lastEventSeqIdBeforeRepair =
      lastEventSeqIdInit + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * repetitions

    verifyAchsConsistency(lastEventSeqIdBeforeRepair)

    // Snapshot ACHS state before repair
    val achsSizeBefore = getAchsSize
    val (validAtBefore, lastPopulatedBefore, lastRemovedBefore) = getAchsState

    achsSizeBefore should be > 0L
    getAchsStateRowCount shouldBe 1

    // restart indexer in repair mode
    restartIndexer(config = indexerConfig, repairMode = true)

    // ingest repair transactions
    val repairTxSize = 3
    val repairRepetitions = 10
    val repairUpdates = (1 to repairRepetitions)
      .map { _ =>
        repairCreates(recordTime = nextRecordTime, payloadLength = 42)(repairTxSize)
      }
      .toVector
      .appended(CommitRepair() -> Vector.empty) // commit the repair to advance the ledger end

    ingestUpdates(repairUpdates*)

    val lastEventSeqIdAfterRepair = getLastEventSeqId
    lastEventSeqIdAfterRepair should be > lastEventSeqIdBeforeRepair

    // verify ACHS remains unchanged
    getAchsSize shouldBe achsSizeBefore
    getAchsState shouldBe ((validAtBefore, lastPopulatedBefore, lastRemovedBefore))

    // restart indexer in normal mode and verify ACHS catches up with the new data
    restartIndexer(config = indexerConfig)

    verifyAchsConsistency(lastEventSeqIdAfterRepair)

    eventually() {
      val achsSizeAfter = getAchsSize
      achsSizeAfter should be >= achsSizeBefore
    }
  }

  behavior of "ACHS pruning"

  it should "prune ACHS entries" in {
    val txsCreatedThenArchived = 10
    val txsCreatedNotArchived = 1
    val txSize = 20
    val repetitions = 2

    // Phase 1: ingest data with ACHS enabled so ACHS is populated
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

    val lastEventSeqIdBefore = getLastEventSeqId
    ingestUpdates(allUpdates*)

    val lastEventSeqId =
      lastEventSeqIdBefore + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * repetitions
    verifyAchsConsistency(lastEventSeqId)

    val achsSizeBefore = getAchsSize
    achsSizeBefore should be > 0L

    // prune up to the ledger end (all deactivations are in range, their activations should be pruned)
    val ledgerEnd = index.currentLedgerEnd().futureValue.value

    index
      .prune(
        previousPruneUpToInclusive = None,
        previousIncompleteReassignmentOffsets = Vector.empty,
        pruneUpToInclusive = ledgerEnd,
        incompleteReassignmentOffsets = Vector.empty,
      )
      .futureValue

    // verify ACHS is still consistent after pruning.
    verifyAchsConsistency(getLastEventSeqId)

    // after pruning, the remaining ACHS entries should be <= what was there before pruning
    val achsSizeAfter = getAchsSize
    achsSizeAfter should be < achsSizeBefore

    // all ACHS entries should reference existing activation events
    getAchsEventSeqIds shouldBe getActivateEventSeqIds

    // restart with ACHS enabled and verify ACHS rebuilds consistently
    restartIndexer(config = indexerConfig)
    verifyAchsConsistency(getLastEventSeqId)
  }

  behavior of "ACHS initialization"

  // The different combinations of positive/negative diffs simulate various scenarios of how the new config's distance
  // targets relate to the original config.

  it should "maintain ACHS consistency after restart with positive populate and negative remove work distances" in {
    // validAtDistanceDiff +20 so remove work negative, lastPopulatedDistanceDiff -30 so populate work positive
    testRestartWithNewConfig(validAtDistanceDiff = +20L, lastPopulatedDistanceDiff = -30L)
  }

  it should "maintain ACHS consistency after restart with negative populate and positive remove work distances" in {
    // validAtDistanceDiff -20 so remove work positive, lastPopulatedDistanceDiff +30 so populate work negative
    testRestartWithNewConfig(validAtDistanceDiff = -20L, lastPopulatedDistanceDiff = +30L)
  }

  it should "maintain ACHS consistency after restart with negative populate and negative remove work distances" in {
    // validAtDistanceDiff +20 so remove work negative, lastPopulatedDistanceDiff +30 so populate work negative
    testRestartWithNewConfig(validAtDistanceDiff = +20L, lastPopulatedDistanceDiff = +30L)
  }

  it should "maintain ACHS consistency after restart with positive populate and positive remove work distances" in {
    // validAtDistanceDiff -30 so remove work positive, lastPopulatedDistanceDiff -20 so populate work positive
    testRestartWithNewConfig(validAtDistanceDiff = -30L, lastPopulatedDistanceDiff = -20L)
  }

  it should "maintain ACHS consistency after restart with zero populate and zero remove work distances" in {
    // Same distances as original config, initialWork = 0 for both dimensions
    testRestartWithNewConfig(validAtDistanceDiff = 0L, lastPopulatedDistanceDiff = 0L)
  }

  it should "clear ACHS state and snapshot when disabling ACHS" in {
    // ensure we start from the original config
    restartIndexer(indexerConfig)

    val txsCreatedThenArchived = 5
    val txsCreatedNotArchived = 1
    val txSize = 3
    val repetitions = 50

    // Phase 1: ingest data with ACHS enabled and verify it was populated
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

    val lastEventSeqIdBefore = getLastEventSeqId
    ingestUpdates(allUpdates*)

    val lastEventSeqId =
      lastEventSeqIdBefore + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * repetitions
    verifyAchsConsistency(lastEventSeqId)

    val achsSize = getAchsSize

    // Confirm ACHS has data before disabling
    achsSize should be > 0L
    getAchsStateRowCount shouldBe 1

    // Phase 2: restart with ACHS disabled (achsConfig = None)
    restartIndexer(
      config = indexerConfig.copy(achsConfig = None)
    )

    // Both the ACHS state row and filter data should be cleared
    getAchsSize shouldBe 0L
    getAchsStateRowCount shouldBe 0

    // Phase 3: restart again with ACHS enabled and verify consistency is rebuilt
    restartIndexer(config = indexerConfig)

    verifyAchsConsistency(lastEventSeqId)

    // ACHS should be re-populated and consistent
    getAchsSize should be >= achsSize - aggregationThreshold
    getAchsSize should be <= achsSize + aggregationThreshold
    getAchsStateRowCount shouldBe 1
  }

  it should "skip removal phase when initializing fresh ACHS with large lastPopulatedDistanceTarget" in {
    // Start with ACHS disabled so that we get a fresh ACHS (lastPopulated = 0)
    restartIndexer(config = indexerConfig.copy(achsConfig = None))

    val txsCreatedThenArchived = 5
    val txsCreatedNotArchived = 1
    val txSize = 3
    val repetitions = 50

    // Ingest enough data so that there is significant removal work during initialization
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

    val lastEventSeqIdBefore = getLastEventSeqId
    ingestUpdates(allUpdates*)

    val lastEventSeqId =
      lastEventSeqIdBefore + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * repetitions

    eventually()(getLastEventSeqId shouldBe lastEventSeqId)

    getAchsSize shouldBe 0L
    getAchsStateRowCount shouldBe 0

    // Restart with a config that has large lastPopulatedDistanceTarget and small validAtDistanceTarget.
    // This means during initialization:
    //   remove work = positive removal work
    //   populate work = negative
    // Since lastPopulated = 0 (fresh ACHS), all removal work ranges will have populationEnd = 0, and removal will be skipped.
    val largePopDistConfig = AchsConfig(
      validAtDistanceTarget = NonNegativeLong.tryCreate(10L),
      lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(1000000L),
      aggregationThreshold = aggregationThreshold,
      initAggregationThreshold = aggregationThreshold,
    )

    val skipMessage = "Skipping ACHS removal as no population has been assigned up to this point"
    loggerFactory.assertLogsSeq(
      SuppressionRule.LevelAndAbove(Level.DEBUG)
    )(
      restartIndexer(config = indexerConfig.copy(achsConfig = Some(largePopDistConfig))),
      logs => {
        val skipLogs = logs.filter(_.message.contains(skipMessage))
        withClue(
          s"Expected logs with '$skipMessage' not found. Captured logs:\n${logs.map(_.message).mkString("\n")}"
        ) {
          skipLogs.size should be > 100
        }
      },
    )
  }

  // Ingest data, restart with ACHS config adjusted by the given diffs, verify consistency, ingest more data, verify consistency again.
  private def testRestartWithNewConfig(
      validAtDistanceDiff: Long,
      lastPopulatedDistanceDiff: Long,
  ): Unit = {
    // Ensure we start from the original config (a previous test may have left a different config active)
    restartIndexer(indexerConfig)

    val txsCreatedThenArchived = 5
    val txsCreatedNotArchived = 1
    val txSize = 3
    val repetitions = 50

    // Phase 1: ingest data with original config and verify ACHS consistency
    val initialUpdates = (1 to repetitions).flatMap { _ =>
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

    val lastEventSeqIdBefore = getLastEventSeqId
    ingestUpdates(initialUpdates*)

    val lastEventSeqId1 =
      lastEventSeqIdBefore + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * repetitions

    verifyAchsConsistency(lastEventSeqId1)

    // Phase 2: restart with the new ACHS config, the initialization should
    // recalculate remaining work and eagerly advance ACHS to match the new config
    val newAchsConfig = achsConfig.copy(
      validAtDistanceTarget =
        NonNegativeLong.tryCreate(achsConfig.validAtDistanceTarget.unwrap + validAtDistanceDiff),
      lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(
        achsConfig.lastPopulatedDistanceTarget.unwrap + lastPopulatedDistanceDiff
      ),
    )

    restartIndexer(
      config = indexerConfig.copy(
        achsConfig = Some(newAchsConfig)
      )
    )

    // After restart, for each dimension use the old config if the work is negative (debt, no-op)
    // or the new config if the work is positive (catch-up done eagerly during initialization).
    // From AchsMaintenancePipe.initialWork:
    //   remove work = -validAtDistanceDiff
    //   populate work = -(validAtDistanceDiff + lastPopulatedDistanceDiff)
    val removeWorkIsDebt = validAtDistanceDiff > 0
    val populateWorkIsDebt = validAtDistanceDiff + lastPopulatedDistanceDiff > 0
    val expectedValidAt =
      if (removeWorkIsDebt) lastEventSeqId1 - achsConfig.validAtDistanceTarget.unwrap
      else lastEventSeqId1 - newAchsConfig.validAtDistanceTarget.unwrap
    val expectedLastPopulated =
      if (populateWorkIsDebt)
        lastEventSeqId1 - achsConfig.validAtDistanceTarget.unwrap - achsConfig.lastPopulatedDistanceTarget.unwrap
      else
        lastEventSeqId1 - newAchsConfig.validAtDistanceTarget.unwrap - newAchsConfig.lastPopulatedDistanceTarget.unwrap
    verifyAchsConsistency(
      expectedValidAt = expectedValidAt,
      expectedLastPopulated = expectedLastPopulated,
    )

    // Phase 3: ingest more data after restart and verify ACHS consistency is maintained
    val moreUpdates = (1 to repetitions).flatMap { _ =>
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

    ingestUpdates(moreUpdates*)

    val lastEventSeqId2 =
      lastEventSeqId1 + txSize * (txsCreatedThenArchived * 2 + txsCreatedNotArchived) * repetitions

    verifyAchsConsistency(lastEventSeqId2, newAchsConfig)

  }

  private def verifyAchsConsistency(
      targetLastEventSeqId: Long,
      config: AchsConfig = achsConfig,
  ): Unit = {
    eventually()(getLastEventSeqId shouldBe targetLastEventSeqId)
    verifyAchsConsistency(
      expectedValidAt = targetLastEventSeqId - config.validAtDistanceTarget.unwrap,
      expectedLastPopulated =
        targetLastEventSeqId - config.validAtDistanceTarget.unwrap - config.lastPopulatedDistanceTarget.unwrap,
    )
  }

  private def verifyAchsConsistency(
      expectedValidAt: Long,
      expectedLastPopulated: Long,
  ): Unit =
    eventually(1.minute) {
      val (validAt, lastPopulated, lastRemoved) = getAchsState
      validAt should be > expectedValidAt - aggregationThreshold
      validAt should be <= expectedValidAt

      lastPopulated should be > expectedLastPopulated - aggregationThreshold
      lastPopulated should be <= expectedLastPopulated

      withClue(s"The lastRemoved pointer was not equal to validAt") {
        lastRemoved shouldBe validAt
      }

      val (activeIds, achsIds) =
        withConnection { implicit connection =>
          val active =
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
              .as(long("event_sequential_id").*)
              .toSet

          val achs =
            SQL"""SELECT event_sequential_id
                  FROM lapi_filter_achs_stakeholder
                  ORDER BY event_sequential_id"""
              .as(long("event_sequential_id").*)
              .toSet

          (active, achs)
        }

      achsIds should not be empty
      achsIds shouldBe activeIds
    }

  private def getAchsSize: Long =
    withConnection { implicit connection =>
      SQL"SELECT COUNT(DISTINCT event_sequential_id) AS count FROM lapi_filter_achs_stakeholder"
        .as(long("count").single)
    }

  private def getAchsValidAt: Long =
    withConnection { implicit connection =>
      SQL"SELECT valid_at FROM lapi_achs_state"
        .as(long("valid_at").single)
    }

  private def getAchsState: (Long, Long, Long) =
    withConnection { implicit connection =>
      SQL"SELECT valid_at, last_populated, last_removed FROM lapi_achs_state"
        .as((long("valid_at") ~ long("last_populated") ~ long("last_removed")).single)(
          connection
        ) match {
        case v ~ lp ~ lr => (v, lp, lr)
      }
    }

  private def getAchsStateRowCount: Long =
    withConnection { implicit connection =>
      SQL"SELECT COUNT(*) AS count FROM lapi_achs_state"
        .as(long("count").single)
    }

  private def getAchsEventSeqIds: List[Long] =
    withConnection { implicit connection =>
      SQL"""SELECT event_sequential_id
          FROM lapi_filter_achs_stakeholder
          ORDER BY event_sequential_id"""
        .as(long("event_sequential_id").*)
    }

  private def getActivateEventSeqIds: List[Long] =
    withConnection { implicit connection =>
      SQL"""SELECT event_sequential_id
          FROM lapi_filter_activate_stakeholder
          ORDER BY event_sequential_id"""
        .as(long("event_sequential_id").*)
    }

  private def offsetForEventSeqId(seqId: Long): Long =
    withConnection { implicit connection =>
      SQL"""SELECT MAX(event_offset) AS max_offset
              FROM lapi_events_activate_contract
              WHERE event_sequential_id <= $seqId"""
        .as(long("max_offset").?.single)
        .getOrElse(0L)
    }

  private def eventSeqIdForOffset(offset: Long): Long =
    withConnection { implicit connection =>
      SQL"""SELECT MAX(event_sequential_id) AS max_seq_id
            FROM lapi_events_activate_contract
            WHERE event_offset <= $offset"""
        .as(long("max_seq_id").?.single)
        .getOrElse(0L)
    }

  private def activeContractIds(activeAt: Long): Seq[(String, Long)] =
    index
      .getActiveContracts(
        eventFormat = allPartyEventFormat,
        activeAt = Some(Offset.tryFromLong(activeAt)),
        rangeInfo = AcsRangeInfo.empty,
      )
      .runWith(Sink.seq)
      .futureValue
      .flatMap(
        _.contractEntry.activeContract
          .flatMap(_.createdEvent.map(event => event.contractId -> event.offset))
      )
}
