// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.daml.ledger.api.v2.update_service.GetUpdateResponse
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.ledger.api.messages.state.AcsRangeInfo
import com.digitalasset.canton.ledger.api.messages.update.GetUpdatesPageRequest
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  ParticipantAuthorizationFormat,
  TopologyFormat,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  TestAcsChangeFactory,
  Update,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.participant.store.PersistedContractInstance
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.indexer.IndexerConfig.AchsConfig
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.{ContractId, WorkflowId}
import com.digitalasset.canton.protocol.{ContractInstance, ReassignmentId}
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.daml.lf.data.Time
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.Span
import org.scalatest.{Assertion, Ignore}

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}

/** Goal of this test is to provide a light-weight approach to ingest synthetic Index DB data for
  * load-testing, benchmarking purposes. This test is not supposed to run in CI (this is why it is
  * ignored, and logs on WARN log level).
  */
@Ignore
class IndexComponentLoadTest
    extends AnyFlatSpec
    with IndexComponentTest
    with PersistenceSqlQueries {
  // How long to wait for a benchmarked data fetch to finish. The test will fail if this is exceeded.
  private val benchmarkedTaskPatience =
    PatienceConfiguration.Timeout(Span.convertDurationToSpan(Duration(2000, "seconds")))

  override val dbConfig: com.digitalasset.canton.config.DbConfig =
    DbBasicConfig(
      username = "postgres",
      password = "",
      dbName = "load_test",
      host = "localhost",
      port = 5432,
      connectionPoolEnabled = true,
    ).toPostgresDbConfig

  override implicit val traceContext: TraceContext = TraceContext.createNew("load-test")

  private val testAcsChangeFactory = TestAcsChangeFactory()

  it should "Index assign/unassign updates" ignore {
    val nextRecordTime = nextRecordTimeFactory()
    logger.warn(s"start preparing updates...")
    val passes = 1
    val batchesPerPasses = 50 // this is doubled: first all assign batches then all unassign batches
    val eventsPerBatch = 10
    val allUpdates =
      (1 to passes).toVector
        .flatMap(_ =>
          allAssignsThenAllUnassigns(
            nextRecordTime = nextRecordTime,
            assignPayloadLength = 400,
            unassignPayloadLength = 150,
            batchSize = eventsPerBatch,
            batches = batchesPerPasses,
          )
        ) :+ assigns(nextRecordTime(), 400)(2)
    val allUpdateSize = allUpdates.size
    logger.warn(s"prepared $allUpdateSize updates")
    indexUpdates(allUpdates)
  }

  // will fail without an explicit flag
  it should "Index CN ACS Export NFR fixture updates" ignore cnNFRIngestionFixture()

  it should "10% CN NFR" ignore cnNFRIngestionFixture(passes = 432)

  it should "Cycle Contract Case" ignore cnNFRIngestionFixture(
    passes = 43,
    txsPerPass = 10000,
    activeTxsPerPass = 2,
  )

  it should "10% CN NFR with ACHS enabled (zero survival region)" ignore {
    val achsConfig = AchsConfig(
      validAtDistanceTarget = NonNegativeLong.tryCreate(40000L),
      lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(0L),
      aggregationThreshold = 10000L,
    )
    restartIndexer(config =
      IndexerConfig(
        achsConfig = Some(achsConfig)
      )
    )
    cnNFRIngestionFixture(
      passes = 432,
      actionName = "ingesting with ACHS maintenance (zero survival region)",
    )

    // Restart to default indexer to not impact other tests
    restartIndexer()
  }

  it should "10% CN NFR with ACHS enabled (big survival region)" ignore {
    val achsConfig = AchsConfig(
      validAtDistanceTarget = NonNegativeLong.tryCreate(40000L),
      // the distance between a contract's creation and archival (if archived) in event sequential ids is (txsCreatedThenArchived + txsCreatedNotArchived) * txSize = 2023 * 5 = 10115
      lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(20000L),
      aggregationThreshold = 10000L,
    )
    restartIndexer(config =
      IndexerConfig(
        achsConfig = Some(achsConfig)
      )
    )
    cnNFRIngestionFixture(
      passes = 432,
      actionName = "ingesting with ACHS maintenance (big survival region)",
    )

    // Restart to default indexer to not impact other tests
    restartIndexer()
  }

  it should "Fetch ACS" ignore TraceContext.withNewTraceContext("ACS fetch") {
    implicit traceContext =>
      fetchAcs()
  }

  it should "Fetch ACS with ACHS" ignore TraceContext.withNewTraceContext("ACS fetch with ACHS") {
    implicit traceContext =>
      restartIndexer(
        config = IndexerConfig(
          achsConfig = Some(
            AchsConfig(
              validAtDistanceTarget = NonNegativeLong.tryCreate(40000L),
              lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(20000L),
            )
          )
        )
      )
      fetchAcs()

      // Restart to default indexer to not impact other tests
      restartIndexer()
  }

  it should "Fetch updates stream in ascending order" ignore {
    fetchUpdatesStream(descendingOrder = false)
  }

  it should "Fetch updates pages of size 100 in ascending order" ignore {
    fetchUpdatesPaged(100, descendingOrder = false)
  }

  it should "Fetch updates pages of size 500 in ascending order" ignore {
    fetchUpdatesPaged(500, descendingOrder = false)
  }

  it should "Fetch updates stream in descending order" ignore {
    fetchUpdatesStream(descendingOrder = true)
  }

  it should "Fetch updates pages of size 100 in descending order" ignore {
    fetchUpdatesPaged(100, descendingOrder = true)
  }

  it should "Fetch updates pages of size 500 in descending order" ignore {
    fetchUpdatesPaged(500, descendingOrder = true)
  }

  it should "Measure ACHS rendering time during initialization with small survival region" ignore {
    measureAchsInitializationTime(
      regionName = "small",
      achsConfig = AchsConfig(
        validAtDistanceTarget = NonNegativeLong.tryCreate(200000L),
        lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(0L),
      ),
    )
  }

  it should "Measure ACHS rendering time during initialization with big survival region" ignore {
    measureAchsInitializationTime(
      regionName = "big",
      achsConfig = AchsConfig(
        validAtDistanceTarget = NonNegativeLong.tryCreate(200000L),
        lastPopulatedDistanceTarget = NonNegativeLong.tryCreate(100000L),
      ),
    )
  }

  private def measureAchsInitializationTime(regionName: String, achsConfig: AchsConfig): Unit = {
    // Step 1: Clear any existing ACHS state by restarting without ACHS
    restartIndexer(config = IndexerConfig(achsConfig = None))

    // Step 2: Measure baseline restart time without ACHS (no rendering work)
    logger.warn("Measuring baseline restart time without ACHS...")
    val baselineStart = System.currentTimeMillis()
    restartIndexer(config = IndexerConfig(achsConfig = None))
    val baselineTime = System.currentTimeMillis() - baselineStart
    logger.warn(s"Baseline restart (no ACHS) completed in ${seconds(baselineTime)} s")

    val lastEventSeqIdBefore = getLastEventSeqId
    logger.warn(s"Last event sequential ID before enabling ACHS: $lastEventSeqIdBefore")

    val achsSizeBefore = getAchsSize
    val achsStateRows = getAchsStateRowCount
    achsSizeBefore shouldBe 0L
    achsStateRows shouldBe 0L

    // Step 3: Enable ACHS and measure rendering time
    logger.warn(
      s"Enabling ACHS with $regionName survival region (validAtDistance=${achsConfig.validAtDistanceTarget}, " +
        s"lastPopulatedDistance=${achsConfig.lastPopulatedDistanceTarget})..."
    )
    val renderStart = System.currentTimeMillis()
    restartIndexer(config = IndexerConfig(achsConfig = Some(achsConfig)))
    val renderTime = System.currentTimeMillis() - renderStart
    val renderOverhead = renderTime - baselineTime
    logger.warn(
      s"ACHS rendering with $regionName survival region completed in ${seconds(renderTime)} s " +
        s"(rendering overhead: ${seconds(renderOverhead)} s)"
    )

    // Log and verify ACHS state after rendering
    logAchsState()
    val achsSize = getAchsSize
    val (achsValidAt, achsLastPopulated, achsLastRemoved) = getAchsState

    val initAggThreshold = AchsConfig.DefaultInitAggregationThreshold

    val removeTarget = lastEventSeqIdBefore - achsConfig.validAtDistanceTarget.unwrap
    val populateTarget = removeTarget - achsConfig.lastPopulatedDistanceTarget.unwrap

    achsValidAt shouldBe achsLastRemoved

    // Pointers should be within [target - threshold, target] due to aggregation rounding
    achsLastRemoved should be <= removeTarget
    achsLastRemoved should be >= (removeTarget - initAggThreshold).max(0L)
    achsLastPopulated should be <= populateTarget.max(0L)
    achsLastPopulated should be >= (populateTarget - initAggThreshold).max(0L)

    achsSize should be > 0L

    // Summary
    logger.warn(s"=== ACHS Rendering Time Summary (${regionName.capitalize} Survival) ===")
    logger.warn(s"Baseline restart (no ACHS): ${seconds(baselineTime)} s")
    logger.warn(
      s"${regionName.capitalize} survival region rendering: ${seconds(renderOverhead)} s (total: ${seconds(renderTime)} s, ACHS size: $achsSize)"
    )
  }

  private def fetchAcs()(implicit traceContext: TraceContext): Unit = {
    val ledgerEndOffset = index.currentLedgerEnd().futureValue
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)
    logger.warn("start fetching acs...")
    val startTime = System.currentTimeMillis()
    index
      .getActiveContracts(
        eventFormat = eventFormat(dsoParty.value),
        activeAt = ledgerEndOffset,
        rangeInfo = AcsRangeInfo.empty,
      )
      .zipWithIndex
      .runWith(Sink.last)
      .map { case (last, lastIndex) =>
        val totalMillis = System.currentTimeMillis() - startTime
        logger.warn(
          s"finished fetching acs in ${seconds(totalMillis)} s, ${lastIndex + 1} active contracts returned."
        )
        logger.warn(s"last active contract acs: $last")
      }
      .futureValue(
        benchmarkedTaskPatience
      )
  }

  private def seconds(milliseconds: Long): String = {
    val secs = milliseconds / 1000
    val millis = milliseconds.abs - secs.abs * 1000
    val milliString = (1000 + millis).toString.substring(1)
    secs.toString + '.' + milliString
  }

  private def nextRecordTimeFactory(): () => CantonTimestamp = {
    logger.warn(s"looking up base record time")
    val ledgerEnd = index.currentLedgerEnd().futureValue
    val baseRecordTime: CantonTimestamp = ledgerEnd match {
      case Some(offset) =>
        // try to get the last one
        logger.warn(s"looks like ledger not empty, getting record time from last update")
        val lastUpdate: GetUpdateResponse.Update = index
          .getUpdateBy(
            LookupKey.ByOffset(offset),
            UpdateFormat(
              includeTransactions = Some(
                TransactionFormat(
                  eventFormat = allPartyEventFormat,
                  transactionShape = TransactionShape.LedgerEffects,
                )
              ),
              includeReassignments = Some(allPartyEventFormat),
              includeTopologyEvents = None,
            ),
          )
          .futureValue
          .value
          .update
        lastUpdate.reassignment
          .flatMap(_.recordTime)
          .orElse(lastUpdate.transaction.flatMap(_.recordTime))
          .map(CantonTimestamp.fromProtoTimestamp(_).value)
          .getOrElse(fail("On LedgerEnd a reassignment or transaction is expected"))
      case None =>
        // empty ledger getting now
        logger.warn(s"looks like ledger is empty, using now as a baseline record time")
        CantonTimestamp.now()
    }
    val recordTime = new AtomicReference(baseRecordTime)
    () => recordTime.updateAndGet(_.immediateSuccessor)
  }

  /** Creates and ingests passes * txsPerPass transactions, each with 5 contracts. After each pass,
    * all but activeTxsPerPass of the txsPerPass transactions are archived.
    */
  private def cnNFRIngestionFixture(
      passes: Int = 4320,
      txsPerPass: Int = 2023,
      activeTxsPerPass: Int = 23,
      yesIReallyWantToRunIt: Boolean = false,
      actionName: String = "ingesting",
  ): Unit = {
    val nextRecordTime: () => CantonTimestamp = nextRecordTimeFactory()
    def ingestionIteration(): Unit = {
      logger.warn(s"start preparing updates...")
      if (!yesIReallyWantToRunIt)
        fail(
          "WARNING! Please check if you really want to do this! The following parameters result in a fixture probably not fitting in the memory. Please verify parameters. Bigger workloads are possible with doing multiple iterations."
        )
      val txSize = 5
      val txsCreatedAndArchivedPerPass = txsPerPass - activeTxsPerPass
      val createPayloadLength = 300
      val archiveArgumentPayloadLengthFromTo = (13, 38)
      val archiveResultPayloadLengthFromTo = (13, 58)
      val allUpdates = (1 to passes).toVector
        .flatMap(_ =>
          createsAndArchives(
            nextRecordTime = nextRecordTime,
            txSize = txSize,
            txsCreatedThenArchived = txsCreatedAndArchivedPerPass,
            txsCreatedNotArchived = activeTxsPerPass,
            createPayloadLength = createPayloadLength,
            archiveArgumentPayloadLengthFromTo = archiveArgumentPayloadLengthFromTo,
            archiveResultPayloadLengthFromTo = archiveResultPayloadLengthFromTo,
          )
        )
      val allUpdateSize = allUpdates.size
      logger.warn(s"prepared $allUpdateSize updates")
      indexUpdates(allUpdates, actionName = actionName)
    }

    (1 to 1).foreach { i =>
      logger.warn(s"ingestion iteration: $i started")
      ingestionIteration()
      logger.warn(s"ingestion iteration: $i finished")
    }
  }

  private def withReporter[UpdateT, ResultT, Out](
      updates: Vector[UpdateT],
      parallelism: Int,
      process: UpdateT => Future[ResultT],
      action: String,
      sink: Sink[ResultT, Future[Out]],
      waitingMessage: String = "",
      endCheck: () => Assertion = () => succeed,
  ): Out = {
    val numOfUpdates = updates.size
    val startTime = System.currentTimeMillis()
    val state = new AtomicLong(0L)
    logger.warn(s"start $action $numOfUpdates updates...")
    val reportingSeconds = 5
    val reporter = system.scheduler.scheduleAtFixedRate(
      initialDelay = FiniteDuration(reportingSeconds, "seconds"),
      interval = FiniteDuration(reportingSeconds, "seconds"),
    )(new Runnable {
      val lastState = new AtomicLong(0L)
      override def run(): Unit = {
        val current = state.get()
        val last = lastState.getAndSet(current)
        val reportRate = (current - last) / reportingSeconds
        val avgRate = current * 1000 / (System.currentTimeMillis() - startTime)
        val minutesLeft = (numOfUpdates - current) / avgRate / 60
        logger.warn(
          s"$action $current/$numOfUpdates, ${100 * current / numOfUpdates}% (since last: ${current - last}, $reportRate update/seconds) (avg: $avgRate update/seconds, estimated minutes left: $minutesLeft)..."
        )
      }
    })
    Source
      .fromIterator(() => updates.iterator)
      .async
      .mapAsync(parallelism)(process)
      .async
      .map { elem =>
        state.incrementAndGet()
        elem
      }
      .runWith(sink)
      .map { result =>
        reporter.cancel()
        logger.warn(
          s"finished $action $numOfUpdates updates to indexer" + waitingMessage
        )
        eventually(
          timeUntilSuccess = FiniteDuration(1000, "seconds"),
          maxPollInterval = FiniteDuration(100, "milliseconds"),
        )(endCheck())
        val avgRate = numOfUpdates * 1000 / (System.currentTimeMillis() - startTime)
        logger.warn(
          s"finished $action $numOfUpdates updates with average rate $avgRate updates/second"
        )
        result
      }
      .futureValue(
        PatienceConfiguration.Timeout(Span.convertDurationToSpan(Duration(200000, "seconds")))
      )
  }

  private def indexUpdates(
      updates: Vector[(Update, Vector[ContractInstance])],
      actionName: String = "ingesting",
  ): Unit = {
    val startTime = System.currentTimeMillis
    val updatesWithIds = fillUpdatesWithInternalContractIds(updates)
    val ledgerEndLongBefore = index.currentLedgerEnd().futureValue.map(_.positive).getOrElse(0L)
    withReporter(
      updates = updatesWithIds,
      parallelism = 1,
      process = ingestUpdateAsync,
      action = actionName,
      sink = Sink.ignore,
      waitingMessage = ", waiting for all to be indexed...",
      endCheck = () =>
        (index
          .currentLedgerEnd()
          .futureValue
          .map(_.positive)
          .getOrElse(0L) - ledgerEndLongBefore) shouldBe updates.size,
    ).discard
    val timeSpan = seconds(System.currentTimeMillis - startTime)
    logger.warn(s"Ingestion cycle completed in $timeSpan seconds")
    logAchsState()
  }

  private def logAchsState(): Unit = {
    val achsStateRows = getAchsStateRowCount
    if (achsStateRows > 0) {
      val lastEventSeqId = getLastEventSeqId
      val achsSize = getAchsSize
      val (achsValidAt, achsLastPopulated, achsLastRemoved) = getAchsState
      logger.warn(
        s"ACHS state: lastEventSequentialId=$lastEventSeqId, size=$achsSize, validAt=$achsValidAt, lastPopulated=$achsLastPopulated, lastRemoved=$achsLastRemoved"
      )
    } else {
      logger.warn("ACHS state: not initialized (no rows in lapi_achs_state)")
    }
  }

  private def fillUpdatesWithInternalContractIds(
      updates: Vector[(Update, Vector[ContractInstance])]
  ): Vector[Update] =
    withReporter[(Update, Vector[ContractInstance]), Update, Seq[Update]](
      updates = updates,
      parallelism = 100,
      process = (storeContracts _).tupled,
      action = "storing contracts of updates",
      sink = Sink.seq,
    ).toVector

  private def allAssignsThenAllUnassigns(
      nextRecordTime: () => CantonTimestamp,
      assignPayloadLength: Int,
      unassignPayloadLength: Int,
      batchSize: Int,
      batches: Int,
  ): Vector[(Update.SequencedReassignmentAccepted, Vector[ContractInstance])] = {
    val assigned =
      Vector
        .fill(batches)(batchSize)
        .map(size => assigns(nextRecordTime(), assignPayloadLength)(size))

    val cidBatches =
      assigned.map(_._2.map(_.contractId))

    assigned ++ cidBatches.map(cids =>
      unassigns(nextRecordTime(), unassignPayloadLength)(cids) -> Vector.empty
    )
  }

  private def assigns(recordTime: CantonTimestamp, payloadLength: Int)(
      size: Int
  ): (Update.SequencedReassignmentAccepted, Vector[ContractInstance]) = {
    val (reassignments, contracts) =
      (0 until size)
        .map(index =>
          assign(
            nodeId = index,
            ledgerEffectiveTime = recordTime.underlying,
            argumentPayload = randomString(payloadLength),
          )
        )
        .unzip

    reassignment(
      sourceSynchronizerId = synchronizer2,
      targetSynchronizerId = synchronizer1,
      synchronizerId = synchronizer1,
      recordTime = recordTime,
      workflowId = None,
    )(reassignments) -> contracts.toVector
  }

  private def unassigns(recordTime: CantonTimestamp, payloadLength: Int)(
      coids: Seq[ContractId]
  ): Update.SequencedReassignmentAccepted =
    reassignment(
      sourceSynchronizerId = synchronizer1,
      targetSynchronizerId = synchronizer2,
      synchronizerId = synchronizer1,
      recordTime = recordTime,
      workflowId = Some(
        WorkflowId.assertFromString(randomString(payloadLength))
      ), // mimick unassign payload with workflowID. This is also stored with all events.
    )(coids.zipWithIndex.map { case (coid, index) =>
      unassign(
        coid = coid,
        nodeId = index,
      )
    })

  private def assign(
      nodeId: Int,
      ledgerEffectiveTime: Time.Timestamp,
      argumentPayload: String,
  ): (Reassignment.Assign, ContractInstance) = {
    val contract = genContract(
      argumentPayload = argumentPayload,
      template = templates(0),
      signatories = Set(dsoParty),
      ledgerEffectiveTime = ledgerEffectiveTime,
    )
    Reassignment.Assign(
      reassignmentCounter = 10L,
      nodeId = nodeId,
      persistedContractInstance = PersistedContractInstance(
        internalContractId = -1, // will be filled later
        inst = contract.inst,
      ),
    ) -> contract
  }

  private def unassign(
      coid: ContractId,
      nodeId: Int,
  ): Reassignment.Unassign =
    Reassignment.Unassign(
      contractId = coid,
      templateId = templates(0),
      packageName = packageName,
      stakeholders = Set(dsoParty.value),
      assignmentExclusivity = None,
      reassignmentCounter = 11L,
      nodeId = nodeId,
    )

  private def reassignment(
      sourceSynchronizerId: SynchronizerId,
      targetSynchronizerId: SynchronizerId,
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      workflowId: Option[WorkflowId],
  )(reassignments: Seq[Reassignment]): Update.SequencedReassignmentAccepted =
    Update.SequencedReassignmentAccepted(
      optCompletionInfo = None,
      workflowId = workflowId,
      updateId = randomUpdateId,
      reassignmentInfo = ReassignmentInfo(
        sourceSynchronizer = ReassignmentTag.Source(sourceSynchronizerId),
        targetSynchronizer = ReassignmentTag.Target(targetSynchronizerId),
        submitter = Some(dsoParty.value),
        reassignmentId = ReassignmentId.tryCreate("000123"),
        isReassigningParticipant = false,
      ),
      reassignment = Reassignment.Batch(reassignments.head, reassignments.tail*),
      recordTime = recordTime,
      synchronizerId = synchronizerId,
      acsChangeFactory = testAcsChangeFactory,
    )

  def updateFormat(transactionShape: TransactionShape) = UpdateFormat(
    includeTransactions = Some(
      TransactionFormat(
        eventFormat = EventFormat(
          filtersByParty = Map(),
          filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter(true)),
          verbose = true,
        ),
        transactionShape = transactionShape,
      )
    ),
    includeReassignments = Some(
      EventFormat(
        filtersByParty = Map(),
        filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter(true)),
        verbose = true,
      )
    ),
    includeTopologyEvents = Some(
      TopologyFormat(
        Some(
          ParticipantAuthorizationFormat(None)
        )
      )
    ),
  )

  private def fetchUpdatesStream(descendingOrder: Boolean): Unit = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)
    logger.warn("start fetching updates stream...")
    val startTime = System.currentTimeMillis()
    val fetchAndCountUpdates = for {
      ledgerEnd <- index.currentLedgerEnd()
      source = index.updates(
        begin = None,
        endAt = ledgerEnd,
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = descendingOrder,
        skipPruningChecks = false,
      )
      updates <- source.grouped(1000).map(_.size).runWith(Sink.seq)(materializer)
    } yield updates.sum

    fetchAndCountUpdates
      .map { count =>
        val totalMillis = System.currentTimeMillis() - startTime
        logger.warn(
          s"finished fetching updates in ${if (descendingOrder) "descending"
            else "ascending"} order in ${seconds(totalMillis)} s, $count transactions returned."
        )
      }
      .futureValue(
        benchmarkedTaskPatience
      )
  }

  private def fetchUpdatesPaged(pageSize: Int, descendingOrder: Boolean): Unit = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)

    def fetchAllPages(
        request: GetUpdatesPageRequest,
        fetchedSoFar: Int,
    ): Future[Int] =
      index
        .updatesPage(request)
        .flatMap(response =>
          response.nextPageToken match {
            case Some(_) =>
              fetchAllPages(
                request.copy(continueStreamFromIncl =
                  Some(
                    if (descendingOrder)
                      Offset.tryFromLong(response.lowestPageOffsetExclusive)
                    else
                      Offset.tryFromLong(response.highestPageOffsetInclusive + 1)
                  )
                ),
                fetchedSoFar + response.updates.size,
              )
            case None => Future.successful(fetchedSoFar + response.updates.size)
          }
        )

    logger.warn(s"start fetching updates pages($pageSize) in ${if (descendingOrder) "descending"
      else "ascending"} order...")
    val startTime = System.currentTimeMillis()
    val fetchAndCountUpdates = for {
      ledgerEnd <- index.currentLedgerEnd()
      request = GetUpdatesPageRequest(
        startExclusive = None,
        endInclusive = ledgerEnd,
        continueStreamFromIncl = None,
        maxPageSize = pageSize,
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = descendingOrder,
        requestChecksum = ByteString.empty(),
        participantChecksum = ByteString.empty(),
      )
      res <- fetchAllPages(request, 0)
    } yield res

    fetchAndCountUpdates
      .map { count =>
        val totalMillis = System.currentTimeMillis() - startTime
        logger.warn(
          s"finished fetching paged($pageSize) updates in ${if (descendingOrder) "descending"
            else "ascending"} order in ${seconds(totalMillis)} s, $count transactions returned."
        )
      }
      .futureValue(
        benchmarkedTaskPatience
      )
  }
}
