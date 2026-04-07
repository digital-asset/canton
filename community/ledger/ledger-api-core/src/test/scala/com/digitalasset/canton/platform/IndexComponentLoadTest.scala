// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.api.v2.update_service.GetUpdateResponse
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.api.{
  AcsContinuationToken,
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
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.protocol.{ContractInstance, ReassignmentId}
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag
import com.digitalasset.daml.lf.data.{Bytes, Time}
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.time.Span
import org.scalatest.{Assertion, Ignore}

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, FiniteDuration}

/** Goal of this test is to provide a light-weight approach to ingest synthetic Index DB data for
  * load-testing, benchmarking purposes. This test is not supposed to run in CI (this is why it is
  * ignored, and logs on WARN log level).
  */
@Ignore
class IndexComponentLoadTest extends AnyFlatSpec with IndexComponentTest {

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

  it should "Fetch ACS" ignore TraceContext.withNewTraceContext("ACS fetch") {
    implicit traceContext =>
      val ledgerEndOffset = index.currentLedgerEnd().futureValue
      implicit val loggingContextWithTrace: LoggingContextWithTrace =
        LoggingContextWithTrace(loggerFactory)
      logger.warn("start fetching acs...")
      val startTime = System.currentTimeMillis()
      index
        .getActiveContracts(
          eventFormat = eventFormat(dsoParty),
          activeAt = ledgerEndOffset,
          continuationToken = None,
          checksum = AcsContinuationToken.emptyChecksum,
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
          PatienceConfiguration.Timeout(Span.convertDurationToSpan(Duration(2000, "seconds")))
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
      indexUpdates(allUpdates)
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

  private def indexUpdates(updates: Vector[(Update, Vector[ContractInstance])]): Unit = {
    val startTime = System.currentTimeMillis
    val updatesWithIds = fillUpdatesWithInternalContractIds(updates)
    val ledgerEndLongBefore = index.currentLedgerEnd().futureValue.map(_.positive).getOrElse(0L)
    withReporter(
      updates = updatesWithIds,
      parallelism = 1,
      process = ingestUpdateAsync,
      action = "ingesting",
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
  }

  private def fillUpdatesWithInternalContractIds(
      updates: Vector[(Update, Vector[ContractInstance])]
  ): Vector[Update] =
    withReporter[(Update, Vector[ContractInstance]), Update, Seq[Update]](
      updates = updates,
      parallelism = 100,
      process = storeContracts.tupled,
      action = "storing contracts of updates",
      sink = Sink.seq,
    ).toVector

  private val random = new scala.util.Random
  private def randomString(length: Int) = {
    val sb = new mutable.StringBuilder()
    for (i <- 1 to length) {
      sb.append(random.alphanumeric.head)
    }
    sb.toString
  }

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
    )
    Reassignment.Assign(
      ledgerEffectiveTime = ledgerEffectiveTime,
      createNode = contract.inst.toCreateNode,
      contractAuthenticationData = Bytes.Empty,
      reassignmentCounter = 10L,
      nodeId = nodeId,
      internalContractId = -1, // will be filled later
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
      stakeholders = Set(dsoParty),
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
        submitter = Some(dsoParty),
        reassignmentId = ReassignmentId.tryCreate("000123"),
        isReassigningParticipant = false,
      ),
      reassignment = Reassignment.Batch(reassignments.head, reassignments.tail*),
      recordTime = recordTime,
      synchronizerId = synchronizerId,
      acsChangeFactory = testAcsChangeFactory,
    )
}
