// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.LocalOffset
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.EventLogId.ParticipantEventLogId
import com.digitalasset.canton.participant.store.MultiDomainEventLog.PublicationData
import com.digitalasset.canton.participant.sync.{DefaultLedgerSyncEvent, TimestampedEvent}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{
  DbIndexedStringStore,
  DbTest,
  DelayedDbStore,
  H2Test,
  PostgresTest,
}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, MonadUtil}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import io.functionmeta.functionFullName
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

trait DbMultiDomainEventLogLastGlobalOffsetCacheTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with DbTest {
  implicit val system: ActorSystem = ActorSystem(
    classOf[DbMultiDomainEventLogLastGlobalOffsetCacheTest].getSimpleName
  )
  implicit val materializer: Materializer = Materializer(system)

  override def beforeAll(): Unit = {
    DbMultiDomainEventLogTest.acquireLinearizedEventLogLock(
      ErrorLoggingContext.fromTracedLogger(logger)
    )
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    DbMultiDomainEventLogTest.releaseLinearizedEventLogLock(
      ErrorLoggingContext.fromTracedLogger(logger)
    )
  }

  "DbMultiDomainEventLOg.lastGlobalOffsetCache" should {
    "be updated in case of concurrent read/write" in {
      /*
        The goal is to test that the last global offset cache is correctly updated in the following situation:
        - Cache has some initial value `n` (i.e., multi-domain event log is not empty)
        - There is a read from the DB, which takes some time
        - New publication data is inserted with `n' > n`
        - Cache should contain the new value `n´`
       */

      val promiseDelayRead = Promise[Unit]()

      val initialGlobalOffset = DbMultiDomainEventLog
        .lastOffsetAndPublicationTime(storage)
        .futureValue
        .map(_._1)
        .getOrElse(0L)

      val delayedDbStore = new DelayedDbStore(
        underlying = storage,
        delayAfterRead = Map(
          (
            "com.digitalasset.canton.participant.store.db.DbMultiDomainEventLog.lastOffsetAndPublicationTime", // TODO(#12739) Improve with a macro
            0,
          ) -> promiseDelayRead.future
        ),
        timeouts = timeouts,
        loggerFactory = loggerFactory,
      )

      val fixture =
        new DbMultiDomainEventLogLastGlobalOffsetCacheTest.LastGlobalOffsetConcurrentReadWriteFixture(
          storage = storage,
          delayedDbStore = delayedDbStore,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )

      val multiDomainEventLog = fixture.eventLog

      multiDomainEventLog.publish(fixture.publicationData1).futureValue

      val globalOffsetAfterFirstPublication = eventually() {
        val res = DbMultiDomainEventLog.lastOffsetAndPublicationTime(storage).futureValue

        inside(res) { case Some((globalOffset, _)) =>
          globalOffset should be > initialGlobalOffset
          globalOffset
        }
      }

      // ---------------------------------------
      // Initial setup is done

      // This read will be delayed until the insertion is done
      val readF = multiDomainEventLog.lastGlobalOffset().value

      // Insertion
      FutureUtil.doNotAwait(
        multiDomainEventLog.publish(fixture.publicationData2),
        "Unable to publish data into the multi domain event log",
      )

      val expectedFinalGlobalOffset = eventually() {
        val res = DbMultiDomainEventLog.lastOffsetAndPublicationTime(storage).futureValue

        inside(res) { case Some((globalOffset, _)) =>
          globalOffset should be > globalOffsetAfterFirstPublication
          globalOffset
        }
      }

      // We let the DB return the read (globalOffset = 1)
      promiseDelayRead.trySuccess(())
      readF.futureValue

      // Finally, the cache should contain the new value
      eventually() {
        multiDomainEventLog.getLastGlobalOffsetCache.value shouldBe expectedFinalGlobalOffset
      }

      eventually() {
        delayedDbStore.uncalledReads() shouldBe empty
      }
    }
  }

  override protected def cleanDb(storage: DbStorage): Future[Unit] = {
    import storage.api.*

    import DbMultiDomainEventLogLastGlobalOffsetCacheTest.*

    storage.update(
      DBIO.seq(
        sqlu"truncate table linearized_event_log", // table guarded by DbMultiDomainEventLogTest.acquireLinearizedEventLogLock
        sqlu"delete from event_log where ${indexedStringStore.minIndex} <= log_id and log_id <= ${indexedStringStore.maxIndex}", // table shared with other tests
        sqlu"delete from event_log where log_id = $participantEventLogId", // table shared with other tests
      ),
      functionFullName,
    )
  }
}

private object DbMultiDomainEventLogLastGlobalOffsetCacheTest {
  lazy val participantEventLogId: ParticipantEventLogId =
    DbEventLogTestResources.dbMultiDomainEventLogTestParticipantEventLogId

  lazy val indexedStringStore: InMemoryIndexedStringStore =
    DbEventLogTestResources.dbMultiDomainEventLogTestIndexedStringStore

  class LastGlobalOffsetConcurrentReadWriteFixture(
      storage: DbStorage,
      delayedDbStore: DelayedDbStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit traceContext: TraceContext, executionContext: ExecutionContext, mat: Materializer) {
    private val storeIndex = 0
    private val participantEventLogId = ParticipantEventLogId.tryCreate(storeIndex)

    private val clock = new WallClock(DefaultProcessingTimeouts.testing, loggerFactory)

    private def publicationData(localOffset: LocalOffset): PublicationData = PublicationData(
      eventLogId = participantEventLogId,
      event = TimestampedEvent(
        DefaultLedgerSyncEvent.dummyStateUpdate(CantonTimestamp.Epoch.plusSeconds(localOffset)),
        localOffset,
        None,
        None,
      ),
      inFlightReference = None,
    )

    val publicationData1: PublicationData = publicationData(1)
    val publicationData2: PublicationData = publicationData(2)

    val eventLog = new DbMultiDomainEventLog(
      initialLastGlobalOffset = None,
      publicationTimeBoundInclusive = CantonTimestamp.MaxValue,
      lastLocalOffsets = TrieMap(),
      maxBatchSize = PositiveInt.MaxValue,
      participantEventLogId = participantEventLogId,
      storage = delayedDbStore,
      clock = clock,
      metrics = ParticipantTestMetrics,
      indexedStringStore = new DbIndexedStringStore(storage, timeouts, loggerFactory),
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )

    val single = new DbSingleDimensionEventLog(
      participantEventLogId,
      storage,
      indexedStringStore,
      BaseTest.testedReleaseProtocolVersion,
      timeouts,
      loggerFactory,
    )

    // To be able to publish data in the multi-domain event log, it first needs to be inserted in the single dimension event log
    Await.result(
      MonadUtil.sequentialTraverse_(Seq(publicationData1, publicationData2)) { publicationData =>
        single.insertsUnlessEventIdClash(Seq(publicationData.event))
      },
      timeouts.default.duration,
    )
  }
}

class DbMultiDomainEventLogLastGlobalOffsetCacheTestPostgres
    extends DbMultiDomainEventLogLastGlobalOffsetCacheTest
    with PostgresTest

class DbMultiDomainEventLogLastGlobalOffsetCacheTestH2
    extends DbMultiDomainEventLogLastGlobalOffsetCacheTest
    with H2Test
