// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.entries.LoggingEntries
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.concurrent.{ExecutionContextIdlenessExecutorService, Threading}
import com.digitalasset.canton.config.{ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.ledger.api.LedgerApiStore.LastSynchronizerOffset
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawParticipantAuthorization,
  SequentialIdBatch,
  SynchronizerOffset,
}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.common.QueryStrategy
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.cache.MutableLedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.platform.store.{DbSupport, FlywayMigrations}
import com.digitalasset.canton.platform.{ResourceCloseable, ResourceOwnerFlagCloseableOps}
import com.digitalasset.canton.resource.{DbStorage, Storage}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{LedgerParticipantId, config}
import com.google.common.annotations.VisibleForTesting

import java.sql.Connection
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

class LedgerApiStore(
    val ledgerApiDbSupport: DbSupport,
    val ledgerApiStorage: LedgerApiStorage,
    val ledgerEndCache: MutableLedgerEndCache,
    val stringInterningView: StringInterningView,
    val metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
) extends ResourceCloseable {
  private val integrityStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createIntegrityStorageBackend
  private val parameterStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createParameterStorageBackend(stringInterningView)
  private val eventStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createEventStorageBackend(
      ledgerEndCache,
      stringInterningView,
      loggerFactory,
    )
  private val stringInterningStorageBackend =
    ledgerApiDbSupport.storageBackendFactory.createStringInterningStorageBackend

  private def executeSql[T](databaseMetrics: DatabaseMetrics)(
      sql: Connection => T
  )(implicit traceContext: TraceContext): Future[T] =
    ledgerApiDbSupport.dbDispatcher.executeSql(databaseMetrics)(sql)(
      new LoggingContextWithTrace(LoggingEntries.empty, traceContext)
    )

  private def executeSqlUS[T](databaseMetrics: DatabaseMetrics)(
      sql: Connection => T
  )(implicit traceContext: TraceContext, ec: ExecutionContext): FutureUnlessShutdown[T] =
    FutureUnlessShutdown.outcomeF(
      ledgerApiDbSupport.dbDispatcher.executeSql(databaseMetrics)(sql)(
        new LoggingContextWithTrace(LoggingEntries.empty, traceContext)
      )
    )

  @VisibleForTesting
  def verifyIntegrity(failForEmptyDB: Boolean = true)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    executeSqlUS(DatabaseMetrics.ForTesting("checkIntegrity"))(
      integrityStorageBackend.verifyIntegrity(failForEmptyDB, ledgerApiStorage.inMemoryCantonStore)
    )

  @VisibleForTesting
  def moveLedgerEndBackToScratch()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    executeSqlUS(DatabaseMetrics.ForTesting("onlyForTestingMoveLedgerEndBackToScratch"))(
      integrityStorageBackend.moveLedgerEndBackToScratch()
    )

  @VisibleForTesting
  def lockPruning(
      releaseLock: Promise[Unit],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[FutureUnlessShutdown[Unit]] = {
    val locked = Promise[Unit]()
    val released = executeSqlUS(DatabaseMetrics.ForTesting("onlyForTestingLockPruning")) {
      QueryStrategy.withoutNetworkTimeout { connection =>
        eventStorageBackend.lockExclusivelyPruningProcessingTable(connection)
        locked.trySuccess(()).discard
        Threading.sleep(1000)
        Await.result(releaseLock.future, timeout)
      }(_, noTracingLogger)
    }
    FutureUnlessShutdown.outcomeF(locked.future).map(_ => released)
  }

  @VisibleForTesting
  def numberOfAcceptedTransactionsFor(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Int] =
    executeSqlUS(DatabaseMetrics.ForTesting("numberOfAcceptedTransactionsFor"))(
      integrityStorageBackend.numberOfAcceptedTransactionsFor(synchronizerId)
    )

  /** The latest SynchronizerIndex for a synchronizerId until all events are processed fully and
    * published to the Ledger API DB.
    */
  def cleanSynchronizerIndex(synchronizerId: SynchronizerId)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[SynchronizerIndex]] =
    executeSqlUS(metrics.index.db.getCleanSynchronizerIndex)(
      parameterStorageBackend.cleanSynchronizerIndex(synchronizerId)
    )

  def ledgerEnd(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[LedgerEnd]] =
    executeSqlUS(metrics.index.db.getLedgerEnd)(
      parameterStorageBackend.ledgerEnd
    )

  def topologyPartyEventBatch(eventSequentialIds: SequentialIdBatch)(implicit
      traceContext: TraceContext
  ): Future[Vector[RawParticipantAuthorization]] =
    executeSql(metrics.index.db.getTopologyEventOffsetPublishedOnRecordTime)(
      eventStorageBackend.topologyPartyEventBatch(eventSequentialIds)
    )

  def topologyEventOffsetPublishedOnRecordTime(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[Offset]] =
    executeSql(metrics.index.db.getTopologyEventOffsetPublishedOnRecordTime)(
      eventStorageBackend.topologyEventOffsetPublishedOnRecordTime(synchronizerId, recordTime)
    )

  def firstSynchronizerOffsetAfterOrAt(
      synchronizerId: SynchronizerId,
      afterOrAtRecordTimeInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[SynchronizerOffset]] =
    executeSqlUS(metrics.index.db.firstSynchronizerOffsetAfterOrAt)(
      eventStorageBackend.firstSynchronizerOffsetAfterOrAt(
        synchronizerId,
        afterOrAtRecordTimeInclusive.underlying,
      )
    )

  def lastSynchronizerOffsetBeforeOrAt(
      synchronizerId: SynchronizerId,
      beforeOrAtOffsetInclusive: Offset,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[SynchronizerOffset]] =
    executeSqlUS(metrics.index.db.lastSynchronizerOffsetBeforeOrAt)(
      eventStorageBackend.lastSynchronizerOffsetBeforeOrAt(
        Some(synchronizerId),
        beforeOrAtOffsetInclusive,
      )
    )

  def lastSynchronizerOffsetBeforeOrAt(
      beforeOrAtOffsetInclusive: Offset
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[SynchronizerOffset]] =
    executeSqlUS(metrics.index.db.lastSynchronizerOffsetBeforeOrAt)(
      eventStorageBackend.lastSynchronizerOffsetBeforeOrAt(None, beforeOrAtOffsetInclusive)
    )

  def synchronizerOffset(offset: Offset)(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[SynchronizerOffset]] =
    executeSqlUS(metrics.index.db.synchronizerOffset)(
      eventStorageBackend.synchronizerOffset(offset)
    )

  def firstSynchronizerOffsetAfterOrAtPublicationTime(
      afterOrAtPublicationTimeInclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[SynchronizerOffset]] =
    executeSqlUS(metrics.index.db.firstSynchronizerOffsetAfterOrAtPublicationTime)(
      eventStorageBackend.firstSynchronizerOffsetAfterOrAtPublicationTime(
        afterOrAtPublicationTimeInclusive.underlying
      )
    )

  def lastSynchronizerOffsetBeforeOrAtPublicationTime(
      beforeOrAtPublicationTimeInclusive: CantonTimestamp
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[SynchronizerOffset]] =
    executeSqlUS(metrics.index.db.lastSynchronizerOffsetBeforeOrAtPublicationTime)(
      eventStorageBackend.lastSynchronizerOffsetBeforeOrAtPublicationTime(
        beforeOrAtPublicationTimeInclusive.underlying
      )
    )

  def lastSynchronizerOffsetBeforeOrAtRecordTime(
      synchronizerId: SynchronizerId,
      beforeOrAtRecordTimeInclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[LastSynchronizerOffset]] =
    executeSqlUS(metrics.index.db.lastSynchronizerOffsetBeforeOrAtRecordTime)(connection =>
      for {
        ledgerEnd <- parameterStorageBackend.ledgerEnd(connection)
        synchronizerIndex <- parameterStorageBackend.cleanSynchronizerIndex(synchronizerId)(
          connection
        )
        lastSynchronizerOffset = eventStorageBackend.lastSynchronizerOffsetBeforeOrAtRecordTime(
          synchronizerId,
          beforeOrAtRecordTimeInclusive.underlying,
          ledgerEnd.lastOffset,
        )(connection)
      } yield LastSynchronizerOffset(
        ledgerEnd = ledgerEnd,
        syncrhonizerIndex = synchronizerIndex,
        lastSynchronizerOffset = lastSynchronizerOffset,
      )
    )

  def lastRecordTimeBeforeOrAtSynchronizerOffset(
      synchronizerId: SynchronizerId,
      beforeOrAtOffsetInclusive: Offset,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[CantonTimestamp]] =
    executeSqlUS(metrics.index.db.lastRecordTimeBeforeOrAtSynchronizerOffset)(
      eventStorageBackend.lastRecordTimeBeforeOrAtSynchronizerOffset(
        synchronizerId,
        beforeOrAtOffsetInclusive,
      )
    )

  def prunableContracts(
      fromExclusive: Option[Offset],
      toInclusive: Offset,
  )(implicit traceContext: TraceContext, ec: ExecutionContext): FutureUnlessShutdown[Set[Long]] =
    executeSqlUS(metrics.index.db.prunableContracts)(
      eventStorageBackend.prunableContracts(fromExclusive, toInclusive)
    )

  private[api] def initializeInMemoryState(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[Unit] =
    for {
      currentLedgerEnd <- ledgerEnd
      _ <- FutureUnlessShutdown.outcomeF(
        stringInterningView.update(
          currentLedgerEnd.map(_.lastStringInterningId)
        )((fromExclusive, toInclusive) =>
          executeSql(metrics.index.db.loadStringInterningEntries)(
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          )
        )
      )
    } yield {
      ledgerEndCache.set(currentLedgerEnd)
    }
}

object LedgerApiStore {
  def initialize(
      storageConfig: StorageConfig,
      storage: Option[Storage],
      ledgerParticipantId: LedgerParticipantId,
      ledgerApiDatabaseConnectionTimeout: config.NonNegativeFiniteDuration,
      ledgerApiPostgresDataSourceConfig: PostgresDataSourceConfig,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      metrics: LedgerApiServerMetrics,
      onlyForTesting_DoNotInitializeInMemoryState: Boolean = false,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContextIdlenessExecutorService,
  ): FutureUnlessShutdown[LedgerApiStore] = {
    val initializationLogger = loggerFactory.getTracedLogger(LedgerApiStore.getClass)
    val ledgerApiStorage = LedgerApiStorage
      .fromStorageConfig(storageConfig, ledgerParticipantId)
      .fold(
        error => throw new IllegalStateException(s"Constructing LedgerApiStorage failed: $error"),
        identity,
      )
    val dbConfig = DbSupport.DbConfig(
      jdbcUrl = ledgerApiStorage.jdbcUrl,
      connectionPool = DbSupport.ConnectionPoolConfig(
        connectionPoolSize = storageConfig.numConnectionsLedgerApiServer.unwrap,
        connectionTimeout = ledgerApiDatabaseConnectionTimeout.underlying,
      ),
      postgres = ledgerApiPostgresDataSourceConfig,
    )
    val numLedgerApi = dbConfig.connectionPool.connectionPoolSize
    initializationLogger.info(s"Creating ledger API storage num-ledger-api: $numLedgerApi")

    for {
      _ <- storageConfig match {
        // ledger api server needs an H2 db to run in memory
        case _: StorageConfig.Memory =>
          FutureUnlessShutdown.outcomeF(
            new FlywayMigrations(
              ledgerApiStorage.jdbcUrl,
              loggerFactory,
            ).migrate()
          )
        case _ => FutureUnlessShutdown.unit
      }
      dbSupportOwner = (storageConfig, storage) match {
        case (_: com.digitalasset.canton.config.DbConfig.H2, Some(h2DbStorage: DbStorage)) =>
          ResourceOwner.forValue(() =>
            DbSupport.forH2DbStorage(
              h2DbStorage = h2DbStorage,
              metrics = metrics,
              loggerFactory = loggerFactory,
            )
          )
        case _ =>
          DbSupport.owner(
            serverRole = ServerRole.ApiServer,
            metrics = metrics,
            dbConfig = dbConfig,
            loggerFactory = loggerFactory,
          )
      }
      ledgerApiStoreOwner = dbSupportOwner.map(dbSupport =>
        new LedgerApiStore(
          ledgerApiDbSupport = dbSupport,
          ledgerApiStorage = ledgerApiStorage,
          ledgerEndCache = MutableLedgerEndCache(),
          stringInterningView = new StringInterningView(loggerFactory),
          metrics = metrics,
          loggerFactory = loggerFactory,
          timeouts = timeouts,
        )
      )

      ledgerApiStore <- FutureUnlessShutdown.outcomeF(
        new ResourceOwnerFlagCloseableOps(ledgerApiStoreOwner).acquireFlagCloseable(
          "Ledger API DB Support"
        )
      )
      _ <-
        if (onlyForTesting_DoNotInitializeInMemoryState) FutureUnlessShutdown.unit
        else ledgerApiStore.initializeInMemoryState
    } yield ledgerApiStore
  }

  final case class LastSynchronizerOffset(
      ledgerEnd: LedgerEnd,
      syncrhonizerIndex: SynchronizerIndex,
      lastSynchronizerOffset: Option[SynchronizerOffset],
  )
}
