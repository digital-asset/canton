// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import com.daml.executors.InstrumentedExecutors
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.daml.resources.ProgramResource.StartupException
import com.daml.timer.RetryStrategy
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.error.IndexErrors.IndexDbException
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexService
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.InMemoryState
import com.digitalasset.canton.platform.apiserver.TimedIndexService
import com.digitalasset.canton.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.digitalasset.canton.platform.configuration.IndexServiceConfig
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.cache.*
import com.digitalasset.canton.platform.store.dao.events.{
  BufferedTransactionsReader,
  LfValueTranslation,
}
import com.digitalasset.canton.platform.store.dao.{
  BufferedCommandCompletionsReader,
  JdbcLedgerDao,
  LedgerReadDao,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.control.NoStackTrace

final class IndexServiceOwner(
    config: IndexServiceConfig,
    dbSupport: DbSupport,
    initialLedgerId: LedgerId,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    engine: Engine,
    participantId: Ref.ParticipantId,
    inMemoryState: InMemoryState,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
    incompleteOffsets: (Offset, Set[Ref.Party], TraceContext) => Future[Vector[Offset]],
) extends ResourceOwner[IndexService]
    with NamedLogging {
  private val initializationRetryDelay = 100.millis
  private val initializationMaxAttempts = 3000 // give up after 5min

  def acquire()(implicit context: ResourceContext): Resource[IndexService] = {
    val ledgerDao = createLedgerReadDao(
      ledgerEndCache = inMemoryState.ledgerEndCache,
      stringInterning = inMemoryState.stringInterningView,
    )

    for {
      ledgerId <- Resource.fromFuture(verifyLedgerId(ledgerDao))
      _ <- Resource.fromFuture(waitForInMemoryStateInitialization())

      contractStore = new MutableCacheBackedContractStore(
        metrics,
        ledgerDao.contractsReader,
        contractStateCaches = inMemoryState.contractStateCaches,
        loggerFactory = loggerFactory,
      )(servicesExecutionContext)

      lfValueTranslation = new LfValueTranslation(
        metrics = metrics,
        engineO = Some(engine),
        loadPackage = (packageId, loggingContext) =>
          ledgerDao.getLfArchive(packageId)(loggingContext),
        loggerFactory = loggerFactory,
      )

      inMemoryFanOutExecutionContext <- buildInMemoryFanOutExecutionContext(
        metrics = metrics,
        threadPoolSize = config.inMemoryFanOutThreadPoolSize,
      ).acquire()

      bufferedTransactionsReader = BufferedTransactionsReader(
        delegate = ledgerDao.transactionsReader,
        transactionsBuffer = inMemoryState.inMemoryFanoutBuffer,
        lfValueTranslation = lfValueTranslation,
        metrics = metrics,
        eventProcessingParallelism = config.bufferedEventsProcessingParallelism,
      )(inMemoryFanOutExecutionContext)

      bufferedCommandCompletionsReader = BufferedCommandCompletionsReader(
        inMemoryFanoutBuffer = inMemoryState.inMemoryFanoutBuffer,
        delegate = ledgerDao.completions,
        metrics = metrics,
      )(inMemoryFanOutExecutionContext)

      indexService = new IndexServiceImpl(
        ledgerId = ledgerId,
        participantId = participantId,
        ledgerDao = ledgerDao,
        transactionsReader = bufferedTransactionsReader,
        commandCompletionsReader = bufferedCommandCompletionsReader,
        contractStore = contractStore,
        pruneBuffers = inMemoryState.inMemoryFanoutBuffer.prune,
        dispatcher = () => inMemoryState.dispatcherState.getDispatcher,
        packageMetadataView = inMemoryState.packageMetadataView,
        metrics = metrics,
        loggerFactory = loggerFactory,
      )
    } yield new TimedIndexService(indexService, metrics)
  }

  private def waitForInMemoryStateInitialization()(implicit
      executionContext: ExecutionContext
  ): Future[Unit] =
    RetryStrategy.constant(
      attempts = Some(initializationMaxAttempts),
      waitTime = initializationRetryDelay,
    ) { case InMemoryStateNotInitialized => true } { (attempt, _) =>
      if (!inMemoryState.initialized) {
        logger.info(
          s"Participant in-memory state not initialized on attempt $attempt/$initializationMaxAttempts. Retrying again in $initializationRetryDelay."
        )(TraceContext.empty)
        Future.failed(InMemoryStateNotInitialized)
      } else {
        Future.unit
      }
    }

  private def verifyLedgerId(
      ledgerDao: LedgerReadDao
  )(implicit
      executionContext: ExecutionContext
  ): Future[LedgerId] = {
    // If the index database is not yet fully initialized,
    // querying for the ledger ID will throw different errors,
    // depending on the database, and how far the initialization is.
    val isRetryable: PartialFunction[Throwable, Boolean] = {
      case _: IndexDbException => true
      case _: LedgerIdNotFoundException => true
      case _: MismatchException.LedgerId => false
      case _ => false
    }

    RetryStrategy.constant(
      attempts = Some(initializationMaxAttempts),
      waitTime = initializationRetryDelay,
    )(isRetryable) { (attempt, _) =>
      implicit val loggingContext: LoggingContextWithTrace =
        LoggingContextWithTrace(loggerFactory)(TraceContext.empty)
      ledgerDao
        .lookupLedgerId()
        .flatMap {
          case Some(`initialLedgerId`) =>
            logger.info(s"Found existing ledger with ID: $initialLedgerId")
            Future.successful(initialLedgerId)
          case Some(foundLedgerId) =>
            Future.failed(
              new MismatchException.LedgerId(foundLedgerId, initialLedgerId) with StartupException
            )
          case None =>
            logger.info(
              s"Ledger ID not found in the index database on attempt $attempt/$initializationMaxAttempts. Retrying again in $initializationRetryDelay."
            )
            Future.failed(new LedgerIdNotFoundException(attempt))
        }
    }
  }

  private def createLedgerReadDao(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): LedgerReadDao =
    JdbcLedgerDao.read(
      dbSupport = dbSupport,
      servicesExecutionContext = servicesExecutionContext,
      metrics = metrics,
      engine = Some(engine),
      participantId = participantId,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      completionsPageSize = config.completionsPageSize,
      acsStreamsConfig = config.acsStreams,
      transactionFlatStreamsConfig = config.transactionFlatStreams,
      transactionTreeStreamsConfig = config.transactionTreeStreams,
      globalMaxEventIdQueries = config.globalMaxEventIdQueries,
      globalMaxEventPayloadQueries = config.globalMaxEventPayloadQueries,
      tracer = tracer,
      loggerFactory = loggerFactory,
      incompleteOffsets = incompleteOffsets,
    )

  private def buildInMemoryFanOutExecutionContext(
      metrics: Metrics,
      threadPoolSize: Int,
  ): ResourceOwner[ExecutionContextExecutorService] =
    ResourceOwner
      .forExecutorService(() =>
        InstrumentedExecutors.newWorkStealingExecutor(
          metrics.daml.lapi.threadpool.inMemoryFanOut.toString,
          threadPoolSize,
          metrics.executorServiceMetrics,
        )
      )

  private object InMemoryStateNotInitialized extends NoStackTrace
}
