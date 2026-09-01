// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.daml.executors.InstrumentedExecutors
import com.daml.executors.executors.NamedExecutionContextExecutorService
import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.participant.state.index.IndexService
import com.digitalasset.canton.ledger.participant.state.metrics.TimedSyncService
import com.digitalasset.canton.ledger.participant.state.{
  InternalIndexService,
  InternalIndexServiceImpl,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.{
  LedgerApiServerBootstrapUtils,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.platform.config.{IndexServiceConfig, UpdateServiceConfig}
import com.digitalasset.canton.platform.index.IndexServiceOwner
import com.digitalasset.canton.platform.index.IndexServiceOwner.GetPackagePreferenceForViewsUpgrading
import com.digitalasset.canton.platform.store.dao.events.{ContractLoader, LfValueTranslation}
import com.digitalasset.canton.platform.{
  PackagePreferenceBackend,
  ResourceCloseable,
  ResourceOwnerFlagCloseableOps,
  ResourceOwnerOps,
}
import com.digitalasset.canton.tracing.{TraceContext, TracerProvider}
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId}
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer

import scala.concurrent.Future

class LedgerApiIndexService(
    val queryExecutionContext: NamedExecutionContextExecutorService,
    val packagePreferenceBackend: PackagePreferenceBackend,
    val lfValueTranslation: LfValueTranslation,
    val indexService: IndexService,
    val internalIndexService: InternalIndexService,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
) extends ResourceCloseable
    with NamedLogging

object LedgerApiIndexService {
  def initialize(
      config: ParticipantNodeConfig,
      ledgerApiServerBootstrapUtils: LedgerApiServerBootstrapUtils,
      ledgerApiIndexer: LedgerApiIndexer,
      loggerFactory: NamedLoggerFactory,
      metrics: LedgerApiServerMetrics,
      parameters: ParticipantNodeParameters,
      participantId: LedgerParticipantId,
      syncService: CantonSyncService,
      tracerProvider: TracerProvider,
      updateServiceConfig: UpdateServiceConfig,
  )(implicit
      actorSystem: ActorSystem,
      executionContext: ExecutionContextIdlenessExecutorService,
      traceContext: TraceContext,
  ): Future[LedgerApiIndexService] = {
    val initializationLogger = loggerFactory.getTracedLogger(LedgerApiIndexService.getClass)
    val initializationNonTracedLogger = loggerFactory.getLogger(LedgerApiIndexService.getClass)
    initializationLogger.info("Starting ledger API Index Service.")

    val serverConfig = config.ledgerApi
    val indexServiceConfig = serverConfig.indexService

    val ledgerApiStore = ledgerApiIndexer.ledgerApiStore
    val dbSupport = ledgerApiStore.ledgerApiDbSupport
    val inMemoryState = ledgerApiIndexer.inMemoryState
    val timedSyncService = new TimedSyncService(syncService, metrics)
    val ledgerApiContractStore = ledgerApiIndexer.contractStore
    val packagePreferenceBackend = new PackagePreferenceBackend(
      clock = ledgerApiServerBootstrapUtils.clock,
      adminParty = LfPartyId.assertFromString(participantId),
      syncService = timedSyncService,
      loggerFactory = loggerFactory,
    )
    val lfValueTranslation = new LfValueTranslation(
      metrics = metrics,
      engineO = Some(ledgerApiServerBootstrapUtils.engine),
      loadPackage = (packageId, traceContext) =>
        timedSyncService.getLfArchive(packageId)(traceContext),
      loggerFactory = loggerFactory,
    )
    val getPackagePreferenceForUpgrading: GetPackagePreferenceForViewsUpgrading =
      (
          packageName: PackageName,
          candidatePackageIds: Set[PackageId],
          candidatePackageIdsDescription: String,
          loggingContext: LoggingContextWithTrace,
      ) =>
        packagePreferenceBackend
          .getPreferredPackageVersionForParticipant(
            packageName,
            candidatePackageIds,
            candidatePackageIdsDescription,
          )(
            loggingContext.traceContext
          )

    initializationLogger.debug(
      s"Ledger API Index Service Server is initializing with ledgerApiStore=$ledgerApiStore, ledgerApiIndexer=$ledgerApiIndexer, dbSupport=$dbSupport, inMemoryState=$inMemoryState"
    )

    (for {
      contractLoader <- {
        import parameters.ledgerApiServerParameters.contractLoader.*
        ContractLoader
          .create(
            participantContractStore = ledgerApiContractStore,
            contractStorageBackend = dbSupport.storageBackendFactory.createContractStorageBackend(
              inMemoryState.stringInterningView,
              inMemoryState.ledgerEndCache,
            ),
            dbDispatcher = dbSupport.dbDispatcher,
            metrics = metrics,
            maxQueueSize = maxQueueSize.value,
            maxBatchSize = maxBatchSize.value,
            parallelism = parallelism.value,
            loggerFactory = loggerFactory,
          )
          .afterReleased(initializationLogger.info("ContractLoader released"))
      }
      queryExecutionContext <- ResourceOwner
        .forExecutorService(() =>
          InstrumentedExecutors.newWorkStealingExecutor(
            name = metrics.lapi.threadpool.apiQueryServices.toString,
            parallelism = indexServiceConfig.apiQueryServicesThreadPoolSize.getOrElse(
              IndexServiceConfig.DefaultQueryServicesThreadPoolSize(initializationNonTracedLogger)
            ),
          )
        )
        .afterReleased(initializationLogger.info("ReadApiServiceExecutionContext released"))
      indexService <- new IndexServiceOwner(
        dbSupport = dbSupport,
        config = indexServiceConfig,
        participantId = participantId,
        metrics = metrics,
        inMemoryState = inMemoryState,
        tracer = tracerProvider.tracer,
        loggerFactory = loggerFactory,
        incompleteOffsets = (off, ps, tc) =>
          timedSyncService.incompleteReassignmentOffsets(off, ps.getOrElse(Set.empty))(tc),
        contractLoader = contractLoader,
        getPackageMetadataSnapshot = timedSyncService.getPackageMetadataSnapshot(_),
        lfValueTranslation = lfValueTranslation,
        queryExecutionContext = queryExecutionContext,
        commandExecutionContext = executionContext,
        getPackagePreference = getPackagePreferenceForUpgrading,
        participantContractStore = ledgerApiContractStore,
        materializer = implicitly[Materializer],
        updateServiceConfig = updateServiceConfig,
        scheduler = actorSystem.scheduler,
      )
    } yield new LedgerApiIndexService(
      queryExecutionContext = queryExecutionContext,
      packagePreferenceBackend = packagePreferenceBackend,
      lfValueTranslation = lfValueTranslation,
      indexService = indexService,
      internalIndexService = new InternalIndexServiceImpl(indexService),
      timeouts = parameters.processingTimeouts,
      loggerFactory = loggerFactory,
    ))
      .acquireFlagCloseable("Ledger API Index Service")
  }

}
