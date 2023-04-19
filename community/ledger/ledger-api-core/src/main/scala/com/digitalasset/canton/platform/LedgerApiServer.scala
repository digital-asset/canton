// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.executors.executors.{NamedExecutor, QueueAwareExecutor}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.metrics.Metrics
import com.daml.tracing.Telemetry
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.ledger.api.auth.{AuthService, JwtVerifierLoader}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.configuration.LedgerId
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexService
import com.digitalasset.canton.ledger.participant.state.v2.metrics.{
  TimedReadService,
  TimedWriteService,
}
import com.digitalasset.canton.ledger.participant.state.v2.{ReadService, WriteService}
import com.digitalasset.canton.platform.apiserver.*
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.platform.apiserver.ratelimiting.RateLimitingInterceptor
import com.digitalasset.canton.platform.config.ParticipantConfig
import com.digitalasset.canton.platform.configuration.{IndexServiceConfig, ServerRole}
import com.digitalasset.canton.platform.index.{InMemoryStateUpdater, IndexServiceOwner}
import com.digitalasset.canton.platform.indexer.IndexerServiceOwner
import com.digitalasset.canton.platform.localstore.*
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.DbSupport.ParticipantDataSourceConfig
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

class LedgerApiServer(
    authService: AuthService,
    jwtVerifierLoader: JwtVerifierLoader,
    buildWriteService: IndexService => ResourceOwner[WriteService],
    engine: Engine,
    authorityResolver: AuthorityResolver,
    ledgerFeatures: LedgerFeatures,
    ledgerId: LedgerId,
    participantConfig: ParticipantConfig,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    participantId: Ref.ParticipantId,
    readService: ReadService,
    timeServiceBackendO: Option[TimeServiceBackend],
    servicesExecutionContext: ExecutionContextExecutorService,
    metrics: Metrics,
    // TODO(i11145) ED: Remove flag once explicit disclosure is deemed stable and all
    //          backing ledgers implement proper validation against malicious clients.
    //          Currently, we provide this flag outside the HOCON configuration objects
    //          in order to ensure that participants cannot be configured to accept explicitly disclosed contracts.
    explicitDisclosureUnsafeEnabled: Boolean = false,
    rateLimitingInterceptor: Option[
      QueueAwareExecutor with NamedExecutor => RateLimitingInterceptor
    ] = None,
    telemetry: Telemetry,
    tracer: Tracer,
)(implicit actorSystem: ActorSystem, materializer: Materializer) {

  def owner: ResourceOwner[ApiService] = {
    newLoggingContextWith("participantId" -> participantId) { implicit loggingContext =>
      for {
        (inMemoryState, inMemoryStateUpdaterFlow) <-
          LedgerApiServer.createInMemoryStateAndUpdater(
            participantConfig.indexService,
            metrics,
            servicesExecutionContext,
          )

        timedReadService = new TimedReadService(readService, metrics)
        indexerHealthChecks <-
          for {
            indexerHealth <- new IndexerServiceOwner(
              participantId = participantId,
              participantDataSourceConfig = participantDataSourceConfig,
              readService = timedReadService,
              config = participantConfig.indexer,
              metrics = metrics,
              inMemoryState = inMemoryState,
              inMemoryStateUpdaterFlow = inMemoryStateUpdaterFlow,
              executionContext = servicesExecutionContext,
            )
          } yield new HealthChecks(
            "read" -> timedReadService,
            "indexer" -> indexerHealth,
          )

        readDbSupport <- DbSupport
          .owner(
            serverRole = ServerRole.ApiServer,
            metrics = metrics,
            dbConfig = participantConfig.dataSourceProperties.createDbConfig(
              participantDataSourceConfig
            ),
          )

        // TODO(i12284): Add test asserting that the indexService retries until IndexDB persistence comes up
        indexService <- new IndexServiceOwner(
          config = participantConfig.indexService,
          dbSupport = readDbSupport,
          initialLedgerId = domain.LedgerId(ledgerId),
          metrics = metrics,
          engine = engine,
          servicesExecutionContext = servicesExecutionContext,
          participantId = participantId,
          inMemoryState = inMemoryState,
          tracer = tracer,
        )(loggingContext)

        writeService <- buildWriteService(indexService)

        apiService <- buildApiService(
          ledgerFeatures,
          engine,
          authorityResolver,
          indexService,
          metrics,
          servicesExecutionContext,
          new TimedWriteService(writeService, metrics),
          indexerHealthChecks,
          timeServiceBackendO,
          readDbSupport,
          ledgerId,
          participantConfig.apiServer,
          participantId,
          explicitDisclosureUnsafeEnabled,
          jwtVerifierLoader,
          telemetry = telemetry,
        )
      } yield apiService
    }
  }

  private def buildApiService(
      ledgerFeatures: LedgerFeatures,
      sharedEngine: Engine,
      authorityResolver: AuthorityResolver,
      indexService: IndexService,
      metrics: Metrics,
      servicesExecutionContext: ExecutionContextExecutorService,
      writeService: WriteService,
      healthChecksWithIndexer: HealthChecks,
      timeServiceBackend: Option[TimeServiceBackend],
      dbSupport: DbSupport,
      ledgerId: LedgerId,
      apiServerConfig: ApiServerConfig,
      participantId: Ref.ParticipantId,
      explicitDisclosureUnsafeEnabled: Boolean,
      jwtVerifierLoader: JwtVerifierLoader,
      telemetry: Telemetry,
  )(implicit
      actorSystem: ActorSystem,
      loggingContext: LoggingContext,
  ): ResourceOwner[ApiService] = {
    val identityProviderStore =
      PersistentIdentityProviderConfigStore.cached(
        dbSupport = dbSupport,
        metrics = metrics,
        cacheExpiryAfterWrite = apiServerConfig.identityProviderManagement.cacheExpiryAfterWrite,
        maxIdentityProviders = IdentityProviderManagementConfig.MaxIdentityProviders,
      )(servicesExecutionContext, loggingContext)

    val healthChecks = healthChecksWithIndexer + ("write" -> writeService)
    metrics.daml.health
      .registerHealthGauge("ledger-api", () => healthChecks.isHealthy(None))
      .discard

    ApiServiceOwner(
      indexService = indexService,
      ledgerId = ledgerId,
      config = apiServerConfig,
      optWriteService = Some(writeService),
      healthChecks = healthChecks,
      metrics = metrics,
      timeServiceBackend = timeServiceBackend,
      otherInterceptors =
        rateLimitingInterceptor.map(provider => provider(dbSupport.dbDispatcher.executor)).toList,
      engine = sharedEngine,
      authorityResolver = authorityResolver,
      servicesExecutionContext = servicesExecutionContext,
      userManagementStore = PersistentUserManagementStore.cached(
        dbSupport = dbSupport,
        metrics = metrics,
        cacheExpiryAfterWriteInSeconds =
          apiServerConfig.userManagement.cacheExpiryAfterWriteInSeconds,
        maxCacheSize = apiServerConfig.userManagement.maxCacheSize,
        maxRightsPerUser = apiServerConfig.userManagement.maxRightsPerUser,
        timeProvider = TimeProvider.UTC,
      )(servicesExecutionContext, loggingContext),
      identityProviderConfigStore = identityProviderStore,
      partyRecordStore = new PersistentPartyRecordStore(
        dbSupport = dbSupport,
        metrics = metrics,
        timeProvider = TimeProvider.UTC,
        executionContext = servicesExecutionContext,
      ),
      ledgerFeatures = ledgerFeatures,
      participantId = participantId,
      authService = authService,
      jwtVerifierLoader = jwtVerifierLoader,
      jwtTimestampLeeway = participantConfig.jwtTimestampLeeway,
      explicitDisclosureUnsafeEnabled = explicitDisclosureUnsafeEnabled,
      telemetry = telemetry,
    )
  }
}

object LedgerApiServer {
  def createInMemoryStateAndUpdater(
      indexServiceConfig: IndexServiceConfig,
      metrics: Metrics,
      executionContext: ExecutionContext,
  )(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[(InMemoryState, InMemoryStateUpdater.UpdaterFlow)] =
    for {
      inMemoryState <- InMemoryState.owner(
        apiStreamShutdownTimeout = indexServiceConfig.apiStreamShutdownTimeout,
        bufferedStreamsPageSize = indexServiceConfig.bufferedStreamsPageSize,
        maxContractStateCacheSize = indexServiceConfig.maxContractStateCacheSize,
        maxContractKeyStateCacheSize = indexServiceConfig.maxContractKeyStateCacheSize,
        maxTransactionsInMemoryFanOutBufferSize =
          indexServiceConfig.maxTransactionsInMemoryFanOutBufferSize,
        executionContext = executionContext,
        metrics = metrics,
      )

      inMemoryStateUpdater <- InMemoryStateUpdater.owner(
        inMemoryState = inMemoryState,
        prepareUpdatesParallelism = indexServiceConfig.inMemoryStateUpdaterParallelism,
        preparePackageMetadataTimeOutWarning =
          indexServiceConfig.preparePackageMetadataTimeOutWarning,
        metrics = metrics,
      )
    } yield inMemoryState -> inMemoryStateUpdater
}