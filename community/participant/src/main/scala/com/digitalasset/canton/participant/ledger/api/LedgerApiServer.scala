// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.daml.ledger.api.v2.experimental_features.ExperimentalCommandInspectionService
import com.daml.ledger.api.v2.version_service.OffsetCheckpointFeature
import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.auth.*
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.NonNegativeDurationConverter.NonNegativeDurationToMillisConverter
import com.digitalasset.canton.config.{ApiLoggingConfig, ProcessingTimeout}
import com.digitalasset.canton.connection.GrpcApiInfoService
import com.digitalasset.canton.connection.v30.ApiInfoServiceGrpc
import com.digitalasset.canton.health.HealthChecks
import com.digitalasset.canton.http.HttpApiServer
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.localstore.*
import com.digitalasset.canton.ledger.participant.state.metrics.TimedSyncService
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.LifeCycle.FastCloseableChannel
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.ratelimiting.ActiveRequestCounterInterceptor
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcRequestLoggingInterceptor}
import com.digitalasset.canton.participant.config.{
  ParticipantNodeConfig,
  ParticipantStoreConfig,
  TestingTimeServiceConfig,
}
import com.digitalasset.canton.participant.extension.{
  ExtensionServiceExternalCallHandler,
  ExtensionServiceManager,
}
import com.digitalasset.canton.participant.store.ParticipantNodePersistentState
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.{
  LedgerApiServerBootstrapUtils,
  ParticipantNodeParameters,
}
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.ratelimiting.RateLimitingInterceptorFactory
import com.digitalasset.canton.platform.apiserver.services.ApiContractService
import com.digitalasset.canton.platform.apiserver.services.admin.{PartyReplicationEndpoints, Utils}
import com.digitalasset.canton.platform.apiserver.services.command.TrafficEnforcementBackend
import com.digitalasset.canton.platform.apiserver.{
  ApiServiceOwner,
  InProcessGrpcName,
  LedgerFeatures,
}
import com.digitalasset.canton.platform.config.IdentityProviderManagementConfig
import com.digitalasset.canton.platform.packages.DeduplicatingPackageLoader
import com.digitalasset.canton.platform.{
  PackagePreferenceBackend,
  ResourceCloseable,
  ResourceOwnerFlagCloseableOps,
  ResourceOwnerOps,
}
import com.digitalasset.canton.time.{RemoteClock, SimClock}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.user.store.UserManagementStore
import com.digitalasset.canton.user.{IdentityProviderId, User, UserRight}
import com.digitalasset.canton.util.ContractValidator
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.language.Ast
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.{BindableService, ServerServiceDefinition}
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.Future

class LedgerApiServer(
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
) extends ResourceCloseable
    with NamedLogging

object LedgerApiServer {
  def initialize(
      ledgerApiIndexService: LedgerApiIndexService,
      adminParty: Party,
      adminTokenDispenser: CantonAdminTokenDispenser,
      teaTokenDispenserO: Option[CantonAdminTokenDispenser],
      commandProgressTracker: CommandProgressTracker,
      config: ParticipantNodeConfig,
      httpApiMetrics: HttpApiMetrics,
      ledgerApiServerBootstrapUtils: LedgerApiServerBootstrapUtils,
      ledgerApiIndexer: LedgerApiIndexer,
      loggerFactory: NamedLoggerFactory,
      metrics: LedgerApiServerMetrics,
      parameters: ParticipantNodeParameters,
      participantId: LedgerParticipantId,
      participantNodePersistentState: ParticipantNodePersistentState,
      syncService: CantonSyncService,
      partyReplicationEndpointsO: Option[PartyReplicationEndpoints],
      trafficEnforcementBackendO: Option[TrafficEnforcementBackend],
      pruningConfig: ParticipantStoreConfig,
      warnOnJwtScopeUsage: Boolean,
      extensionServiceManagerO: Option[ExtensionServiceManager],
  )(implicit
      actorSystem: ActorSystem,
      executionContext: ExecutionContextIdlenessExecutorService,
      tracer: Tracer,
      traceContext: TraceContext,
  ): Future[LedgerApiServer] = {
    val initializationLogger = loggerFactory.getTracedLogger(LedgerApiServer.getClass)
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory)
    initializationLogger.info("Starting ledger API server.")

    val serverConfig = config.ledgerApi
    val adminTokenConfig = serverConfig.adminTokenConfig.merge(config.adminApi.adminTokenConfig)
    val maxDeduplicationDuration =
      participantNodePersistentState.settingsStore.settings.maxDeduplicationDuration
        .getOrElse(
          throw new IllegalArgumentException(s"Unknown maxDeduplicationDuration")
        )
        .toConfig

    val ledgerApiStore = participantNodePersistentState.ledgerApiStore
    val dbSupport = ledgerApiStore.ledgerApiDbSupport
    val inMemoryState = ledgerApiIndexer.inMemoryState
    val timedSyncService = new TimedSyncService(syncService, metrics)
    val userManagementStore = PersistentUserManagementStore.cached(
      dbSupport = dbSupport,
      metrics = metrics,
      timeProvider = TimeProvider.UTC,
      cacheExpiryAfterWriteInSeconds =
        serverConfig.userManagementService.cacheExpiryAfterWriteInSeconds,
      maxCacheSize = serverConfig.userManagementService.maxCacheSize,
      maxRightsPerUser = serverConfig.userManagementService.maxRightsPerUser,
      loggerFactory = loggerFactory,
      flagCloseable = ledgerApiStore,
    )
    val partyRecordStore = new PersistentPartyRecordStore(
      dbSupport = dbSupport,
      metrics = metrics,
      timeProvider = TimeProvider.UTC,
      executionContext = executionContext,
      loggerFactory = loggerFactory,
    )
    val identityProviderConfigStore = PersistentIdentityProviderConfigStore.cached(
      dbSupport = dbSupport,
      metrics = metrics,
      cacheExpiryAfterWrite =
        serverConfig.identityProviderManagement.cacheExpiryAfterWrite.underlying,
      maxIdentityProviders = IdentityProviderManagementConfig.MaxIdentityProviders,
      loggerFactory = loggerFactory,
    )

    val ledgerTestingTimeService = (config.testingTime, ledgerApiServerBootstrapUtils.clock) match {
      case (Some(TestingTimeServiceConfig.MonotonicTime), clock) =>
        Some(
          new CantonTimeServiceBackend(
            clock,
            ledgerApiServerBootstrapUtils.testingTimeService,
            loggerFactory,
          )
        )
      case (_clockNotAdvanceableThroughLedgerApi, simClock: SimClock) =>
        Some(new CantonExternalClockBackend(simClock, loggerFactory))
      case (_clockNotAdvanceableThroughLedgerApi, remoteClock: RemoteClock) =>
        Some(new CantonExternalClockBackend(remoteClock, loggerFactory))
      case _ => None
    }
    val authServices =
      if (serverConfig.authServices.isEmpty)
        List(AuthServiceWildcard)
      else
        Seq[AuthService](
          new CantonAdminTokenAuthService(
            adminTokenDispenser,
            Some(adminParty),
            adminTokenConfig,
          )
        ) ++
          teaTokenDispenserO.map(new TeaTokenAuthService(_)).toList ++
          serverConfig.authServices.map(
            _.create(
              serverConfig.jwksCacheConfig,
              serverConfig.jwtTimestampLeeway,
              loggerFactory,
              warnOnJwtScopeUsage,
              serverConfig.maxTokenLifetime,
            )
          )
    val jwtVerifierLoader =
      new CachedJwtVerifierLoader(
        cacheMaxSize = serverConfig.jwksCacheConfig.cacheMaxSize,
        cacheExpiration = serverConfig.jwksCacheConfig.cacheExpiration.underlying,
        connectionTimeout = serverConfig.jwksCacheConfig.connectionTimeout.underlying,
        readTimeout = serverConfig.jwksCacheConfig.readTimeout.underlying,
        jwtTimestampLeeway = serverConfig.jwtTimestampLeeway,
        maxTokenLife = serverConfig.maxTokenLifetime.toMillisOrNone(),
        autoRefreshAfter = serverConfig.jwksCacheConfig.autoRefreshAfter.underlying,
        metrics = Some(metrics.identityProviderConfigStore.verifierCache),
        loggerFactory = loggerFactory,
      )
    val apiInfoService = new GrpcApiInfoService(CantonGrpcUtil.ApiName.LedgerApi)
      with BindableService {
      override def bindService(): ServerServiceDefinition =
        ApiInfoServiceGrpc.bindService(this, executionContext)
    }
    val packageLoader = new DeduplicatingPackageLoader()
    val packageResolver: PackageResolver = new PackageResolver {
      override protected def resolveInternal(packageId: PackageId)(implicit
          traceContext: TraceContext
      ): FutureUnlessShutdown[Option[Ast.Package]] =
        FutureUnlessShutdown.outcomeF(
          packageLoader.loadPackage(
            packageId = packageId,
            delegate = packageId => timedSyncService.getLfArchive(packageId)(traceContext),
            metric = metrics.index.db.translation.getLfPackage,
          )
        )
    }
    val contractValidator = ContractValidator(
      syncService.pureCryptoApi,
      ledgerApiServerBootstrapUtils.engine,
      packageResolver,
    )
    def lookupTopologyClient(synchronizerId: SynchronizerId) =
      syncService
        .activePsidForLsid(synchronizerId)
        .flatMap(psid => syncService.lookupTopologyClient(psid))
    def lookupSynchronizerCryptoClient(synchronizerId: SynchronizerId) =
      for {
        psid <- syncService.activePsidForLsid(synchronizerId)
        params <- syncService.syncPersistentStateManager.staticSynchronizerParameters(psid)
        syncCryptoClient <- syncService.syncCrypto.forSynchronizer(psid, params)
      } yield syncCryptoClient
    // TODO(i21582) The prepare endpoint of the interactive submission service does not suffix
    // contract IDs of the transaction yet. This means enrichment of the transaction may fail
    // when processing unsuffixed contract IDs. For that reason we disable this requirement via the flag below.
    // When CIDs are suffixed, we can re-use the LfValueTranslation from the index service created above
    val interactiveSubmissionEnricher = new InteractiveSubmissionEnricher(
      new Engine(
        ledgerApiServerBootstrapUtils.engine.config.copy(forbidLocalContractIds = false),
        loggerFactory,
      ),
      packageResolver = packageResolver,
    )
    val apiContractService = new ApiContractService(
      ledgerApiContractStore = ledgerApiIndexer.contractStore,
      lfValueTranslation = ledgerApiIndexService.lfValueTranslation,
      loggerFactory = loggerFactory,
    )
    val externalCallHandler = ExtensionServiceExternalCallHandler.create(extensionServiceManagerO)
    val interceptors = List(
      new GrpcRequestLoggingInterceptor(
        loggerFactory,
        parameters.loggingConfig.api,
      ),
      TraceContextGrpc.reportingServerInterceptor(tracer),
    ) ::: (
      serverConfig.rateLimit
        .map(rateLimit =>
          RateLimitingInterceptorFactory.create(
            loggerFactory = loggerFactory,
            config = rateLimit,
          )
        )
        .toList
    ) ::: (
      serverConfig.limits
        .map(cfg =>
          new ActiveRequestCounterInterceptor(
            "ledger-api",
            cfg.active,
            cfg.warnOnUndefinedLimits,
            cfg.throttleLoggingRatePerSecond,
            metrics.requests,
            loggerFactory,
          )
        )
        .toList
    )
    val ledgerFeatures = LedgerFeatures(
      staticTime = ledgerTestingTimeService.isDefined,
      commandInspectionService =
        ExperimentalCommandInspectionService.of(supported = serverConfig.enableCommandInspection),
      offsetCheckpointFeature = OffsetCheckpointFeature.of(
        maxOffsetCheckpointEmissionDelay = Some(
          (serverConfig.indexService.offsetCheckpointCacheUpdateInterval + serverConfig.indexService.idleStreamOffsetCheckpointTimeout).toProtoPrimitive
        )
      ),
      topologyAwarePackageSelection = serverConfig.topologyAwarePackageSelection.enabled,
      tapsMaxPassesDefault = serverConfig.topologyAwarePackageSelection.maxPassesDefault,
      tapsMaxPassesLimit = serverConfig.topologyAwarePackageSelection.maxPassesLimit,
    )
    val healthChecks = new HealthChecks(
      // TODO(i21015): Possible issues with health check reporting: disconnected sequencer can be reported as healthy; possibly reporting protocol processing/CantonSyncService general health needed
      "write" -> (() => syncService.currentWriteHealth()),
      "indexer" -> ledgerApiIndexer.indexerHealth,
    )

    val createAdditionalAdminUserIfNeeded =
      serverConfig.userManagementService.additionalAdminUserId
        .fold(ResourceOwner.unit) { rawUserId =>
          ResourceOwner.forFuture { () =>
            val userId = Ref.UserId.assertFromString(rawUserId)
            userManagementStore
              .createUser(
                user = User(
                  id = userId,
                  primaryParty = None,
                  identityProviderId = IdentityProviderId.Default,
                ),
                rights = Set(UserRight.ParticipantAdmin),
              )
              .flatMap {
                case Left(UserManagementStore.UserExists(_)) =>
                  initializationLogger.info(
                    s"Creating admin user with id $userId failed. User with this id already exists"
                  )
                  Future.unit
                case other =>
                  Utils
                    .handleResult("creating extra admin user")(other)(
                      ErrorLoggingContext(initializationLogger, implicitly)
                    )
                    .map(_ => ())
              }
          }
        }

    def startHttpApiIfEnabled(
        authInterceptor: AuthInterceptor,
        packagePreferenceBackend: PackagePreferenceBackend,
        apiLoggingConfig: ApiLoggingConfig,
    ): ResourceOwner[Unit] =
      if (!config.httpLedgerApi.enabled)
        ResourceOwner.unit
      else
        for {
          channel <- ResourceOwner
            .forReleasable(() =>
              InProcessChannelBuilder
                .forName(InProcessGrpcName.forPort(serverConfig.clientConfig.port))
                .executor(executionContext.execute(_))
                .build()
            )(channel =>
              Future(
                new FastCloseableChannel(channel, initializationLogger, "JSON-API").close()
              )
            )
            .afterReleased(initializationLogger.info("JSON-API gRPC channel is released"))
          _ <- HttpApiServer(
            config = config.httpLedgerApi.copy(
              maxInboundMessageSize = config.httpLedgerApi.maxInboundMessageSize.orElse(
                Some(serverConfig.maxInboundMessageSize)
              )
            ),
            httpsConfiguration = serverConfig.tls,
            channel = channel,
            packageSyncService = timedSyncService,
            loggerFactory = loggerFactory,
            authInterceptor = authInterceptor,
            packagePreferenceBackend = packagePreferenceBackend,
            trafficEnforcementEnabled = trafficEnforcementBackendO.isDefined,
            apiLoggingConfig = apiLoggingConfig,
          )(httpApiMetrics)
            .afterReleased(initializationLogger.info("JSON-API HTTP Server is released"))
        } yield ()

    initializationLogger.debug(
      s"Ledger API Server is initializing with ledgerApiStore=$ledgerApiStore, ledgerApiIndexer=$ledgerApiIndexer, dbSupport=$dbSupport, inMemoryState=$inMemoryState"
    )

    (for {
      _ <- createAdditionalAdminUserIfNeeded
      (_, authInterceptor) <- ApiServiceOwner(
        indexService = ledgerApiIndexService.indexService,
        transactionSubmissionTracker = inMemoryState.transactionSubmissionTracker,
        reassignmentSubmissionTracker = inMemoryState.reassignmentSubmissionTracker,
        partyAllocationTracker = inMemoryState.partyAllocationTracker,
        commandProgressTracker = commandProgressTracker,
        userManagementStore = userManagementStore,
        identityProviderConfigStore = identityProviderConfigStore,
        partyRecordStore = partyRecordStore,
        participantId = participantId,
        command = serverConfig.commandService,
        managementServiceTimeout = serverConfig.managementServiceTimeout,
        userManagement = serverConfig.userManagementService,
        partyManagementServiceConfig = serverConfig.partyManagementService,
        packageServiceConfig = serverConfig.packageService,
        updateServiceConfig = serverConfig.updateService,
        stateServiceConfig = serverConfig.stateService,
        tls = serverConfig.tls,
        address = Some(serverConfig.address),
        maxInboundMessageSize = serverConfig.maxInboundMessageSize.unwrap,
        maxInboundMetadataSize = serverConfig.maxInboundMetadataSize.unwrap,
        maxConcurrentCallsPerConnection = serverConfig.maxConcurrentCallsPerConnection.unwrap,
        port = serverConfig.port,
        syncService = timedSyncService,
        partyReplicationEndpointsO = partyReplicationEndpointsO,
        healthChecks = healthChecks,
        metrics = metrics,
        timeServiceBackend = ledgerTestingTimeService,
        otherServices = Seq(apiInfoService),
        otherInterceptors = interceptors,
        engine = ledgerApiServerBootstrapUtils.engine,
        queryExecutionContext = ledgerApiIndexService.queryExecutionContext,
        commandExecutionContext = executionContext,
        checkOverloaded = syncService.checkOverloaded,
        ledgerFeatures = ledgerFeatures,
        maxDeduplicationDuration = maxDeduplicationDuration,
        authServices = authServices,
        jwtVerifierLoader = jwtVerifierLoader,
        jwtTimestampLeeway = serverConfig.jwtTimestampLeeway,
        tokenExpiryGracePeriodForStreams =
          parameters.ledgerApiServerParameters.tokenExpiryGracePeriodForStreams,
        loggerFactory = loggerFactory,
        contractAuthenticator = contractValidator.authenticateHash,
        dynParamGetter = syncService.dynamicSynchronizerParameterGetter,
        interactiveSubmissionServiceConfig = serverConfig.interactiveSubmissionService,
        interactiveSubmissionEnricher = interactiveSubmissionEnricher,
        keepAlive = serverConfig.keepAliveServer,
        packagePreferenceBackend = ledgerApiIndexService.packagePreferenceBackend,
        apiLoggingConfig = parameters.loggingConfig.api,
        apiContractService = apiContractService,
        safeToPruneCommitmentState = pruningConfig.safeToPruneCommitmentState,
        trafficEnforcementBackendO = trafficEnforcementBackendO,
        externalCallHandler = externalCallHandler,
        lookupTopologyClient = lookupTopologyClient,
        lookupSynchronizerCryptoClient = lookupSynchronizerCryptoClient,
        pureCryptoApi = syncService.pureCryptoApi,
      )
      _ <- startHttpApiIfEnabled(
        authInterceptor,
        ledgerApiIndexService.packagePreferenceBackend,
        parameters.loggingConfig.api,
      )
    } yield new LedgerApiServer(
      timeouts = parameters.processingTimeouts,
      loggerFactory = loggerFactory,
    ))
      .acquireFlagCloseable("Ledger API Server")
  }

  sealed trait LedgerApiServerError extends Product with Serializable with PrettyPrinting {
    protected def errorMessage: String = ""
    def cause: Throwable
    def asRuntimeException(additionalMessage: String = ""): RuntimeException =
      new RuntimeException(
        if (additionalMessage.isEmpty) errorMessage else s"$additionalMessage $errorMessage",
        cause,
      )
  }

  sealed trait LedgerApiServerErrorWithoutCause extends LedgerApiServerError {
    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    override def cause: Throwable = null
  }

  final case class FailedToConfigureLedgerApiStorage(override protected val errorMessage: String)
      extends LedgerApiServerErrorWithoutCause {
    override protected def pretty: Pretty[FailedToConfigureLedgerApiStorage] =
      prettyOfClass(unnamedParam(_.errorMessage.unquoted))
  }
}
