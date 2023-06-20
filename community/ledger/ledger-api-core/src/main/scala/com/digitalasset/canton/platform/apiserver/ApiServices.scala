// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine.*
import com.daml.metrics.Metrics
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.SubmissionIdGenerator
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.auth.services.*
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.{GrpcHealthService, GrpcTransactionService}
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.participant.state.index.v2.*
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.configuration.{
  LedgerConfigurationInitializer,
  LedgerConfigurationSubscription,
}
import com.digitalasset.canton.platform.apiserver.execution.*
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.services.*
import com.digitalasset.canton.platform.apiserver.services.admin.*
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.apiserver.services.transaction.{
  ApiEventQueryService,
  ApiTransactionService,
}
import com.digitalasset.canton.platform.configuration.{
  CommandConfiguration,
  InitialLedgerConfiguration,
}
import com.digitalasset.canton.platform.localstore.UserManagementConfig
import com.digitalasset.canton.platform.localstore.api.{
  IdentityProviderConfigStore,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.platform.services.time.TimeProviderType
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.BindableService
import io.grpc.protobuf.services.ProtoReflectionService

import java.time.Instant
import scala.collection.immutable
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

trait ApiServices {
  val services: Iterable[BindableService]

  def withServices(otherServices: immutable.Seq[BindableService]): ApiServices
}

private final case class ApiServicesBundle(services: immutable.Seq[BindableService])
    extends ApiServices {

  override def withServices(otherServices: immutable.Seq[BindableService]): ApiServices =
    copy(services = services ++ otherServices)

}

object ApiServices {

  final class Owner(
      participantId: Ref.ParticipantId,
      optWriteService: Option[state.WriteService],
      indexService: IndexService,
      userManagementStore: UserManagementStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
      partyRecordStore: PartyRecordStore,
      authorizer: Authorizer,
      engine: Engine,
      authorityResolver: AuthorityResolver,
      timeProvider: TimeProvider,
      timeProviderType: TimeProviderType,
      submissionTracker: SubmissionTracker,
      configurationLoadTimeout: Duration,
      initialLedgerConfiguration: Option[InitialLedgerConfiguration],
      commandConfig: CommandConfiguration,
      optTimeServiceBackend: Option[TimeServiceBackend],
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      healthChecks: HealthChecks,
      seedService: SeedService,
      managementServiceTimeout: FiniteDuration,
      checkOverloaded: TraceContext => Option[state.SubmissionResult],
      ledgerFeatures: LedgerFeatures,
      userManagementConfig: UserManagementConfig,
      apiStreamShutdownTimeout: scala.concurrent.duration.Duration,
      meteringReportKey: MeteringReportKey,
      explicitDisclosureUnsafeEnabled: Boolean,
      createExternalServices: () => List[BindableService] = () => Nil,
      telemetry: Telemetry,
      val loggerFactory: NamedLoggerFactory,
      multiDomainEnabled: Boolean,
  )(implicit
      materializer: Materializer,
      esf: ExecutionSequencerFactory,
  ) extends ResourceOwner[ApiServices]
      with NamedLogging {
    private val configurationService: IndexConfigurationService = indexService
    private val identityService: IdentityProvider = indexService
    private val packagesService: IndexPackagesService = indexService
    private val activeContractsService: IndexActiveContractsService = indexService
    private val transactionsService: IndexTransactionsService = indexService
    private val eventQueryService: IndexEventQueryService = indexService
    private val contractStore: ContractStore = indexService
    private val maximumLedgerTimeService: MaximumLedgerTimeService = indexService
    private val completionsService: IndexCompletionsService = indexService
    private val partyManagementService: IndexPartyManagementService = indexService
    private val configManagementService: IndexConfigManagementService = indexService
    private val meteringStore: MeteringStore = indexService

    private val configurationInitializer = new LedgerConfigurationInitializer(
      indexService = indexService,
      optWriteService = optWriteService,
      timeProvider = timeProvider,
      materializer = materializer,
      servicesExecutionContext = servicesExecutionContext,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    )

    override def acquire()(implicit context: ResourceContext): Resource[ApiServices] = {
      logger.info(engine.info.toString)(TraceContext.empty)
      for {
        currentLedgerConfiguration <- configurationInitializer.initialize(
          initialLedgerConfiguration = initialLedgerConfiguration,
          configurationLoadTimeout = configurationLoadTimeout,
        )
        services <- Resource(
          Future(
            createServices(identityService.ledgerId, currentLedgerConfiguration, checkOverloaded)(
              servicesExecutionContext
            ) ++ createExternalServices()
          )
        )(services =>
          Future {
            services.foreach {
              case closeable: AutoCloseable => closeable.close()
              case _ => ()
            }
          }
        )
      } yield ApiServicesBundle(services)
    }

    private def createServices(
        ledgerId: LedgerId,
        ledgerConfigurationSubscription: LedgerConfigurationSubscription,
        checkOverloaded: TraceContext => Option[state.SubmissionResult],
    )(implicit
        executionContext: ExecutionContext
    ): List[BindableService] = {

      val apiTransactionService =
        ApiTransactionService.create(
          ledgerId,
          transactionsService,
          metrics,
          telemetry,
          loggerFactory,
        )

      val apiEventQueryService =
        ApiEventQueryService.create(ledgerId, eventQueryService, telemetry, loggerFactory)

      val apiLedgerIdentityService =
        ApiLedgerIdentityService.create(ledgerId, telemetry, loggerFactory)

      val apiVersionService =
        ApiVersionService.create(
          ledgerFeatures,
          userManagementConfig = userManagementConfig,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

      val apiPackageService =
        ApiPackageService.create(ledgerId, packagesService, telemetry, loggerFactory)

      val apiConfigurationService =
        ApiLedgerConfigurationService.create(
          ledgerId,
          configurationService,
          telemetry,
          loggerFactory,
        )

      val apiCompletionService =
        ApiCommandCompletionService.create(
          ledgerId,
          completionsService,
          metrics,
          telemetry,
          loggerFactory,
        )

      val apiActiveContractsService =
        ApiActiveContractsService.create(
          ledgerId,
          activeContractsService,
          metrics,
          telemetry,
          loggerFactory,
        )

      val apiTimeServiceOpt =
        optTimeServiceBackend.map(tsb =>
          new TimeServiceAuthorization(
            ApiTimeService
              .create(ledgerId, tsb, apiStreamShutdownTimeout, telemetry, loggerFactory),
            authorizer,
          )
        )

      val (ledgerApiV2Services, ledgerApiUpdateService) = if (multiDomainEnabled) {
        val apiTimeServiceOpt =
          optTimeServiceBackend.map(tsb =>
            new TimeServiceV2Authorization(
              new ApiTimeServiceV2(tsb, telemetry, loggerFactory),
              authorizer,
            )
          )
        val apiCommandCompletionService = new ApiCommandCompletionServiceV2(
          completionsService,
          metrics,
          telemetry,
          loggerFactory,
        )
        val apiEventQueryService =
          new ApiEventQueryServiceV2(eventQueryService, telemetry, loggerFactory)
        val apiPackageService = new ApiPackageServiceV2(packagesService, telemetry, loggerFactory)
        val apiUpdateService =
          new ApiUpdateService(transactionsService, metrics, telemetry, loggerFactory)
        val apiVersionService =
          new ApiVersionServiceV2(ledgerFeatures, userManagementConfig, telemetry, loggerFactory)

        val v2Services = apiTimeServiceOpt.toList :::
          List(
            new CommandCompletionServiceV2Authorization(apiCommandCompletionService, authorizer),
            new EventQueryServiceV2Authorization(apiEventQueryService, authorizer),
            new PackageServiceV2Authorization(apiPackageService, authorizer),
            new UpdateServiceAuthorization(apiUpdateService, authorizer),
            apiVersionService,
          )

        v2Services -> Some(apiUpdateService)
      } else Nil -> None

      val writeServiceBackedApiServices =
        intitializeWriteServiceBackedApiServices(
          ledgerId,
          ledgerConfigurationSubscription,
          apiTransactionService,
          ledgerApiUpdateService,
          checkOverloaded,
        )

      val apiReflectionService = ProtoReflectionService.newInstance()

      val apiHealthService = new GrpcHealthService(healthChecks, telemetry, loggerFactory)

      val userManagementServices: List[BindableService] =
        if (userManagementConfig.enabled) {
          val apiUserManagementService =
            new ApiUserManagementService(
              userManagementStore = userManagementStore,
              maxUsersPageSize = userManagementConfig.maxUsersPageSize,
              submissionIdGenerator = SubmissionIdGenerator.Random,
              identityProviderExists = new IdentityProviderExists(identityProviderConfigStore),
              partyRecordExist = new PartyRecordsExist(partyRecordStore),
              indexPartyManagementService = partyManagementService,
              telemetry = telemetry,
              loggerFactory = loggerFactory,
            )
          val identityProvider =
            new ApiIdentityProviderConfigService(
              identityProviderConfigStore,
              telemetry,
              loggerFactory,
            )
          List(
            new UserManagementServiceAuthorization(
              apiUserManagementService,
              authorizer,
              loggerFactory,
            ),
            new IdentityProviderConfigServiceAuthorization(identityProvider, authorizer),
          )
        } else {
          List.empty
        }

      val apiMeteringReportService =
        new ApiMeteringReportService(
          participantId,
          meteringStore,
          meteringReportKey,
          telemetry,
          loggerFactory,
        )

      ledgerApiV2Services :::
        apiTimeServiceOpt.toList :::
        writeServiceBackedApiServices :::
        List(
          new LedgerIdentityServiceAuthorization(apiLedgerIdentityService, authorizer),
          new PackageServiceAuthorization(apiPackageService, authorizer),
          new LedgerConfigurationServiceAuthorization(apiConfigurationService, authorizer),
          new TransactionServiceAuthorization(apiTransactionService, authorizer),
          new EventQueryServiceAuthorization(apiEventQueryService, authorizer),
          new CommandCompletionServiceAuthorization(apiCompletionService, authorizer),
          new ActiveContractsServiceAuthorization(apiActiveContractsService, authorizer),
          apiReflectionService,
          apiHealthService,
          apiVersionService,
          new MeteringReportServiceAuthorization(apiMeteringReportService, authorizer),
        ) ::: userManagementServices
    }

    private def intitializeWriteServiceBackedApiServices(
        ledgerId: LedgerId,
        ledgerConfigurationSubscription: LedgerConfigurationSubscription,
        apiTransactionService: GrpcTransactionService,
        ledgerApiV2Enabled: Option[ApiUpdateService],
        checkOverloaded: TraceContext => Option[state.SubmissionResult],
    )(implicit
        executionContext: ExecutionContext
    ): List[BindableService] = {
      optWriteService.toList.flatMap { writeService =>
        val commandExecutor = new TimedCommandExecutor(
          new LedgerTimeAwareCommandExecutor(
            new StoreBackedCommandExecutor(
              engine,
              participantId,
              packagesService,
              contractStore,
              authorityResolver,
              metrics,
            ),
            new ResolveMaximumLedgerTime(maximumLedgerTimeService),
            maxRetries = 3,
            metrics,
          ),
          metrics,
        )

        val (apiSubmissionService, commandSubmissionService) = ApiSubmissionService.create(
          ledgerId,
          writeService,
          timeProvider,
          timeProviderType,
          ledgerConfigurationSubscription,
          seedService,
          commandExecutor,
          checkOverloaded,
          metrics,
          explicitDisclosureUnsafeEnabled,
          telemetry,
          loggerFactory,
        )

        // Note: the command service uses the command submission, command completion, and transaction
        // services internally. These connections do not use authorization, authorization wrappers are
        // only added here to all exposed services.
        val apiCommandService = ApiCommandService.create(
          submissionTracker = submissionTracker,
          // Using local services skips the gRPC layer, improving performance.
          submit = apiSubmissionService.submit,
          configuration = ApiCommandService.Configuration(
            ledgerId,
            commandConfig.defaultTrackingTimeout,
          ),
          transactionServices = new ApiCommandService.TransactionServices(
            getTransactionById = apiTransactionService.getTransactionById,
            getFlatTransactionById = apiTransactionService.getFlatTransactionById,
          ),
          timeProvider = timeProvider,
          ledgerConfigurationSubscription = ledgerConfigurationSubscription,
          explicitDisclosureUnsafeEnabled = explicitDisclosureUnsafeEnabled,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val apiPartyManagementService = ApiPartyManagementService.createApiService(
          partyManagementService,
          new IdentityProviderExists(identityProviderConfigStore),
          partyRecordStore,
          transactionsService,
          writeService,
          managementServiceTimeout,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val apiPackageManagementService = ApiPackageManagementService.createApiService(
          indexService,
          transactionsService,
          writeService,
          managementServiceTimeout,
          engine,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val apiConfigManagementService = ApiConfigManagementService.createApiService(
          configManagementService,
          writeService,
          timeProvider,
          telemetry = telemetry,
          loggerFactory = loggerFactory,
        )

        val participantPruningService = Option
          .when(!multiDomainEnabled)( // TODO(i13540): pruning is not supported for multi domain
            new ParticipantPruningServiceAuthorization(
              ApiParticipantPruningService.createApiService(
                indexService,
                writeService,
                metrics,
                telemetry,
                loggerFactory,
              ),
              authorizer,
            )
          )
          .toList

        val ledgerApiV2Services = ledgerApiV2Enabled.toList.flatMap { apiUpdateService =>
          val apiSubmissionServiceV2 = new ApiSubmissionServiceV2(
            commandSubmissionService = commandSubmissionService,
            writeService = writeService,
            explicitDisclosureUnsafeEnabled = explicitDisclosureUnsafeEnabled,
            currentLedgerTime = () => timeProvider.getCurrentTime,
            currentUtcTime = () => Instant.now,
            maxDeduplicationDuration = () =>
              ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationDuration),
            submissionIdGenerator = SubmissionIdGenerator.Random,
            metrics = metrics,
            telemetry = telemetry,
            loggerFactory = loggerFactory,
          )
          val apiCommandService = new ApiCommandServiceV2(
            transactionServices = new ApiCommandServiceV2.TransactionServices(
              getTransactionTreeById = apiUpdateService.getTransactionTreeById,
              getTransactionById = apiUpdateService.getTransactionById,
            ),
            submissionTracker = submissionTracker,
            submit = apiSubmissionServiceV2.submit,
            defaultTrackingTimeout = commandConfig.defaultTrackingTimeout,
            currentLedgerTime = () => timeProvider.getCurrentTime,
            currentUtcTime = () => Instant.now,
            maxDeduplicationDuration = () =>
              ledgerConfigurationSubscription.latestConfiguration().map(_.maxDeduplicationDuration),
            generateSubmissionId = SubmissionIdGenerator.Random,
            explicitDisclosureUnsafeEnabled = explicitDisclosureUnsafeEnabled,
            telemetry = telemetry,
            loggerFactory = loggerFactory,
          )

          List(
            new CommandSubmissionServiceV2Authorization(apiSubmissionServiceV2, authorizer),
            new CommandServiceV2Authorization(apiCommandService, authorizer),
          )
        }

        List(
          new CommandSubmissionServiceAuthorization(apiSubmissionService, authorizer),
          new CommandServiceAuthorization(apiCommandService, authorizer),
          new PartyManagementServiceAuthorization(apiPartyManagementService, authorizer),
          new PackageManagementServiceAuthorization(apiPackageManagementService, authorizer),
          new ConfigManagementServiceAuthorization(apiConfigManagementService, authorizer),
        ) ::: participantPruningService ::: ledgerApiV2Services
      }
    }
  }

}
