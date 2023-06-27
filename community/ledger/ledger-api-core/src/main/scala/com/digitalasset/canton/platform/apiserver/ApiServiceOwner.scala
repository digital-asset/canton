// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.buildinfo.BuildInfo
import com.daml.jwt.JwtTimestampLeeway
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.daml.ports.{Port, PortFiles}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.auth.*
import com.digitalasset.canton.ledger.api.auth.interceptor.AuthorizationInterceptor
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.api.health.HealthChecks
import com.digitalasset.canton.ledger.configuration.LedgerId
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexService
import com.digitalasset.canton.ledger.participant.state.v2.ReadService
import com.digitalasset.canton.ledger.participant.state.v2 as state
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.platform.apiserver.execution.AuthorityResolver
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey
import com.digitalasset.canton.platform.apiserver.meteringreport.MeteringReportKey.CommunityKey
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.localstore.api.{
  IdentityProviderConfigStore,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.platform.services.time.TimeProviderType
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.{BindableService, ServerInterceptor}
import scalaz.{-\/, \/-}

import java.time.Clock
import scala.collection.immutable
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object ApiServiceOwner {

  def apply(
      indexService: IndexService,
      submissionTracker: SubmissionTracker,
      userManagementStore: UserManagementStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
      partyRecordStore: PartyRecordStore,
      ledgerId: LedgerId,
      participantId: Ref.ParticipantId,
      config: ApiServerConfig,
      optWriteService: Option[state.WriteService],
      readService: ReadService,
      healthChecks: HealthChecks,
      metrics: Metrics,
      timeServiceBackend: Option[TimeServiceBackend] = None,
      otherServices: immutable.Seq[BindableService] = immutable.Seq.empty,
      otherInterceptors: List[ServerInterceptor] = List.empty,
      engine: Engine,
      authorityResolver: AuthorityResolver,
      servicesExecutionContext: ExecutionContextExecutor,
      checkOverloaded: TraceContext => Option[state.SubmissionResult] =
        _ => None, // Used for Canton rate-limiting,
      ledgerFeatures: LedgerFeatures,
      authService: AuthService,
      jwtVerifierLoader: JwtVerifierLoader,
      meteringReportKey: MeteringReportKey = CommunityKey,
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      explicitDisclosureUnsafeEnabled: Boolean = false,
      createExternalServices: () => List[BindableService] = () => Nil,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
      multiDomainEnabled: Boolean,
  )(implicit
      actorSystem: ActorSystem,
      materializer: Materializer,
      traceContext: TraceContext,
  ): ResourceOwner[ApiService] = {

    def writePortFile(port: Port): Try[Unit] = {
      config.portFile match {
        case Some(path) =>
          PortFiles.write(path, port) match {
            case -\/(err) => Failure(new RuntimeException(err.toString))
            case \/-(()) => Success(())
          }
        case None =>
          Success(())
      }
    }

    val authorizer = new Authorizer(
      Clock.systemUTC.instant _,
      ledgerId,
      participantId,
      userManagementStore,
      servicesExecutionContext,
      userRightsCheckIntervalInSeconds = config.userManagement.cacheExpiryAfterWriteInSeconds,
      akkaScheduler = actorSystem.scheduler,
      jwtTimestampLeeway = jwtTimestampLeeway,
      loggerFactory = loggerFactory,
    )(LoggingContextWithTrace(loggerFactory))
    // TODO(i12283) LLP: Consider fusing the index health check with the indexer health check
    val healthChecksWithIndexService = healthChecks + ("index" -> indexService)

    val identityProviderConfigLoader = new IdentityProviderConfigLoader {
      override def getIdentityProviderConfig(issuer: LedgerId)(implicit
          loggingContext: LoggingContextWithTrace
      ): Future[domain.IdentityProviderConfig] =
        identityProviderConfigStore.getActiveIdentityProviderByIssuer(issuer)
    }

    for {
      executionSequencerFactory <- new ExecutionSequencerFactoryOwner()
      apiServicesOwner = new ApiServices.Owner(
        participantId = participantId,
        optWriteService = optWriteService,
        readService = readService,
        indexService = indexService,
        authorizer = authorizer,
        engine = engine,
        authorityResolver = authorityResolver,
        timeProvider = timeServiceBackend.getOrElse(TimeProvider.UTC),
        timeProviderType =
          timeServiceBackend.fold[TimeProviderType](TimeProviderType.WallClock)(_ =>
            TimeProviderType.Static
          ),
        submissionTracker = submissionTracker,
        configurationLoadTimeout = config.configurationLoadTimeout,
        initialLedgerConfiguration = config.initialLedgerConfiguration,
        commandConfig = config.command,
        optTimeServiceBackend = timeServiceBackend,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        healthChecks = healthChecksWithIndexService,
        seedService = SeedService(config.seeding),
        managementServiceTimeout = config.managementServiceTimeout,
        checkOverloaded = checkOverloaded,
        userManagementStore = userManagementStore,
        identityProviderConfigStore = identityProviderConfigStore,
        partyRecordStore = partyRecordStore,
        ledgerFeatures = ledgerFeatures,
        userManagementConfig = config.userManagement,
        apiStreamShutdownTimeout = config.apiStreamShutdownTimeout,
        meteringReportKey = meteringReportKey,
        explicitDisclosureUnsafeEnabled = explicitDisclosureUnsafeEnabled,
        createExternalServices = createExternalServices,
        telemetry = telemetry,
        loggerFactory = loggerFactory,
        multiDomainEnabled = multiDomainEnabled,
      )(materializer, executionSequencerFactory)
        .map(_.withServices(otherServices))
      apiService <- new LedgerApiService(
        apiServicesOwner,
        config.port,
        config.maxInboundMessageSize,
        config.address,
        config.tls,
        AuthorizationInterceptor(
          authService = authService,
          Option.when(config.userManagement.enabled)(userManagementStore),
          new IdentityProviderAwareAuthServiceImpl(
            identityProviderConfigLoader = identityProviderConfigLoader,
            jwtVerifierLoader = jwtVerifierLoader,
            loggerFactory = loggerFactory,
          )(servicesExecutionContext),
          telemetry,
          loggerFactory,
          servicesExecutionContext,
        ) :: otherInterceptors,
        servicesExecutionContext,
        metrics,
        loggerFactory,
      )
      _ <- ResourceOwner.forTry(() => writePortFile(apiService.port))
    } yield {
      loggerFactory
        .getTracedLogger(getClass)
        .info(
          s"Initialized API server version ${BuildInfo.Version} with ledger-id = $ledgerId, port = ${apiService.port}."
        )
      apiService
    }
  }
}
