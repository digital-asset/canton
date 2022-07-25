// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing

import akka.actor.ActorSystem
import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, FutureSupervisor, Threading}
import com.digitalasset.canton.config.{
  LocalNodeParameters,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.{Crypto, DomainSyncCryptoClient}
import com.digitalasset.canton.domain.admin.v0.{
  SequencerAdministrationServiceGrpc,
  SequencerVersionServiceGrpc,
  TopologyBootstrapServiceGrpc,
}
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.authentication.grpc.{
  SequencerAuthenticationServerInterceptor,
  SequencerConnectServerInterceptor,
}
import com.digitalasset.canton.domain.sequencing.authentication.{
  MemberAuthenticationService,
  MemberAuthenticationStore,
}
import com.digitalasset.canton.domain.sequencing.sequencer._
import com.digitalasset.canton.domain.sequencing.sequencer.errors.RegisterMemberError.AlreadyRegisteredError
import com.digitalasset.canton.domain.sequencing.sequencer.errors.{
  OperationError,
  RegisterMemberError,
  SequencerWriteError,
}
import com.digitalasset.canton.domain.sequencing.service._
import com.digitalasset.canton.domain.service.ServiceAgreementManager
import com.digitalasset.canton.domain.service.grpc.GrpcDomainService
import com.digitalasset.canton.domain.topology.client.DomainInitializationObserver
import com.digitalasset.canton.health.admin.data.{SequencerHealthStatus, TopologyQueueStatus}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client._
import com.digitalasset.canton.sequencing.handlers.{
  DiscardIgnoredEvents,
  EnvelopeOpener,
  StripSignature,
}
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.store.{
  IndexedStringStore,
  SendTrackerStore,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{
  Clock,
  DomainTimeTracker,
  DomainTimeTrackerConfig,
  NonNegativeFiniteDuration,
}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ErrorUtil
import io.grpc.health.v1.HealthCheckResponse.ServingStatus
import io.grpc.{ServerInterceptors, ServerServiceDefinition}
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.ScheduledExecutorService
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}

case class SequencerAuthenticationConfig(
    agreementManager: Option[ServiceAgreementManager],
    nonceExpirationTime: NonNegativeFiniteDuration,
    tokenExpirationTime: NonNegativeFiniteDuration,
) {
  // only authentication tokens are supported
  val check: AuthenticationCheck = AuthenticationCheck.AuthenticationToken
}

object SequencerAuthenticationConfig {
  val Disabled: Option[SequencerAuthenticationConfig] = None
}

/** Run a sequencer and its supporting services.
  * @param authenticationConfig Authentication setup if supported, otherwise none.
  * @param sharedTopologyProcessor If true, the topology processor is shared and the subscriptions will be handled outside. If false, the sequencer must setup the connection and close
  */
class SequencerRuntime(
    sequencerFactory: SequencerFactory,
    staticDomainParameters: StaticDomainParameters,
    localNodeParameters: LocalNodeParameters,
    timeTrackerConfig: DomainTimeTrackerConfig,
    testingConfig: TestingConfigInternal,
    val metrics: SequencerMetrics,
    val domainId: DomainId,
    crypto: Crypto,
    sequencedTopologyStore: TopologyStore[TopologyStoreId.DomainStore],
    topologyClientMember: Member,
    topologyClient: DomainTopologyClientWithInit,
    topologyProcessor: TopologyTransactionProcessor,
    sharedTopologyProcessor: Boolean, // means we are running in embedded mode
    storage: Storage,
    clock: Clock,
    auditLogger: TracedLogger,
    initialState: Option[SequencerInitialState],
    authenticationConfig: SequencerAuthenticationConfig,
    additionalAdminServiceFactory: Sequencer => Option[ServerServiceDefinition],
    registerSequencerMember: Boolean,
    indexedStringStore: IndexedStringStore,
    futureSupervisor: FutureSupervisor,
    agreementManager: Option[ServiceAgreementManager],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, tracer: Tracer, actorSystem: ActorSystem)
    extends FlagCloseable
    with HasCloseContext
    with NamedLogging {

  override protected def timeouts: ProcessingTimeout = localNodeParameters.processingTimeouts

  private val sequencerId = SequencerId(domainId)
  private val syncCrypto =
    new DomainSyncCryptoClient(
      sequencerId,
      domainId,
      topologyClient,
      crypto,
      localNodeParameters.cachingConfigs,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )

  private val sequencer = sequencerFactory
    .create(
      domainId,
      storage,
      clock,
      topologyClientMember,
      syncCrypto,
      initialState,
      localNodeParameters,
      staticDomainParameters.protocolVersion,
    )

  private val healthManager = new io.grpc.protobuf.services.HealthStatusManager()
  sequencer.onHealthChange(status =>
    healthManager.setStatus(
      CantonGrpcUtil.sequencerHealthCheckServiceName,
      if (status.isActive) ServingStatus.SERVING else ServingStatus.NOT_SERVING,
    )
  )(TraceContext.empty)

  private val keyCheckF =
    syncCrypto
      .currentSnapshotApproximation(TraceContext.empty)
      .ipsSnapshot
      .signingKey(sequencerId)
      .map { keyO =>
        import TraceContext.Implicits.Empty._
        ErrorUtil.requireState(keyO.nonEmpty, "Missing sequencer keys.")
      }

  private val registerInitialMembers = withNewTraceContext { implicit traceContext =>
    import cats.syntax.traverse._

    logger.debug("Registering initial sequencer members")

    // only register the sequencer itself if we have remote sequencers that will necessitate topology transactions
    // being sent to them
    DomainMember
      .list(domainId, includeSequencer = registerSequencerMember)
      .toList
      .traverse(
        sequencer
          .ensureRegistered(_)
          .leftFlatMap[Unit, SequencerWriteError[RegisterMemberError]] {
            // if other sibling sequencers are initializing at the same time and try to run this step,
            // or if a sequencer automatically registers the idm (eg the Ethereum sequencer)
            // we might get AlreadyRegisteredError which we can safely ignore
            case OperationError(_: AlreadyRegisteredError) => EitherT.pure(())
            case otherError => EitherT.leftT(otherError)
          }
      )
      .value
  }

  private def init(): Future[Either[String, SequencerClientDiscriminator]] =
    for {
      _ <- keyCheckF: Future[Unit]
      err <- registerInitialMembers
      disc <- SequencerClientDiscriminator.fromDomainMember(sequencerId, indexedStringStore)
    } yield err.leftMap(_.toString).map(_ => disc)

  private val clientDiscriminator = {
    import TraceContext.Implicits.Empty._
    localNodeParameters.processingTimeouts.unbounded
      .await(s"Initialising sequencer runtime")(init()) match {
      case Right(value) => value
      case Left(err) =>
        ErrorUtil.internalError(new RuntimeException(s"Failed to init sequencer runtime: $err"))
    }
  }

  /* If we're running separately from the domain node we create a sequencer client and connect it to a topology client
   * to power sequencer authentication.
   */
  val sequencerNodeComponentsO
      : Option[(SequencerClient, DomainTimeTracker, Future[DomainInitializationObserver])] =
    if (!sharedTopologyProcessor) {
      withNewTraceContext { implicit traceContext =>
        logger.debug(s"Creating sequencer client for ${clientDiscriminator}")
        val sequencedEventStore =
          SequencedEventStore(
            storage,
            clientDiscriminator,
            staticDomainParameters.protocolVersion,
            timeouts,
            loggerFactory,
          )

        val client = new SequencerClient(
          domainId,
          sequencerId,
          new DirectSequencerClientTransport(
            sequencer,
            localNodeParameters.processingTimeouts,
            loggerFactory,
          ),
          localNodeParameters.sequencerClient,
          testingConfig,
          staticDomainParameters,
          localNodeParameters.processingTimeouts,
          _ => _ => EitherT.rightT(()),
          clock,
          sequencedEventStore,
          new SendTracker(Map(), SendTrackerStore(storage), metrics.sequencerClient, loggerFactory),
          metrics.sequencerClient,
          None,
          replayEnabled = false,
          localNodeParameters.loggingConfig,
          loggerFactory,
          initialState.flatMap(_.snapshot.heads.get(sequencerId).map(_ + 1)),
        )
        val timeTracker = DomainTimeTracker(timeTrackerConfig, clock, client, loggerFactory)

        val topologyManagerSequencerCounterTrackerStore =
          SequencerCounterTrackerStore(storage, clientDiscriminator, timeouts, loggerFactory)

        val eventHandler = StripSignature(topologyProcessor.createHandler(domainId))

        logger.debug("Subscribing topology client within sequencer runtime")
        localNodeParameters.processingTimeouts.unbounded
          .await("Failed to subscribe for the identity dispatcher sequencer client")(
            client.subscribeTracking(
              topologyManagerSequencerCounterTrackerStore,
              DiscardIgnoredEvents {
                EnvelopeOpener(staticDomainParameters.protocolVersion, crypto.pureCrypto)(
                  eventHandler
                )
              },
              timeTracker,
            )
          )

        val initializationObserver =
          DomainInitializationObserver(
            domainId,
            topologyClient,
            sequencedTopologyStore,
            mustHaveActiveMediator = sharedTopologyProcessor,
            localNodeParameters.processingTimeouts,
            loggerFactory,
          )
        Some((client, timeTracker, initializationObserver))
      }
    } else None

  private val timeoutScheduler: ScheduledExecutorService =
    Threading.singleThreadScheduledExecutor(loggerFactory.threadName + "-env-scheduler", logger)

  private val sequencerService = GrpcSequencerService(
    sequencer,
    metrics,
    auditLogger,
    authenticationConfig.check,
    clock,
    staticDomainParameters.maxRatePerParticipant,
    staticDomainParameters.maxBatchMessageSize,
    localNodeParameters.processingTimeouts,
    loggerFactory,
  )

  private val sequencerAdministrationService = new GrpcSequencerAdministrationService(sequencer)

  private case class AuthenticationServices(
      memberAuthenticationService: MemberAuthenticationService,
      sequencerAuthenticationService: GrpcSequencerAuthenticationService,
      authenticationInterceptor: SequencerAuthenticationServerInterceptor,
  )

  private val authenticationServices = {
    // if we're a separate sequencer node assume we should wait for our local topology client to observe
    // the required topology transactions to at least authorize the domain members
    val isTopologyInitializedF = sequencerNodeComponentsO
      .map { case (_, _, domainInitializationObserver) =>
        domainInitializationObserver.flatMap(_.waitUntilInitialisedAndEffective.unwrap.map(_ => ()))
      }
      .getOrElse(Future.unit)

    val authenticationService = new MemberAuthenticationService(
      domainId,
      syncCrypto,
      MemberAuthenticationStore(storage, timeouts, loggerFactory, closeContext),
      authenticationConfig.agreementManager,
      clock,
      authenticationConfig.nonceExpirationTime.unwrap,
      authenticationConfig.tokenExpirationTime.unwrap,
      // closing the subscription when the token expires will force the client to try to reconnect
      // immediately and notice it is unauthenticated, which will cause it to also start reauthenticating
      // it's important to disconnect the member AFTER we expired the token, as otherwise, the member
      // can still re-subscribe with the token just before we removed it
      Traced.lift(sequencerService.disconnectMember(_)(_)),
      isTopologyInitializedF,
      localNodeParameters.processingTimeouts,
      loggerFactory,
      auditLogger,
    )
    topologyProcessor.subscribe(authenticationService)

    val sequencerAuthenticationService =
      new GrpcSequencerAuthenticationService(
        authenticationService,
        staticDomainParameters.protocolVersion,
        loggerFactory,
      )

    val sequencerAuthInterceptor =
      new SequencerAuthenticationServerInterceptor(authenticationService, loggerFactory)

    AuthenticationServices(
      authenticationService,
      sequencerAuthenticationService,
      sequencerAuthInterceptor,
    )
  }

  def health(implicit traceContext: TraceContext): Future[SequencerHealthStatus] = sequencer.health

  def topologyQueue: TopologyQueueStatus = TopologyQueueStatus(
    manager = 0,
    dispatcher = 0,
    clients = topologyClient.numPendingChanges,
  )

  def fetchActiveMembers(): Future[Seq[Member]] =
    Future.successful(sequencerService.membersWithActiveSubscriptions)

  def registerAdminGrpcServices(
      register: ServerServiceDefinition => Unit
  )(implicit ec: ExecutionContext): Unit = {
    register(
      SequencerAdministrationServiceGrpc.bindService(sequencerAdministrationService, ec)
    )

    register(
      SequencerVersionServiceGrpc
        .bindService(
          new GrpcSequencerVersionService(staticDomainParameters.protocolVersion, loggerFactory),
          executionContext,
        )
    )

    sequencerNodeComponentsO.foreach { case (client, _, initializationObserver) =>
      // intentionally just querying whether the topology client is initialized, and not waiting for it to complete
      def isInitialized: Future[Boolean] =
        initializationObserver.flatMap(_.initialisedAtHead)(ec)

      register(
        TopologyBootstrapServiceGrpc
          .bindService(
            new GrpcSequencerTopologyBootstrapService(
              domainId,
              staticDomainParameters.protocolVersion,
              syncCrypto,
              client,
              () => isInitialized,
              loggerFactory,
            )(ec),
            executionContext,
          )
      )
    }

    // hook for registering enterprise administration service if in an appropriate environment
    additionalAdminServiceFactory(sequencer).foreach(register)
  }

  /** Separate from [[registerPublicGrpcServices]] because this should only be called when the sequencer is external to the domain node (as opposed to embedded).
    * That's because embedded sequencers (especially the CCF sequencer) still rely on the domain having its own domain service, so in that case we do not
    * register it twice.
    */
  @nowarn("cat=deprecation")
  def registerDomainService(
      register: ServerServiceDefinition => Unit
  )(implicit ec: ExecutionContext): Unit = {
    register(
      v0.DomainServiceGrpc.bindService(
        new GrpcDomainService(authenticationConfig.agreementManager, loggerFactory),
        executionContext,
      )
    )

    register(
      ServerInterceptors.intercept(
        v0.SequencerConnectServiceGrpc.bindService(
          new GrpcSequencerConnectService(
            domainId,
            staticDomainParameters,
            syncCrypto,
            agreementManager,
            loggerFactory,
          )(
            ec
          ),
          executionContext,
        ),
        new SequencerConnectServerInterceptor(loggerFactory),
      )
    )
    ()
  }

  def registerPublicGrpcServices(
      register: ServerServiceDefinition => Unit
  )(implicit ec: ExecutionContext): Unit = {
    import scala.jdk.CollectionConverters._

    register(
      SequencerVersionServiceGrpc.bindService(
        new GrpcSequencerVersionService(staticDomainParameters.protocolVersion, loggerFactory),
        ec,
      )
    )

    register(
      v0.SequencerAuthenticationServiceGrpc
        .bindService(authenticationServices.sequencerAuthenticationService, ec)
    )

    // use the auth service interceptor if available
    val interceptors = List(authenticationServices.authenticationInterceptor).asJava

    register(
      ServerInterceptors.intercept(
        v0.SequencerServiceGrpc.bindService(sequencerService, ec),
        interceptors,
      )
    )

    register(healthManager.getHealthService.bindService())
  }

  override def onClosed(): Unit =
    Lifecycle.close(
      () =>
        sequencerNodeComponentsO foreach { case (client, timeTracker, _) =>
          Lifecycle.close(timeTracker, client)(logger)
        },
      () =>
        if (!sharedTopologyProcessor) {
          // if this component has a topology processor, we need to close it together with the client
          topologyClient.close()
          topologyProcessor.close()
        },
      sequencerService,
      authenticationServices.memberAuthenticationService,
      sequencer,
      ExecutorServiceExtensions(timeoutScheduler)(logger, timeouts),
    )(logger)

}
