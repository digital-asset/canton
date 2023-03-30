// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import akka.actor.ActorSystem
import better.files.File
import cats.data.{EitherT, OptionT}
import cats.syntax.option.*
import com.daml.metrics.HealthMetrics
import com.daml.metrics.api.MetricName
import com.daml.metrics.grpc.GrpcServerMetrics
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.{
  ExecutionContextIdlenessExecutorService,
  FutureSupervisor,
}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{
  InitConfigBase,
  LocalNodeConfig,
  ProcessingTimeout,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.GrpcVaultServiceFactory
import com.digitalasset.canton.crypto.admin.v0.VaultServiceGrpc
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.crypto.store.{CryptoPrivateStoreError, CryptoPublicStoreError}
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.health.HealthReporting.{HealthService, ServiceHealthStatusManager}
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.health.admin.grpc.GrpcStatusService
import com.digitalasset.canton.health.admin.v0.StatusServiceGrpc
import com.digitalasset.canton.health.{GrpcHealthReporter, GrpcHealthServer, HealthReporting}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.DbStorageMetrics
import com.digitalasset.canton.metrics.MetricHandle.MetricsFactory
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.resource.StorageFactory
import com.digitalasset.canton.store.IndexedStringStore
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.IdentityProvidingServiceClient
import com.digitalasset.canton.topology.store.InitializationStore
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TracerProvider}
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.RetryUtil.NoExnRetryable
import com.digitalasset.canton.version.ReleaseProtocolVersion
import io.functionmeta.functionFullName
import io.grpc.protobuf.services.ProtoReflectionService
import io.opentelemetry.api.trace.Tracer
import org.slf4j.event.Level

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.concurrent.{Future, blocking}

/** When a canton node is created it first has to obtain an identity before most of its services can be started.
  * This process will begin when `start` is called and will try to perform as much as permitted by configuration automatically.
  * If external action is required before this process can complete `start` will return successfully but `isInitialized` will still be false.
  * When the node is successfully initialized the underlying node will be available through `getNode`.
  */
trait CantonNodeBootstrap[+T <: CantonNode] extends FlagCloseable with NamedLogging {

  def name: InstanceName
  def clock: Clock
  def getId: Option[NodeId]
  def isInitialized: Boolean

  def start(): EitherT[Future, String, Unit]

  /** Initialize the node with an externally provided identity. */
  def initializeWithProvidedId(id: NodeId): EitherT[Future, String, Unit]
  def getNode: Option[T]

  /** Access to the private and public store to support local key inspection commands */
  def crypto: Crypto
}

object CantonNodeBootstrap {
  type HealthDumpFunction = () => Future[File]
}

trait BaseMetrics {
  def prefix: MetricName
  @nowarn("cat=deprecation")
  def metricsFactory: MetricsFactory
  def grpcMetrics: GrpcServerMetrics
  def healthMetrics: HealthMetrics
  def storageMetrics: DbStorageMetrics
}

final case class CantonNodeBootstrapCommonArguments[
    +NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    M <: BaseMetrics,
](
    name: InstanceName,
    config: NodeConfig,
    parameterConfig: ParameterConfig,
    testingConfig: TestingConfigInternal,
    clock: Clock,
    metrics: M,
    storageFactory: StorageFactory,
    cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
    grpcVaultServiceFactory: GrpcVaultServiceFactory,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
    writeHealthDumpToFile: HealthDumpFunction,
    configuredOpenTelemetry: ConfiguredOpenTelemetry,
)

abstract class CantonNodeBootstrapCommon[
    T <: CantonNode,
    NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    Metrics <: BaseMetrics,
](
    protected val arguments: CantonNodeBootstrapCommonArguments[
      NodeConfig,
      ParameterConfig,
      Metrics,
    ]
)(
    implicit val executionContext: ExecutionContextIdlenessExecutorService,
    implicit val scheduler: ScheduledExecutorService,
    implicit val actorSystem: ActorSystem,
) extends CantonNodeBootstrap[T]
    with HasCloseContext
    with NoTracing {

  override def name: InstanceName = arguments.name
  override def clock: Clock = arguments.clock
  def config: NodeConfig = arguments.config
  def parameterConfig: ParameterConfig = arguments.parameterConfig
  // TODO(#11255) unify
  def parameters: ParameterConfig = parameterConfig
  protected def futureSupervisor: FutureSupervisor = arguments.futureSupervisor
  override def loggerFactory: NamedLoggerFactory = arguments.loggerFactory

  protected val cryptoConfig = config.crypto
  protected val adminApiConfig = config.adminApi
  protected val initConfig = config.init
  protected val tracerProvider =
    TracerProvider.Factory(arguments.configuredOpenTelemetry, name.unwrap)
  implicit val tracer: Tracer = tracerProvider.tracer

  private val isRunningVar = new AtomicBoolean(true)
  protected def isRunning: Boolean = isRunningVar.get()

  /** Can this node be initialized by a replica */
  protected val supportsReplicaInitialization: Boolean = false
  private val initializationWatcherRef = new AtomicReference[Option[InitializationWatcher]](None)

  // reference to the node once it has been started
  private val ref: AtomicReference[Option[T]] = new AtomicReference(None)
  private val starting = new AtomicBoolean(false)

  /** kick off initialisation during startup */
  protected def startInstanceUnlessClosing(
      instanceET: => EitherT[Future, String, T]
  ): EitherT[Future, String, Unit] = {
    if (isInitialized) {
      logger.warn("Will not start instance again as it is already initialised")
      EitherT.pure[Future, String](())
    } else {
      performUnlessClosingEitherT(functionFullName, "Aborting startup due to shutdown") {
        if (starting.compareAndSet(false, true))
          instanceET.map { instance =>
            val previous = ref.getAndSet(Some(instance))
            // potentially over-defensive, but ensures a runner will not be set twice.
            // if called twice it indicates a bug in initialization.
            previous.foreach { shouldNotBeThere =>
              logger.error(s"Runner has already been set: $shouldNotBeThere")
            }
          }
        else {
          logger.warn("Will not start instance again as it is already starting up")
          EitherT.pure[Future, String](())
        }
      }
    }
  }

  // accessors to both the running node and for testing whether it has been set
  def getNode: Option[T] = ref.get()
  def isInitialized: Boolean = ref.get().isDefined

  // first, we load the crypto modules
  private val nodeId = new AtomicReference[Option[NodeId]](None)

  // This absolutely must be a "def", because it is used during class initialization.
  protected def connectionPoolForParticipant: Boolean = false

  lazy val timeouts: ProcessingTimeout = parameterConfig.processingTimeouts

  // TODO(i3168): Move to a error-safe node initialization approach
  protected val storage =
    arguments.storageFactory
      .tryCreate(
        connectionPoolForParticipant,
        arguments.parameterConfig.logQueryCost,
        clock,
        Some(scheduler),
        arguments.metrics.storageMetrics,
        parameterConfig.processingTimeouts,
        loggerFactory,
      )

  /** Health service component of the node
    */
  protected def nodeHealthService: HealthReporting.HealthService

  // Service that will always return `SERVING`. Useful to be targeted by k8s liveness probes.
  private val livenessService = HealthService("liveness")
  protected val healthReporter: GrpcHealthReporter = new GrpcHealthReporter(loggerFactory)
  private lazy val grpcNodeHealthManager =
    ServiceHealthStatusManager(
      "Health API",
      new io.grpc.protobuf.services.HealthStatusManager(),
      Set(nodeHealthService, livenessService),
    )

  protected val initializationStore = InitializationStore(storage, timeouts, loggerFactory)
  protected val indexedStringStore =
    IndexedStringStore.create(
      storage,
      parameterConfig.cachingConfigs.indexedStrings,
      timeouts,
      loggerFactory,
    )

  override val crypto: Crypto = timeouts.unbounded.await(
    description = "initialize CryptoFactory",
    logFailing = Some(Level.ERROR),
  )(
    CryptoFactory
      .create(
        cryptoConfig,
        storage,
        arguments.cryptoPrivateStoreFactory,
        ReleaseProtocolVersion.latest,
        timeouts,
        loggerFactory,
      )
      .valueOr(err => throw new RuntimeException(s"Failed to initialize crypto: $err"))
  )
  val certificateGenerator = new X509CertificateGenerator(crypto, loggerFactory)

  protected val ips = new IdentityProvidingServiceClient()

  protected def isActive: Boolean

  private def status: Future[NodeStatus[NodeStatus.Status]] = {
    getNode
      .map(_.status.map(NodeStatus.Success(_)))
      .getOrElse(Future.successful(NodeStatus.NotInitialized(isActive)))
  }

  locally {
    arguments.metrics.healthMetrics
      .registerHealthGauge(
        name.toProtoPrimitive,
        () => getNode.map(_.status.map(_.active)).getOrElse(Future(false)),
      )
      .discard // we still want to report the health even if the node is closed
  }

  // The admin-API services
  logger.info(s"Starting admin-api services on ${adminApiConfig}")
  protected val (adminServer, adminServerRegistry) = {
    val builder = CantonServerBuilder
      .forConfig(
        adminApiConfig,
        arguments.metrics.prefix,
        arguments.metrics.metricsFactory,
        executionContext,
        loggerFactory,
        parameterConfig.loggingConfig.api,
        parameterConfig.tracing,
        arguments.metrics.grpcMetrics,
      )

    val registry = builder.mutableHandlerRegistry()

    val server = builder
      .addService(
        StatusServiceGrpc.bindService(
          new GrpcStatusService(
            status,
            arguments.writeHealthDumpToFile,
            parameterConfig.processingTimeouts,
          ),
          executionContext,
        )
      )
      .addService(
        VaultServiceGrpc.bindService(
          arguments.grpcVaultServiceFactory.create(crypto, certificateGenerator, loggerFactory),
          executionContext,
        )
      )
      .addService(ProtoReflectionService.newInstance(), false)
      .build
      .start()
    (Lifecycle.toCloseableServer(server, logger, "AdminServer"), registry)
  }

  protected def startWithStoredNodeId(id: NodeId): EitherT[Future, String, Unit] = {
    if (nodeId.compareAndSet(None, Some(id))) {
      logger.info(s"Resuming as existing instance with uid=${id}")
      initialize(id).leftMap { err =>
        logger.info(s"Failed to initialize node, trying to clean up: $err")
        close()
        err
      }
    } else {
      EitherT.leftT[Future, Unit]("Node identity has already been initialized")
    }
  }

  private val grpcHealthServer = config.monitoring.grpcHealthServer.map { healthConfig =>
    healthReporter.registerHealthManager(grpcNodeHealthManager)

    val executor = Executors.newFixedThreadPool(healthConfig.parallelism)

    new GrpcHealthServer(
      healthConfig,
      arguments.metrics.metricsFactory,
      executor,
      loggerFactory,
      parameterConfig.loggingConfig.api,
      parameterConfig.tracing,
      arguments.metrics.grpcMetrics,
      timeouts,
      grpcNodeHealthManager.manager,
    )
  }

  /** When a node is not initialized and auto-init is false, we skip initialization.
    * This can be overridden to give a chance to a node to still perform some operation after it has been started
    * but not yet initialized.
    */
  def runOnSkippedInitialization: EitherT[Future, String, Unit] = EitherT.pure[Future, String](())

  /** Attempt to start the node.
    * If a previously initialized identifier is available the node will be immediately initialized.
    * If there is no existing identity and autoinit is enabled an identity will be automatically generated and then the node will initialize.
    * If there is no existing identity and autoinit is disabled start will immediately exit to wait for an identity to be externally provided through [[initializeWithProvidedId]].
    */
  def start(): EitherT[Future, String, Unit] = {
    // The passive replica waits for the active replica to initialize the unique identifier
    def waitForActiveId(): EitherT[Future, String, NodeId] = {
      val timeout = parameterConfig.processingTimeouts
      OptionT(
        retry
          .Pause(
            logger,
            FlagCloseable(logger, timeout),
            timeout.activeInit.retries(50.millis),
            50.millis,
            functionFullName,
          )
          .apply(initializationStore.id, NoExnRetryable)
      )
        .toRight("Active replica failed to initialize unique identifier")
    }

    for {
      // if we're a passive replica but the node is set to auto-initialize, wait here until the node has established an id
      id <-
        if (!storage.isActive && initConfig.autoInit) waitForActiveId().map(Some(_))
        else
          EitherT.right[String](
            initializationStore.id
          ) // otherwise just fetch what's that immediately
      _ <- id.fold(
        if (initConfig.autoInit) {
          logger.info("Node is not initialized yet. Performing automated default initialization.")
          autoInitializeIdentity(initConfig)
        } else {
          logger.info(
            "Node is not initialized yet. You have opted for manual configuration by yourself."
          )
          runOnSkippedInitialization
        }
      )(startWithStoredNodeId)
    } yield {
      // if we're still not initialized and support a replica doing on our behalf, start a watcher to handle that happening
      if (getId.isEmpty && supportsReplicaInitialization) waitForReplicaInitialization()
    }
  }

  /** Attempt to start the node with this identity. */
  protected def initialize(uid: NodeId): EitherT[Future, String, Unit]

  /** Generate an identity for the node. */
  protected def autoInitializeIdentity(
      initConfigBase: InitConfigBase
  ): EitherT[Future, String, Unit]

  final protected def storeId(id: NodeId): EitherT[Future, String, Unit] =
    for {
      previous <- EitherT.right(initializationStore.id)
      result <- previous match {
        case Some(existing) =>
          EitherT.leftT[Future, Unit](s"Node is already initialized with id [$existing]")
        case None =>
          logger.info(s"Initializing node with id $id")
          EitherT.right[String](for {
            _ <- initializationStore.setId(id)
            _ = nodeId.set(Some(id))
          } yield ())
      }
    } yield result

  def getId: Option[NodeId] = nodeId.get()

  override protected def onClosed(): Unit = blocking {
    synchronized {
      if (isRunningVar.getAndSet(false)) {
        val stores = List(
          initializationStore,
          indexedStringStore,
        )
        val instances = grpcHealthServer.toList ++ List(
          Lifecycle.toCloseableOption(initializationWatcherRef.get()),
          adminServerRegistry,
          adminServer,
        ) ++ getNode.toList ++ stores ++ List(crypto, storage, clock)
        Lifecycle.close(instances: _*)(logger)
        logger.debug(s"Successfully completed shutdown of $name")
      } else {
        logger.warn(
          s"Unnecessary second close of node $name invoked. Ignoring it.",
          new Exception("location"),
        )
      }
    }
  }

  protected def getOrCreateSigningKey(
      name: String
  )(implicit traceContext: TraceContext): EitherT[Future, String, SigningPublicKey] =
    getOrCreateKey(
      "signing",
      crypto.cryptoPublicStore.findSigningKeyIdByName,
      name => crypto.generateSigningKey(name = name).leftMap(_.toString),
      crypto.cryptoPrivateStore.existsSigningKey,
      name,
    )

  protected def getOrCreateEncryptionKey(
      name: String
  )(implicit traceContext: TraceContext): EitherT[Future, String, EncryptionPublicKey] =
    getOrCreateKey(
      "encryption",
      crypto.cryptoPublicStore.findEncryptionKeyIdByName,
      name => crypto.generateEncryptionKey(name = name).leftMap(_.toString),
      crypto.cryptoPrivateStore.existsPrivateKey,
      name,
    )

  private def getOrCreateKey[P <: PublicKey](
      typ: String,
      findPubKeyIdByName: KeyName => EitherT[Future, CryptoPublicStoreError, Option[P]],
      generateKey: Option[KeyName] => EitherT[Future, String, P],
      existPrivateKeyByFp: Fingerprint => EitherT[Future, CryptoPrivateStoreError, Boolean],
      name: String,
  ): EitherT[Future, String, P] = for {
    keyName <- EitherT.fromEither[Future](KeyName.create(name))
    keyIdO <- findPubKeyIdByName(keyName)
      .leftMap(err => s"Failure while looking for $typ key $name in public store: ${err}")
    pubKey <- keyIdO.fold(
      generateKey(Some(keyName))
        .leftMap(err => s"Failure while generating $typ key for $name: $err")
    ) { keyWithName =>
      val fingerprint = keyWithName.fingerprint
      existPrivateKeyByFp(fingerprint)
        .leftMap(err =>
          s"Failure while looking for $typ key $fingerprint of $name in private key store: $err"
        )
        .transform {
          case Right(true) => Right(keyWithName)
          case Right(false) =>
            Left(s"Broken private key store: Could not find $typ key $fingerprint of $name")
          case Left(err) => Left(err)
        }
    }
  } yield pubKey

  /** Poll the datastore to see if the id has been initialized in case a replica initializes the node */
  private def waitForReplicaInitialization(): Unit = blocking {
    synchronized {
      withNewTraceContext { implicit traceContext =>
        if (isRunning && initializationWatcherRef.get().isEmpty) {
          val initializationWatcher = new InitializationWatcher(loggerFactory)
          initializationWatcher.watch()
          initializationWatcherRef.set(initializationWatcher.some)
        }
      }
    }
  }

  private class InitializationWatcher(protected val loggerFactory: NamedLoggerFactory)
      extends FlagCloseable
      with NamedLogging {
    override protected def timeouts: ProcessingTimeout =
      CantonNodeBootstrapCommon.this.parameterConfig.processingTimeouts

    def watch()(implicit traceContext: TraceContext): Unit = {
      logger.debug(s"Waiting for a node id to be stored to start this node instance")

      // we try forever - 1 to avoid logging every attempt at warning
      retry
        .Backoff(
          logger,
          this,
          retry.Forever - 1,
          initialDelay = 500.millis,
          maxDelay = 5.seconds,
          "waitForIdInitialization",
        )
        .apply(initializationStore.id, NoExnRetryable)
        .foreach(_.foreach { id =>
          if (getId.isDefined) {
            logger.debug("A stored id has been found but the id has already been set so ignoring")
          } else {
            logger.info("Starting node as we have found a stored id")
            startWithStoredNodeId(id).value.foreach {
              case Left(error) =>
                // if we are already successfully initialized likely this was just called twice due to a race between
                // the waiting and an initialize call
                if (isInitialized) {
                  logger.debug(
                    s"An error was returned when starting the node due to finding a stored id but we are already initialized: $error"
                  )
                } else if (isClosing) {
                  // If the node is currently shutting down, not being able to initialize is anyway not a problem.
                  // The error is most likely due to part of the start up procedure failing due to the shutdown.
                  logger.debug(
                    s"An error was returned when starting the node due to finding a stored id, but the node is currently shutting down: $error"
                  )
                } else {
                  logger.error(s"Failed to start the node when finding a stored id: $error")
                }
              case _ =>
            }
          }
        })
    }
  }
}
