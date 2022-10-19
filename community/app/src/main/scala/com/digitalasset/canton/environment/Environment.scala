// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import akka.actor.ActorSystem
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.traverse.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.canton.concurrent.*
import com.digitalasset.canton.config.*
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  ConsoleGrpcAdminCommandRunner,
  ConsoleOutput,
  GrpcAdminCommandRunner,
  HealthDumpGenerator,
  StandardConsoleOutput,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.DomainNodeBootstrap
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.environment.Environment.*
import com.digitalasset.canton.health.HealthServer
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsFactory
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.{ParticipantNode, ParticipantNodeBootstrap}
import com.digitalasset.canton.resource.DbMigrationsFactory
import com.digitalasset.canton.time.*
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, TracerProvider}
import com.digitalasset.canton.util.{AkkaUtil, SingleUseCell}
import com.digitalasset.canton.{DiscardOps, DomainAlias}
import com.google.common.annotations.VisibleForTesting
import io.circe.Encoder
import io.opentelemetry.api.trace.Tracer
import org.slf4j.bridge.SLF4JBridgeHandler

import java.util.concurrent.ScheduledExecutorService
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, blocking}
import scala.util.control.NonFatal

/** Holds all significant resources held by this process.
  */
trait Environment extends NamedLogging with AutoCloseable with NoTracing {

  type Config <: CantonConfig
  type Console <: ConsoleEnvironment

  val config: Config
  val testingConfig: TestingConfigInternal

  val loggerFactory: NamedLoggerFactory

  protected def participantNodeFactory
      : ParticipantNodeBootstrap.Factory[Config#ParticipantConfigType]
  protected def domainFactory: DomainNodeBootstrap.Factory[Config#DomainConfigType]
  protected def migrationsFactory: DbMigrationsFactory

  def isEnterprise: Boolean

  def createConsole(
      consoleOutput: ConsoleOutput = StandardConsoleOutput,
      createAdminCommandRunner: ConsoleEnvironment => ConsoleGrpcAdminCommandRunner =
        new ConsoleGrpcAdminCommandRunner(_),
  ): Console = {
    val console = _createConsole(consoleOutput, createAdminCommandRunner)
    healthDumpGenerator
      .putIfAbsent(createHealthDumpGenerator(console.grpcAdminCommandRunner))
      .discard
    console
  }

  protected def _createConsole(
      consoleOutput: ConsoleOutput = StandardConsoleOutput,
      createAdminCommandRunner: ConsoleEnvironment => ConsoleGrpcAdminCommandRunner =
        new ConsoleGrpcAdminCommandRunner(_),
  ): Console

  protected def createHealthDumpGenerator(
      commandRunner: GrpcAdminCommandRunner
  ): HealthDumpGenerator[_]

  /* We can't reliably use the health administration instance of the console because:
   * 1) it's tied to the console environment, which we don't have access to yet when the environment is instantiated
   * 2) there might never be a console environment when running in daemon mode
   * Therefore we create an immutable lazy value for the health administration that can be set either with the console
   * health admin when/if it gets created, or with a headless health admin, whichever comes first.
   */
  private val healthDumpGenerator = new SingleUseCell[HealthDumpGenerator[_]]

  // Function passed down to the node boostrap used to generate a health dump file
  val writeHealthDumpToFile: HealthDumpFunction = () =>
    Future {
      healthDumpGenerator
        .getOrElse {
          val tracerProvider =
            TracerProvider.Factory(config.monitoring.tracing.tracer, "admin_command_runner")
          implicit val tracer: Tracer = tracerProvider.tracer

          val commandRunner = new GrpcAdminCommandRunner(this, config.parameters.timeouts.console)
          val newGenerator = createHealthDumpGenerator(commandRunner)
          val previous = healthDumpGenerator.putIfAbsent(newGenerator)
          previous match {
            // If somehow the cell was set concurrently in the meantime, close the newly created command runner and use
            // the existing one
            case Some(value) =>
              commandRunner.close()
              value
            case None =>
              newGenerator
          }
        }
        .generateHealthDump(
          better.files.File.newTemporaryFile(
            prefix = "canton-remote-health-dump"
          )
        )
    }

  installJavaUtilLoggingBridge()
  logger.debug(config.portDescription)

  implicit val scheduler: ScheduledExecutorService =
    Threading.singleThreadScheduledExecutor(loggerFactory.threadName + "-env-scheduler", logger)

  implicit val executionContext: ExecutionContextIdlenessExecutorService =
    Threading.newExecutionContext(loggerFactory.threadName + "-env-execution-context", logger)

  private val deadlockConfig = config.monitoring.deadlockDetection
  protected def timeouts: ProcessingTimeout = config.parameters.timeouts.processing

  protected val futureSupervisor =
    if (config.monitoring.logSlowFutures)
      new FutureSupervisor.Impl(timeouts.slowFutureWarn)
    else FutureSupervisor.Noop

  private val monitorO = if (deadlockConfig.enabled) {
    val mon = new ExecutionContextMonitor(
      loggerFactory,
      deadlockConfig.interval,
      deadlockConfig.maxReports,
      deadlockConfig.reportAsWarnings,
      timeouts,
    )
    mon.monitor(executionContext)
    Some(mon)
  } else None

  implicit val actorSystem: ActorSystem = AkkaUtil.createActorSystem(loggerFactory.threadName)

  implicit val executionSequencerFactory: ExecutionSequencerFactory =
    AkkaUtil.createExecutionSequencerFactory(
      loggerFactory.threadName + "-admin-workflow-services",
      // don't log the number of threads twice, as we log it already when creating the first pool
      NamedLogging.noopLogger,
    )

  // additional closeables
  private val userCloseables = ListBuffer[AutoCloseable]()

  /** Sim-clock if environment is using static time
    */
  val simClock: Option[DelegatingSimClock] = config.parameters.clock match {
    case ClockConfig.SimClock =>
      logger.info("Starting environment with sim-clock")
      Some(
        new DelegatingSimClock(
          () =>
            runningNodes.map(_.clock).collect { case c: SimClock =>
              c
            },
          loggerFactory = loggerFactory,
        )
      )
    case ClockConfig.WallClock(_) => None
    case ClockConfig.RemoteClock(_) => None
  }

  val clock: Clock = simClock.getOrElse(createClock(None))

  protected def createClock(nodeTypeAndName: Option[(String, String)]): Clock = {
    val clockLoggerFactory = nodeTypeAndName.fold(loggerFactory) { case (nodeType, name) =>
      loggerFactory.append(nodeType, name)
    }
    config.parameters.clock match {
      case ClockConfig.SimClock =>
        val parent = simClock.getOrElse(sys.error("This should not happen"))
        val clock = new SimClock(
          parent.start,
          clockLoggerFactory,
        )
        clock.advanceTo(parent.now)
        clock
      case ClockConfig.RemoteClock(clientConfig) =>
        new RemoteClock(clientConfig, config.parameters.timeouts.processing, clockLoggerFactory)
      case ClockConfig.WallClock(skewW) =>
        val skewMs = skewW.toScala.toMillis
        val tickTock =
          if (skewMs == 0) TickTock.Native
          else new TickTock.RandomSkew(Math.min(skewMs, Int.MaxValue).toInt)
        new WallClock(timeouts, clockLoggerFactory, tickTock)
    }
  }

  private val testingTimeService = new TestingTimeService(clock, () => simClocks)
  // public for buildDocs task to be able to construct a fake participant and domain to document available metrics via reflection
  val metricsFactory = MetricsFactory.forConfig(config.monitoring.metrics)
  private val healthServer =
    config.monitoring.health.map(
      HealthServer(_, metricsFactory.health, timeouts, loggerFactory)(this)
    )

  private val envQueueSize =
    metricsFactory.forEnv.registerExecutionContextQueueSize(() => executionContext.queueSize)

  lazy val domains =
    new DomainNodes(
      createDomain,
      migrationsFactory,
      timeouts,
      config.domainsByString,
      config.domainNodeParametersByString,
      loggerFactory,
    )
  lazy val participants =
    new ParticipantNodes(
      createParticipant,
      migrationsFactory,
      timeouts,
      config.participantsByString,
      config.participantNodeParametersByString,
      loggerFactory,
    )

  // convenient grouping of all node collections for performing operations
  // intentionally defined in the order we'd like to start them
  protected def allNodes: List[Nodes[CantonNode, CantonNodeBootstrap[CantonNode]]] =
    List(domains, participants)
  private def runningNodes: Seq[CantonNodeBootstrap[CantonNode]] = allNodes.flatMap(_.running)

  private def autoConnectLocalNodes(): Either[StartupError, Unit] = {
    val activeDomains = domains.running
      .filter(_.isActive)
      .filter(_.config.topology.open)
    def toDomainConfig(domain: DomainNodeBootstrap): Either[StartupError, DomainConnectionConfig] =
      (for {
        connection <- domain.config.sequencerConnectionConfig.toConnection
        name <- DomainAlias.create(domain.name.unwrap)
      } yield DomainConnectionConfig(name, connection)).leftMap(err =>
        StartFailed(domain.name.unwrap, s"Can not parse config for auto-connect: ${err}")
      )
    val connectParticipants =
      participants.running.filter(_.isActive).flatMap(x => x.getNode.map((x.name, _)).toList)
    def connect(
        name: String,
        node: ParticipantNode,
        configs: Seq[DomainConnectionConfig],
    ): Either[StartupError, Unit] =
      configs.traverse_ { config =>
        val connectET =
          node.autoConnectLocalDomain(config).leftMap(err => StartFailed(name, err.toString))
        this.config.parameters.timeouts.processing.unbounded
          .await("auto-connect to local domain")(connectET.value)
      }
    logger.info(s"Auto-connecting local participants ${connectParticipants
        .map(_._1.unwrap)} to local domains ${activeDomains.map(_.name.unwrap)}")
    activeDomains
      .traverse(toDomainConfig)
      .traverse_(config =>
        connectParticipants.traverse_ { case (name, node) => connect(name.unwrap, node, config) }
      )
  }

  /** Try to startup all nodes in the configured environment and reconnect them to one another.
    * The first error will prevent further nodes from being started.
    * If an error is returned previously started nodes will not be stopped.
    */
  def startAndReconnect(autoConnectLocal: Boolean): Either[StartupError, Unit] =
    withNewTraceContext { implicit traceContext =>
      if (config.parameters.manualStart) {
        logger.info("Manual start requested.")
        Right(())
      } else {
        logger.info("Automatically starting all instances")

        val startup = for {
          _ <- allNodes.traverse(_.attemptStartAll)
          _ <- reconnectParticipants
          _ <- if (autoConnectLocal) autoConnectLocalNodes() else Right(())
        } yield writePortsFile()

        // log results
        startup
          .bimap(
            error => logger.error(s"Failed to start ${error.name}: ${error.message}"),
            _ => logger.info("Successfully started all nodes"),
          )
          .discard
        startup
      }

    }

  private def writePortsFile()(implicit
      traceContext: TraceContext
  ): Unit = {
    case class ParticipantApis(ledgerApi: Int, adminApi: Int)
    config.parameters.portsFile.foreach { portsFile =>
      val items = participants.running.map { node =>
        (
          node.name.unwrap,
          ParticipantApis(node.config.ledgerApi.port.unwrap, node.config.adminApi.port.unwrap),
        )
      }.toMap
      import io.circe.syntax.*
      implicit val encoder: Encoder[ParticipantApis] =
        Encoder.forProduct2("ledgerApi", "adminApi") { apis =>
          (apis.ledgerApi, apis.adminApi)
        }
      val out = items.asJson.spaces2
      try {
        better.files.File(portsFile).overwrite(out)
      } catch {
        case NonFatal(ex) =>
          logger.warn(s"Failed to write to port file ${portsFile}. Will ignore the error", ex)
      }
    }
  }

  private def reconnectParticipants(implicit
      traceContext: TraceContext
  ): Either[StartupError, Unit] = {
    def reconnect(instance: ParticipantNodeBootstrap): Either[StartupError, Unit] = {
      for {
        _ <- instance.getNode match {
          case None =>
            // should not happen, but if it does, display at least a warning.
            if (instance.config.init.autoInit) {
              logger.error(
                s"Auto-initialisation failed or was too slow for ${instance.name}. Will not automatically re-connect to domains."
              )
            }
            Right(())
          case Some(node) =>
            Await
              .result(
                node.reconnectDomainsIgnoreFailures().value,
                config.parameters.timeouts.processing.unbounded.unwrap.minus(25.milliseconds),
              )
              .leftMap(err => StartFailed(instance.name.unwrap, err.toString))
        }
      } yield ()
    }

    participants.running.traverse_(reconnect)
  }

  /** Return current time of environment
    */
  def now: CantonTimestamp = clock.now

  /** Start all instances described in the configuration
    */
  def startAll(): Either[Seq[StartupError], Unit] = {
    val errors = allNodes
      .map(_.startAll)
      .flatMap(_.left.getOrElse(Seq.empty))

    Either.cond(errors.isEmpty, (), errors)
  }

  @VisibleForTesting
  protected def createParticipant(
      name: String,
      participantConfig: config.ParticipantConfigType,
  ): ParticipantNodeBootstrap = {
    participantNodeFactory
      .create(
        name,
        participantConfig,
        config.participantNodeParametersByString(name),
        createClock(Some(ParticipantNodeBootstrap.LoggerFactoryKeyName -> name)),
        testingTimeService,
        metricsFactory.forParticipant(name),
        testingConfig,
        futureSupervisor,
        loggerFactory,
        writeHealthDumpToFile,
        envQueueSize,
      )
      .valueOr(err => throw new RuntimeException(s"Failed to create participant bootstrap: $err"))
  }

  @VisibleForTesting
  protected def createDomain(
      name: String,
      domainConfig: config.DomainConfigType,
  ): DomainNodeBootstrap =
    domainFactory
      .create(
        name,
        domainConfig,
        testingConfig,
        config.domainNodeParametersByString(name),
        createClock(Some(DomainNodeBootstrap.LoggerFactoryKeyName -> name)),
        metricsFactory.forDomain(name),
        futureSupervisor,
        loggerFactory,
        writeHealthDumpToFile,
      )
      .valueOr(err => throw new RuntimeException(s"Failed to create domain bootstrap: $err"))

  private def simClocks: Seq[SimClock] = {
    val clocks = clock +: (participants.running.map(_.clock) ++ domains.running.map(_.clock))
    val simclocks = clocks.collect { case sc: SimClock => sc }
    if (simclocks.sizeCompare(clocks) < 0)
      logger.warn(s"Found non-sim clocks, testing time service will be broken.")
    simclocks
  }

  def addUserCloseable(closeable: AutoCloseable): Unit = userCloseables.append(closeable)

  override def close(): Unit = blocking(this.synchronized {
    val closeActorSystem: AutoCloseable =
      Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts)

    val closeExecutionContext: AutoCloseable =
      ExecutorServiceExtensions(executionContext)(logger, timeouts)
    val closeScheduler: AutoCloseable = ExecutorServiceExtensions(scheduler)(logger, timeouts)

    val closeHealthServer: AutoCloseable = () => healthServer.foreach(_.close())

    val closeHeadlessHealthAdministration: AutoCloseable =
      () => healthDumpGenerator.get.foreach(_.grpcAdminCommandRunner.close())

    // the allNodes list is ordered in ideal startup order, so reverse to shutdown
    val instances =
      monitorO.toList ++ userCloseables ++ allNodes.reverse :+ metricsFactory :+ clock :+ closeHealthServer :+
        closeHeadlessHealthAdministration :+ executionSequencerFactory :+ closeActorSystem :+ closeExecutionContext :+
        closeScheduler
    Lifecycle.close((instances.toSeq): _*)(logger)
  })
}

object Environment {

  /** Ensure all java.util.logging statements are routed to slf4j instead and can be configured with logback.
    * This should be paired with adding a LevelChangePropagator to the logback configuration to avoid the performance impact
    * of translating all JUL log statements (regardless of whether they are being used).
    * See for more details: https://logback.qos.ch/manual/configuration.html#LevelChangePropagator
    */
  def installJavaUtilLoggingBridge(): Unit = {
    if (!SLF4JBridgeHandler.isInstalled) {
      // we want everything going to slf4j so remove any default loggers
      SLF4JBridgeHandler.removeHandlersForRootLogger()
      SLF4JBridgeHandler.install()
    }
  }

}

trait EnvironmentFactory[E <: Environment] {
  def create(
      config: E#Config,
      loggerFactory: NamedLoggerFactory,
      testingConfigInternal: TestingConfigInternal = TestingConfigInternal(),
  ): E
}
