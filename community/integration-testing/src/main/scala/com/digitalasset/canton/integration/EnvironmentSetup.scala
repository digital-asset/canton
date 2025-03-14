// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.CloseableTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  CantonConfig,
  CantonEdition,
  DefaultPorts,
  TestingConfigInternal,
}
import com.digitalasset.canton.logging.{LogEntry, NamedLogging, SuppressingLogger}
import com.digitalasset.canton.metrics.{MetricsFactoryType, ScopedInMemoryMetricsFactory}
import org.scalatest.{Assertion, BeforeAndAfterAll, Suite}

import scala.util.control.NonFatal

/** Provides an ability to create a canton environment when needed for test. Include
  * [[IsolatedEnvironments]] or [[SharedEnvironment]] to determine when this happens. Uses
  * [[ConcurrentEnvironmentLimiter]] to ensure we limit the number of concurrent environments in a
  * test run.
  */
sealed trait EnvironmentSetup[TCE <: TestConsoleEnvironment] extends BeforeAndAfterAll {
  this: Suite with HasEnvironmentDefinition[TCE] with NamedLogging =>

  private lazy val envDef = environmentDefinition

  val edition: CantonEdition

  // plugins are registered during construction from a single thread
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var plugins: Seq[EnvironmentSetupPlugin[TCE]] = Seq()

  protected[integration] def registerPlugin(plugin: EnvironmentSetupPlugin[TCE]): Unit =
    plugins = plugins :+ plugin

  override protected def beforeAll(): Unit = {
    plugins.foreach(_.beforeTests())
    super.beforeAll()
  }

  override protected def afterAll(): Unit =
    try super.afterAll()
    finally plugins.foreach(_.afterTests())

  /** Provide an environment for an individual test either by reusing an existing one or creating a
    * new one depending on the approach being used.
    */
  def provideEnvironment: TCE

  /** Optional hook for implementors to know when a test has finished and be provided the
    * environment instance. This is required over a afterEach hook as we need the environment
    * instance passed.
    */
  def testFinished(environment: TCE): Unit = {}

  override val loggerFactory: SuppressingLogger = SuppressingLogger(getClass)

  def logsToBeHandledAtStartup: Option[Seq[LogEntry] => Assertion] = None

  protected def handleStartupLogs[T](start: => T): T =
    logsToBeHandledAtStartup
      .map(assertion => loggerFactory.assertLoggedWarningsAndErrorsSeq(start, assertion))
      .getOrElse(start)

  /** Creates a new environment manually for a test without concurrent environment limitation and
    * with optional config transformation.
    *
    * @param initialConfig
    *   specifies which configuration to start from with the default being a NEW one created from
    *   the current environment.
    * @param configTransform
    *   a function that applies changes to the initial configuration (with the plugins applied on
    *   top)
    * @param runPlugins
    *   a function that expects a plugin reference and returns whether or not it's supposed to be
    *   run against the initial configuration
    * @return
    *   a new test console environment
    */
  protected def manualCreateEnvironment(
      initialConfig: CantonConfig = envDef.generateConfig,
      configTransform: ConfigTransform = identity,
      runPlugins: EnvironmentSetupPlugin[TCE] => Boolean = _ => true,
      testConfigTransform: TestingConfigInternal => TestingConfigInternal = identity,
  ): TCE = {
    val testConfig = initialConfig
    // note: beforeEnvironmentCreate may well have side-effects (e.g. starting databases or docker containers)
    val pluginConfig =
      plugins.foldLeft(testConfig)((config, plugin) =>
        if (runPlugins(plugin))
          plugin.beforeEnvironmentCreated(config)
        else
          config
      )

    // Once all the plugins and config transformation is done apply the defaults
    val finalConfig =
      configTransform(pluginConfig).withDefaults(new DefaultPorts(), edition)

    val scopedMetricsFactory = new ScopedInMemoryMetricsFactory
    val environmentFixture =
      envDef.environmentFactory.create(
        finalConfig,
        loggerFactory,
        testConfigTransform(
          envDef.testingConfig.copy(
            metricsFactoryType =
              /* If metrics reporters were configured for the test then it's an externally observed test
               * therefore actual metrics have to be reported.
               * The in memory metrics are used when no reporters are configured and the metrics are
               * observed directly in the test scenarios.
               *
               * In this case, you can grab the metrics from the [[MetricsRegistry.generateMetricsFactory]] method,
               * which is accessible using env.environment.metricsRegistry
               *
               * */
              if (finalConfig.monitoring.metrics.reporters.isEmpty)
                MetricsFactoryType.InMemory(scopedMetricsFactory)
              else MetricsFactoryType.External,
            initializeGlobalOpenTelemetry = false,
            sequencerTransportSeed = Some(1L),
          )
        ),
      )

    try {
      val testEnvironment: TCE =
        envDef.createTestConsole(environmentFixture, loggerFactory)

      plugins.foreach(plugin =>
        if (runPlugins(plugin)) plugin.afterEnvironmentCreated(finalConfig, testEnvironment)
      )

      if (!finalConfig.parameters.manualStart)
        handleStartupLogs(testEnvironment.startAll())

      envDef.setups.foreach(setup => setup(testEnvironment))

      testEnvironment
    } catch {
      case NonFatal(ex) =>
        // attempt to ensure the environment is shutdown if anything in the startup of initialization fails
        try {
          environmentFixture.close()
        } catch {
          case NonFatal(shutdownException) =>
            // we suppress the exception thrown by the shutdown as we want to propagate the original
            // exception, however add it to the suppressed list on this thrown exception
            ex.addSuppressed(shutdownException)
        }
        // rethrow exception
        throw ex
    }
  }

  protected def createEnvironment(): TCE =
    ConcurrentEnvironmentLimiter.create(getClass.getName, numPermits)(
      manualCreateEnvironment()
    )

  protected def manualDestroyEnvironment(environment: TCE): Unit = {
    val config = environment.actualConfig
    plugins.foreach(_.beforeEnvironmentDestroyed(config, environment))
    try {
      environment.close()
    } finally {
      envDef.teardown(())
      plugins.foreach(_.afterEnvironmentDestroyed(config))
    }
  }

  protected def destroyEnvironment(environment: TCE): Unit = {
    environment.verifyParticipantLapiIntegrity(plugins)
    ConcurrentEnvironmentLimiter.destroy(getClass.getName, numPermits) {
      manualDestroyEnvironment(environment)
    }
  }

  /** number of permits required by this test
    *
    * this can be used for heavy tests to ensure that we have less other tests running concurrently
    */
  protected def numPermits: PositiveInt = PositiveInt.one

}

/** Starts an environment in a beforeAll test and uses it for all tests. Destroys it in an afterAll
  * hook.
  *
  * As a result, the environment state at the beginning of a test case equals the state at the end
  * of the previous test case.
  */
trait SharedEnvironment[TCE <: TestConsoleEnvironment]
    extends EnvironmentSetup[TCE]
    with CloseableTest {
  this: Suite with HasEnvironmentDefinition[TCE] with NamedLogging =>

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var sharedEnvironment: Option[TCE] = None

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sharedEnvironment = Some(createEnvironment())
  }

  override def afterAll(): Unit =
    try {
      sharedEnvironment.foreach(destroyEnvironment)
    } finally super.afterAll()

  override def provideEnvironment: TCE =
    sharedEnvironment.getOrElse(
      sys.error("beforeAll should have run before providing a shared environment")
    )
}

/** Creates an environment for each test. As a result, every test case starts with a fresh
  * environment.
  *
  * Try to use SharedEnvironment instead to avoid the cost of frequently creating environments in
  * CI.
  */
trait IsolatedEnvironments[TCE <: TestConsoleEnvironment] extends EnvironmentSetup[TCE] {
  this: Suite with HasEnvironmentDefinition[TCE] with NamedLogging =>

  override def provideEnvironment: TCE = createEnvironment()
  override def testFinished(environment: TCE): Unit = destroyEnvironment(environment)
}
