// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.digitalasset.canton.CloseableTest
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.logging.NamedLogging
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.util.control.NonFatal

/** Provides an ability to create a canton environment when needed for test.
  * Include [[IsolatedEnvironments]] or [[SharedEnvironment]] to determine when this happens.
  * Uses [[ConcurrentEnvironmentLimiter]] to ensure we limit the number of concurrent environments in a test run.
  */
sealed trait EnvironmentSetup[E <: Environment, TCE <: TestConsoleEnvironment[E]]
    extends BeforeAndAfterAll {
  this: Suite with HasEnvironmentDefinition[E, TCE] with NamedLogging =>

  private lazy val envDef = environmentDefinition

  // plugins are registered during construction from a single thread
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var plugins: Seq[EnvironmentSetupPlugin[E, TCE]] = Seq()

  protected[integration] def registerPlugin(plugin: EnvironmentSetupPlugin[E, TCE]): Unit =
    plugins = plugins :+ plugin

  override protected def beforeAll(): Unit = {
    plugins.foreach(_.beforeTests())
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try super.afterAll()
    finally plugins.foreach(_.afterTests())
  }

  /** Provide an environment for an individual test either by reusing an existing one or creating a new one
    * depending on the approach being used.
    */
  def provideEnvironment: TCE

  /** Optional hook for implementors to know when a test has finished and be provided the environment instance.
    * This is required over a afterEach hook as we need the environment instance passed.
    */
  def testFinished(environment: TCE): Unit = {}

  /** Creates a new environment manually for a test without concurrent environment limitation and with optional config transformation. */
  protected def manualCreateEnvironment(
      configTransform: E#Config => E#Config = identity,
      runPlugins: Boolean = true,
  ): TCE = {
    val testConfig = envDef.generateConfig
    // note: beforeEnvironmentCreate may well have side-effects (e.g. starting databases or docker containers)
    val pluginConfig = {
      if (runPlugins)
        plugins.foldLeft(testConfig)((config, plugin) => plugin.beforeEnvironmentCreated(config))
      else testConfig
    }
    val finalConfig = configTransform(pluginConfig)
    val environmentFixture =
      envDef.environmentFactory.create(
        finalConfig,
        loggerFactory,
        envDef.testingConfig,
      )

    try {
      val testEnvironment: TCE =
        envDef.createTestConsole(environmentFixture, loggerFactory)

      if (runPlugins) {
        plugins.foreach(_.afterEnvironmentCreated(finalConfig, testEnvironment))
      }

      if (!finalConfig.parameters.manualStart)
        testEnvironment.startAll()

      envDef.setup(testEnvironment)

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
    ConcurrentEnvironmentLimiter.create(getClass.getName)(manualCreateEnvironment())

  protected def destroyEnvironment(environment: TCE): Unit =
    ConcurrentEnvironmentLimiter.destroy(getClass.getName) {
      val config = environment.actualConfig
      plugins.foreach(_.beforeEnvironmentDestroyed(config, environment))
      try {
        environment.close()
      } finally {
        envDef.teardown(())
        plugins.foreach(_.afterEnvironmentDestroyed(config))
      }
    }
}

/** Starts an environment in a beforeAll test and uses it for all tests.
  * Destroys it in an afterAll hook.
  */
trait SharedEnvironment[E <: Environment, TCE <: TestConsoleEnvironment[E]]
    extends EnvironmentSetup[E, TCE]
    with CloseableTest {
  this: Suite with HasEnvironmentDefinition[E, TCE] with NamedLogging =>

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var sharedEnvironment: Option[TCE] = None

  override def beforeAll(): Unit = {
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

/** Creates an environment for each test. */
trait IsolatedEnvironments[E <: Environment, TCE <: TestConsoleEnvironment[E]]
    extends EnvironmentSetup[E, TCE] {
  this: Suite with HasEnvironmentDefinition[E, TCE] with NamedLogging =>

  override def provideEnvironment: TCE = createEnvironment()
  override def testFinished(environment: TCE): Unit = destroyEnvironment(environment)
}
