// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.junit.jupiter

import com.digitalasset.canton.config.{CantonCommunityConfig, TestingConfigInternal}
import com.digitalasset.canton.environment.{
  CommunityConsoleEnvironment,
  CommunityEnvironment,
  CommunityEnvironmentFactory,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import org.junit.jupiter.api.extension.{
  AfterAllCallback,
  BeforeAllCallback,
  ExtensionContext,
  ParameterContext,
  ParameterResolver,
}

import java.io.File

// TODO(i11791): This is a PoC
final class CantonExtension extends ParameterResolver with BeforeAllCallback with AfterAllCallback {

  // Initializing lazily to avoid null initialization, will throw in ``beforeAll``
  private lazy val env: CommunityEnvironment = {
    val config = CantonCommunityConfig.parseAndLoad(
      Seq(
        new File(
          this.getClass.getClassLoader
            .getResource("com/digitalasset/canton/integration/junit/jupiter/testing-topology.conf")
            .toURI
        )
      )
    ) match {
      case Left(error) =>
        @SuppressWarnings(Array("org.wartremover.warts.Null"))
        val nullableCause = error.throwableO.orNull
        throw new Exception("error while loading the configuration", nullableCause)
      case Right(config) => config
    }
    CommunityEnvironmentFactory.create(
      config,
      NamedLoggerFactory.root, // TODO(i11792) use sensible logger
      TestingConfigInternal(initializeGlobalOpenTelemetry =
        false
      ), // TODO (i11793) fails if ``true``, investigate improvements in configuration
    )
  }

  private lazy val console: CommunityConsoleEnvironment = env.createConsole()

  override def supportsParameter(
      parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): Boolean =
    parameterContext.isAnnotated(classOf[Canton])

  override def resolveParameter(
      parameterContext: ParameterContext,
      extensionContext: ExtensionContext,
  ): AnyRef =
    console

  @throws[Exception]
  override def beforeAll(context: ExtensionContext): Unit =
    env
      .startAll()
      .fold(
        errors =>
          throw new Exception(
            errors.mkString("The following errors occurred while starting Canton: '", "', '", "'.")
          ),
        identity,
      )

  @throws[Exception]
  override def afterAll(context: ExtensionContext): Unit =
    env.close()

}
