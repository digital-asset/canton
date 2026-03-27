// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.{EngineExtensionsConfig, ExtensionServiceConfig}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Manages extension service connections with one client per configured extension.
  *
  * This manager is responsible for:
  * - Creating and managing extension clients
  * - Dispatching external call requests to the appropriate extension service
  * - Validating extension configurations on startup
  *
  * @param extensionConfigs Map of extension ID to configuration
  * @param engineExtensionsConfig Engine extensions configuration
  * @param resourcesFactory HTTP resource factory for extension clients
  * @param runtime Runtime side effects used by extension clients
  * @param loggerFactory Logger factory
  * @param ec Execution context
  */
class ExtensionServiceManager private[extension] (
    extensionConfigs: Map[String, ExtensionServiceConfig],
    engineExtensionsConfig: EngineExtensionsConfig,
    resourcesFactory: HttpExtensionClientResourcesFactory,
    runtime: HttpExtensionClientRuntime,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  def this(
      extensionConfigs: Map[String, ExtensionServiceConfig],
      engineExtensionsConfig: EngineExtensionsConfig,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) =
    this(
      extensionConfigs = extensionConfigs,
      engineExtensionsConfig = engineExtensionsConfig,
      resourcesFactory = new JdkHttpExtensionClientResourcesFactory(loggerFactory),
      runtime = HttpExtensionClientRuntime.system,
      loggerFactory = loggerFactory,
    )

  override val timeouts: ProcessingTimeout = ProcessingTimeout()

  // Extension clients by ID
  private val clients: Map[String, ExtensionServiceClient] = {
    if (engineExtensionsConfig.echoMode) {
      logger.info("Extension services running in echo mode - external calls will return input as output")(TraceContext.empty)
      extensionConfigs.map { case (id, _) =>
        id -> new EchoExtensionServiceClient(id)
      }
    } else {
      extensionConfigs.map { case (id, config) =>
        id -> new HttpExtensionServiceClient(
          id,
          config,
          resourcesFactory,
          runtime,
          loggerFactory,
        )
      }
    }
  }

  /** Get a client for the specified extension.
    *
    * @param extensionId The extension identifier
    * @return Some(client) if configured, None otherwise
    */
  def getClient(extensionId: String): Option[ExtensionServiceClient] =
    clients.get(extensionId)

  /** Handle an external call question from the engine.
    *
    * This is the main entry point for external calls from the Daml engine.
    *
    * @param extensionId The extension identifier
    * @param functionId Function identifier within the extension
    * @param configHash Configuration hash (hex) for version validation
    * @param input Input data (hex)
    * @param mode Execution mode ("submission" or "validation")
    * @return Either an error or the response body (hex)
    */
  def handleExternalCall(
      extensionId: String,
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] = {
    clients.get(extensionId) match {
      case Some(client) =>
        client.call(functionId, configHash, input, mode)
      case None =>
        FutureUnlessShutdown.pure(Left(ExtensionCallError(
          statusCode = 404,
          message = s"Extension '$extensionId' not configured. Available extensions: ${clients.keys.mkString(", ")}",
          requestId = None,
        )))
    }
  }

  /** Validate all configured extensions on startup.
    *
    * @return Map of extension ID to validation result
    */
  def validateAllExtensions()(implicit tc: TraceContext): FutureUnlessShutdown[Map[String, ExtensionValidationResult]] = {
    if (!engineExtensionsConfig.validateExtensionsOnStartup) {
      logger.info("Extension validation on startup is disabled")
      FutureUnlessShutdown.pure(Map.empty)
    } else {
      logger.info(s"Validating ${clients.size} configured extension(s)...")
      FutureUnlessShutdown.sequence(
        clients.map { case (id, client) =>
          client.validateConfiguration().map { result =>
            result match {
              case ExtensionValidationResult.Valid =>
                logger.info(s"Extension '$id' validation: OK")
              case ExtensionValidationResult.Invalid(errors) =>
                logger.warn(s"Extension '$id' validation failed: ${errors.mkString(", ")}")
            }
            id -> result
          }
        }.toSeq
      ).map(_.toMap)
    }
  }

  /** Check if the manager has any configured extensions. */
  def hasExtensions: Boolean = clients.nonEmpty

  /** Get the list of configured extension IDs. */
  def extensionIds: Set[String] = clients.keySet

  override def onClosed(): Unit = {
    logger.debug("ExtensionServiceManager closed")(TraceContext.empty)
  }
}

object ExtensionServiceManager {

  /** Create an ExtensionServiceManager with no extensions configured.
    * Useful for tests or when no extensions are needed.
    */
  def empty(loggerFactory: NamedLoggerFactory)(implicit ec: ExecutionContext): ExtensionServiceManager =
    new ExtensionServiceManager(
      Map.empty,
      EngineExtensionsConfig.default,
      loggerFactory,
    )
}

/** Echo extension service client for testing.
  * Returns the input as the output without making any HTTP calls.
  */
private class EchoExtensionServiceClient(override val extensionId: String)
    extends ExtensionServiceClient {

  override def call(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] = {
    // Echo mode: return input as output
    FutureUnlessShutdown.pure(Right(input))
  }

  override def getDeclaredConfigHash(functionId: String): Option[String] = None

  override def validateConfiguration()(implicit tc: TraceContext): FutureUnlessShutdown[ExtensionValidationResult] =
    FutureUnlessShutdown.pure(ExtensionValidationResult.Valid)
}
