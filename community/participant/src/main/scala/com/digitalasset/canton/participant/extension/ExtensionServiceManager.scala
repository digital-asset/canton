// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, Threading}
import com.digitalasset.canton.config.{PemFileOrString, ProcessingTimeout}
import com.digitalasset.canton.http.HttpService
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ExtensionServiceConfig
import com.digitalasset.canton.platform.execution.ExternalCallMode
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherUtil.*

import java.net.http.HttpClient
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor}
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Manages extension service connections with per-extension pooled HTTP clients.
  *
  * This manager is responsible for:
  *   - Creating and managing per-extension HTTP clients with connection pooling
  *   - Dispatching external call requests to the appropriate extension service
  *
  * @param extensionConfigs
  *   Map of extension ID to configuration
  * @param loggerFactory
  *   Logger factory
  * @param timeouts
  *   Processing timeouts used for lifecycle synchronization
  * @param ec
  *   Execution context
  */
class ExtensionServiceManager(
    extensionConfigs: Map[String, ExtensionServiceConfig],
    override protected val loggerFactory: NamedLoggerFactory,
    override protected val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val timeoutScheduler: Option[ScheduledExecutorService] =
    Option.when(extensionConfigs.nonEmpty) {
      val scheduler = Threading.singleThreadScheduledExecutor(
        loggerFactory.threadName + "-extension-service-http-timeout",
        noTracingLogger,
      )
      scheduler match {
        case executor: ScheduledThreadPoolExecutor =>
          executor.setRemoveOnCancelPolicy(true)
        case _ => ()
      }
      scheduler
    }

  // Extension clients by ID
  private val clients: Map[String, HttpExtensionServiceClient] =
    timeoutScheduler.fold(Map.empty[String, HttpExtensionServiceClient]) { scheduler =>
      try {
        extensionConfigs.map { case (id, config) =>
          id -> new HttpExtensionServiceClient(
            id,
            config,
            ExtensionServiceManager.createHttpClient(config),
            scheduler,
            this,
            loggerFactory,
          )
        }
      } catch {
        case NonFatal(exception) =>
          closeTimeoutScheduler(scheduler)
          throw exception
      }
    }

  /** Get a client for the specified extension.
    *
    * @param extensionId
    *   The extension identifier
    * @return
    *   Some(client) if configured, None otherwise
    */
  def getClient(extensionId: String): Option[ExtensionServiceClient] =
    clients.get(extensionId)

  /** Handle an external call question from the engine.
    *
    * This is the main entry point for external calls from the Daml engine.
    *
    * @param extensionId
    *   The extension identifier
    * @param functionId
    *   Function identifier within the extension
    * @param configHash
    *   Configuration hash (hex) for version validation
    * @param input
    *   Input data (hex)
    * @param mode
    *   Execution mode
    * @return
    *   Either an error or the response body (hex)
    */
  def handleExternalCall(
      extensionId: String,
      functionId: String,
      configHash: String,
      input: String,
      mode: ExternalCallMode,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] = {
    val result = clients.get(extensionId) match {
      case Some(client) =>
        client.call(functionId, configHash, input, mode)
      case None =>
        FutureUnlessShutdown.pure(
          Left(
            ExtensionCallError(
              statusCode = 404,
              message = s"Extension '$extensionId' not configured",
              requestId = None,
              retryable = false,
            )
          )
        )
    }
    // Log the full error here, at the last common point before the per-consumer
    // sanitization drops the message.
    result.map(_.tapLeft { error =>
      val requestId = error.requestId.fold("")(id => s", requestId=$id")
      logger.warn(
        s"External call to extension '$extensionId' (function '$functionId') failed: status=${error.statusCode}, retryable=${error.retryable}$requestId, message=${error.message}"
      )
    })
  }

  def initializeOnStartup()(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[Either[String, Unit]] = {
    val localPreflightErrors =
      clients.toSeq.flatMap { case (id, client) =>
        client.startupLocalPreflight().left.toOption.map(error => s"Extension '$id': $error")
      }

    if (localPreflightErrors.nonEmpty) {
      val message =
        s"Extension startup local preflight failed: ${localPreflightErrors.mkString("; ")}"
      logger.error(message)
      FutureUnlessShutdown.pure(Left(message))
    } else {
      val clientsToValidate =
        clients.toSeq.filter { case (id, _) =>
          extensionConfigs.get(id).exists(_.validateOnStartup)
        }

      if (clientsToValidate.isEmpty) {
        FutureUnlessShutdown.pure(Right(()))
      } else {
        logger.info(
          s"Validating ${clientsToValidate.size} extension service connection(s) on startup"
        )
        FutureUnlessShutdown
          .sequence(
            clientsToValidate.map { case (id, client) =>
              client.validateConnection().map(result => id -> result)
            }
          )
          .map { results =>
            val invalidResults = results.collect {
              case (id, ExtensionValidationResult.Invalid(errors)) =>
                s"Extension '$id': ${errors.mkString(", ")}"
            }

            if (invalidResults.isEmpty) {
              Right(())
            } else {
              val message =
                s"Extension startup validation failed: ${invalidResults.mkString("; ")}"
              logger.error(message)
              Left(message)
            }
          }
      }
    }
  }

  /** Check if the manager has any configured extensions. */
  def hasExtensions: Boolean = clients.nonEmpty

  /** Get the list of configured extension IDs. */
  def extensionIds: Set[String] = clients.keySet

  override protected def onClosed(): Unit =
    timeoutScheduler.foreach(closeTimeoutScheduler)

  private def closeTimeoutScheduler(scheduler: ScheduledExecutorService): Unit =
    LifeCycle.close(ExecutorServiceExtensions(scheduler)(logger, timeouts))(logger)
}

object ExtensionServiceManager {

  private[extension] def createHttpClient(
      config: ExtensionServiceConfig
  )(implicit ec: ExecutionContext): HttpClient = {
    val builder = HttpClient
      .newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(config.connectTimeout.asJava)
      .executor((command: Runnable) => ec.execute(command))

    config.tls.filter(_.enabled).flatMap(_.trustCollectionFile).foreach { trustCollectionFile =>
      builder.sslContext(sslContext(trustCollectionFile))
    }

    builder
      .build()
  }

  private def sslContext(trustCollectionFile: PemFileOrString) = {
    val certificateFactory = CertificateFactory.getInstance("X.509")
    val certificates =
      certificateFactory.generateCertificates(trustCollectionFile.pemStream).asScala

    require(
      certificates.nonEmpty,
      "TLS trust collection must contain at least one X.509 certificate",
    )

    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    loadEmpty(keyStore)
    certificates.zipWithIndex.foreach { case (certificate, index) =>
      keyStore.setCertificateEntry(s"certificate-$index", certificate)
    }

    HttpService.buildSSLContext(keyStore)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def loadEmpty(keyStore: KeyStore): Unit =
    keyStore.load(null, null)

  /** Create an ExtensionServiceManager with no extensions configured. Useful for tests or when no
    * extensions are needed.
    */
  def empty(
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit ec: ExecutionContext): ExtensionServiceManager =
    new ExtensionServiceManager(
      Map.empty,
      loggerFactory,
      timeouts,
    )
}
