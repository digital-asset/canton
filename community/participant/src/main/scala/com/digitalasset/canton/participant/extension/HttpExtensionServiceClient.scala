// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ExtensionServiceConfig
import com.digitalasset.canton.tracing.TraceContext

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future, blocking}

/** HTTP client implementation for extension services with retry logic,
  * JWT authentication, and TLS support.
  *
  * @param extensionId The extension identifier (key from config map)
  * @param config Configuration for this extension service
  * @param resourcesFactory HTTP resource factory for this extension service
  * @param runtime Runtime side effects used by retry and request generation
  * @param requestBuilder Request construction helper
  * @param responseMapper Response and exception mapping helper
  * @param loggerFactory Logger factory
  */
class HttpExtensionServiceClient private[extension] (
    override val extensionId: String,
    config: ExtensionServiceConfig,
    resourcesFactory: HttpExtensionClientResourcesFactory,
    runtime: HttpExtensionClientRuntime,
    requestBuilder: HttpExtensionRequestBuilder,
    responseMapper: HttpExtensionResponseMapper,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ExtensionServiceClient
    with NamedLogging {

  def this(
      extensionId: String,
      config: ExtensionServiceConfig,
      resourcesFactory: HttpExtensionClientResourcesFactory,
      runtime: HttpExtensionClientRuntime,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext) =
    this(
      extensionId = extensionId,
      config = config,
      resourcesFactory = resourcesFactory,
      runtime = runtime,
      requestBuilder = new HttpExtensionRequestBuilder(config),
      responseMapper = new HttpExtensionResponseMapper,
      loggerFactory = loggerFactory,
    )

  private lazy val resources = resourcesFactory.create(config)

  // Declared function config hashes for validation
  private val declaredConfigHashes: Map[String, String] =
    config.declaredFunctions.map(f => f.functionId -> f.configHash).toMap

  override def getDeclaredConfigHash(functionId: String): Option[String] =
    declaredConfigHashes.get(functionId)

  override def call(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] = {
    FutureUnlessShutdown.outcomeF {
      Future {
        blocking {
          callWithRetry(functionId, configHash, input, mode)
        }
      }
    }
  }

  override def validateConfiguration()(implicit tc: TraceContext): FutureUnlessShutdown[ExtensionValidationResult] = {
    FutureUnlessShutdown.outcomeF {
      Future {
        blocking {
          // Try to make a simple health check call
          // For now, we just verify we can establish a connection
          try {
            val requestId = runtime.newRequestId()
            val request = requestBuilder.buildValidationRequest(
              timeout = Duration.ofMillis(config.connectTimeout.underlying.toMillis),
              requestId = requestId,
            )

            // We don't really care about the response, just that we can connect
            val resp = resources.resourceTransport.send(request)

            // Any response (even 4xx) means the service is reachable
            if (resp.statusCode >= 200 && resp.statusCode < 600) {
              ExtensionValidationResult.Valid
            } else {
              ExtensionValidationResult.Invalid(Seq(s"Unexpected response code: ${resp.statusCode}"))
            }
          } catch {
            case e: java.net.ConnectException =>
              ExtensionValidationResult.Invalid(Seq(s"Cannot connect to extension service: ${e.getMessage}"))
            case e: java.net.http.HttpTimeoutException =>
              ExtensionValidationResult.Invalid(Seq(s"Connection timeout: ${e.getMessage}"))
            case e: Exception =>
              ExtensionValidationResult.Invalid(Seq(s"Validation failed: ${e.getMessage}"))
          }
        }
      }
    }
  }

  /** Make an HTTP call with retry logic */
  private def callWithRetry(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] = {
    val deadlineMs = runtime.nowMillis() + config.maxTotalTimeout.underlying.toMillis

    def loop(attempt: Int, lastError: Option[ExtensionCallError]): Either[ExtensionCallError, String] = {
      val nowMs = runtime.nowMillis()
      if (nowMs >= deadlineMs) {
        val finalError = lastError.getOrElse(
          ExtensionCallError(504, "Total timeout exceeded", None)
        )
        logger.error(
          s"External call to extension '$extensionId' exceeded maximum total timeout of ${config.maxTotalTimeout} after $attempt attempts: ${finalError.message}"
        )
        Left(finalError)
      } else if (attempt > config.maxRetries.value) {
        val finalError = lastError.getOrElse(
          ExtensionCallError(500, "Max retries exceeded", None)
        )
        logger.error(
          s"External call to extension '$extensionId' failed after ${config.maxRetries} retries: ${finalError.message} (status=${finalError.statusCode})"
        )
        Left(finalError)
      } else {
        val result = singleCall(functionId, configHash, input, mode)

        result match {
          case Right(response) =>
            if (attempt > 0) {
              logger.info(s"External call to extension '$extensionId' succeeded after $attempt retries")
            }
            Right(response)

          case Left(error) if shouldRetry(error) && attempt < config.maxRetries.value =>
            val remainingTimeMs = deadlineMs - runtime.nowMillis()
            val delay = calculateBackoff(attempt + 1, error.retryAfter, remainingTimeMs)

            if (delay >= remainingTimeMs) {
              logger.warn(
                s"External call to extension '$extensionId' failed (attempt ${attempt + 1}/${config.maxRetries}): ${error.message} (status=${error.statusCode}). Cannot retry: insufficient time remaining (${remainingTimeMs}ms)"
              )
              Left(error)
            } else {
              logger.warn(
                s"External call to extension '$extensionId' failed (attempt ${attempt + 1}/${config.maxRetries}): ${error.message} (status=${error.statusCode}). Retrying in ${delay}ms"
              )

              runtime.sleepMillis(delay)
              loop(attempt + 1, Some(error))
            }

          case Left(error) =>
            logger.error(
              s"External call to extension '$extensionId' failed with non-retryable error: ${error.message} (status=${error.statusCode}, requestId=${error.requestId.getOrElse("none")})"
            )
            Left(error)
        }
      }
    }

    loop(0, None)
  }

  /** Make a single HTTP call without retry logic */
  private def singleCall(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] = {
    val requestId = runtime.newRequestId()

    try {
      val request = requestBuilder.buildCallRequest(
        functionId = functionId,
        configHash = configHash,
        input = input,
        mode = mode,
        timeout = Duration.ofMillis(config.requestTimeout.underlying.toMillis),
        requestId = requestId,
      )

      logger.debug(
        s"Making external call to extension '$extensionId': functionId=$functionId, mode=$mode, requestId=$requestId"
      )

      val resp = resources.resourceTransport.send(request)

      responseMapper.mapResponse(resp, requestId) match {
        case Right(response) =>
          logger.debug(s"External call to extension '$extensionId' succeeded: requestId=$requestId")
          Right(response)

        case Left(error) =>
          Left(error)
      }
    } catch {
      case e: java.net.http.HttpTimeoutException =>
        logger.warn(s"External call to extension '$extensionId' timed out: requestId=$requestId")
        Left(responseMapper.mapException(e, requestId))

      case e: java.net.ConnectException =>
        logger.error(s"External call to extension '$extensionId' connection failed: requestId=$requestId, error=${e.getMessage}")
        Left(responseMapper.mapException(e, requestId))

      case e: java.io.IOException =>
        logger.error(s"External call to extension '$extensionId' I/O error: requestId=$requestId, error=${e.getMessage}")
        Left(responseMapper.mapException(e, requestId))

      case e: Exception =>
        logger.error(s"External call to extension '$extensionId' unexpected error: requestId=$requestId, error=${e.getMessage}")
        Left(responseMapper.mapException(e, requestId))
    }
  }

  private def shouldRetry(error: ExtensionCallError): Boolean = {
    error.statusCode match {
      case 408 | 429 | 500 | 502 | 503 | 504 => true
      case _ => false
    }
  }

  private def calculateBackoff(attempt: Int, retryAfter: Option[Int], remainingTimeMs: Long): Long = {
    val connectTimeoutMs = config.connectTimeout.underlying.toMillis
    val requestTimeoutMs = config.requestTimeout.underlying.toMillis
    val minTimeForNextRequest = connectTimeoutMs + requestTimeoutMs
    val availableForBackoff = (remainingTimeMs - minTimeForNextRequest).max(0L)
    val retryInitialDelayMs = config.retryInitialDelay.underlying.toMillis
    val retryMaxDelayMs = config.retryMaxDelay.underlying.toMillis

    val baseDelay = retryAfter match {
      case Some(seconds) =>
        (seconds * 1000L).min(retryMaxDelayMs).min(availableForBackoff)

      case None =>
        val exponentialDelay = retryInitialDelayMs * Math.pow(2, (attempt - 1).toDouble).toLong
        val jitter = (runtime.nextRetryJitterDouble() * 0.3 * exponentialDelay).toLong
        (exponentialDelay + jitter).min(retryMaxDelayMs).min(availableForBackoff)
    }

    baseDelay
  }

  private implicit class ExtensionCallErrorOps(error: ExtensionCallError) {
    def retryAfter: Option[Int] = responseMapper.retryAfter(error)
  }
}
