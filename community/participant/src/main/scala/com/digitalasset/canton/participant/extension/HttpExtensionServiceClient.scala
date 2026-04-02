// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.{ExtensionServiceAuthConfig, ExtensionServiceConfig}
import com.digitalasset.canton.participant.extension.HttpExtensionServiceClient.{
  OAuthTokenAcquisitionResolution,
  OAuthTokenAcquisitionStartReason,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}

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
    oauthAssertionFactory: Option[() => String],
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
      oauthAssertionFactory = None,
      loggerFactory = loggerFactory,
    )

  private lazy val resources = resourcesFactory.create(config)
  private lazy val oauthTokenClient: Option[HttpExtensionOAuthTokenClient] = config.auth match {
    case _: ExtensionServiceAuthConfig.OAuth =>
      val tokenTransport = resources.tokenTransport.getOrElse {
        throw new IllegalStateException(
          s"OAuth extension '$extensionId' requires a dedicated token transport"
        )
      }
      val buildAssertion = oauthAssertionFactory.getOrElse {
        val assertionFactory = new HttpExtensionOAuthClientAssertionFactory(
          config = config,
          nowMillis = () => runtime.nowMillis(),
        )
        () => assertionFactory.buildClientAssertion()
      }
      Some(
        new HttpExtensionOAuthTokenClient(
          transport = tokenTransport,
          requestBuilder = new HttpExtensionOAuthTokenRequestBuilder(config),
          buildClientAssertion = buildAssertion,
          responseParser = new HttpExtensionOAuthTokenResponseParser,
          nowMillis = () => runtime.nowMillis(),
          responseMapper = responseMapper,
        )
      )
    case _ => None
  }
  private val oauthTokenLock = new Mutex()
  private val cachedOAuthToken = new AtomicReference[Option[HttpExtensionOAuthAccessToken]](None)
  private val inFlightOAuthTokenAcquisition
      : AtomicReference[Option[Promise[Either[ExtensionCallError, HttpExtensionOAuthAccessToken]]]] =
    new AtomicReference(None)

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
        val result = singleCall(functionId, configHash, input, mode, deadlineMs)

        result match {
          case Right(response) =>
            if (attempt > 0) {
              logger.info(s"External call to extension '$extensionId' succeeded after $attempt retries")
            }
            Right(response)

          case Left(error) if shouldRetry(error) && attempt < config.maxRetries.value =>
            val remainingTimeMs = deadlineMs - runtime.nowMillis()
            if (remainingTimeMs <= 0) {
              logger.warn(
                s"External call to extension '$extensionId' failed (attempt ${attempt + 1}/${config.maxRetries}): ${error.message} (status=${error.statusCode}). Cannot retry: insufficient time remaining (${remainingTimeMs}ms)"
              )
              Left(error)
            } else {
              val delay = calculateBackoff(attempt + 1, error.retryAfter, remainingTimeMs)
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
      deadlineMs: Long,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] = {
    currentBearerToken(deadlineMs) match {
      case Left(error) =>
        Left(error)

      case Right(bearerToken) =>
        sendResourceRequest(
          functionId = functionId,
          configHash = configHash,
          input = input,
          mode = mode,
          bearerToken = bearerToken,
          deadlineMs = deadlineMs,
        ).flatMap { case (response, requestId) =>
          if (shouldReplayAfterUnauthorized(response, bearerToken)) {
            bearerToken.foreach(invalidateCachedOAuthTokenIfMatches)
            currentBearerToken(deadlineMs).flatMap { freshBearerToken =>
              sendResourceRequest(
                functionId = functionId,
                configHash = configHash,
                input = input,
                mode = mode,
                bearerToken = freshBearerToken,
                deadlineMs = deadlineMs,
              ).flatMap { case (replayResponse, replayRequestId) =>
                if (replayResponse.statusCode == 401) {
                  Left(
                    ExtensionCallError(
                      401,
                      "Unauthorized - OAuth token rejected by resource server",
                      Some(replayRequestId),
                    )
                  )
                } else {
                  mapResourceResponse(replayResponse, replayRequestId)
                }
              }
            }
          } else {
            mapResourceResponse(response, requestId)
          }
        }
    }
  }

  private def sendResourceRequest(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
      bearerToken: Option[String],
      deadlineMs: Long,
  )(implicit tc: TraceContext): Either[ExtensionCallError, (HttpExtensionClientResponse, String)] =
    requestTimeoutForRemainingBudget(deadlineMs).flatMap { timeout =>
      val requestId = runtime.newRequestId()
      try {
        val request = requestBuilder.buildCallRequest(
          functionId = functionId,
          configHash = configHash,
          input = input,
          mode = mode,
          timeout = timeout,
          requestId = requestId,
          bearerToken = bearerToken,
        )

        logger.debug(
          s"Making external call to extension '$extensionId': functionId=$functionId, mode=$mode, requestId=$requestId"
        )

        Right(resources.resourceTransport.send(request) -> requestId)
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

  private def mapResourceResponse(
      response: HttpExtensionClientResponse,
      requestId: String,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] =
    responseMapper.mapResponse(response, requestId) match {
      case Right(result) =>
        logger.debug(s"External call to extension '$extensionId' succeeded: requestId=$requestId")
        Right(result)
      case Left(error) =>
        Left(error)
    }

  private def shouldReplayAfterUnauthorized(
      response: HttpExtensionClientResponse,
      bearerToken: Option[String],
  ): Boolean =
    response.statusCode == 401 && bearerToken.nonEmpty && oauthTokenClient.nonEmpty

  private def invalidateCachedOAuthTokenIfMatches(sentToken: String)(implicit tc: TraceContext): Unit =
    oauthTokenLock.exclusive {
      cachedOAuthToken.get() match {
        case Some(cachedToken) if cachedToken.value == sentToken =>
          cachedOAuthToken.set(None)
          logger.debug(
            s"Invalidated cached OAuth token for extension '$extensionId' after resource-server 401"
          )
        case _ =>
          ()
      }
    }

  private def currentBearerToken(deadlineMs: Long)(implicit
      tc: TraceContext
  ): Either[ExtensionCallError, Option[String]] =
    try {
      oauthTokenClient match {
        case None => Right(None)
        case Some(tokenClient) =>
          acquireSharedOAuthToken(tokenClient, deadlineMs).map(token => Some(token.value))
      }
    } catch {
      case e: Exception =>
        Left(logAndMapPreOutboundLocalFailure(e))
    }

  private def acquireSharedOAuthToken(
      tokenClient: HttpExtensionOAuthTokenClient,
      deadlineMs: Long,
  )(implicit tc: TraceContext): Either[ExtensionCallError, HttpExtensionOAuthAccessToken] =
    nextOAuthTokenAcquisitionResolution() match {
      case OAuthTokenAcquisitionResolution.Cached(token) =>
        logger.debug(s"Reusing cached OAuth token for extension '$extensionId'")
        Right(token)
      case OAuthTokenAcquisitionResolution.Join(promise) =>
        blocking(Await.result(promise.future, scala.concurrent.duration.Duration.Inf))
      case OAuthTokenAcquisitionResolution.Start(promise, startReason) =>
        if (startReason == OAuthTokenAcquisitionStartReason.Expired) {
          logger.debug(s"Reacquiring OAuth token for extension '$extensionId' after expiry")
        }
        val result = tryAcquireOAuthToken(tokenClient, deadlineMs)
        oauthTokenLock.exclusive {
          result.foreach(token => cachedOAuthToken.set(Some(token)))
          inFlightOAuthTokenAcquisition.set(None)
        }
        val _ = promise.trySuccess(result)
        result
    }

  private def nextOAuthTokenAcquisitionResolution(): OAuthTokenAcquisitionResolution =
    oauthTokenLock.exclusive {
      val nowMillis = runtime.nowMillis()
      cachedOAuthToken.get() match {
        case Some(token) if token.expiresAtMillis > nowMillis =>
          OAuthTokenAcquisitionResolution.Cached(token)
        case None =>
          inFlightOAuthTokenAcquisition.get() match {
            case Some(existingPromise) =>
              OAuthTokenAcquisitionResolution.Join(existingPromise)
            case None =>
              val promise = Promise[Either[ExtensionCallError, HttpExtensionOAuthAccessToken]]()
              inFlightOAuthTokenAcquisition.set(Some(promise))
              OAuthTokenAcquisitionResolution.Start(
                promise,
                OAuthTokenAcquisitionStartReason.Missing,
              )
          }
        case Some(_) =>
          inFlightOAuthTokenAcquisition.get() match {
            case Some(existingPromise) =>
              OAuthTokenAcquisitionResolution.Join(existingPromise)
            case None =>
              val promise = Promise[Either[ExtensionCallError, HttpExtensionOAuthAccessToken]]()
              inFlightOAuthTokenAcquisition.set(Some(promise))
              OAuthTokenAcquisitionResolution.Start(
                promise,
                OAuthTokenAcquisitionStartReason.Expired,
              )
          }
      }
    }

  private def tryAcquireOAuthToken(
      tokenClient: HttpExtensionOAuthTokenClient,
      deadlineMs: Long,
  )(implicit tc: TraceContext): Either[ExtensionCallError, HttpExtensionOAuthAccessToken] =
    try {
      requestTimeoutForRemainingBudget(deadlineMs).flatMap { timeout =>
        val tokenRequestId = runtime.newRequestId()
        logger.debug(
          s"Starting OAuth token acquisition for extension '$extensionId': requestId=$tokenRequestId"
        )
        val result = tokenClient.acquireToken(
          timeout = timeout,
          requestId = tokenRequestId,
        )
        result match {
          case Right(_) =>
            logger.debug(
              s"OAuth token acquisition succeeded for extension '$extensionId': requestId=$tokenRequestId"
            )
          case Left(error) =>
            logger.warn(
              s"OAuth token acquisition failed for extension '$extensionId': status=${error.statusCode}, requestId=${error.requestId.getOrElse("none")}, message=${error.message}"
            )
        }
        result
      }
    } catch {
      case e: Exception =>
        Left(logAndMapPreOutboundLocalFailure(e))
    }

  private def requestTimeoutForRemainingBudget(deadlineMs: Long): Either[ExtensionCallError, Duration] = {
    val remainingBudgetMs = deadlineMs - runtime.nowMillis()
    if (remainingBudgetMs <= 0) {
      Left(totalTimeoutExceededError)
    } else {
      Right(Duration.ofMillis(math.min(config.requestTimeout.underlying.toMillis, remainingBudgetMs)))
    }
  }

  private def totalTimeoutExceededError: ExtensionCallError =
    ExtensionCallError(504, "Total timeout exceeded", None)

  private def preOutboundLocalFailure(exception: Exception): ExtensionCallError =
    ExtensionCallError(500, s"Unexpected error: ${exception.getMessage}", None)

  private def logAndMapPreOutboundLocalFailure(exception: Exception)(implicit
      tc: TraceContext
  ): ExtensionCallError = {
    logger.error(
      s"External call to extension '$extensionId' local OAuth initialization failed before outbound HTTP: error=${exception.getMessage}"
    )
    preOutboundLocalFailure(exception)
  }

  private def shouldRetry(error: ExtensionCallError): Boolean = {
    error.statusCode match {
      case 408 | 429 | 500 | 502 | 503 | 504 => true
      case _ => false
    }
  }

  private def calculateBackoff(attempt: Int, retryAfter: Option[Int], remainingTimeMs: Long): Long = {
    val availableForBackoff = (remainingTimeMs - 1L).max(0L)
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

private[extension] object HttpExtensionServiceClient {
  sealed trait OAuthTokenAcquisitionResolution

  sealed trait OAuthTokenAcquisitionStartReason

  object OAuthTokenAcquisitionStartReason {
    case object Missing extends OAuthTokenAcquisitionStartReason
    case object Expired extends OAuthTokenAcquisitionStartReason
  }

  object OAuthTokenAcquisitionResolution {
    final case class Cached(token: HttpExtensionOAuthAccessToken)
        extends OAuthTokenAcquisitionResolution
    final case class Join(promise: Promise[Either[ExtensionCallError, HttpExtensionOAuthAccessToken]])
        extends OAuthTokenAcquisitionResolution
    final case class Start(
        promise: Promise[Either[ExtensionCallError, HttpExtensionOAuthAccessToken]],
        reason: OAuthTokenAcquisitionStartReason,
    )
        extends OAuthTokenAcquisitionResolution
  }
}
