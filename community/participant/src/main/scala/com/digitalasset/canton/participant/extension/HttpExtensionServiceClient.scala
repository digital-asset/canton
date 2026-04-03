// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.{ExtensionServiceAuthConfig, ExtensionServiceConfig}
import com.digitalasset.canton.tracing.TraceContext

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.Try

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
  private lazy val oauthClientAssertionBuilder: Option[() => String] = config.auth match {
    case _: ExtensionServiceAuthConfig.OAuth =>
      Some(
        oauthAssertionFactory.getOrElse {
          val assertionFactory = new HttpExtensionOAuthClientAssertionFactory(
            config = config,
            nowMillis = () => runtime.nowMillis(),
          )
          () => assertionFactory.buildClientAssertion()
        }
      )
    case _ => None
  }
  private lazy val oauthTokenClient: Option[HttpExtensionOAuthTokenClient] = config.auth match {
    case _: ExtensionServiceAuthConfig.OAuth =>
      val tokenTransport = resources.tokenTransport.getOrElse {
        throw new IllegalStateException(
          s"OAuth extension '$extensionId' requires a dedicated token transport"
        )
      }
      val buildAssertion = oauthClientAssertionBuilder match {
        case Some(builder) => builder
        case None =>
          throw new IllegalStateException(
            s"OAuth extension '$extensionId' requires an assertion builder"
          )
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
  private lazy val oauthTokenManager: Option[HttpExtensionOAuthTokenManager] = config.auth match {
    case _: ExtensionServiceAuthConfig.OAuth =>
      Some(
        new HttpExtensionOAuthTokenManager(
          extensionId = extensionId,
          requestTimeoutForRemainingBudget = requestTimeoutForRemainingBudget,
          acquireToken = (timeout, requestId) =>
            oauthTokenClient match {
              case Some(tokenClient) =>
                tokenClient.acquireToken(
                  timeout = timeout,
                  requestId = requestId,
                )
              case None =>
                Left(ExtensionCallError(500, "OAuth token client not available", None))
            },
          nowMillis = () => runtime.nowMillis(),
          newRequestId = () => runtime.newRequestId(),
          loggerFactory = loggerFactory,
        )
      )
    case _ => None
  }

  // Declared function config hashes for validation
  private val declaredConfigHashes: Map[String, String] =
    config.declaredFunctions.map(f => f.functionId -> f.configHash).toMap

  override def getDeclaredConfigHash(functionId: String): Option[String] =
    declaredConfigHashes.get(functionId)

  private[extension] def startupLocalPreflight(): Either[String, Unit] =
    Try {
      val _ = resources.resourceTransport
      val _ = oauthTokenClient
      oauthClientAssertionBuilder.foreach(_.apply())
      ()
    }.toEither.left.map(error => s"Extension '$extensionId' startup local preflight failed: ${error.getMessage}")

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
          validateConfigurationInternal()
        }
      }
    }
  }

  private def validateConfigurationInternal(): ExtensionValidationResult = {
    val timeout = Duration.ofMillis(config.connectTimeout.underlying.toMillis)
    config.auth match {
      case _: ExtensionServiceAuthConfig.OAuth =>
        validateOAuthConfiguration(timeout)
      case _ =>
        validateReachabilityConfiguration(timeout, bearerToken = None, treat401And403AsInvalid = false)
    }
  }

  private def validateOAuthConfiguration(timeout: Duration): ExtensionValidationResult =
    try {
      val tokenRequestId = runtime.newRequestId()
      oauthTokenClient match {
        case Some(tokenClient) =>
          tokenClient.acquireToken(timeout = timeout, requestId = tokenRequestId) match {
            case Right(token) =>
              validateReachabilityConfiguration(
                timeout = timeout,
                bearerToken = Some(token.value),
                treat401And403AsInvalid = true,
              )
            case Left(error) =>
              ExtensionValidationResult.Invalid(
                Seq(s"OAuth token acquisition failed: ${error.message}")
              )
          }
        case None =>
          ExtensionValidationResult.Invalid(Seq("OAuth token client not available"))
      }
    } catch {
      case e: Exception =>
        ExtensionValidationResult.Invalid(Seq(s"Validation failed: ${e.getMessage}"))
    }

  private def validateReachabilityConfiguration(
      timeout: Duration,
      bearerToken: Option[String],
      treat401And403AsInvalid: Boolean,
  ): ExtensionValidationResult =
    try {
      val requestId = runtime.newRequestId()
      val request = requestBuilder.buildValidationRequest(
        timeout = timeout,
        requestId = requestId,
        bearerToken = bearerToken,
      )
      val response = resources.resourceTransport.send(request)

      response.statusCode match {
        case 401 if treat401And403AsInvalid =>
          ExtensionValidationResult.Invalid(
            Seq(
              s"OAuth validation request failed with Unauthorized: ${responseMapper.responseBodyOrDefault(response, "Unauthorized")}"
            )
          )
        case 403 if treat401And403AsInvalid =>
          ExtensionValidationResult.Invalid(
            Seq(
              s"OAuth validation request failed with Forbidden: ${responseMapper.responseBodyOrDefault(response, "Forbidden")}"
            )
          )
        case code if code >= 200 && code < 600 =>
          ExtensionValidationResult.Valid
        case code =>
          ExtensionValidationResult.Invalid(Seq(s"Unexpected response code: $code"))
      }
    } catch {
      case e: java.net.ConnectException =>
        ExtensionValidationResult.Invalid(Seq(s"Cannot connect to extension service: ${e.getMessage}"))
      case e: java.net.http.HttpTimeoutException =>
        ExtensionValidationResult.Invalid(Seq(s"Connection timeout: ${e.getMessage}"))
      case e: Exception =>
        ExtensionValidationResult.Invalid(Seq(s"Validation failed: ${e.getMessage}"))
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
        sendWithOptionalOAuthReplay(
          functionId = functionId,
          configHash = configHash,
          input = input,
          mode = mode,
          bearerToken = bearerToken,
          deadlineMs = deadlineMs,
        )
    }
  }

  private def sendWithOptionalOAuthReplay(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
      bearerToken: Option[String],
      deadlineMs: Long,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] =
    sendResourceRequest(
      functionId = functionId,
      configHash = configHash,
      input = input,
      mode = mode,
      bearerToken = bearerToken,
      deadlineMs = deadlineMs,
    ).flatMap { case (response, requestId) =>
      if (shouldReplayAfterUnauthorized(response, bearerToken)) {
        replayResourceRequestAfterUnauthorized(
          functionId = functionId,
          configHash = configHash,
          input = input,
          mode = mode,
          sentBearerToken = bearerToken,
          deadlineMs = deadlineMs,
        )
      } else {
        mapResourceResponse(response, requestId)
      }
    }

  private def replayResourceRequestAfterUnauthorized(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
      sentBearerToken: Option[String],
      deadlineMs: Long,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] = {
    sentBearerToken.foreach(invalidateCachedOAuthTokenIfMatches)
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
    response.statusCode == 401 &&
      bearerToken.nonEmpty &&
      oauthTokenClient.nonEmpty &&
      hasBearerInvalidTokenChallenge(response)

  private def hasBearerInvalidTokenChallenge(response: HttpExtensionClientResponse): Boolean =
    response.headers.iterator.collect {
      case (name, values) if name.equalsIgnoreCase("WWW-Authenticate") => values
    }.flatten.exists { headerValue =>
      HttpExtensionServiceClient.BearerInvalidTokenChallengePattern.findFirstIn(headerValue).nonEmpty
    }

  private def invalidateCachedOAuthTokenIfMatches(sentToken: String)(implicit tc: TraceContext): Unit =
    oauthTokenManager.foreach(_.invalidateCachedTokenIfMatches(sentToken))

  private def currentBearerToken(deadlineMs: Long)(implicit
      tc: TraceContext
  ): Either[ExtensionCallError, Option[String]] =
    try {
      oauthTokenManager match {
        case None => Right(None)
        case Some(tokenManager) =>
          tokenManager.currentBearerToken(deadlineMs).map(Some(_))
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
  // Keep OAuth-specific replay conservative: only explicit Bearer invalid_token challenges qualify.
  private val BearerInvalidTokenChallengePattern =
    """(?i)(?:^|,)\s*Bearer\b.*\berror\s*=\s*"invalid_token"""".r
}
