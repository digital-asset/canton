// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PerformUnlessClosing,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.{
  ExtensionServiceAuthConfig,
  ExtensionServiceConfig,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.canton.util.retry.{Backoff, NoExceptionRetryPolicy, Success}

import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import java.util.concurrent.{
  CompletableFuture,
  CompletionStage,
  Flow,
  ScheduledExecutorService,
  TimeUnit,
}
import java.util.{List as JList, UUID}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.Try
import scala.util.control.NonFatal

/** HTTP client implementation for extension services with retry logic, bearer token authentication,
  * and TLS support.
  *
  * @param extensionId
  *   The extension identifier (key from config map)
  * @param config
  *   Configuration for this extension service
  * @param httpClient
  *   HTTP client for this extension service, with connection pooling
  * @param timeoutScheduler
  *   Scheduler for hard per-request timeout tasks
  * @param performUnlessClosing
  *   Close context used to stop retry delays and avoid scheduling requests during shutdown
  * @param loggerFactory
  *   Logger factory
  */
class HttpExtensionServiceClient(
    override val extensionId: String,
    config: ExtensionServiceConfig,
    httpClient: HttpClient,
    timeoutScheduler: ScheduledExecutorService,
    performUnlessClosing: PerformUnlessClosing,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ExtensionServiceClient
    with NamedLogging {

  // Construct the endpoint URL
  private val scheme = if (config.tls.exists(_.enabled)) "https" else "http"
  private val endpoint: URI =
    HttpExtensionServiceClient.serviceUri(
      scheme,
      config.address,
      config.port,
      s"/api/${config.version}/external-call",
    )
  private val versionEndpoint: URI =
    HttpExtensionServiceClient.serviceUri(
      scheme,
      config.address,
      config.port,
      s"/api/${config.version}/version",
    )

  private val authorizationHeader: Either[String, Option[String]] =
    config.auth match {
      case ExtensionServiceAuthConfig.None => Right(None)
      case ExtensionServiceAuthConfig.BearerTokenFile(tokenFile) =>
        val file = tokenFile.unwrap
        Try(
          HttpExtensionServiceClient.stripTrailingLineTerminators(Files.readString(file.toPath))
        ).toEither match {
          case Left(_) =>
            Left("Failed to read bearer token file")
          case Right(token) if token.exists(HttpExtensionServiceClient.isInvalidBearerTokenChar) =>
            Left("Bearer token file contains invalid characters")
          case Right(token) if token.nonEmpty =>
            Right(Some(s"Bearer $token"))
          case Right(_) =>
            Left("Bearer token file is empty")
        }
    }
  private val maxResponseBodyBytes: Long = config.maxResponseBodyBytes.unwrap.toLong

  override def call(
      functionId: String,
      configHash: String,
      input: String,
      mode: ExternalCallMode,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] =
    callWithRetry(functionId, configHash, input, mode)

  private[extension] def startupLocalPreflight(): Either[String, Unit] =
    authorizationHeader match {
      case Left(error) =>
        Left(s"Invalid extension service authentication configuration: $error")
      case Right(_) =>
        Right(())
    }

  private[extension] def validateConnection()(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[ExtensionValidationResult] =
    startupLocalPreflight() match {
      case Left(error) =>
        FutureUnlessShutdown.pure(ExtensionValidationResult.Invalid(Seq(error)))

      case Right(_) =>
        validateVersionEndpoint()
    }

  private def validateVersionEndpoint()(implicit
      tc: TraceContext
  ): FutureUnlessShutdown[ExtensionValidationResult] = {
    val requestId = UUID.randomUUID().toString
    val request = HttpRequest
      .newBuilder()
      .uri(versionEndpoint)
      .timeout(config.requestTimeout.asJava)
      .header("Accept", "application/json")
      .header("X-Request-Id", requestId)
      .GET()
      .build()

    sendValidationRequest(request, requestId).map {
      case Right(response) =>
        response.statusCode() match {
          case 200 =>
            ExtensionValidationResult.Valid
          case code =>
            ExtensionValidationResult.Invalid(
              Seq(s"Version endpoint returned HTTP $code")
            )
        }
      case Left(error) =>
        ExtensionValidationResult.Invalid(Seq(error))
    }
  }

  private def sendValidationRequest(
      request: HttpRequest,
      requestId: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[String, HttpResponse[String]]] =
    performUnlessClosing
      .synchronizeWithClosingF("extension-service-startup-validation") {
        sendAsyncWithTimeout(request, config.requestTimeout.asJava)
      }
      .map(response => Right(response))
      .recover { case NonFatal(e) =>
        UnlessShutdown.Outcome(Left(validationExceptionMessage(e, requestId)))
      }

  private def sendAsyncWithTimeout(
      request: HttpRequest,
      requestTimeout: Duration,
  ): Future[HttpResponse[String]] = {
    val result = Promise[HttpResponse[String]]()
    val response = httpClient.sendAsync(
      request,
      HttpExtensionServiceClient.boundedUtf8BodyHandler(
        maxResponseBodyBytes
      ),
    )
    val timeoutTask =
      timeoutScheduler.schedule(
        (() => {
          val timeout =
            new java.net.http.HttpTimeoutException(s"Request timed out after $requestTimeout")
          if (result.tryFailure(timeout))
            response.cancel(true).discard
        }): Runnable,
        requestTimeout.toNanos,
        TimeUnit.NANOSECONDS,
      )

    response.whenComplete { (httpResponse: HttpResponse[String], error: Throwable) =>
      val completion: Try[HttpResponse[String]] =
        Option(error).fold[Try[HttpResponse[String]]](scala.util.Success(httpResponse))(error =>
          TryUtil.unwrapCompletionException(scala.util.Failure(error))
        )
      result.tryComplete(completion).discard
      timeoutTask.cancel(false).discard
    }.discard

    result.future
  }

  private def validationExceptionMessage(e: Throwable, requestId: String): String =
    e match {
      case tooLarge: HttpExtensionServiceClient.ResponseBodyTooLarge =>
        s"Response body exceeded maximum size of ${tooLarge.maxResponseBodyBytes} bytes"
      case timeout: java.net.http.HttpTimeoutException =>
        s"Request timeout during startup validation: ${exceptionMessage(timeout)}"
      case connect: java.net.ConnectException =>
        s"Connection failed: ${exceptionMessage(connect)}"
      case io: java.io.IOException =>
        s"I/O error: ${exceptionMessage(io)}"
      case other =>
        s"Unexpected error during startup validation: ${exceptionMessage(other)} (requestId=$requestId)"
    }

  private def exceptionMessage(e: Throwable): String =
    Option(e.getMessage).getOrElse(e.getClass.getSimpleName)

  /** Make an HTTP call with retry logic */
  private def callWithRetry(
      functionId: String,
      configHash: String,
      input: String,
      mode: ExternalCallMode,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] = {
    val idempotencyKey = UUID.randomUUID().toString
    implicit val retrySuccess: Success[Either[ExtensionCallError, String]] =
      Success {
        case Left(error) => !shouldRetry(error)
        case Right(_) => true
      }

    Backoff(
      logger = logger,
      hasSynchronizeWithClosing = performUnlessClosing,
      maxRetries = config.maxRetries.value,
      initialDelay = config.retryInitialDelay.underlying,
      maxDelay = config.retryMaxDelay.underlying,
      operationName = "external-call-http-request",
    ).unlessShutdown(
      singleCall(
        functionId,
        configHash,
        input,
        mode,
        config.requestTimeout.asJava,
        idempotencyKey,
      ),
      NoExceptionRetryPolicy,
    )
  }

  /** Make a single HTTP call without retry logic */
  private def singleCall(
      functionId: String,
      configHash: String,
      input: String,
      mode: ExternalCallMode,
      requestTimeout: Duration,
      idempotencyKey: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] = {
    val requestId = UUID.randomUUID().toString

    HttpExtensionServiceClient.invalidExternalCallHeader(functionId, configHash) match {
      case Some(headerName) =>
        FutureUnlessShutdown.pure(
          Left(
            ExtensionCallError(
              400,
              s"Invalid external-call HTTP header value for $headerName",
              Some(requestId),
              retryable = false,
            )
          )
        )

      case None =>
        try {
          val requestBuilder = HttpRequest
            .newBuilder()
            .uri(endpoint)
            .timeout(requestTimeout)
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("Accept", "text/plain")
            .header("X-Daml-External-Function-Id", functionId)
            .header("X-Daml-External-Config-Hash", configHash)
            .header("X-Daml-External-Mode", mode.headerValue)
            .header("X-Request-Id", requestId)
            .header("Idempotency-Key", idempotencyKey)

          authorizationHeader match {
            case Left(error) =>
              FutureUnlessShutdown.pure(
                Left(
                  ExtensionCallError(
                    400,
                    s"Invalid extension service authentication configuration: $error",
                    Some(requestId),
                    retryable = false,
                  )
                )
              )

            case Right(headerO) =>
              headerO.foreach(requestBuilder.header("Authorization", _))

              val req = requestBuilder
                .POST(HttpRequest.BodyPublishers.ofString(input))
                .build()

              logger.debug(
                s"Making external call to extension '$extensionId': functionId=$functionId, mode=${mode.headerValue}, requestId=$requestId"
              )

              performUnlessClosing
                .synchronizeWithClosingF("external-call-http-request") {
                  sendAsyncWithTimeout(req, requestTimeout)
                }
                .map(response => responseToResult(response, requestId))
                .recover { case NonFatal(e) =>
                  UnlessShutdown.Outcome(Left(exceptionToError(e, requestId)))
                }
          }
        } catch {
          case NonFatal(e) =>
            FutureUnlessShutdown.pure(Left(exceptionToError(e, requestId)))
        }
    }
  }

  private def responseToResult(
      resp: HttpResponse[String],
      requestId: String,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] =
    resp.statusCode() match {
      case 200 =>
        logger.debug(
          s"External call to extension '$extensionId' succeeded: requestId=$requestId"
        )
        Right(resp.body())

      case 400 =>
        Left(errorFromStatus(resp, requestId, "Bad Request"))

      case 401 =>
        Left(errorFromStatus(resp, requestId, "Unauthorized - check bearer token"))

      case 403 =>
        Left(errorFromStatus(resp, requestId, "Forbidden - insufficient permissions"))

      case 404 =>
        Left(errorFromStatus(resp, requestId, "Function not found"))

      case 408 =>
        Left(errorFromStatus(resp, requestId, "Request timeout"))

      case 429 =>
        Left(errorFromStatus(resp, requestId, "Rate limit exceeded"))

      case 500 =>
        Left(errorFromStatus(resp, requestId, "Internal server error"))

      case 502 =>
        Left(errorFromStatus(resp, requestId, "Bad gateway"))

      case 503 =>
        Left(errorFromStatus(resp, requestId, "Service unavailable"))

      case 504 =>
        Left(errorFromStatus(resp, requestId, "Gateway timeout"))

      case code =>
        Left(errorFromStatus(resp, requestId, s"HTTP $code"))
    }

  private def exceptionToError(
      e: Throwable,
      requestId: String,
  )(implicit tc: TraceContext): ExtensionCallError =
    e match {
      case tooLarge: HttpExtensionServiceClient.ResponseBodyTooLarge =>
        logger.warn(
          s"External call to extension '$extensionId' response body exceeded maximum size of ${tooLarge.maxResponseBodyBytes} bytes: requestId=$requestId"
        )
        ExtensionCallError(
          413,
          s"External call response body exceeded maximum size of ${tooLarge.maxResponseBodyBytes} bytes",
          Some(requestId),
          retryable = false,
        )

      case timeout: java.net.http.HttpTimeoutException =>
        logger.warn(s"External call to extension '$extensionId' timed out: requestId=$requestId")
        ExtensionCallError(408, "Request timeout", Some(requestId), retryable = true)

      case connect: java.net.ConnectException =>
        logger.warn(
          s"External call to extension '$extensionId' connection failed: requestId=$requestId"
        )
        ExtensionCallError(503, "Connection failed", Some(requestId), retryable = true)

      case io: java.io.IOException =>
        logger.warn(
          s"External call to extension '$extensionId' I/O error: requestId=$requestId"
        )
        ExtensionCallError(503, "I/O error", Some(requestId), retryable = true)

      case other =>
        logger.error(
          s"External call to extension '$extensionId' unexpected error: requestId=$requestId",
          other,
        )
        ExtensionCallError(500, "Unexpected error", Some(requestId), retryable = false)
    }

  private def errorFromStatus(
      resp: HttpResponse[String],
      requestId: String,
      defaultMessage: String,
  )(implicit tc: TraceContext): ExtensionCallError = {
    debugLogErrorBody(resp, requestId)
    ExtensionCallError(
      resp.statusCode(),
      defaultMessage,
      Some(requestId),
      retryable = HttpExtensionServiceClient.isRetryableHttpStatus(resp.statusCode()),
    )
  }

  private def debugLogErrorBody(resp: HttpResponse[String], requestId: String)(implicit
      tc: TraceContext
  ): Unit =
    if (logger.underlying.isDebugEnabled) {
      val statusCode = resp.statusCode()
      HttpExtensionServiceClient.diagnosticResponseBody(resp.body()).foreach { body =>
        logger.debug(
          s"External call to extension '$extensionId' returned HTTP $statusCode with response body '$body': requestId=$requestId"
        )
      }
    }

  private def shouldRetry(error: ExtensionCallError): Boolean =
    error.retryable
}

object HttpExtensionServiceClient {
  private val MaxDiagnosticResponseBodyChars: Int = 1024

  private[extension] def isRetryableHttpStatus(statusCode: Int): Boolean =
    statusCode match {
      case 408 | 412 | 429 | 500 | 502 | 503 | 504 => true
      case _ => false
    }

  private[extension] def diagnosticResponseBody(body: String): Option[String] =
    Option.when(body.nonEmpty) {
      val prefix = body.take(MaxDiagnosticResponseBodyChars).map(sanitizeDiagnosticChar)
      if (body.length > MaxDiagnosticResponseBodyChars) s"$prefix..."
      else prefix
    }

  private def sanitizeDiagnosticChar(char: Char): Char =
    if (Character.isISOControl(char)) ' ' else char

  private[extension] def serviceUri(
      scheme: String,
      address: String,
      port: Port,
      path: String,
  ): URI = {
    val host = renderHost(address)
    val uri = URI.create(s"$scheme://$host:${port.unwrap}$path")
    require(
      uri.getHost == host || uri.getHost == comparisonHost(address),
      s"Invalid extension service address '$address'",
    )
    uri
  }

  private def renderHost(address: String): String =
    if (address.startsWith("[") && address.endsWith("]")) address
    else if (address.contains(":")) s"[$address]"
    else address

  private def comparisonHost(address: String): String =
    if (address.startsWith("[") && address.endsWith("]"))
      address.substring(1, address.length - 1)
    else address

  private[extension] def stripTrailingLineTerminators(value: String): String =
    value.substring(0, value.lastIndexWhere(ch => ch != '\r' && ch != '\n') + 1)

  private def isInvalidHeaderChar(char: Char): Boolean =
    char < 32 || char > 126

  private def isInvalidBearerTokenChar(char: Char): Boolean =
    char < 33 || char > 126

  private val MaxExternalCallHeaderValueLength = 1024

  private def isInvalidExternalCallHeaderValue(value: String): Boolean =
    value.length > MaxExternalCallHeaderValueLength || value.exists(isInvalidHeaderChar)

  private def invalidExternalCallHeader(
      functionId: String,
      configHash: String,
  ): Option[String] =
    if (isInvalidExternalCallHeaderValue(functionId)) Some("X-Daml-External-Function-Id")
    else if (isInvalidExternalCallHeaderValue(configHash)) Some("X-Daml-External-Config-Hash")
    else None

  private[extension] final case class ResponseBodyTooLarge(maxResponseBodyBytes: Long)
      extends RuntimeException(
        s"External call response body exceeded maximum size of $maxResponseBodyBytes bytes"
      )

  private[extension] def boundedUtf8BodyHandler(
      maxResponseBodyBytes: Long
  ): HttpResponse.BodyHandler[String] =
    _ => new BoundedUtf8BodySubscriber(maxResponseBodyBytes)

  private final class BoundedUtf8BodySubscriber(maxResponseBodyBytes: Long)
      extends HttpResponse.BodySubscriber[String] {
    private val result = new CompletableFuture[String]
    private val output = new ByteArrayOutputStream
    private val subscription = new AtomicReference[Option[Flow.Subscription]](None)
    private val receivedBytes = new AtomicLong(0L)
    private val finished = new AtomicBoolean(false)

    override def getBody: CompletionStage[String] = result

    override def onSubscribe(subscription: Flow.Subscription): Unit =
      if (this.subscription.compareAndSet(None, Some(subscription))) {
        subscription.request(Long.MaxValue)
      } else {
        subscription.cancel()
      }

    override def onNext(items: JList[ByteBuffer]): Unit =
      items.asScala.foreach(append)

    override def onError(throwable: Throwable): Unit =
      if (finished.compareAndSet(false, true)) {
        result.completeExceptionally(throwable).discard
      }

    override def onComplete(): Unit =
      if (finished.compareAndSet(false, true)) {
        val body = new String(output.toByteArray, StandardCharsets.UTF_8)
        result.complete(body).discard
      }

    private def append(buffer: ByteBuffer): Unit =
      if (!finished.get()) {
        appendUnlessTooLarge(buffer)
      }

    private def appendUnlessTooLarge(buffer: ByteBuffer): Unit = {
      val length = buffer.remaining()
      if (receivedBytes.get() + length > maxResponseBodyBytes) {
        subscription.get().foreach(_.cancel())
        onError(ResponseBodyTooLarge(maxResponseBodyBytes))
      } else {
        val bytes = new Array[Byte](length)
        buffer.get(bytes)
        output.write(bytes, 0, bytes.length)
        receivedBytes.addAndGet(length.toLong).discard
      }
    }
  }
}
