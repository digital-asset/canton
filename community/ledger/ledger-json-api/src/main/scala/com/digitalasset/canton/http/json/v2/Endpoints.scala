// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.client.pekko.ClientAdapter
import com.digitalasset.base.error.ErrorCategory
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.auth.{AuthInterceptor, Authorizer, ClaimSet, RequiredClaim}
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.http.{
  RequestTimeoutDirective,
  ServerRequestTimeout,
  WebSocketMessageSizeLimit,
  WebsocketConfig,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidArgument
import com.digitalasset.canton.ledger.error.{JsonApiErrors, LedgerApiErrors}
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}
import com.digitalasset.canton.networking.grpc.CallMetadata
import com.digitalasset.canton.tracing.{HeaderName, TraceContext, W3CTraceContext}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.Error.Preprocessing
import com.digitalasset.daml.lf.language.Ast.TVar
import com.digitalasset.daml.lf.value.Value.ValueUnit
import com.digitalasset.transcode.{
  IncorrectVariantRepresentationException,
  MissingFieldsException,
  UnexpectedFieldsException,
}
import io.circe.{Decoder, Encoder}
import io.grpc.stub.StreamObserver
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.EntityStreamSizeException
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.stream.{
  AbruptStageTerminationException,
  CompletionTimeoutException,
  Materializer,
}
import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.model.{Header, StatusCode}
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.server.ServerEndpoint.Full

import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

trait Endpoints extends NamedLogging {
  type CustomError = (StatusCode, JsCantonError)

  protected def requestLogger: ApiRequestLogger

  protected implicit def executionContext: ExecutionContext

  import Endpoints.*
  import com.digitalasset.canton.http.util.GrpcHttpErrorCodes.`gRPC status  as sttp`

  protected def handleFailure[R](implicit
      traceContext: TraceContext
  ): Try[Either[CustomError, R]] => Try[Either[CustomError, R]] =
    _.recoverWith { case error =>
      handleErrorResponse(traceContext)(Failure(error))
    }
  protected def handleErrorResponse[R](implicit
      traceContext: TraceContext
  ): Try[Either[JsCantonError, R]] => Try[Either[CustomError, R]] = {
    case Success(Right(value)) => Success(Right(value))
    case Success(Left(error)) => Success(Left((httpStatus(error), error)))
    case Failure(t: Throwable) if handleError.isDefinedAt(t) =>
      Success(handleError(traceContext)(t))
    case Failure(unhandled) =>
      unhandled match {
        case _ =>
          val internalError =
            LedgerApiErrors.InternalError.Generic(unhandled.getMessage, Some(unhandled))

          Success(
            Left(
              (StatusCode.InternalServerError, JsCantonError.fromErrorCode(internalError))
            )
          )
      }
  }

  private def httpStatus(error: JsCantonError): StatusCode =
    error.code match {
      case JsonApiErrors.RequestBodyTooLarge.id => StatusCode.PayloadTooLarge
      case _ =>
        error.grpcCodeValue
          .map(Status.fromCodeValue(_))
          .map(_.getCode)
          .getOrElse(Status.Code.UNKNOWN)
          .asSttpStatus
    }

  def json[R: Decoder: Encoder: Schema, P](
      endpoint: Endpoint[CallerContext, P, CustomError, Unit, Any],
      service: CallerContext => TracedInput[P] => Future[Either[JsCantonError, R]],
  )(implicit
      authInterceptor: AuthInterceptor
  ): Full[CallerContext, CallerContext, TracedInput[P], CustomError, R, Any, Future] =
    endpoint
      .mapIn(traceHeadersMapping[P]())
      .serverSecurityLogic(validateJwtToken(endpoint))
      .out(jsonBody[R])
      .serverLogic(callerContext =>
        i =>
          Future
            .delegate(service(callerContext)(i))
            .transform(handleErrorResponse(callerContext.traceContext()))
      )

  protected def websocket[HI, I, O](
      endpoint: Endpoint[
        CallerContext,
        HI,
        CustomError,
        Flow[I, Either[JsCantonError, O], Any],
        PekkoStreams & WebSockets,
      ],
      service: CallerContext => TracedInput[HI] => Flow[I, O, Any],
  )(implicit
      authInterceptor: AuthInterceptor,
      wsConfig: WebsocketConfig,
  ): Full[CallerContext, CallerContext, HI, CustomError, Flow[
    I,
    Either[JsCantonError, O],
    Any,
  ], PekkoStreams & WebSockets, Future] =
    endpoint
      // .in(header(wsSubprotocol))  We send wsSubprotocol header, but we do not enforce it
      .out(header(wsSubprotocol))
      .serverSecurityLogic(validateWebsocketCaller(endpoint))
      .serverLogicSuccess { jwt => i =>
        val errorHandlingService =
          service(jwt)(
            TracedInput(i)
          ) // We do not pass traceheaders on Websockets
            .map(out => Right[JsCantonError, O](out))
            .recover(handleErrorInSocket(jwt.traceContext()))
        // According to tapir documentation pekko-http does not expose control frames (Ping, Pong and Close)
        //  We cannot send error as close frame
        Future.successful(errorHandlingService)
      }

  def error[R](error: JsCantonError): Future[Either[JsCantonError, R]] =
    Future.successful(Left(error))

  private def maxRowsToReturn(requestLimit: Option[Long])(implicit wsConfig: WebsocketConfig) =
    Math.min(
      requestLimit.getOrElse(wsConfig.httpListMaxElementsLimit + 1),
      wsConfig.httpListMaxElementsLimit + 1,
    )

  def asList[Input, Output, R](
      endpoint: Endpoint[CallerContext, StreamList[Input], CustomError, Seq[
        Output
      ], R],
      service: CallerContext => TracedInput[Unit] => Flow[Input, Output, Any],
      timeoutOpenEndedStream: Input => Boolean = (_: Input) => false,
  )(implicit
      wsConfig: WebsocketConfig,
      serverRequestTimeout: ServerRequestTimeout,
      materializer: Materializer,
      authInterceptor: AuthInterceptor,
  ) =
    endpoint
      .mapIn(traceHeadersMapping[StreamList[Input]]())
      .serverSecurityLogic(validateJwtToken(endpoint))
      .serverLogic(caller =>
        (tracedInput: TracedInput[StreamList[Input]]) => {
          implicit val tc = caller.traceContext()
          val flow = service(caller)(tracedInput.copy(in = ()))
          val limit = tracedInput.in.limit
          val elementsLimit = maxRowsToReturn(limit)
          val systemListElementsLimit = wsConfig.httpListMaxElementsLimit
          val requestedIdleWaitTime = tracedInput.in.waitTime
            .map(FiniteDuration.apply(_, TimeUnit.MILLISECONDS))
            .getOrElse(wsConfig.httpListWaitTime)
          val idleWaitTime = requestedIdleWaitTime.min(wsConfig.httpListMaxWaitTime)
          if (requestedIdleWaitTime > wsConfig.httpListMaxWaitTime) {
            logger.info(
              s"Requested idle wait time ${requestedIdleWaitTime.toMillis} ms exceeds the configured " +
                s"maximum ${wsConfig.httpListMaxWaitTime.toMillis} ms; capping to the maximum."
            )
          }
          val source = Source
            .single(tracedInput.in.input)
            .via(flow)
            .take(elementsLimit)
          val streamCompletionTimeout =
            caller.validatedRequestTimeout.getOrElse(serverRequestTimeout.duration)
          (if (timeoutOpenEndedStream(tracedInput.in.input) || tracedInput.in.waitTime.isDefined) {
             source
               .map(Some(_))
               .idleTimeout(idleWaitTime)
               .recover { case _: TimeoutException =>
                 None
               }
               .collect { case Some(elem) =>
                 elem
               }
           } else {
             source
           })
            .completionTimeout(streamCompletionTimeout)
            .runWith(Sink.seq)
            .map(
              handleListLimit(systemListElementsLimit, _)
            )
            .transform(handleFailure)
        }
      )

  private def handleListLimit[R, Output, Input](
      systemListElementsLimit: Long,
      elements: Seq[Output],
  )(implicit traceContext: TraceContext) = {
    def belowSystemLimit = elements.size <= systemListElementsLimit

    if (belowSystemLimit) {
      Right(elements)
    } else {
      Left(
        (
          StatusCode.PayloadTooLarge,
          JsCantonError.fromErrorCode(
            JsonApiErrors.MaximumNumberOfElements
              .Reject(elements.size, systemListElementsLimit)
          ),
        )
      )
    }
  }

  def asPagedList[Input, Output, R](
      endpoint: Endpoint[CallerContext, PagedList[Input], (StatusCode, JsCantonError), Output, R],
      service: CallerContext => TracedInput[PagedList[Input]] => Future[
        Either[JsCantonError, Output]
      ],
  )(implicit
      authInterceptor: AuthInterceptor
  ): Full[CallerContext, CallerContext, TracedInput[
    PagedList[Input]
  ], (StatusCode, JsCantonError), Output, R, Future] =
    endpoint
      .mapIn(traceHeadersMapping[PagedList[Input]]())
      .serverSecurityLogic(validateJwtToken(endpoint))
      .serverLogic(caller =>
        tracedInput => {
          Future
            .delegate(service(caller)(tracedInput))
            .transform(handleErrorResponse(caller.traceContext()))
        }
      )

  def withServerLogic[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, CustomError, Output, R],
      service: CallerContext => TracedInput[Input] => Future[Either[JsCantonError, Output]],
  )(implicit
      authInterceptor: AuthInterceptor
  ): Full[CallerContext, CallerContext, TracedInput[Input], CustomError, Output, R, Future] =
    serve(endpoint, validateJwtToken(endpoint), service)

  /** Rejects callers without `requiredClaims` before the request body is read. */
  def withRequiredClaims[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, CustomError, Output, R],
      service: CallerContext => TracedInput[Input] => Future[Either[JsCantonError, Output]],
      requiredClaims: RequiredClaim[Unit]*
  )(implicit
      authInterceptor: AuthInterceptor,
      authorizer: Authorizer,
  ): Full[CallerContext, CallerContext, TracedInput[Input], CustomError, Output, R, Future] =
    serve(
      endpoint,
      caller =>
        authenticate(endpoint)(caller).map(_.flatMap { case (authenticatedCaller, claims) =>
          requireClaims(requiredClaims)(authenticatedCaller, claims)
        }),
      service,
    )

  private def serve[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, CustomError, Output, R],
      security: CallerContext => Future[Either[CustomError, CallerContext]],
      service: CallerContext => TracedInput[Input] => Future[Either[JsCantonError, Output]],
  ): Full[CallerContext, CallerContext, TracedInput[Input], CustomError, Output, R, Future] =
    endpoint
      .mapIn(traceHeadersMapping[Input]())
      .serverSecurityLogic(security)
      .serverLogic(caller =>
        tracedInput => {
          Future
            .delegate(service(caller)(tracedInput))
            .transform(handleErrorResponse(caller.traceContext()))
        }
      )

  /** Like [[withServerLogic]], but for endpoints that must be reachable without a token, such as
    * health checks. The token, if any, is passed on to the service logic unverified.
    */
  def withPublicServerLogic[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, CustomError, Output, R],
      service: CallerContext => TracedInput[Input] => Future[Either[JsCantonError, Output]],
  ): Full[CallerContext, CallerContext, TracedInput[Input], CustomError, Output, R, Future] =
    endpoint
      .mapIn(traceHeadersMapping[Input]())
      .serverSecurityLogic(caller =>
        Future.successful[Either[CustomError, CallerContext]](Right(caller))
      )
      .serverLogic(caller =>
        tracedInput => {
          Future
            .delegate(service(caller)(tracedInput))
            .transform(handleErrorResponse(caller.traceContext()))
        }
      )

  private def requireClaims(
      requiredClaims: Seq[RequiredClaim[Unit]]
  )(caller: CallerContext, claims: ClaimSet)(implicit
      authorizer: Authorizer
  ): Either[CustomError, CallerContext] = {
    implicit val traceContext: TraceContext = caller.traceContext()
    authorizer
      .authorize(claims, requiredClaims*)(())
      .map(_ => caller)
      .left
      .flatMap { error =>
        requestLogger.logAuthError(caller.call, error)
        handleError[CallerContext](traceContext)(error)
      }
  }

  /** Security logic of websocket endpoints. Websockets additionally accept the JWT from the
    * [[Endpoints.jwtCookieName]] cookie, which browsers attach to handshakes started by any web
    * site. To prevent cross-site websocket hijacking, a cookie-authenticated handshake is only
    * accepted when its `Origin` is in [[WebsocketConfig.cookieAuthAllowedOrigins]]. When no origins
    * are allowed, the cookie is ignored and the request is treated as carrying no token.
    */
  private def validateWebsocketCaller(endpointInfo: EndpointMetaOps)(caller: CallerContext)(implicit
      authInterceptor: AuthInterceptor,
      wsConfig: WebsocketConfig,
  ): Future[Either[CustomError, CallerContext]] =
    caller.cookieAuth match {
      case None => validateJwtToken(endpointInfo)(caller)
      case Some(cookieAuth) =>
        checkCookieAuthOrigin(wsConfig.cookieAuthAllowedOrigins, cookieAuth.origin) match {
          case CookieAuthDecision.Allowed =>
            validateJwtToken(endpointInfo)(caller)
          case CookieAuthDecision.Disabled =>
            validateJwtToken(endpointInfo)(caller.copy(jwt = None, cookieAuth = None))
          case CookieAuthDecision.Rejected(reason) =>
            implicit val traceContext: TraceContext = caller.traceContext()
            val rejection = JsonApiErrors.CookieAuthOriginNotAllowed.Reject(reason)
            requestLogger.logAuthError(caller.call, rejection.asGrpcError)
            Future.successful(
              Left((StatusCode.Forbidden, JsCantonError.fromErrorCode(rejection)))
            )
        }
    }

  private def validateJwtToken(endpointInfo: EndpointMetaOps)(caller: CallerContext)(implicit
      authInterceptor: AuthInterceptor
  ): Future[Either[CustomError, CallerContext]] =
    authenticate(endpointInfo)(caller).map(_.map { case (authenticatedCaller, _) =>
      authenticatedCaller
    })

  private def authenticate(endpointInfo: EndpointMetaOps)(caller: CallerContext)(implicit
      authInterceptor: AuthInterceptor
  ): Future[Either[CustomError, (CallerContext, ClaimSet)]] = {
    implicit val traceContext = caller.traceContext()
    implicit val lc = LoggingContextWithTrace(loggerFactory)
    authInterceptor
      .extractAuthenticatedClaims(
        caller.token().map(token => s"Bearer $token"),
        endpointInfo.showShort,
      )
      .map { claims =>
        val authenticatedCaller = caller.copy(claimSet = Some(claims))
        requestLogger.logAuth(caller.call, claims)
        Right(authenticatedCaller -> claims)
      }
      .recoverWith { error =>
        requestLogger.logAuthError(caller.call, error)
        Future.successful(handleError[(CallerContext, ClaimSet)](lc.traceContext)(error).left.map {
          case (statusCode, jsCantonError) =>
            (
              statusCode,
              // we add info to context about json ledger api origin of the error
              jsCantonError.copy(context = jsCantonError.context + JsCantonError.tokenProblemError),
            )
        })
      }
  }

  protected def withTraceHeaders[P, E](
      endpoint: Endpoint[CallerContext, P, E, Unit, Any]
  ): Endpoint[CallerContext, TracedInput[P], E, Unit, Any] =
    endpoint.mapIn(traceHeadersMapping[P]())

  implicit class FutureOps[R](future: Future[R])(implicit traceContext: TraceContext) {
    def resultToRight: Future[Either[JsCantonError, R]] =
      future
        .map(Right(_))
        .recover(handleError.andThen(_.left.map(_._2)))
  }

  /** Utility to prepare flow from a gRPC method with an observer.
    * @param closeDelay
    *   if true then server will close websocket after a delay when no new elements appear in stream
    */
  protected def prepareSingleWsStream[Req, Resp, JSResp](
      stream: (Req, StreamObserver[Resp]) => Unit,
      mapToJs: Resp => Future[JSResp],
  )(implicit
      esf: ExecutionSequencerFactory
  ): Flow[Req, JSResp, NotUsed] =
    Flow[Req]
      .take(1) // we take only single request elem
      .flatMapConcat { req =>
        ClientAdapter
          .serverStreaming(req, stream)
      }
      .mapAsync(1)(mapToJs)

  private def handleErrorInSocket[T](implicit
      traceContext: TraceContext
  ): PartialFunction[Throwable, Either[JsCantonError, T]] = handleError.andThen(_.left.map(_._2))

  private def handleError[T](implicit
      traceContext: TraceContext
  ): PartialFunction[Throwable, Either[CustomError, T]] = {
    case sre: StatusRuntimeException =>
      Left(
        (
          sre.getStatus.getCode.asSttpStatus,
          DecodedCantonError
            .fromStatusRuntimeException(sre)
            .map(JsCantonError.fromDecodedCantonError)
            .getOrElse {
              // TODO (#27556) we should log these errors / locations and clean all of them up
              //   CantonErrors are logged on creation (normally ...).
              logger.info(
                s"Request failed with legacy error ${sre.getStatus} / ${sre.getMessage}",
                sre.getCause,
              )
              if (redactLegacyErrorDetails(sre.getStatus.getCode))
                JsCantonError.redacted(
                  grpcCode = sre.getStatus.getCode,
                  correlationId = None,
                  traceId = traceContext.traceId,
                )
              else
                JsCantonError.fromUnstructuredError(
                  code = Option(sre.getStatus.getDescription)
                    .getOrElse("Status description not available"),
                  cause = sre.getMessage,
                  grpcCodeValue = Some(sre.getStatus.getCode.value()),
                )
            },
        )
      )
    case unexpected: UnexpectedFieldsException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            InvalidArgument.Reject(
              s"Unexpected fields: ${unexpected.unexpectedFields.mkString}"
            )
          ),
        )
      )
    case fieldMissing: MissingFieldsException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            CommandExecutionErrors.Preprocessing.PreprocessingFailed.Reject(
              Preprocessing.TypeMismatch(
                TVar(Ref.Name.assertFromString("unknown")),
                ValueUnit,
                s"Missing non-optional fields: ${fieldMissing.missingFields}",
              )
            )
          ),
        )
      )
    case incorrectJson: IncorrectVariantRepresentationException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            InvalidArgument.Reject(incorrectJson.getMessage)
          ),
        )
      )
    case tooLarge: WebSocketMessageSizeLimit.MessageTooLargeException =>
      Left(
        (
          StatusCode.PayloadTooLarge,
          JsCantonError.fromErrorCode(
            JsonApiErrors.WebsocketMessageTooLarge.Reject(tooLarge.maxMessageSizeBytes)
          ),
        )
      )
    case _: WebSocketMessageSizeLimit.MessageBinaryNotSupportedException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            JsonApiErrors.WebsocketMessageBinaryNotSupported.Reject()
          ),
        )
      )
    case illegalArgument: IllegalArgumentException =>
      Left(
        (
          StatusCode.BadRequest,
          JsCantonError.fromErrorCode(
            InvalidArgument.Reject(illegalArgument.getMessage)
          ),
        )
      )
    // The request body exceeds `maxInboundMessageSize`, which pekko-http checks when it is read
    case tooLarge: EntityStreamSizeException =>
      val error =
        JsCantonError.fromErrorCode(JsonApiErrors.RequestBodyTooLarge.Reject(tooLarge.limit))
      Left((httpStatus(error), error))
    // Special case for timed out requests
    case _: AbruptStageTerminationException =>
      Left(
        (
          StatusCode.RequestTimeout,
          JsCantonError.fromUnstructuredError(
            code = "REQUEST_TIMEOUT",
            cause = "The request timed out before it completed.",
          ),
        )
      )
    // The backing stream exceeded its maximum allowed duration (see WebsocketConfig.httpListMaxStreamDuration)
    // and was cancelled, tearing down the upstream gRPC stream.
    case _: CompletionTimeoutException =>
      Left(
        (
          StatusCode.RequestTimeout,
          JsCantonError.fromUnstructuredError(
            code = "REQUEST_TIMEOUT",
            cause = "The request timed out before it completed.",
          ),
        )
      )
    case NonFatal(error) =>
      val internalError =
        LedgerApiErrors.InternalError.Generic(
          error.getMessage,
          Some(error),
        )
      Left(
        (StatusCode.InternalServerError, JsCantonError.fromErrorCode(internalError))
      )
  }
}

object Endpoints {
  final case class Jwt(token: String)

  private val redactedLegacyStatusCodes: Set[Status.Code] = {
    val (redactedCategories, unredactedCategories) = ErrorCategory.all.partition(_.redactDetails)
    val redactedCodes = redactedCategories.flatMap(_.grpcCode).toSet
    val unredactedCodes = unredactedCategories.flatMap(_.grpcCode).toSet
    (redactedCodes -- unredactedCodes) + Status.Code.UNKNOWN
  }

  private def redactLegacyErrorDetails(code: Status.Code): Boolean =
    redactedLegacyStatusCodes.contains(code)

  // added to ease burden if we change what is included in SECURITY_INPUT
  final case class CallerContext(
      jwt: Option[Jwt],
      call: CallMetadata,
      claimSet: Option[ClaimSet] = None,
      optTraceContext: Option[TraceContext] = None,
      // set when `jwt` was taken from the `jwtCookieName` cookie (websocket endpoints only)
      cookieAuth: Option[CookieAuth] = None,
      validatedRequestTimeout: Option[FiniteDuration] = None,
  ) {
    def token(): Option[String] = jwt.map(_.token)

    def traceContext(): TraceContext = optTraceContext.getOrElse(TraceContext.empty)
  }

  final case class CookieAuth(origin: Option[String])

  sealed trait CookieAuthDecision extends Product with Serializable
  object CookieAuthDecision {

    /** No origins are allowed, hence the cookie must be ignored. */
    case object Disabled extends CookieAuthDecision
    case object Allowed extends CookieAuthDecision
    final case class Rejected(reason: String) extends CookieAuthDecision
  }

  /** Decides whether a cookie-authenticated request from `origin` may proceed given the configured
    * `allowedOrigins`. Origins are compared after trimming, removing a trailing slash and
    * lowercasing (scheme and host are case-insensitive). A missing `Origin` header or the opaque
    * `null` origin is always rejected, as browsers always send a proper `Origin` on websocket
    * handshakes. A `*` entry allows any (non-null) origin.
    */
  private[v2] def checkCookieAuthOrigin(
      allowedOrigins: Seq[String],
      origin: Option[String],
  ): CookieAuthDecision = {
    val allowed = allowedOrigins.map(normalizeOrigin).filter(_.nonEmpty)
    if (allowed.isEmpty) CookieAuthDecision.Disabled
    else
      origin.map(normalizeOrigin) match {
        case None | Some("") => CookieAuthDecision.Rejected("the Origin header is missing")
        case Some("null") => CookieAuthDecision.Rejected("the opaque 'null' Origin is not allowed")
        case Some(requestOrigin) if allowed.contains("*") || allowed.contains(requestOrigin) =>
          CookieAuthDecision.Allowed
        case Some(requestOrigin) =>
          CookieAuthDecision.Rejected(s"Origin '$requestOrigin' is not an allowed origin")
      }
  }

  private def normalizeOrigin(origin: String): String =
    origin.trim.stripSuffix("/").toLowerCase(Locale.ROOT)

  // TODO (i28204) remove this class - it was once a wrapper over Input and TraceContext - but TraceContext is now part of  Caller
  final case class TracedInput[A](in: A)

  val wsSubprotocol: Header =
    sttp.model.Header("Sec-WebSocket-Protocol", "daml.ws.auth")

  /** Name of the cookie that browser clients may use to pass the JWT instead of using the
    * `Sec-WebSocket-Protocol` workaround. It is only read by websocket endpoints (see
    * [[baseWebsocketEndpoint]]).
    */
  val jwtCookieName: String = "jwt.token"

  // RFC 6455 clients send comma-space separated subprotocol values (e.g.
  // "daml.ws.auth, jwt.token.<TOKEN>"), so each element must be trimmed before
  // matching the jwt.token. prefix, or tokens are silently dropped and the
  // request proceeds unauthenticated.
  private[v2] def extractWsJwtToken(header: Option[String]): Option[Jwt] = {
    val tokenPrefix = "jwt.token."
    header
      .map(_.split(",").toSeq)
      .getOrElse(Seq.empty)
      .map(_.trim)
      .filter(_.startsWith(tokenPrefix))
      .map(_.substring(tokenPrefix.length))
      .headOption
      .map(Jwt.apply)
  }

  lazy val baseEndpoint: Endpoint[CallerContext, Unit, Unit, Unit, Any] = endpoint
    .securityIn(
      auth
        .bearer[Option[String]]()
        .map(bearer => bearer.map(Jwt.apply))(
          _.map(_.token)
        )
        .description("Ledger API standard JWT token")
        .and(
          auth
            .apiKey(header[Option[String]]("Sec-WebSocket-Protocol"))
            .map(extractWsJwtToken)(_.map(_.token))
            .description("Ledger API standard JWT token (websocket)")
        )
        .and(
          extractFromRequest(req =>
            (
              req.headers,
              RequestInterceptorsUtil.extractCallMetadata(req),
            )
          )
            .map { case (headersList: Seq[Header], addr) =>
              val z = W3CTraceContext.fromHeaders(
                headersList.map(h => (HeaderName(h.name), h.value)).toMap
              )
              (z.map(_.toTraceContext), addr)
            } { case (tc1, addr) =>
              (
                tc1
                  .map { case tc =>
                    (W3CTraceContext
                      .extractHeaders(tc)
                      .map { case (k, v) =>
                        Header(k, v)
                      }
                      .toList)

                  }
                  .getOrElse(Nil),
                addr,
              )
            }
        )
        .map { case (httpToken, wsToken, traceContext, call) =>
          CallerContext(httpToken.orElse(wsToken), call, None, traceContext)
        }(cc => (cc.jwt, cc.jwt, cc.optTraceContext, cc.call))
    )
    .securityIn(extractFromRequest(_.header(RequestTimeoutDirective.RequestTimeoutHeaderName)))
    .mapSecurityIn { case (callerContext, rawRequestTimeout) =>
      callerContext
        .copy(validatedRequestTimeout = rawRequestTimeout.flatMap(RequestTimeoutDirective.parse))
    }(callerContext =>
      (callerContext, callerContext.validatedRequestTimeout.map(_.toMillis.toString))
    )

  lazy val v2Endpoint: Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), Unit, Any] =
    baseEndpoint
      .errorOut(statusCode.and(jsonBody[JsCantonError]))
      .in("v2")

  /** Base for websocket endpoints. In addition to the inputs of [[baseEndpoint]], the JWT may be
    * provided in the [[jwtCookieName]] cookie, which is consulted only when neither the
    * `Authorization` nor the `Sec-WebSocket-Protocol` header carries a token. A token taken from
    * the cookie is marked in [[CallerContext.cookieAuth]] together with the request's `Origin`, so
    * that the security logic can enforce [[WebsocketConfig.cookieAuthAllowedOrigins]].
    */
  lazy val baseWebsocketEndpoint: Endpoint[CallerContext, Unit, Unit, Unit, Any] =
    baseEndpoint
      .securityIn(
        auth
          .apiKey(cookie[Option[String]](jwtCookieName))
          .map(_.map(Jwt.apply))(_.map(_.token))
          .description("Ledger API standard JWT token (websocket cookie)")
      )
      .securityIn(extractFromRequest(_.header(sttp.model.HeaderNames.Origin)))
      .mapSecurityIn {
        case (cc, Some(cookieJwt), origin) if cc.jwt.isEmpty =>
          cc.copy(jwt = Some(cookieJwt), cookieAuth = Some(CookieAuth(origin)))
        case (cc, _, _) => cc
      } {
        case cc @ CallerContext(jwt, _, _, _, Some(cookieAuth), _) =>
          (cc.copy(jwt = None, cookieAuth = None), jwt, cookieAuth.origin)
        case cc => (cc, None, None)
      }

  lazy val v2WebsocketEndpoint
      : Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), Unit, Any] =
    baseWebsocketEndpoint
      .errorOut(statusCode.and(jsonBody[JsCantonError]))
      .in("v2")

  def traceHeadersMapping[I](): Mapping[I, TracedInput[I]] =
    new Mapping[I, TracedInput[I]] {

      override def rawDecode(input: I): DecodeResult[TracedInput[I]] =
        DecodeResult.Value(
          TracedInput(
            input
          )
        )

      override def encode(h: TracedInput[I]): I =
        h.in

      override def validator: Validator[TracedInput[I]] = Validator.pass
    }

  def error[R](error: JsCantonError): Future[Either[JsCantonError, R]] =
    Future.successful(Left(error))

  private def addStreamListParamsAndDescription[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, (StatusCode, JsCantonError), Seq[
        Output
      ], R]
  ) = endpoint
    .in(
      query[Option[Long]]("limit").description(
        "maximum number of elements to return, this param is ignored if is bigger than server setting"
      )
    )
    .in(
      query[Option[Long]]("stream_idle_timeout_ms").description(
        "timeout to complete and send result if no new elements are received (for open ended streams)"
      )
    )
    .mapIn(new Mapping[(Input, Option[Long], Option[Long]), StreamList[Input]] {
      override def rawDecode(
          in: (Input, Option[Long], Option[Long])
      ): DecodeResult[StreamList[Input]] = DecodeResult.Value(
        StreamList[Input](in._1, in._2, in._3)
      )

      override def encode(h: StreamList[Input]): (Input, Option[Long], Option[Long]) =
        (h.input, h.limit, h.waitTime)

      override def validator: Validator[StreamList[Input]] = Validator.pass
    })
    .description(
      endpoint.info.description match {
        case Some(desc) =>
          s"$desc\n" + """
                              |Notice: This endpoint should be used for small results set.
                              |When number of results exceeded node configuration limit (`http-list-max-elements-limit`)
                              |there will be an error (`413 Content Too Large`) returned.
                              |Increasing this limit may lead to performance issues and high memory consumption.
                              |Consider using websockets (asyncapi) for better efficiency with larger results.
                              |""".stripMargin.trim
        case None =>
          throw new IllegalArgumentException(s"Description for ${endpoint.info} is missing")
      }
    )

  private def addPagedListParams[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, (StatusCode, JsCantonError), Output, R]
  ) = endpoint
    .in(
      query[Option[Int]]("pageSize").description(
        "maximum number of elements in a returned page"
      )
    )
    .in(
      query[Option[String]]("pageToken").description(
        "token - to continue results from a given page, leave empty to start from the beginning of the list, obtain token from the result of previous page"
      )
    )
    .mapIn(new Mapping[(Input, Option[Int], Option[String]), PagedList[Input]] {
      override def rawDecode(
          in: (Input, Option[Int], Option[String])
      ): DecodeResult[PagedList[Input]] = DecodeResult.Value(
        PagedList[Input](in._1, in._2, in._3)
      )

      override def encode(h: PagedList[Input]): (Input, Option[Int], Option[String]) =
        (h.input, h.pageSize, h.pageToken)

      override def validator: Validator[PagedList[Input]] = Validator.pass
    })

  implicit class StreamListOps[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, (StatusCode, JsCantonError), Seq[
        Output
      ], R]
  ) {
    def inStreamListParamsAndDescription() = addStreamListParamsAndDescription(endpoint)

  }

  implicit class PagedListOps[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, (StatusCode, JsCantonError), Output, R]
  ) {

    def inPagedListParams() = addPagedListParams(endpoint)
  }

  def createProtoRef(methodDescriptor: io.grpc.MethodDescriptor[?, ?]): String =
    ProtoLink(methodDescriptor).toString

  implicit class ProtoRefOps[Input, Output, R](
      endpoint: Endpoint[CallerContext, Input, (StatusCode, JsCantonError), Output, R]
  ) {

    def protoRef(
        methodDescriptor: io.grpc.MethodDescriptor[?, ?]
    ): Endpoint[CallerContext, Input, (StatusCode, JsCantonError), Output, R] =
      endpoint.description(createProtoRef(methodDescriptor))
  }

}

trait DocumentationEndpoints {
  def documentation: Seq[AnyEndpoint]
}

final case class StreamList[Input](input: Input, limit: Option[Long], waitTime: Option[Long])

final case class PagedList[Input](input: Input, pageSize: Option[Int], pageToken: Option[String])

final case class ProtoLink(file: String, service: String, method: String) {

  override def toString: String = s"<gRPC:$file/$service/$method>"
}

object ProtoLink {
  private val ledgerApiGrpcMethodExtractor =
    raw"com\.daml\.ledger\.api\.v2\.((?:(?:[a-z0-9])*\.)*)([A-Za-z0-9]+)".r

  private val teaApiGrpcMethodExtractor =
    raw"com\.digitalasset\.canton\.tea\.v1\.((?:(?:[a-z0-9])*\.)*)([A-Za-z0-9]+)".r

  private val importGRPCCommentPattern =
    raw"<gRPC:([A-Za-z0-9_/]+\.proto)/([A-Za-z0-9_]+)/([A-Za-z0-9_]+)>".r

  def apply(methodDescriptor: io.grpc.MethodDescriptor[?, ?]): ProtoLink = {
    val serviceName: String = methodDescriptor.getServiceName
    val bareMethodName = methodDescriptor.getBareMethodName
    serviceName match {
      case ledgerApiGrpcMethodExtractor(packageName, service) =>
        createProtoLink(bareMethodName, packageName, service)
      case teaApiGrpcMethodExtractor(packageName, service) =>
        createProtoLink(bareMethodName, packageName, service)
      case _ =>
        throw new IllegalArgumentException(
          s"Could not create link to proto documentation for: $methodDescriptor"
        )
    }
  }

  private def createProtoLink(bareMethodName: String, packageName: String, service: String) = {
    val snake = service.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase
    val pck1 = packageName.replace('.', '/')
    ProtoLink(pck1 + snake + ".proto", service, bareMethodName)
  }

  def unapply(link: String): Option[(String, String, String)] = link match {
    case importGRPCCommentPattern(file, service, method) =>
      Some((file, service, method))
    case _ => None
  }
}
