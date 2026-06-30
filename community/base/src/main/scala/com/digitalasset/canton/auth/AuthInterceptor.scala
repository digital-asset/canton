// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.digitalasset.canton.auth.AuthInterceptor.PassThroughClaimResolver
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import io.grpc.*

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** This interceptor uses the given [[AuthService]] to get [[ClaimSet.Claims]] for the current
  * request, and then stores them in the current [[io.grpc.Context]].
  */
class AuthInterceptor(
    authServices: Seq[AuthService],
    val loggerFactory: NamedLoggerFactory,
    implicit val ec: ExecutionContext,
    claimResolver: ClaimResolver = PassThroughClaimResolver,
) extends NamedLogging {
  import LoggingContextWithTrace.implicitExtractTraceContext

  def extractClaims(
      authToken: Option[String],
      serviceName: String,
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): Future[ClaimSet] =
    // Note: Context uses ThreadLocal storage, we need to capture it outside of the async block below.
    // Contexts are immutable and safe to pass around.

    headerToClaims(authToken, serviceName)
      .flatMap(claimResolver.apply)
      .transform {
        case Failure(error: StatusRuntimeException) =>
          Failure(error)
        case Failure(exception: Throwable) =>
          val error = AuthorizationChecksErrors.InternalAuthorizationError
            .Reject(
              message = "Failed to get claims from request metadata",
              throwable = exception,
            )
            .asGrpcError
          Failure(error)
        case Success(claimSet) => Success(claimSet)
      }

  private val deny: Future[(Vector[String], ClaimSet)] =
    Future.successful(Vector.empty[String] -> (ClaimSet.Unauthenticated: ClaimSet))

  def headerToClaims(
      authToken: Option[String],
      serviceName: String,
  )(implicit loggingContextWithTrace: LoggingContextWithTrace): Future[ClaimSet] =
    authServices
      // Try each AuthService in turn while still unauthenticated, accumulating the reason each one
      // gives for not verifying a present token. Once a service authenticates, short-circuit and
      // carry the accumulated reasons forward (they are dropped, since authentication succeeded).
      .foldLeft(deny) { case (acc, elem) =>
        acc.flatMap {
          case (reasons, ClaimSet.Unauthenticated) =>
            elem.decodeToken(authToken, serviceName).map { result =>
              val serviceName = elem.getClass.getSimpleName
              val taggedWarning = result.deferredWarning.map(w => s"[$serviceName] $w")
              (reasons ++ taggedWarning, result.claimSet)
            }
          case authenticated => Future.successful(authenticated)
        }
      }
      .map {
        // No service authenticated a token that was actually presented: log the reasons once.
        case (reasons, ClaimSet.Unauthenticated) if reasons.nonEmpty =>
          val indexedReasons =
            reasons.zipWithIndex.map { case (reason, idx) => s"(${idx + 1}) $reason" }
          logger.warn(
            "Authorization failed: the provided token was not verified by any configured " +
              s"authentication service: ${indexedReasons.mkString("; ")}"
          )
          ClaimSet.Unauthenticated
        // Authenticated, or no token presented at all: stay silent.
        case (_, claimSet) => claimSet
      }
}

object AuthInterceptor {

  val contextKeyClaimSet: Context.Key[ClaimSet] = Context.key[ClaimSet]("AuthServiceDecodedClaim")

  def extractClaimSetFromContext(): Try[ClaimSet] = {
    val claimSet = contextKeyClaimSet.get()
    if (claimSet == null)
      Failure(
        new RuntimeException(
          "Thread local context unexpectedly does not store authorization claims. Perhaps a Future was used in some intermediate computation and changed the executing thread?"
        )
      )
    else
      Success(claimSet)
  }

  case object PassThroughClaimResolver extends ClaimResolver {
    override def apply(claimSet: ClaimSet)(implicit
        loggingContext: LoggingContextWithTrace,
        errorLoggingContext: ErrorLoggingContext,
    ): Future[ClaimSet] =
      Future.successful(claimSet)
  }
}
