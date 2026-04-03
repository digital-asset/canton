// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.extension.HttpExtensionOAuthTokenManager.{
  OAuthTokenAcquisitionResolution,
  OAuthTokenAcquisitionStartReason,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Mutex

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Await, Promise, blocking}

private[extension] final class HttpExtensionOAuthTokenManager(
    extensionId: String,
    requestTimeoutForRemainingBudget: Long => Either[ExtensionCallError, Duration],
    acquireToken: (Duration, String) => Either[ExtensionCallError, HttpExtensionOAuthAccessToken],
    nowMillis: () => Long,
    newRequestId: () => String,
    override protected val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val tokenLock = new Mutex()
  private val cachedToken = new AtomicReference[Option[HttpExtensionOAuthAccessToken]](None)
  private val inFlightTokenAcquisition
      : AtomicReference[Option[Promise[Either[ExtensionCallError, HttpExtensionOAuthAccessToken]]]] =
    new AtomicReference(None)

  def currentBearerToken(deadlineMs: Long)(implicit tc: TraceContext): Either[ExtensionCallError, String] =
    nextTokenAcquisitionResolution() match {
      case OAuthTokenAcquisitionResolution.Cached(token) =>
        logger.debug(s"Reusing cached OAuth token for extension '$extensionId'")
        Right(token.value)
      case OAuthTokenAcquisitionResolution.Join(promise) =>
        blocking(Await.result(promise.future, scala.concurrent.duration.Duration.Inf)).map(_.value)
      case OAuthTokenAcquisitionResolution.Start(promise, startReason) =>
        if (startReason == OAuthTokenAcquisitionStartReason.Expired) {
          logger.debug(s"Reacquiring OAuth token for extension '$extensionId' after expiry")
        }
        val result = tryAcquireOAuthToken(deadlineMs)
        tokenLock.exclusive {
          result.foreach(token => cachedToken.set(token.expiresAtMillis.map(_ => token)))
          inFlightTokenAcquisition.set(None)
        }
        val _ = promise.trySuccess(result)
        result.map(_.value)
    }

  def invalidateCachedTokenIfMatches(sentToken: String)(implicit tc: TraceContext): Unit =
    tokenLock.exclusive {
      cachedToken.get() match {
        case Some(existingToken) if existingToken.value == sentToken =>
          cachedToken.set(None)
          logger.debug(
            s"Invalidated cached OAuth token for extension '$extensionId' after resource-server 401"
          )
        case _ =>
          ()
      }
    }

  private def nextTokenAcquisitionResolution(): OAuthTokenAcquisitionResolution =
    tokenLock.exclusive {
      cachedToken.get() match {
        case Some(token) if token.expiresAtMillis.exists(_ > nowMillis()) =>
          OAuthTokenAcquisitionResolution.Cached(token)
        case None =>
          joinOrStartTokenAcquisitionLocked(OAuthTokenAcquisitionStartReason.Missing)
        case Some(_) =>
          joinOrStartTokenAcquisitionLocked(OAuthTokenAcquisitionStartReason.Expired)
      }
    }

  private def joinOrStartTokenAcquisitionLocked(
      reason: OAuthTokenAcquisitionStartReason
  ): OAuthTokenAcquisitionResolution =
    inFlightTokenAcquisition.get() match {
      case Some(existingPromise) =>
        OAuthTokenAcquisitionResolution.Join(existingPromise)
      case None =>
        val promise = Promise[Either[ExtensionCallError, HttpExtensionOAuthAccessToken]]()
        inFlightTokenAcquisition.set(Some(promise))
        OAuthTokenAcquisitionResolution.Start(promise, reason)
    }

  private def tryAcquireOAuthToken(
      deadlineMs: Long
  )(implicit tc: TraceContext): Either[ExtensionCallError, HttpExtensionOAuthAccessToken] =
    try {
      requestTimeoutForRemainingBudget(deadlineMs).flatMap { timeout =>
        val requestId = newRequestId()
        logger.debug(
          s"Starting OAuth token acquisition for extension '$extensionId': requestId=$requestId"
        )
        val result = acquireToken(timeout, requestId)
        result match {
          case Right(_) =>
            logger.debug(
              s"OAuth token acquisition succeeded for extension '$extensionId': requestId=$requestId"
            )
          case Left(error) =>
            logger.warn(
              s"OAuth token acquisition failed for extension '$extensionId': status=${error.statusCode}, requestId=${error.requestId.getOrElse("none")}, message=${error.message}"
            )
        }
        result
      }
    } catch {
      case error: Exception =>
        Left(logAndMapPreOutboundLocalFailure(error))
    }

  private def logAndMapPreOutboundLocalFailure(error: Exception)(implicit
      tc: TraceContext
  ): ExtensionCallError = {
    logger.error(
      s"External call to extension '$extensionId' local OAuth initialization failed before outbound HTTP: error=${error.getMessage}"
    )
    ExtensionCallError(500, s"Unexpected error: ${error.getMessage}", None)
  }
}

private[extension] object HttpExtensionOAuthTokenManager {

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
    ) extends OAuthTokenAcquisitionResolution
  }
}
