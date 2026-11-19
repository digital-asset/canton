// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.{
  DecodedJwt,
  Error,
  JwksUrl,
  Jwt,
  JwtTimestampLeeway,
  JwtVerifier,
  JwtVerifierBase,
  WithExecuteUnsafe,
}
import com.digitalasset.canton.logging.NamedLoggerFactory

import java.net.URI
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

/** A JWK verifier, where the public keys are automatically fetched from the given JWKS URL.
  *
  * The JWKS is kept in cache, in order to prevent having to do a remote network access for each
  * token validation.
  *
  * The cache is limited in time. The time limit is a safety catch for the case where a public key
  * is used to sign a token without an expiration time and then is revoked.
  *
  * @param url
  *   The URL that points to the JWKS JSON document
  * @param cacheExpiration
  *   Maximum time to keep the fetched JWKS in the cache.
  * @param connectionTimeout
  *   Timeout for connecting to the JWKS URL.
  * @param readTimeout
  *   Timeout for reading from the JWKS URL.
  * @param minRefreshInterval
  *   Minimum time between two fetches of the JWKS.
  */
class JwksVerifier(
    url: JwksUrl,
    cacheExpiration: FiniteDuration,
    connectionTimeout: FiniteDuration,
    readTimeout: FiniteDuration,
    minRefreshInterval: FiniteDuration,
    loggerFactory: NamedLoggerFactory,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    maxTokenLife: Option[Long] = None,
) extends JwtVerifierBase
    with WithExecuteUnsafe {

  // Fetches on the calling thread, because verify is synchronous. Unlike DirectExecutionContext,
  // it does not batch nested tasks, so the futures from the cache are completed when it returns.
  private[this] val callingThread = {
    val logger = loggerFactory.getLogger(getClass)
    ExecutionContext.fromExecutor(
      _.run(),
      logger.error("Failed to verify a token against a JWKS", _),
    )
  }

  private[this] val cache = new JwksCache(
    cacheMaxSize = 1,
    cacheExpiration = cacheExpiration,
    connectionTimeout = connectionTimeout,
    readTimeout = readTimeout,
    minRefreshInterval = minRefreshInterval,
    autoRefreshAfter = Duration.Zero,
    jwtTimestampLeeway = jwtTimestampLeeway,
    maxTokenLife = maxTokenLife,
    metrics = None,
    loggerFactory = loggerFactory,
  )(callingThread)

  private[this] def getVerifier(keyId: String): Either[Error, JwtVerifier] = {
    val result = cache.getVerifier(url, Some(keyId))
    result.value
      .getOrElse(Try(Await.result(result, Duration.Inf)))
      .fold(
        error => Left(Error(Symbol("JwksVerifier.getVerifier"), s"Failed to get verifier: $error")),
        identity,
      )
  }

  def verify(jwt: Jwt): Either[Error, DecodedJwt[String]] =
    for {
      keyIdO <- executeUnsafe(
        Option(com.auth0.jwt.JWT.decode(jwt.value).getKeyId),
        Symbol("JwksVerifier.verify"),
      )
      keyId <- keyIdO.toRight(Error(Symbol("JwksVerifier.verify"), "No Key ID found"))
      verifier <- getVerifier(keyId)
      decoded <- verifier.verify(jwt)
    } yield decoded
}

object JwksVerifier {
  def apply(
      url: String,
      cacheExpiration: FiniteDuration,
      connectionTimeout: FiniteDuration,
      readTimeout: FiniteDuration,
      minRefreshInterval: FiniteDuration,
      loggerFactory: NamedLoggerFactory,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
      maxTokenLife: Option[Long] = None,
  ) =
    new JwksVerifier(
      url = JwksUrl(new URI(url).toURL.toString),
      cacheExpiration = cacheExpiration,
      connectionTimeout = connectionTimeout,
      readTimeout = readTimeout,
      minRefreshInterval = minRefreshInterval,
      loggerFactory = loggerFactory,
      jwtTimestampLeeway = jwtTimestampLeeway,
      maxTokenLife = maxTokenLife,
    )
}
