// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.{JwksUrl, JwtException, JwtTimestampLeeway, JwtVerifier}
import com.daml.metrics.CacheMetrics
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/** A JWK verifier loader, where the public keys are automatically fetched from the given JWKS URL.
  * The keys are then transformed into JWK Verifier
  *
  * The JWKS documents are kept in cache, one entry per JWKS URL, in order to prevent having to do a
  * remote network access for each token validation.
  *
  * The cache is limited both in size and time. A size limit protects against infinitely growing
  * memory consumption. A time limit is a safety catch for the case where a public key is used to
  * sign a token without an expiration time and then is revoked.
  *
  * @param cacheMaxSize
  *   Maximum number of JWKS URLs to keep in the cache.
  * @param cacheExpiration
  *   Maximum time to keep a fetched JWKS in the cache.
  * @param connectionTimeout
  *   Timeout for connecting to the JWKS URL.
  * @param readTimeout
  *   Timeout for reading from the JWKS URL.
  * @param minRefreshInterval
  *   Minimum time between two fetches of the same JWKS URL.
  * @param autoRefreshAfter
  *   When greater than zero, Caffeine's refreshAfterWrite is configured so that stale entries are
  *   refreshed asynchronously while the old value is still served. If the refresh fails, the old
  *   value is retained until hard eviction at cacheExpiration. This provides resilience against
  *   temporary outages. This should not be greater than cacheExpiration.
  */
class CachedJwtVerifierLoader(
    cacheMaxSize: Long,
    cacheExpiration: FiniteDuration,
    connectionTimeout: FiniteDuration,
    readTimeout: FiniteDuration,
    minRefreshInterval: FiniteDuration,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    maxTokenLife: Option[Long] = None,
    metrics: Option[CacheMetrics] = None,
    autoRefreshAfter: FiniteDuration = FiniteDuration(0, "seconds"),
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext, traceContext: TraceContext)
    extends JwtVerifierLoader
    with NamedLogging
    with AutoCloseable {

  if (autoRefreshAfter > FiniteDuration(0, "seconds") && autoRefreshAfter >= cacheExpiration) {
    logger.warn(
      s"autoRefreshTime ($autoRefreshAfter) should be less than cacheExpiration ($cacheExpiration) " +
        "for background refresh to be effective. Entries will be evicted before refresh triggers."
    )
  }

  private val cache = new JwksCache(
    cacheMaxSize = cacheMaxSize,
    cacheExpiration = cacheExpiration,
    connectionTimeout = connectionTimeout,
    readTimeout = readTimeout,
    minRefreshInterval = minRefreshInterval,
    autoRefreshAfter = autoRefreshAfter,
    jwtTimestampLeeway = jwtTimestampLeeway,
    maxTokenLife = maxTokenLife,
    metrics = metrics,
    loggerFactory = loggerFactory,
  )

  override def loadJwtVerifier(jwksUrl: JwksUrl, keyId: Option[String]): Future[JwtVerifier] =
    cache
      .getVerifier(jwksUrl, keyId)
      .flatMap(_.fold(err => Future.failed(JwtException(err)), Future.successful))

  override def close(): Unit = cache.close()
}
