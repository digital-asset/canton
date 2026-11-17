// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.auth0.jwk.{Jwk, UrlJwkProvider}
import com.daml.jwt.{JwksUrl, JwtTimestampLeeway, JwtVerifier, Error as JwtError}
import com.daml.metrics.CacheMetrics
import com.digitalasset.canton.auth.JwksCache.{Attempt, KeySet}
import com.digitalasset.canton.caching.ScaffeineCache
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.github.blemale.scaffeine.Scaffeine

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Caches the JWKS documents fetched from JWKS URLs, one entry per URL, and turns the keys in them
  * into [[com.daml.jwt.JwtVerifier]]s.
  *
  * Key ids come from unverified token headers, i.e., are chosen by unauthenticated clients.
  * Therefore, all fetches of a URL (initial load, background refresh, and refetch for an unknown
  * key id) are rate limited: a fetch starts only if none is in flight and the last one completed at
  * least `minRefreshInterval` ago. Callers that cannot start a fetch use the cached keys and do not
  * wait for a fetch in flight. A failed fetch keeps the previously fetched keys.
  *
  * Fetches run on `executionContext` inside [[scala.concurrent.blocking]].
  *
  * @param cacheMaxSize
  *   Maximum number of JWKS URLs to keep in the cache.
  * @param cacheExpiration
  *   Maximum time to use fetched keys.
  * @param connectionTimeout
  *   Timeout for connecting to the JWKS URL.
  * @param readTimeout
  *   Timeout for reading from the JWKS URL.
  * @param minRefreshInterval
  *   Minimum time between two fetches of the same JWKS URL.
  * @param autoRefreshAfter
  *   When greater than zero, the JWKS is refreshed in the background this long after it was
  *   fetched, while the old keys are still served.
  */
final class JwksCache(
    cacheMaxSize: Long,
    cacheExpiration: FiniteDuration,
    connectionTimeout: FiniteDuration,
    readTimeout: FiniteDuration,
    minRefreshInterval: FiniteDuration,
    autoRefreshAfter: FiniteDuration,
    jwtTimestampLeeway: Option[JwtTimestampLeeway],
    maxTokenLife: Option[Long],
    metrics: Option[CacheMetrics],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with AutoCloseable {

  // The state of last successful fetch, per JWKS URL. Kept outside the cache so
  // that failed fetches do not lose them.
  private val attempts = TrieMap.empty[JwksUrl, AtomicReference[Attempt]]

  // The result of the last fetch (possibly an error, in which case lookups will use
  // the keys in `attempts`, if they are still valid).
  private val cache: ScaffeineCache.TunnelledAsyncLoadingCache[
    Future,
    JwksUrl,
    Either[JwtError, KeySet],
  ] = {
    val base = Scaffeine().expireAfterWrite(cacheExpiration).maximumSize(cacheMaxSize)
    ScaffeineCache.buildAsync[Future, JwksUrl, Either[JwtError, KeySet]](
      cache =
        if (autoRefreshAfter > Duration.Zero) base.refreshAfterWrite(autoRefreshAfter) else base,
      loader = jwksUrl =>
        fetchIfAllowed(jwksUrl).getOrElse(
          Future.successful(
            Left(
              JwtError(
                Symbol("getVerifier"),
                s"JWKS at ${jwksUrl.value} not available: fetch in flight or less than $minRefreshInterval ago",
              )
            )
          )
        ),
      metrics = metrics,
    )(logger, "cache")
  }

  /** Returns the verifier for the key with the given id from the JWKS at the given URL. Without a
    * key id, the JWKS must contain exactly one key.
    */
  def getVerifier(jwksUrl: JwksUrl, keyId: Option[String]): Future[Either[JwtError, JwtVerifier]] =
    cache.get(jwksUrl).flatMap { cached =>
      lookup(jwksUrl, keyId).fold {
        val latest = fetchIfAllowed(jwksUrl).fold(Future.successful(cached))(_.map { fetched =>
          cache.put(jwksUrl, fetched)
          fetched
        })
        latest.map(_.flatMap(_.lookup(keyId).getOrElse {
          val kid = keyId.getOrElse("null")
          Left(JwtError(Symbol("getVerifier"), s"No key found in ${jwksUrl.value} with kid $kid"))
        }))
      }(Future.successful)
    }

  override def close(): Unit = {
    cache.invalidateAll()
    cache.cleanUp()
  }

  private def fetchIfAllowed(jwksUrl: JwksUrl): Option[Future[Either[JwtError, KeySet]]] = {
    val attempt = attempts.getOrElseUpdate(jwksUrl, new AtomicReference(Attempt.none))
    val last = attempt.get()
    val mayFetch = !last.inFlight &&
      last.completedAtNanos.forall(System.nanoTime() - _ >= minRefreshInterval.toNanos)
    Option.when(mayFetch && attempt.compareAndSet(last, last.copy(inFlight = true))) {
      Future(blocking(fetchKeySet(jwksUrl))).map { result =>
        attempt.set(
          Attempt(inFlight = false, Some(System.nanoTime()), result.toOption.orElse(last.keys))
        )
        if (autoRefreshAfter > Duration.Zero) {
          result.left.foreach { error =>
            val url = jwksUrl.toURL
            val safeUrl = s"${url.getProtocol}://${url.getHost}${url.getPath}"
            noTracingLogger.warn(
              s"Failed to load jwt verifier for JWKS URL $safeUrl: ${error.message}"
            )
          }
        }
        result
      }
    }
  }

  /** Looks up the key in the keys of the last successful fetch, unless they have expired. */
  private def lookup(
      jwksUrl: JwksUrl,
      keyId: Option[String],
  ): Option[Either[JwtError, JwtVerifier]] =
    attempts
      .get(jwksUrl)
      .flatMap(_.get().keys)
      .filter(keys => System.nanoTime() - keys.fetchedAtNanos < cacheExpiration.toNanos)
      .flatMap(_.lookup(keyId))

  private def fetchKeySet(jwksUrl: JwksUrl): Either[JwtError, KeySet] =
    catchErrors {
      val provider =
        new UrlJwkProvider(
          jwksUrl.toURL,
          connectionTimeout.toMillis.toInt,
          readTimeout.toMillis.toInt,
        )
      val keys = provider.getAll.asScala.toSeq.map(jwk => Option(jwk.getId) -> toVerifier(jwk))
      Right(KeySet(keys, fetchedAtNanos = System.nanoTime()))
    }

  // Per key, so that an unsupported key does not make the other keys of the JWKS unusable
  private def toVerifier(jwk: Jwk): Either[JwtError, JwtVerifier] =
    catchErrors(JwtVerifier.fromPublicKey(jwk.getPublicKey, jwtTimestampLeeway, maxTokenLife))

  private def catchErrors[A](body: => Either[JwtError, A]): Either[JwtError, A] =
    Try(body).toEither.left.map(e => JwtError(Symbol("getVerifier"), e.toString)).flatten
}

object JwksCache {

  /** Lookups behave like [[com.auth0.jwk.UrlJwkProvider.get]]: lookups without key only work if
    * there's a single key in the set.
    */
  private final case class KeySet(
      byKeyId: Map[String, Either[JwtError, JwtVerifier]],
      singleKey: Option[Either[JwtError, JwtVerifier]],
      fetchedAtNanos: Long,
  ) {
    def lookup(keyId: Option[String]): Option[Either[JwtError, JwtVerifier]] =
      keyId.fold(singleKey)(byKeyId.get)
  }

  private object KeySet {
    def apply(
        keys: Seq[(Option[String], Either[JwtError, JwtVerifier])],
        fetchedAtNanos: Long,
    ): KeySet = KeySet(
      byKeyId = keys
        .collect { case (Some(keyId), verifier) => keyId -> verifier }
        .groupMapReduce(_._1)(_._2)((first, _) => first),
      singleKey = keys match {
        case Seq((_, verifier)) => Some(verifier)
        case _ => None
      },
      fetchedAtNanos = fetchedAtNanos,
    )
  }

  /** @param keys
    *   The keys of the last successful fetch.
    */
  private final case class Attempt(
      inFlight: Boolean,
      completedAtNanos: Option[Long],
      keys: Option[KeySet],
  )

  private object Attempt {
    val none: Attempt = Attempt(inFlight = false, completedAtNanos = None, keys = None)
  }
}
