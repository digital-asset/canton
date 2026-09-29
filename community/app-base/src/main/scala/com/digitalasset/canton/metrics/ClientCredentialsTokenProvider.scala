// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.auth0.jwt.JWT
import com.digitalasset.canton.metrics.ClientCredentialsTokenProvider.CachedToken
import com.digitalasset.canton.metrics.OtlpAuth.OauthClientCredentials
import com.digitalasset.canton.time.Clock
import io.circe.Decoder
import io.circe.parser.decode
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import org.apache.pekko.http.scaladsl.model.{FormData, HttpMethods, HttpRequest, Uri}
import org.apache.pekko.stream.{Materializer, SystemMaterializer}

import java.time.{Duration, Instant}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

trait TokenProvider {
  def ensureToken(): Future[CachedToken]
  def authorizationHeader(): String
}

/** ClientCredentialsTokenProvider handles obtaining (and refreshing) a token from an OAuth server
  * using the Client Credentials flow
  *
  * @param config
  * @param clock
  * @param actorSystem
  * @param executionContext
  */
final class ClientCredentialsTokenProvider(
    config: OauthClientCredentials,
    clock: Clock,
)(implicit actorSystem: ActorSystem, executionContext: ExecutionContext)
    extends TokenProvider {

  import ClientCredentialsTokenProvider.*

  private implicit val materializer: Materializer = SystemMaterializer(actorSystem).materializer

  private val cachedToken = new AtomicReference[Option[CachedToken]](None)
  private val refreshInFlight = new AtomicReference[Option[Future[CachedToken]]](None)

  def authorizationHeader(): String =
    cachedToken
      .get()
      .filter(token => clock.now.toInstant.isBefore(token.expiresAt))
      .map(token => s"Bearer ${token.accessToken}")
      .getOrElse {
        throw new IllegalStateException(
          "No valid OAuth2 access token is available"
        )
      }

  private def isUsable(token: CachedToken, now: Instant): Boolean =
    now.isBefore(token.expiresAt.minus(RefreshSkew))

  // returns existing non-expired token, if there is one. Otherwise, it fetches a new token
  def ensureToken(): Future[CachedToken] = {
    val now = clock.now.toInstant

    cachedToken
      .get()
      .filter(token => isUsable(token, now)) match {
      case Some(token) => Future.successful(token)
      case None =>
        refreshInFlight.get() match {
          case Some(refresh) => refresh
          case None => startRefresh()
        }
    }
  }

  private def startRefresh(): Future[CachedToken] = {
    val promise = Promise[CachedToken]()
    val installed = Some(promise.future)

    if (!refreshInFlight.compareAndSet(None, installed)) {
      ensureToken()
    } else {
      requestToken().onComplete { result =>
        result.foreach(token => cachedToken.set(Some(token)))

        refreshInFlight.compareAndSet(installed, None)
        promise.tryComplete(result)
      }

      promise.future
    }
  }

  private def requestToken(): Future[CachedToken] = {
    val fields =
      Seq("grant_type" -> "client_credentials") ++
        config.scope
          .filter(_.nonEmpty)
          .map(scope => "scope" -> scope)

    val request =
      HttpRequest(
        method = HttpMethods.POST,
        uri = Uri(config.tokenUrl),
        headers = List(
          Authorization(
            BasicHttpCredentials(
              config.clientId,
              config.clientSecret,
            )
          )
        ),
        entity = FormData(fields*).toEntity,
      )

    Http(actorSystem)
      .singleRequest(request)
      .flatMap { response =>
        if (!response.status.isSuccess()) {
          // The entity must be drained, otherwise the connection is leaked until the connection
          // pool times out the unsubscribed response entity.
          response.entity.discardBytes(materializer).future().flatMap { _ =>
            Future.failed(
              new IllegalStateException(
                s"OAuth2 token endpoint returned HTTP ${response.status.intValue}"
              )
            )
          }
        } else {
          response.entity
            .toStrict(5.second)(materializer)
            .map { entity =>
              val accessToken =
                decode[TokenResponse](entity.data.utf8String)
                  .fold(
                    error =>
                      throw new IllegalStateException(
                        "Failed to decode OAuth2 token response",
                        error,
                      ),
                    identity,
                  )
                  .accessToken

              CachedToken(
                accessToken = accessToken,
                expiresAt = JWT.decode(accessToken).getExpiresAtAsInstant,
              )
            }
        }
      }
  }
}

object ClientCredentialsTokenProvider {

  private val RefreshSkew = Duration.ofSeconds(30)

  final case class CachedToken(accessToken: String, expiresAt: Instant)
  private final case class TokenResponse(accessToken: String)
  private object TokenResponse {
    implicit val decoder: Decoder[TokenResponse] =
      Decoder.forProduct1("accessToken")(TokenResponse.apply)
  }
}
