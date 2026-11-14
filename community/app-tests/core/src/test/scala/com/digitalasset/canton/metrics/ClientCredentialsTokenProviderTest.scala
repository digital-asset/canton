// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, HttpResponse}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Duration
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Promise
import scala.concurrent.duration.*

class ClientCredentialsTokenProviderTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with HasActorSystem {

  "ClientCredentialsTokenProvider" should {

    "fetch and refresh tokens" in {
      val clock = new SimClock(loggerFactory = loggerFactory)
      val requests = new AtomicInteger()
      val receivedRequest = Promise[(HttpRequest, String)]()

      val binding =
        Http()
          .newServerAt("127.0.0.1", 0)
          .bind { request =>
            request.entity.toStrict(1.second).map { entity =>
              requests.incrementAndGet()

              receivedRequest.trySuccess(
                request -> entity.data.utf8String
              )

              request
                .header[Authorization]
                .map(_.credentials) shouldBe Some(
                BasicHttpCredentials(
                  "test-client",
                  "test-secret",
                )
              )

              entity.data.utf8String shouldBe
                "grant_type=client_credentials&scope=test-scope"

              val token = createToken(clock)

              HttpResponse(
                entity = HttpEntity(
                  ContentTypes.`application/json`,
                  s"""{"accessToken":"$token"}""",
                )
              )
            }
          }
          .futureValue

      try {
        val tokenProvider = new ClientCredentialsTokenProvider(
          OtlpAuth.OauthClientCredentials(
            tokenUrl = s"http://127.0.0.1:${binding.localAddress.getPort}/token",
            clientId = "test-client",
            clientSecret = "test-secret",
            scope = Some("test-scope"),
          ),
          clock,
        )

        val first = tokenProvider.ensureToken().futureValue
        val (request, body) = receivedRequest.future.futureValue
        request
          .header[Authorization]
          .map(_.credentials) shouldBe Some(
          BasicHttpCredentials(
            "test-client",
            "test-secret",
          )
        )
        body shouldBe "grant_type=client_credentials&scope=test-scope"
        requests.get() shouldBe 1
        tokenProvider.authorizationHeader() shouldBe s"Bearer ${first.accessToken}"

        clock.advance(Duration.ofSeconds(29))
        tokenProvider.ensureToken().futureValue shouldBe first
        requests.get() shouldBe 1

        clock.advance(Duration.ofSeconds(2))
        val second = tokenProvider.ensureToken().futureValue
        requests.get() shouldBe 2
        second.accessToken should not be first.accessToken
      } finally {
        binding.unbind().futureValue
      }
    }

  }

  private def createToken(clock: SimClock): String =
    JWT
      .create()
      .withExpiresAt(
        Date.from(
          clock.now
            .add(Duration.ofSeconds(60))
            .toInstant
        )
      )
      .sign(Algorithm.HMAC256("test-secret"))

}
