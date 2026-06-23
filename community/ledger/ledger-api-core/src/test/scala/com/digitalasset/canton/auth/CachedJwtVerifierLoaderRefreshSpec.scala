// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.SuppressionRule.{LevelAndAbove, LoggerNameContains}
import com.digitalasset.canton.util.{DelayUtil, ResourceUtil}
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.WARN

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class CachedJwtVerifierLoaderRefreshSpec
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasActorSystem {

  private val suppressExpectedRefreshWarnings =
    LevelAndAbove(WARN) &&
      (LoggerNameContains("CachedJwtVerifierLoader") || LoggerNameContains("BoundedLocalCache"))

  private val keySize = 2048
  private val kpg = KeyPairGenerator.getInstance("RSA")
  kpg.initialize(keySize)

  private val keyPair = kpg.generateKeyPair()
  private val publicKey = keyPair.getPublic.asInstanceOf[RSAPublicKey]
  private val privateKey = keyPair.getPrivate.asInstanceOf[RSAPrivateKey]

  private val jwks = KeyUtils.generateJwks(Map("test-key-1" -> publicKey))

  private val keyId = "test-key-1"

  private def generateToken(): Jwt = {
    val jwtHeader = s"""{"alg": "RS256", "typ": "JWT", "kid": "$keyId"}"""
    val jwtPayload = s"""{"test": "CachedJwtVerifierLoaderRefreshSpec"}"""
    JwtSigner.RSA256
      .sign(DecodedJwt(jwtHeader, jwtPayload), privateKey)
      .fold(e => fail("Failed to generate signed token: " + e.prettyPrint), identity)
  }

  private val staticJwksServer = new ControllableJwksServer(jwks).start()
  private val localJwksUrl = staticJwksServer.url

  override def afterAll(): Unit =
    try staticJwksServer.close()
    finally super.afterAll()

  private def createCachedLoader(
      cacheExpiration: FiniteDuration = 10.minutes,
      connectionTimeout: FiniteDuration = 1.second,
      readTimeout: FiniteDuration = 1.second,
      autoRefreshTime: FiniteDuration = 0.seconds,
  ): CachedJwtVerifierLoader =
    new CachedJwtVerifierLoader(
      cacheMaxSize = 10,
      cacheExpiration = cacheExpiration,
      connectionTimeout = connectionTimeout,
      readTimeout = readTimeout,
      autoRefreshAfter = autoRefreshTime,
      loggerFactory = loggerFactory,
    )

  "CachedJwtVerifierLoader with autoRefreshTime=0 (original logic)" should {
    "successfully load and verify when JWKS is available" in {
      ResourceUtil.withResourceM(createCachedLoader()) { loader =>
        val token = generateToken()
        loader.loadJwtVerifier(localJwksUrl, Some(keyId)).map { verifier =>
          verifier.verify(token).isRight shouldBe true
        }
      }
    }

    "serve from cache on second load" in {
      ResourceUtil.withResourceM(createCachedLoader()) { loader =>
        ResourceUtil.withResourceM(new ControllableJwksServer(jwks).start()) { jwksServer =>
          val token = generateToken()
          for {
            verifier1 <- loader.loadJwtVerifier(jwksServer.url, Some(keyId))
            _ = jwksServer.fail()
            verifier2 <- loader.loadJwtVerifier(jwksServer.url, Some(keyId))
          } yield {
            verifier1.verify(token).isRight shouldBe true
            verifier2.verify(token).isRight shouldBe true
          }
        }
      }
    }

    "fail if cache expired and server down" in {
      ResourceUtil.withResourceM(createCachedLoader(cacheExpiration = 100.milliseconds)) { loader =>
        ResourceUtil.withResourceM(new ControllableJwksServer(jwks).start()) { jwksServer =>
          val token = generateToken()

          def loadUntilFailure(deadline: Deadline): Future[Throwable] =
            loader.loadJwtVerifier(jwksServer.url, Some(keyId)).transformWith {
              case Failure(t) => Future.successful(t)
              case Success(_) if deadline.hasTimeLeft() =>
                DelayUtil.delay(50.milliseconds).flatMap(_ => loadUntilFailure(deadline))
              case Success(_) =>
                Future.successful(
                  new IllegalStateException("JWKS load kept succeeding after cache expiry")
                )
            }

          loggerFactory.assertLogsSeq(LevelAndAbove(WARN))(
            for {
              verifier1 <- loader.loadJwtVerifier(jwksServer.url, Some(keyId))
              _ = verifier1.verify(token).isRight shouldBe true
              _ = jwksServer.fail()
              failure <- loadUntilFailure(5.seconds.fromNow)
            } yield failure shouldBe a[JwtException],
            entries =>
              forAll(entries)(entry =>
                entry.warningMessage should include("Failed to load jwt verifier")
              ),
          )
        }
      }
    }
  }

  "CachedJwtVerifierLoader with autoRefreshTime > 0" should {
    "serve cached entry during JWKS outage within the refresh window" in {
      ResourceUtil.withResourceM(
        createCachedLoader(cacheExpiration = 30.seconds, autoRefreshTime = 100.millis)
      ) { loader =>
        ResourceUtil.withResourceM(new ControllableJwksServer(jwks).start()) { jwksServer =>
          val token = generateToken()

          def serveWhileRefreshFails(deadline: Deadline): Future[Unit] =
            loader.loadJwtVerifier(jwksServer.url, Some(keyId)).flatMap { verifier =>
              verifier.verify(token).isRight shouldBe true
              if (deadline.hasTimeLeft())
                DelayUtil.delay(50.milliseconds).flatMap(_ => serveWhileRefreshFails(deadline))
              else Future.unit
            }

          loggerFactory.assertEventuallyLogsSeq(suppressExpectedRefreshWarnings)(
            within = for {
              verifier1 <- loader.loadJwtVerifier(jwksServer.url, Some(keyId))
              _ = verifier1.verify(token).isRight shouldBe true
              _ = jwksServer.fail()
              _ <- serveWhileRefreshFails(1.second.fromNow)
            } yield succeed,
            assertion = entries => {
              val loaderWarnings =
                entries.filter(_.loggerName.contains("CachedJwtVerifierLoader"))
              loaderWarnings should not be empty
              forAll(loaderWarnings) { entry =>
                val message = entry.warningMessage
                message should (
                  include("Failed to refresh jwt verifier") or
                    include("Failed to load jwt verifier")
                )
              }
            },
          )
        }
      }
    }

    "fail after hard eviction when JWKS is still unavailable" in {
      // autoRefreshTime == cacheExpiration triggers a constructor warning, asserted here.
      val createdLoader = loggerFactory.assertLogsSeq(LevelAndAbove(WARN))(
        createCachedLoader(
          cacheExpiration = 1.second,
          connectionTimeout = 500.millis,
          readTimeout = 500.millis,
          autoRefreshTime = 1.second,
        ),
        entries =>
          forExactly(1, entries)(
            _.warningMessage should include("autoRefreshTime")
          ),
      )
      ResourceUtil.withResourceM(createdLoader) { loader =>
        ResourceUtil.withResourceM(new ControllableJwksServer(jwks).start()) { jwksServer =>
          val token = generateToken()

          // Non-blocking poll: keep loading until the entry is hard-evicted (after
          // cacheExpiration) and the load against the still-failing server finally fails.
          def loadUntilFailure(deadline: Deadline): Future[Throwable] =
            loader.loadJwtVerifier(jwksServer.url, Some(keyId)).transformWith {
              case Failure(t) => Future.successful(t)
              case Success(_) if deadline.hasTimeLeft() =>
                DelayUtil.delay(50.milliseconds).flatMap(_ => loadUntilFailure(deadline))
              case Success(_) =>
                Future.successful(
                  new IllegalStateException("JWKS load kept succeeding after hard eviction")
                )
            }

          loggerFactory.assertEventuallyLogsSeq(suppressExpectedRefreshWarnings)(
            within = for {
              verifier1 <- loader.loadJwtVerifier(jwksServer.url, Some(keyId))
              _ = verifier1.verify(token).isRight shouldBe true
              _ = jwksServer.fail()
              failure <- loadUntilFailure(10.seconds.fromNow)
            } yield failure shouldBe a[JwtException],
            assertion = entries => {
              val loaderWarnings =
                entries.filter(_.loggerName.contains("CachedJwtVerifierLoader"))
              loaderWarnings should not be empty
              forAll(loaderWarnings) { entry =>
                val message = entry.warningMessage
                message should (
                  include("Failed to refresh jwt verifier") or
                    include("Failed to load jwt verifier")
                )
              }
            },
          )
        }
      }
    }
  }
}

private class ControllableJwksServer(jwksJson: String)(implicit system: ActorSystem)
    extends AutoCloseable {
  private val serving = new AtomicBoolean(true)

  private val route: Route = path("result") {
    get {
      if (serving.get())
        complete(HttpEntity(ContentTypes.`application/json`, jwksJson))
      else
        complete(StatusCodes.ServiceUnavailable)
    }
  }

  private val binding: Http.ServerBinding =
    Await.result(Http().newServerAt("localhost", 0).bind(route), 10.seconds)

  def start(): ControllableJwksServer = this

  def stop(): Unit = Await.result(binding.unbind(), 10.seconds).discard

  def url: JwksUrl = JwksUrl(s"http://localhost:${binding.localAddress.getPort}/result")

  def serve(): Unit = serving.set(true)

  def fail(): Unit = serving.set(false)

  override def close(): Unit = stop()
}
