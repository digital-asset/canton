// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressionRule.{LevelAndAbove, LoggerNameContains}
import com.digitalasset.canton.util.{DelayUtil, ResourceUtil}
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.WARN

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

class CachedJwtVerifierLoaderRefreshSpec
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasActorSystem {

  private val suppressExpectedRefreshWarnings =
    LevelAndAbove(WARN) && LoggerNameContains("JwksCache")

  private val keySize = 2048
  private val kpg = KeyPairGenerator.getInstance("RSA")
  kpg.initialize(keySize)

  private val keyPair = kpg.generateKeyPair()
  private val publicKey = keyPair.getPublic.asInstanceOf[RSAPublicKey]
  private val privateKey = keyPair.getPrivate.asInstanceOf[RSAPrivateKey]

  private val jwks = KeyUtils.generateJwks(Map("test-key-1" -> publicKey))

  private val keyId = "test-key-1"

  private def generateToken(): Jwt = {
    val jwtPayload = s"""{"test": "CachedJwtVerifierLoaderRefreshSpec"}"""
    JwtSigner
      .RSA256(privateKey, kid = Some(keyId))
      .signPayload(jwtPayload)
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
      minRefreshInterval: FiniteDuration = 30.seconds,
  ): CachedJwtVerifierLoader =
    new CachedJwtVerifierLoader(
      cacheMaxSize = 10,
      cacheExpiration = cacheExpiration,
      connectionTimeout = connectionTimeout,
      readTimeout = readTimeout,
      minRefreshInterval = minRefreshInterval,
      autoRefreshAfter = autoRefreshTime,
      loggerFactory = loggerFactory,
    )

  private def endServerOutage(
      loader: CachedJwtVerifierLoader,
      server: ControllableJwksServer,
  ): Future[Unit] = {
    server.serve()
    val fetchesAtRecovery = server.fetchCount

    def loadUntilNextFetch(deadline: Deadline): Future[Unit] =
      if (server.fetchCount > fetchesAtRecovery) Future.unit
      else if (deadline.isOverdue())
        Future.failed(new IllegalStateException("No JWKS fetch after the outage ended"))
      else
        loader
          .loadJwtVerifier(server.url, Some(keyId))
          .transformWith(_ => DelayUtil.delay(50.milliseconds))
          .flatMap(_ => loadUntilNextFetch(deadline))

    loadUntilNextFetch(10.seconds.fromNow)
  }

  private def everyFailedFetchLoggedWarning(
      warnings: Seq[LogEntry],
      server: ControllableJwksServer,
  ): Assertion =
    warnings.size should be >= server.failedFetchCount

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
        // The rate limit for fetches also applies to background refreshes
        createCachedLoader(
          cacheExpiration = 30.seconds,
          autoRefreshTime = 100.millis,
          minRefreshInterval = 100.millis,
        )
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
              _ <- endServerOutage(loader, jwksServer)
            } yield succeed,
            assertion = entries => {
              entries should not be empty
              everyFailedFetchLoggedWarning(entries, jwksServer)
              forAll(entries)(_.warningMessage should include("Failed to load jwt verifier"))
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
          minRefreshInterval = 100.millis,
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
              _ <- endServerOutage(loader, jwksServer)
            } yield failure shouldBe a[JwtException],
            assertion = entries => {
              entries should not be empty
              everyFailedFetchLoggedWarning(entries, jwksServer)
              forAll(entries)(_.warningMessage should include("Failed to load jwt verifier"))
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
  private val currentJwks = new AtomicReference[String](jwksJson)
  private val fetches = new AtomicInteger(0)
  private val failures = new AtomicInteger(0)
  private val held = new AtomicReference[Future[Unit]](Future.unit)

  private val route: Route = path("result") {
    get {
      fetches.incrementAndGet().discard
      onSuccess(held.get()) {
        if (serving.get())
          complete(HttpEntity(ContentTypes.`application/json`, currentJwks.get()))
        else {
          failures.incrementAndGet().discard
          complete(StatusCodes.ServiceUnavailable)
        }
      }
    }
  }

  private val binding: Http.ServerBinding =
    Await.result(Http().newServerAt("localhost", 0).bind(route), 10.seconds)

  def start(): ControllableJwksServer = this

  def stop(): Unit = Await.result(binding.unbind(), 10.seconds).discard

  def url: JwksUrl = JwksUrl(s"http://localhost:${binding.localAddress.getPort}/result")

  def serve(): Unit = serving.set(true)

  def fail(): Unit = serving.set(false)

  /** Replaces the served JWKS document, e.g. to simulate a key rotation at the IDP. */
  def setJwks(jwksJson: String): Unit = currentJwks.set(jwksJson)

  /** Holds the responses to subsequent requests until the returned promise is completed, e.g. to
    * simulate a slow IDP.
    */
  def holdResponses(): Promise[Unit] = {
    val release = Promise[Unit]()
    held.set(release.future)
    release
  }

  /** Number of JWKS requests received so far, including failed ones. */
  def fetchCount: Int = fetches.get()

  /** Number of JWKS requests answered with an error. */
  def failedFetchCount: Int = failures.get()

  override def close(): Unit = stop()
}
