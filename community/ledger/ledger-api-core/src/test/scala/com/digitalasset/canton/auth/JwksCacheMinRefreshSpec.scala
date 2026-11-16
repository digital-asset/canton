// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.*
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Availability
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import com.digitalasset.canton.util.{MonadUtil, ResourceUtil}
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.*

/** Checks that tokens carrying key ids chosen by an unauthenticated client cannot force the
  * participant to fetch the JWKS document from the configured URL for every request, while key
  * rotations at the IDP are still picked up.
  *
  * Runs against both JWKS-based verifiers: [[JwksVerifier]] (used by the `jwt-jwks` auth service)
  * and [[CachedJwtVerifierLoader]] (used for identity provider configs).
  */
class JwksCacheMinRefreshSpec
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasActorSystem {

  private val securityAsset: SecurityTest =
    SecurityTest(property = Availability, asset = "JWKS-configured Resource")

  private def attack(threat: String): Attack = Attack(
    actor = "Unauthenticated Ledger API client",
    threat = threat,
    mitigation = "Do not fetch the JWKS document for every token with an unknown key id",
  )

  private val kpg = KeyPairGenerator.getInstance("RSA")
  kpg.initialize(2048)

  private val keyPair1 = kpg.generateKeyPair()
  private val publicKey1 = keyPair1.getPublic.asInstanceOf[RSAPublicKey]
  private val privateKey1 = keyPair1.getPrivate.asInstanceOf[RSAPrivateKey]

  private val keyPair2 = kpg.generateKeyPair()
  private val publicKey2 = keyPair2.getPublic.asInstanceOf[RSAPublicKey]
  private val privateKey2 = keyPair2.getPrivate.asInstanceOf[RSAPrivateKey]

  private def generateToken(keyId: String, privateKey: RSAPrivateKey): Jwt =
    JwtSigner
      .RSA256(privateKey, kid = Some(keyId))
      .signPayload("""{"test": "JwksCacheMinRefreshSpec"}""")
      .fold(e => fail("Failed to generate signed token: " + e.prettyPrint), identity)

  /** Common interface over the two JWKS-based verifiers. */
  private trait TokenVerifier extends AutoCloseable {

    /** Returns whether the token verified. Runs asynchronously, so that a verifier that blocks the
      * calling thread while fetching the JWKS does not serialize concurrent calls.
      */
    def verify(token: Jwt): Future[Boolean]
  }

  private val cacheMaxSize = 10L
  private val cacheExpiration = 10.minutes
  private val connectionTimeout = 1.second
  // Longer than any response held by a test
  private val readTimeout = 30.seconds

  private def jwksVerifier(url: JwksUrl, minRefreshInterval: FiniteDuration): TokenVerifier =
    new TokenVerifier {
      private val verifier =
        JwksVerifier(
          url.value,
          cacheExpiration = cacheExpiration,
          connectionTimeout = connectionTimeout,
          readTimeout = readTimeout,
          minRefreshInterval = minRefreshInterval,
          loggerFactory = loggerFactory,
        )

      override def verify(token: Jwt): Future[Boolean] = Future(verifier.verify(token).isRight)

      override def close(): Unit = ()
    }

  private def cachedJwtVerifierLoader(
      url: JwksUrl,
      minRefreshInterval: FiniteDuration,
  ): TokenVerifier = new TokenVerifier {
    private val loader = new CachedJwtVerifierLoader(
      cacheMaxSize = cacheMaxSize,
      cacheExpiration = cacheExpiration,
      connectionTimeout = connectionTimeout,
      readTimeout = readTimeout,
      minRefreshInterval = minRefreshInterval,
      loggerFactory = loggerFactory,
    )

    override def verify(token: Jwt): Future[Boolean] =
      Future {
        // Like IdentityProviderAwareAuthService, take the key id from the unverified header.
        val keyId = Option(com.auth0.jwt.JWT.decode(token.value).getKeyId)
        loader.loadJwtVerifier(url, keyId).map(_.verify(token).isRight).recover {
          case _: JwtException => false
        }
      }.flatten

    override def close(): Unit = loader.close()
  }

  private def jwksWith(keys: (String, RSAPublicKey)*): String = KeyUtils.generateJwks(keys.toMap)

  private val implementations: Seq[(String, (JwksUrl, FiniteDuration) => TokenVerifier)] = Seq(
    "JwksVerifier" -> jwksVerifier,
    "CachedJwtVerifierLoader" -> cachedJwtVerifierLoader,
  )

  implementations.foreach { case (name, createVerifier) =>
    name should {
      "not fetch the JWKS for every token with an unknown key id" taggedAs securityAsset.setAttack(
        attack("Exhaust threads and the IDP by sending tokens with random unknown key ids")
      ) in {
        val requestsPerPhase = 20
        ResourceUtil.withResourceM(
          new ControllableJwksServer(jwksWith("test-key-1" -> publicKey1))
        ) { jwksServer =>
          ResourceUtil.withResourceM(createVerifier(jwksServer.url, 10.minutes)) { verifier =>
            // A fresh key id per request, so that remembering individual unknown key ids does not help.
            def attempt(): Future[Boolean] =
              verifier.verify(generateToken(s"unknown-${UUID.randomUUID()}", privateKey1))

            for {
              concurrentResults <- Future.sequence(Seq.fill(requestsPerPhase)(attempt()))
              sequentialResults <- MonadUtil.sequentialTraverse(1 to requestsPerPhase)(_ =>
                attempt()
              )
            } yield {
              forAll(concurrentResults ++ sequentialResults)(_ shouldBe false)
              withClue(s"JWKS fetches for ${2 * requestsPerPhase} tokens with unknown key ids:") {
                jwksServer.fetchCount shouldBe 1
              }
            }
          }
        }
      }

      "not make tokens wait for a refetch started by another token" taggedAs securityAsset
        .setAttack(
          attack("Exhaust threads by sending tokens with unknown key ids while the IDP is slow")
        ) in {
        ResourceUtil.withResourceM(
          new ControllableJwksServer(jwksWith("test-key-1" -> publicKey1))
        ) { jwksServer =>
          // Without a minimum interval, only the refetch in flight prevents further fetches
          ResourceUtil.withResourceM(createVerifier(jwksServer.url, Duration.Zero)) { verifier =>
            val validToken = generateToken("test-key-1", privateKey1)
            val unknownKeyIdToken = generateToken("unknown", privateKey1)

            for {
              initial <- verifier.verify(validToken)
              release = jwksServer.holdResponses()
              refetch = verifier.verify(unknownKeyIdToken)
              _ <- eventuallyAsync(maxPollInterval = 10.milliseconds)(
                jwksServer.fetchCount shouldBe 2
              ).failOnShutdown
              validDuringRefetch <- verifier.verify(validToken)
              unknownDuringRefetch <- verifier.verify(unknownKeyIdToken)
              refetchStillRunning = !refetch.isCompleted
              _ = release.success(())
              refetchResult <- refetch
            } yield {
              initial shouldBe true
              validDuringRefetch shouldBe true
              unknownDuringRefetch shouldBe false
              withClue("Tokens waited for the refetch started by another token:") {
                refetchStillRunning shouldBe true
              }
              refetchResult shouldBe false
              jwksServer.fetchCount shouldBe 2
            }
          }
        }
      }

      "pick up a new key after the JWKS has been rotated" taggedAs securityAsset.setHappyCase(
        "Verify tokens signed with a key that was added to the JWKS after the first fetch"
      ) in {
        ResourceUtil.withResourceM(
          new ControllableJwksServer(jwksWith("test-key-1" -> publicKey1))
        ) { jwksServer =>
          ResourceUtil.withResourceM(createVerifier(jwksServer.url, 100.milliseconds)) { verifier =>
            val token1 = generateToken("test-key-1", privateKey1)
            val token2 = generateToken("test-key-2", privateKey2)
            for {
              token1BeforeRotation <- verifier.verify(token1)
              token2BeforeRotation <- verifier.verify(token2)
              _ = jwksServer.setJwks(
                jwksWith("test-key-1" -> publicKey1, "test-key-2" -> publicKey2)
              )
              _ <- eventuallyAsync(maxPollInterval = 100.milliseconds)(
                verifier.verify(token2).futureValue shouldBe true
              ).failOnShutdown
              token1AfterRotation <- verifier.verify(token1)
            } yield {
              token1BeforeRotation shouldBe true
              token2BeforeRotation shouldBe false
              token1AfterRotation shouldBe true
            }
          }
        }
      }

      "recover after a failed fetch" taggedAs securityAsset.setHappyCase(
        "Verify tokens again once the JWKS endpoint recovers from a failure"
      ) in {
        ResourceUtil.withResourceM(
          new ControllableJwksServer(jwksWith("test-key-1" -> publicKey1))
        ) { jwksServer =>
          ResourceUtil.withResourceM(createVerifier(jwksServer.url, 100.milliseconds)) { verifier =>
            val token = generateToken("test-key-1", privateKey1)
            jwksServer.fail()
            loggerFactory.suppressWarnings(
              for {
                duringFailure <- verifier.verify(token)
                _ = jwksServer.serve()
                _ <- eventuallyAsync() {
                  verifier.verify(token).futureValue shouldBe true
                }.failOnShutdown
              } yield duringFailure shouldBe false
            )
          }
        }
      }
    }
  }
}
