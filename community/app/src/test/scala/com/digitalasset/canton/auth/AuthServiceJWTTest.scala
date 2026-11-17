// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.{DecodedJwt, Error, JwtVerifierBase, StandardJWTPayload, StandardJWTTokenFormat}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DeprecatedFeaturesConfig
import com.digitalasset.canton.logging.SuppressingLogger
import org.scalatest.enablers.InspectorAsserting
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

class AuthServiceJWTTest extends AnyWordSpec with BaseTest {

  /** Rejects every token, for tests that only construct the auth service. */
  object DummyVerifier extends JwtVerifierBase {
    override def verify(jwt: com.daml.jwt.Jwt): Either[Error, DecodedJwt[String]] =
      Left(Error(Symbol("DummyVerifier"), "Not needed for config testing"))
  }

  /** Treats the raw token text as the verified JSON payload, for tests that parse tokens. */
  object PassThroughVerifier extends JwtVerifierBase {
    override def verify(jwt: com.daml.jwt.Jwt): Either[Error, DecodedJwt[String]] =
      Right(DecodedJwt("{}", jwt.value))
  }

  private def authService(
      targetAudience: Option[String],
      targetScope: Option[String],
      enableScopeOnlyJwt: Boolean,
      loggerFactory: SuppressingLogger,
      verifier: JwtVerifierBase = DummyVerifier,
      privileged: Boolean = false,
  ): AuthServiceJWTBase =
    AuthServiceJWT.apply(
      verifier = verifier,
      targetAudience = targetAudience,
      targetScope = targetScope,
      privileged = privileged,
      accessLevel = AccessLevel.Admin,
      loggerFactory = loggerFactory,
      users = Seq.empty,
      enableScopeOnlyJwt = enableScopeOnlyJwt,
    )

  /** Runs `f` with scope-only JWTs enabled and disabled, naming the flag value on failure. */
  private def forEveryScopeOnlyJwtFlag[A](
      f: Boolean => A
  )(implicit asserting: InspectorAsserting[A]): asserting.Result =
    forEvery(Seq(true, false)) { enableScopeOnlyJwt =>
      withClue(s"enableScopeOnlyJwt = $enableScopeOnlyJwt")(f(enableScopeOnlyJwt))
    }

  "AuthServiceJWT" should {

    // the config validation rejects a target scope without a target audience when the flag is off
    "log a deprecation warning for scope-only configurations when scope-only JWTs are enabled" in {
      val testLogger = SuppressingLogger(getClass)

      testLogger.assertLogs(
        authService(
          targetAudience = None,
          targetScope = Some("my-scope"),
          enableScopeOnlyJwt = true,
          loggerFactory = testLogger,
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include(DeprecatedFeaturesConfig.EnableScopeOnlyJwtKey)
          log.message should include("scope-only tokens are accepted")
        },
      )
    }

    "log an ambiguity warning when both audience and scope are defined" in {
      val testLogger = SuppressingLogger(getClass)

      testLogger.assertLogs(
        authService(
          targetAudience = Some("my-audience"),
          targetScope = Some("my-scope"),
          enableScopeOnlyJwt = true,
          loggerFactory = testLogger,
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include("Ambiguous configuration")
          log.message should include("both targetScope and targetAudience have been specified")
        },
      )
    }

    "throw an IllegalArgumentException for privileged configs with neither scope nor audience" in {
      val testLogger = SuppressingLogger(getClass)

      an[IllegalArgumentException] should be thrownBy {
        authService(
          targetAudience = None,
          targetScope = None,
          enableScopeOnlyJwt = true,
          loggerFactory = testLogger,
          privileged = true,
        )
      }
    }

    "log nothing and succeed for a clean, audience-based configuration" in {
      forEveryScopeOnlyJwtFlag { enableScopeOnlyJwt =>
        val testLogger = SuppressingLogger(getClass)

        testLogger.assertLoggedWarningsAndErrorsSeq(
          authService(
            targetAudience = Some("clean-audience"),
            targetScope = None,
            enableScopeOnlyJwt = enableScopeOnlyJwt,
            loggerFactory = testLogger,
          ),
          logs => logs shouldBe empty,
        )
      }
    }

    "log nothing and succeed for a configuration with neither target audience nor scope (default fallback)" in {
      val testLogger = SuppressingLogger(getClass)

      testLogger.assertLoggedWarningsAndErrorsSeq(
        authService(
          targetAudience = None,
          targetScope = None,
          enableScopeOnlyJwt = false,
          loggerFactory = testLogger,
        ),
        logs => logs shouldBe empty,
      )
    }

    "log a deprecation warning for the default fallback configuration when scope-only JWTs are enabled" in {
      val testLogger = SuppressingLogger(getClass)

      testLogger.assertLogs(
        authService(
          targetAudience = None,
          targetScope = None,
          enableScopeOnlyJwt = true,
          loggerFactory = testLogger,
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include(DeprecatedFeaturesConfig.EnableScopeOnlyJwtKey)
          log.message should include("scope-only tokens are accepted")
        },
      )
    }
  }

  "Runtime token parsing" should {
    val legacyScopeBasedTokenJson = """{"sub": "legacyUser", "scope": "daml_ledger_api"}"""
    val audienceTokenJson =
      """{"sub": "modernUser", "aud": "https://daml.com/jwt/aud/participant/p1"}"""

    // the two configurations that parse a token into the scope-only format
    val scopeOnlyAcceptingTargets = Seq(
      "no target" -> None,
      "a target scope" -> Some("daml_ledger_api"),
    )

    def parsingAuthService(
        targetScope: Option[String],
        enableScopeOnlyJwt: Boolean,
    ): AuthServiceJWTBase = {
      val testLogger = SuppressingLogger(this.getClass)
      // a target scope always warns at startup and no target warns when the flag is on, see above
      testLogger.suppressWarningsAndErrors(
        authService(
          targetAudience = None,
          targetScope = targetScope,
          enableScopeOnlyJwt = enableScopeOnlyJwt,
          loggerFactory = testLogger,
          verifier = PassThroughVerifier,
        )
      )
    }

    "reject legacy scope tokens when scope-only JWTs are disabled" in {
      forEvery(scopeOnlyAcceptingTargets) { case (label, targetScope) =>
        withClue(label) {
          val result = parsingAuthService(targetScope, enableScopeOnlyJwt = false)
            .decodeToken(Some(s"Bearer $legacyScopeBasedTokenJson"), "ledger-api")
            .futureValue
          result.claimSet shouldBe ClaimSet.Unauthenticated
          result.deferredWarning.value should include(
            DeprecatedFeaturesConfig.ScopeOnlyJwtDisabledMessage
          )
        }
      }
    }

    "accept legacy scope tokens when scope-only JWTs are enabled" in {
      forEvery(scopeOnlyAcceptingTargets) { case (label, targetScope) =>
        withClue(label) {
          val result = parsingAuthService(targetScope, enableScopeOnlyJwt = true)
            .decodeToken(Some(s"Bearer $legacyScopeBasedTokenJson"), "ledger-api")
            .futureValue
          result.claimSet shouldBe ClaimSet.AuthenticatedUser(
            identityProviderId = None,
            participantId = None,
            userId = "legacyUser",
            expiration = None,
          )
        }
      }
    }

    "accept audience-based tokens regardless of the flag" in {
      forEveryScopeOnlyJwtFlag { enableScopeOnlyJwt =>
        val result = parsingAuthService(targetScope = None, enableScopeOnlyJwt)
          .decodeToken(Some(s"Bearer $audienceTokenJson"), "ledger-api")
          .futureValue
        result.claimSet shouldBe ClaimSet.AuthenticatedUser(
          identityProviderId = None,
          participantId = Some("p1"),
          userId = "modernUser",
          expiration = None,
        )
      }
    }
  }

  "rejectScopeOnlyJwtUnlessEnabled" should {
    def payload(format: StandardJWTTokenFormat): StandardJWTPayload = StandardJWTPayload(
      issuer = None,
      userId = "user",
      participantId = None,
      exp = None,
      format = format,
      audiences = List.empty,
      scope = None,
    )
    val scopePayload = payload(StandardJWTTokenFormat.Scope)
    val audiencePayload = payload(StandardJWTTokenFormat.Audience)

    "reject a scope-only payload when scope-only JWTs are disabled" in {
      val error = AuthServiceJWT
        .rejectScopeOnlyJwtUnlessEnabled(scopePayload, enableScopeOnlyJwt = false)
        .left
        .value
      error.what.name shouldBe "scopeOnlyJwtDisabled"
      error.message shouldBe DeprecatedFeaturesConfig.ScopeOnlyJwtDisabledMessage
    }

    "accept a scope-only payload when scope-only JWTs are enabled" in {
      AuthServiceJWT
        .rejectScopeOnlyJwtUnlessEnabled(scopePayload, enableScopeOnlyJwt = true)
        .value shouldBe scopePayload
    }

    "accept an audience-based payload regardless of the flag" in {
      forEveryScopeOnlyJwtFlag { enableScopeOnlyJwt =>
        AuthServiceJWT
          .rejectScopeOnlyJwtUnlessEnabled(audiencePayload, enableScopeOnlyJwt)
          .value shouldBe audiencePayload
      }
    }
  }
}
