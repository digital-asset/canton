// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.{AuthServiceJWTCodec, DecodedJwt, Error, JwtVerifierBase}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.logging.SuppressingLogger
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

class AuthServiceJWTTest extends AnyWordSpec with BaseTest {

  object DummyVerifier extends JwtVerifierBase {
    override def verify(jwt: com.daml.jwt.Jwt): Either[Error, DecodedJwt[String]] =
      Left(Error(Symbol("DummyVerifier"), "Not needed for config testing"))
  }

  "AuthServiceJWT" should {

    "log a deprecation warning for scope-only configurations" in {
      val testLogger = SuppressingLogger(getClass)

      testLogger.assertLogs(
        AuthServiceJWT.apply(
          verifier = DummyVerifier,
          targetAudience = None,
          targetScope = Some("my-scope"),
          privileged = false,
          accessLevel = AccessLevel.Admin,
          loggerFactory = testLogger,
          users = Seq.empty,
          warnOnJwtScopeUsage = false,
        ),
        log => {
          log.level shouldBe Level.WARN
          log.message should include("Scope-based tokens have been configured")
          log.message should include("This feature is deprecated")
        },
      )
    }

    "log an ambiguity warning when both audience and scope are defined" in {
      val testLogger = SuppressingLogger(getClass)

      testLogger.assertLogs(
        AuthServiceJWT.apply(
          verifier = DummyVerifier,
          targetAudience = Some("my-audience"),
          targetScope = Some("my-scope"),
          privileged = false,
          accessLevel = AccessLevel.Admin,
          loggerFactory = testLogger,
          users = Seq.empty,
          warnOnJwtScopeUsage = false,
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
        AuthServiceJWT.apply(
          verifier = DummyVerifier,
          targetAudience = None,
          targetScope = None,
          privileged = true,
          accessLevel = AccessLevel.Admin,
          loggerFactory = testLogger,
          users = Seq.empty,
          warnOnJwtScopeUsage = false,
        )
      }
    }

    "log nothing and succeed for a clean, audience-based configuration" in {
      val testLogger = SuppressingLogger(getClass)

      testLogger.assertLoggedWarningsAndErrorsSeq(
        AuthServiceJWT.apply(
          verifier = DummyVerifier,
          targetAudience = Some("clean-audience"),
          targetScope = None,
          privileged = false,
          accessLevel = AccessLevel.Admin,
          loggerFactory = testLogger,
          users = Seq.empty,
          warnOnJwtScopeUsage = false,
        ),
        logs => logs shouldBe empty,
      )
    }
    "log nothing and succeed for a configuration with neither target audience nor scope (default fallback)" in {
      val testLogger = SuppressingLogger(getClass)

      testLogger.assertLoggedWarningsAndErrorsSeq(
        AuthServiceJWT.apply(
          verifier = DummyVerifier,
          targetAudience = None,
          targetScope = None,
          privileged = false,
          accessLevel = AccessLevel.Admin,
          loggerFactory = testLogger,
          users = Seq.empty,
          warnOnJwtScopeUsage = false,
        ),
        logs => logs shouldBe empty,
      )
    }
  }

  // Normally, tests use `SuppressingLogger(getClass)` to catch and silence warnings.
  // However, the deprecation warning for JWT scopes is executed inside `AuthServiceJWTCodec`,
  // which is a Scala `object` using a standard static SLF4J logger instance
  // instead of Canton's context-aware `NamedLoggerFactory`, which normal Canton classes
  // obtain by extending `NamedLogging`.
  // To verify this runtime warning, the tests below directly manipulate Logback, the Java logging engine underlying
  // the SLF4J interface.
  "Runtime token parsing" should {
    import ch.qos.logback.classic.spi.ILoggingEvent
    import ch.qos.logback.classic.Logger as LogbackLogger
    import ch.qos.logback.core.read.ListAppender
    import org.slf4j.LoggerFactory
    import scala.jdk.CollectionConverters.*

    // mock verifier that treats raw token text as verified internal JSON payload
    object PassThroughVerifier extends JwtVerifierBase {
      override def verify(jwt: com.daml.jwt.Jwt): Either[Error, DecodedJwt[String]] =
        Right(DecodedJwt("{}", jwt.value))
    }

    val legacyScopeBasedTokenJson = """{"sub": "legacyUser", "scope": "daml_ledger_api"}"""
    val audienceTokenJson =
      """{"sub": "modernUser", "aud": "https://daml.com/jwt/aud/participant/p1"}"""

    "remain silent when processing audience-based tokens" in {
      val codecLogger =
        LoggerFactory.getLogger(AuthServiceJWTCodec.getClass).asInstanceOf[LogbackLogger]

      val listAppender = new ListAppender[ILoggingEvent]()
      listAppender.start()
      codecLogger.addAppender(listAppender)

      val oldAdditive = codecLogger.isAdditive
      codecLogger.setAdditive(false)

      try {
        val testLogger = SuppressingLogger(this.getClass)
        val authService = AuthServiceJWT.apply(
          verifier = PassThroughVerifier,
          targetAudience = None, // config with no audience or scope set
          targetScope = None,
          privileged = false,
          accessLevel = AccessLevel.Admin,
          loggerFactory = testLogger,
          users = Seq.empty,
          warnOnJwtScopeUsage = true,
        )

        // Send an audience-based token
        authService.decodeToken(Some(s"Bearer $audienceTokenJson"), "ledger-api").futureValue
        listAppender.list.asScala.exists(_.getLevel.toString == "WARN") shouldBe false

      } finally {
        codecLogger.detachAppender(listAppender)
        codecLogger.setAdditive(oldAdditive)
      }
    }

    "log a warning exactly once for legacy scope tokens when warnOnJwtScopeUsage is true and neither scope nor audience is configured" in {
      // The generic SLF4J interface (org.slf4j.Logger) returned by LoggerFactory only exposes log-writing methods
      // (.info, .warn). It lacks runtime configuration switches.
      // We cast the SLF4J facade to Logback to expose configuration methods like .addAppender, .setAdditive
      val codecLogger =
        LoggerFactory.getLogger(AuthServiceJWTCodec.getClass).asInstanceOf[LogbackLogger]

      // Intercepts the log stream and stores events in a list for assertions
      val listAppender = new ListAppender[ILoggingEvent]()
      listAppender.start()
      codecLogger.addAppender(listAppender)

      // save additivity state to restore later
      val oldAdditive = codecLogger.isAdditive

      // set additivity state to false in order to suppress log propagation
      codecLogger.setAdditive(false)

      try {
        val testLogger = SuppressingLogger(this.getClass)
        val authService = AuthServiceJWT.apply(
          verifier = PassThroughVerifier,
          targetAudience = None, // config with no audience or scope set
          targetScope = None,
          privileged = false,
          accessLevel = AccessLevel.Admin,
          loggerFactory = testLogger,
          users = Seq.empty,
          warnOnJwtScopeUsage = true,
        )

        // Send a legacy scope-based token -> triggers the warning
        authService
          .decodeToken(Some(s"Bearer $legacyScopeBasedTokenJson"), "ledger-api")
          .futureValue
        val warnLogs = listAppender.list.asScala.filter(_.getLevel.toString == "WARN")
        warnLogs.size shouldBe 1
        warnLogs.head.getFormattedMessage should include(
          "Received scope-based token. Scope-based tokens are deprecated"
        )

        // send another legacy scope-based token -> this should not trigger the warning again
        authService
          .decodeToken(Some(s"Bearer $legacyScopeBasedTokenJson"), "ledger-api")
          .futureValue
        listAppender.list.asScala.count(_.getLevel.toString == "WARN") shouldBe 1

      } finally {
        // clean up: detach and restore standard logging
        // failing to restore this state would break logging across all subsequent test suites.
        codecLogger.detachAppender(listAppender)
        codecLogger.setAdditive(oldAdditive)
      }
    }

    "remain silent for legacy scope tokens when warnOnJwtScopeUsage is false" in {

      val codecLogger =
        LoggerFactory.getLogger(AuthServiceJWTCodec.getClass).asInstanceOf[LogbackLogger]
      val listAppender = new ListAppender[ILoggingEvent]()
      listAppender.start()
      codecLogger.addAppender(listAppender)

      val oldAdditive = codecLogger.isAdditive
      codecLogger.setAdditive(false)

      try {
        val testLogger = SuppressingLogger(this.getClass)
        val authService = AuthServiceJWT.apply(
          verifier = PassThroughVerifier,
          targetAudience = None,
          targetScope = None,
          privileged = false,
          accessLevel = AccessLevel.Admin,
          loggerFactory = testLogger,
          users = Seq.empty,
          warnOnJwtScopeUsage = false,
        )

        authService
          .decodeToken(Some(s"Bearer $legacyScopeBasedTokenJson"), "ledger-api")
          .futureValue
        listAppender.list.asScala.exists(_.getLevel.toString == "WARN") shouldBe false
      } finally {
        codecLogger.detachAppender(listAppender)
        codecLogger.setAdditive(oldAdditive)
      }
    }
  }
}
