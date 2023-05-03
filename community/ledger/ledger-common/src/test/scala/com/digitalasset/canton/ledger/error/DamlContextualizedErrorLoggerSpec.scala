// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error

import ch.qos.logback.classic.Level
import com.daml.error.{BaseError, ErrorCategory, ErrorClass, ErrorCode}
import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.error.testpackage.SeriousError
import com.digitalasset.canton.testing.TestingLogCollector.ThrowableEntry
import com.digitalasset.canton.testing.{
  ErrorAssertionsWithLogCollectorAssertions,
  TestingLogCollector,
}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class DamlContextualizedErrorLoggerSpec
    extends AnyFreeSpec
    with BeforeAndAfter
    with Matchers
    with Eventually
    with IntegrationPatience
    with ErrorAssertionsWithLogCollectorAssertions {

  private val contextualizedErrorLoggerF =
    (correlationId: Option[String]) =>
      DamlContextualizedErrorLogger.forTesting(getClass, correlationId)

  before {
    TestingLogCollector.clear[this.type]
  }

  object FooErrorCodeSecuritySensitive
      extends ErrorCode(
        "FOO_ERROR_CODE_SECURITY_SENSITIVE",
        ErrorCategory.SystemInternalAssumptionViolated,
      )(ErrorClass.root())

  classOf[DamlContextualizedErrorLogger].getSimpleName - {

    "log information pertaining to a security sensitive error" - {
      class FooErrorBig(override val code: ErrorCode) extends BaseError {
        override val cause: String = "cause123"

        override def context: Map[String, String] =
          super.context ++ Map(
            "contextKey1" -> "contextValue1",
            "key????" -> "keyWithInvalidCharacters",
          )

        override def throwableO: Option[Throwable] =
          Some(new RuntimeException("runtimeException123"))
      }

      val errorLoggerBig = DamlContextualizedErrorLogger.forClass(
        clazz = getClass,
        correlationId = Some("123correlationId"),
        loggingContext = LoggingContext(
          "loggingEntryKey" -> "loggingEntryValue"
        ),
      )

      val testedError = new FooErrorBig(FooErrorCodeSecuritySensitive)
      testedError.logWithContext(Map.empty)(errorLoggerBig)

      assertSingleLogEntry[this.type, this.type](
        expectedLogLevel = Level.ERROR,
        expectedMsg = "FOO_ERROR_CODE_SECURITY_SENSITIVE(4,123corre): cause123",
        expectedMarkerAsString =
          """{loggingEntryKey: "loggingEntryValue", err-context: "{contextKey1=contextValue1, key????=keyWithInvalidCharacters, location=DamlContextualizedErrorLoggerSpec.scala:<line-number>}"}""",
        expectedThrowableEntry = Some(
          ThrowableEntry(
            className = "java.lang.RuntimeException",
            message = "runtimeException123",
          )
        ),
      )

    }

    "log the error message with the correct markers" in {
      val error = SeriousError.Error(
        "the error argument",
        context = Map("extra-context-key" -> "extra-context-value"),
      )
      val errorLogger = DamlContextualizedErrorLogger.forTesting(getClass, Some("1234567890"))

      error.logWithContext()(errorLogger)

      assertLogEntries[this.type, this.type] { actualLogs =>
        actualLogs.size shouldBe 1
        assertLogEntry(
          actualLogs.head,
          expectedLogLevel = Level.ERROR,
          expectedMsg = "BLUE_SCREEN(4,12345678): the error argument",
          expectedMarkerRegex = Some(
            "\\{err-context: \"\\{extra-context-key=extra-context-value, location=DamlContextualizedErrorLoggerSpec.scala:\\d+\\}\"\\}"
          ),
        )
        succeed
      }
    }

    s"truncate the cause size if larger than ${ErrorCode.MaxCauseLogLength}" in {
      val veryLongCause = "o" * (ErrorCode.MaxCauseLogLength * 2)
      val error =
        SeriousError.Error(
          veryLongCause,
          context = Map("extra-context-key" -> "extra-context-value"),
        )

      error.logWithContext()(contextualizedErrorLoggerF(None))

      val expectedErrorLog = "BLUE_SCREEN(4,0): " + ("o" * ErrorCode.MaxCauseLogLength + "...")
      assertLogEntries[this.type, this.type](
        _.map(entry => entry.level -> entry.msg) shouldBe Seq(Level.ERROR -> expectedErrorLog)
      )
    }
  }

}
