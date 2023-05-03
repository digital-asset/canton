// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing

import com.daml.error.utils.ErrorDetails
import com.daml.error.{ContextualizedErrorLogger, ErrorsAssertions}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.ledger.error.DamlContextualizedErrorLogger
import com.digitalasset.canton.testing.TestingLogCollector.ExpectedLogEntry
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

trait ErrorAssertionsWithLogCollectorAssertions extends ErrorsAssertions with LoggingAssertions {
  self: Matchers with Eventually with IntegrationPatience =>

  private val logger = ContextualizedLogger.get(getClass)
  private val loggingContext = LoggingContext.ForTesting
  private val errorLogger = new DamlContextualizedErrorLogger(logger, loggingContext, None)

  def assertError(
      actual: StatusRuntimeException,
      expectedF: ContextualizedErrorLogger => StatusRuntimeException,
  ): Unit = {
    assertError(
      actual = actual,
      expected = expectedF(errorLogger),
    )
  }

  def assertError[Test: ClassTag, Logger: ClassTag](
      actual: StatusRuntimeException,
      expectedCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
      expectedLogEntry: ExpectedLogEntry,
  ): Unit = {
    assertError(
      actual = actual,
      expectedStatusCode = expectedCode,
      expectedMessage = expectedMessage,
      expectedDetails = expectedDetails,
    )

    assertLogEntries[Test, Logger] { actualLogs =>
      actualLogs should have size 1
      assertLogEntry(actualLogs.head, expectedLogEntry)
      succeed
    }
  }
}
