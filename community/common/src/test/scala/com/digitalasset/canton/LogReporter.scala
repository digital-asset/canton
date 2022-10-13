// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import org.scalatest.Reporter
import org.scalatest.events.*

/** Logs when a test case or suite is started or completed.
  * To use it, register this class with "-C" when ScalaTest is started.
  */
class LogReporter extends Reporter with NamedLogging with NoTracing {

  override protected def loggerFactory: NamedLoggerFactory = NamedLoggerFactory.root

  override def apply(event: Event): Unit = event match {
    case _: RunStarting => logger.info("Starting test run...")
    case _: RunCompleted => logger.info("Completed test run.")
    case _: RunStopped => logger.warn("Stopped test run.")
    case _: RunAborted => logger.warn("Aborted test run.")
    case event: SuiteStarting => logger.info(s"Starting test suite '${event.suiteName}'...")
    case event: SuiteCompleted => logger.info(s"Completed test suite '${event.suiteName}'.")
    case event: SuiteAborted => logger.warn(s"Aborted test suite '${event.suiteName}'.")
    case event: ScopeOpened => logger.info(s"Entering '${event.message}'")
    case event: ScopeClosed => logger.info(s"Leaving '${event.message}'")
    case event: TestStarting => logger.info(s"Starting '${event.suiteName}/${event.testName}'...")
    case event: TestSucceeded =>
      logger.info(s"Test succeeded: '${event.suiteName}/${event.testName}'")
    case event: TestFailed => logger.warn(s"Test failed: '${event.suiteName}/${event.testName}'")
    case event: TestCanceled =>
      logger.info(s"Test canceled: '${event.suiteName}/${event.testName}'")
    case event: TestIgnored => logger.info(s"Test ignored: '${event.suiteName}/${event.testName}'")
    case _ =>
  }
}
