// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing

import ch.qos.logback.classic.Level
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

class TestingLogCollectorSpec
    extends AsyncFlatSpec
    with Matchers
    with LoggingAssertions
    with Eventually
    with IntegrationPatience {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(60, Seconds)),
    interval = scaled(Span(150, Millis)),
  )

  private val LoggerFactoryAccessParallelism = 16

  override implicit def executionContext: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newWorkStealingPool(LoggerFactoryAccessParallelism))

  behavior of classOf[TestingLogCollector].getSimpleName

  it should "allow asserting logs even under race conditions" in {
    Future
      .sequence(
        (1 to LoggerFactoryAccessParallelism) map { idx =>
          Future {
            val loggerName = s"${getClass.getName}.logger$idx"
            // Get the logger in here to ensure concurrency in the LoggerFactory initialization
            val logger = LoggerFactory.getLogger(loggerName)
            val message = s"Dummy logging $idx"
            // Log the message
            logger.info(message)
            // Assert that the message is visible
            assertLogEntries[this.type](loggerName)(
              _.view.map(entry => entry.level -> entry.msg) should contain(Level.INFO -> message)
            )
          }
        }
      )
      .map(_ => succeed)
  }
}
