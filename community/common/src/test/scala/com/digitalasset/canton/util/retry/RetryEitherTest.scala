// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import scala.collection.mutable

class RetryEitherTest extends AnyWordSpec with BaseTest {

  private def newCloseable(): FlagCloseable =
    FlagCloseable(logger, DefaultProcessingTimeouts.testing)

  /** Body that fails `failures` times and records the time of each attempt. */
  private class CountingBody(failures: Int) {
    val attemptsAtNanos: mutable.ArrayBuffer[Long] = mutable.ArrayBuffer.empty
    def apply(): Either[String, Int] = {
      attemptsAtNanos += System.nanoTime()
      if (attemptsAtNanos.sizeIs <= failures) Left(s"failure ${attemptsAtNanos.size}")
      else Right(attemptsAtNanos.size)
    }
  }

  "RetryEither.retryWithBackoff" should {

    "return the first success" in {
      implicit val closeContext: CloseContext = CloseContext(newCloseable())
      val body = new CountingBody(failures = 3)
      val result = RetryEither
        .retryWithBackoff[String, Int](
          maxRetries = 10,
          initialWaitInMs = 1,
          maxWaitInMs = 50,
          "test",
          retryLogLevel = Level.DEBUG,
        )(body())
        .value

      result shouldBe Outcome(Right(4))
      body.attemptsAtNanos should have size 4
    }

    "double the wait between attempts up to the maximum" in {
      implicit val closeContext: CloseContext = CloseContext(newCloseable())
      val body = new CountingBody(failures = 6)
      RetryEither
        .retryWithBackoff[String, Int](
          maxRetries = 100,
          initialWaitInMs = 5,
          maxWaitInMs = 20,
          "test",
          retryLogLevel = Level.DEBUG,
        )(body())
        .value shouldBe Outcome(Right(7))

      val attempts: Seq[Long] = body.attemptsAtNanos.toSeq
      val gapsInMs: Seq[Long] =
        attempts.zip(attempts.drop(1)).map { case (a, b) => (b - a) / 1000000L }
      // Planned waits: 5, 10, 20, 20, 20, 20. Each gap includes a sleep of the planned length, so it
      // is at least that long (minus 1 ms for clock granularity). Upper bounds are not checked because
      // the scheduler may delay any attempt.
      gapsInMs should have size 6
      gapsInMs.zip(Seq(5L, 10L, 20L, 20L, 20L, 20L)).foreach { case (gap, planned) =>
        gap should be >= (planned - 1)
      }
    }

    "give up once the waits use up maxRetries * maxWaitInMs" in {
      implicit val closeContext: CloseContext = CloseContext(newCloseable())
      val body = new CountingBody(failures = Int.MaxValue)
      val result = loggerFactory.assertLogs(
        RetryEither
          .retryWithBackoff[String, Int](
            maxRetries = 2,
            initialWaitInMs = 10,
            maxWaitInMs = 40,
            "test",
            retryLogLevel = Level.DEBUG,
          )(body())
          .value,
        _.warningMessage should include("exhausted retries"),
      )

      result shouldBe Outcome(Left("failure 5"))
      // Budget 2 * 40 = 80ms: waits 10, 20, 40, 10 (the remainder), then one last attempt.
      body.attemptsAtNanos should have size 5
    }

    "stop on a Left matching stopOnLeft" in {
      implicit val closeContext: CloseContext = CloseContext(newCloseable())
      val body = new CountingBody(failures = Int.MaxValue)
      val result = loggerFactory.assertLogs(
        RetryEither
          .retryWithBackoff[String, Int](
            maxRetries = Int.MaxValue,
            initialWaitInMs = 1,
            maxWaitInMs = 10,
            "test",
            stopOnLeft = Some(_ == "failure 3"),
            retryLogLevel = Level.DEBUG,
          )(body())
          .value,
        _.warningMessage should include("stopping retries"),
      )

      result shouldBe Outcome(Left("failure 3"))
      body.attemptsAtNanos should have size 3
    }

    "abort when the close context is closing" in {
      val closeable = newCloseable()
      implicit val closeContext: CloseContext = CloseContext(closeable)
      closeable.close()
      val body = new CountingBody(failures = Int.MaxValue)
      RetryEither
        .retryWithBackoff[String, Int](
          maxRetries = Int.MaxValue,
          initialWaitInMs = 1,
          maxWaitInMs = 10,
          "test",
        )(body())
        .value shouldBe AbortedDueToShutdown
      body.attemptsAtNanos shouldBe empty
    }

    "reject an initial wait above the maximum" in {
      implicit val closeContext: CloseContext = CloseContext(newCloseable())
      an[IllegalArgumentException] should be thrownBy
        RetryEither.retryWithBackoff[String, Int](
          maxRetries = 1,
          initialWaitInMs = 50,
          maxWaitInMs = 10,
          "test",
        )(Right(1))
    }
  }
}
