// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.{Eventually, EventuallyException}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class EventuallySpec extends AsyncWordSpec with Matchers {

  "eventually" should {
    "enhance the throwable message with the assertion name" in {
      recoverToExceptionIf[EventuallyException] {
        Eventually.eventually(assertionName = "test", attempts = 1, firstWaitTime = 0.millis) {
          Future.failed(new RuntimeException())
        }
      }.map(_.getMessage() should startWith("test failed after 2 attempts and "))
    }

    "report the number of attempts and preserve the cause" in {
      val attempts = new AtomicInteger(0)
      recoverToExceptionIf[EventuallyException] {
        Eventually.eventually(assertionName = "measured", attempts = 1, firstWaitTime = 0.millis) {
          attempts.incrementAndGet()
          Future.failed(new RuntimeException("last observation"))
        }
      }.map { exception =>
        exception.attempts shouldBe 2
        exception.elapsedMillis should be >= 0L
        exception.getCause.getMessage shouldBe "last observation"
      }
    }
  }

  "maxDeadline" should {
    // 10, 20, 40 ... 2560 adds up to 5110ms, then nine more sleeps at the 3 second cap.
    "add up the sleeps and stop doubling at the cap" in {
      Eventually.maxDeadline(18, 10.millis, 3.seconds) shouldBe 32110.millis
    }
  }
}
