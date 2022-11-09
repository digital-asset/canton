// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.scalatest.prop.TableFor3

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Future
import scala.concurrent.duration.*

@SuppressWarnings(Array("org.wartremover.warts.While"))
class RateLimiterTest extends BaseTestWordSpec with HasExecutionContext {

  lazy val testCases: TableFor3[Int, Int, Long] = Table(
    ("maxTasksPerSecond", "maxBurstSize", "cycleMillis"),
    (1, 1, 1000),
    (9, 1, 111),
    (10, 1, 100),
    (100, 10, 100),
    (200, 20, 100),
  )

  "A rate limiter" when {
    testCases.forEvery { case (maxTasksPerSecond, maxBurstSize, cycleMillis) =>
      s"the maximum rate is $maxTasksPerSecond" must {
        s"allow for a maximum burst of size $maxBurstSize" in {
          val rateLimiter = new RateLimiter(NonNegativeInt.tryCreate(maxTasksPerSecond))
          val start = System.nanoTime()
          val results = (0 until maxBurstSize * 10).toList.parTraverse { _ =>
            Future { rateLimiter.checkAndUpdateRate() }
          }.futureValue
          val stop = System.nanoTime()
          // If the test took longer than a single cycle, we should expect to see several bursts.
          val allowedCycles = (stop - start) / (cycleMillis * 1000000) + 1
          results.count(identity).toLong should be <= (maxBurstSize * allowedCycles)
        }

        "maintain the target rate on average if executed sequentially" in {
          val duration = 5.seconds

          val (numSuccessful, _) = testRateLimiter(duration, 1, 0)

          numSuccessful should be <= (maxTasksPerSecond * duration.toSeconds + maxBurstSize)
          numSuccessful should be >= ((maxTasksPerSecond * duration.toSeconds * 8 / 10) max 1)
        }

        def testRateLimiter(
            duration: FiniteDuration,
            parallelism: Int,
            sleepMillis: Long,
        ): (Long, Long) = {
          val rateLimiter = new RateLimiter(NonNegativeInt.tryCreate(maxTasksPerSecond))
          val numSuccessful = new AtomicLong()
          val numFailures = new AtomicLong()
          val deadline = duration.fromNow
          while (deadline.hasTimeLeft()) {
            (0 until parallelism).toList.parTraverse_ { _ =>
              Future {
                if (rateLimiter.checkAndUpdateRate()) numSuccessful.incrementAndGet()
                else numFailures.incrementAndGet()
              }
            }.futureValue
            Threading.sleep(sleepMillis)
          }
          (numSuccessful.get(), numFailures.get())
        }

        "maintain the target rate on average if executed in parallel" in {
          val duration = 5.seconds

          val (numSuccessful, _) = testRateLimiter(duration, maxBurstSize * 2, 0)

          numSuccessful should be <= (maxTasksPerSecond * duration.toSeconds + maxBurstSize)
          numSuccessful should be >= ((maxTasksPerSecond * duration.toSeconds * 8 / 10) max 1)
        }

        "accept all tasks if tasks are submitted just below target rate" in {
          val duration = 5.seconds

          val (_, numFailures) = testRateLimiter(
            duration,
            maxBurstSize,
            cycleMillis + 10, // adding 10 millis to reliably trigger a new cycle
          )

          numFailures shouldBe 0
        }
      }
    }

    "the maximum rate is zero" should {
      "reject all tasks" in {
        val rateLimiter = new RateLimiter(NonNegativeInt.tryCreate(0))
        rateLimiter.checkAndUpdateRate() shouldBe false
      }
    }
  }
}
