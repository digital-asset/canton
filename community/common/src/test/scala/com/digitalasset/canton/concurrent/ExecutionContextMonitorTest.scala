// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.logging.{LogEntry, SuppressingLogger, TracedLogger}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ExecutionContextMonitorTest extends AnyWordSpec with BaseTest {

  def runAndCheck(loggerFactory: SuppressingLogger, check: Seq[LogEntry] => Assertion): Unit = {
    val logger = TracedLogger(loggerFactory.getLogger(getClass))
    implicit val scheduler: ScheduledExecutorService =
      Threading.singleThreadScheduledExecutor(loggerFactory.threadName + "-test-scheduler", logger)

    val ecName = loggerFactory.threadName + "test-my-ec"
    implicit val ec = Threading.newExecutionContext(ecName, logger)
    val monitor =
      new ExecutionContextMonitor(
        loggerFactory,
        NonNegativeFiniteDuration.ofSeconds(1),
        5,
        true,
        DefaultProcessingTimeouts.testing,
      )
    monitor.monitor(ec)

    // As we are setting min num threads in fork join pool to 2, we also need to
    // set this to 2 here as otherwise this test becomes flaky when running in the
    // sequential test
    val numThreads = Math.max(2, Threading.detectNumberOfThreads(logger))

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        for (_ <- 1 to 2) {
          val futs = (1 to (numThreads * 2)).map(_ =>
            Future {
              logger.debug("Starting to block")
              Thread.sleep(2000)
              logger.debug("Stopping to block")
            }
          )
          Await.result(Future.sequence(futs), 120.seconds)
          monitor.close()
          scheduler.shutdown()
        }
      },
      check,
    )
  }

  "execution context monitor" should {

    "report nicely if futures are stuck" in {

      // Separate suppression logger that does not skip the warnings coming from the execution context monitor
      val loggerFactory = SuppressingLogger(getClass, skipLogEntry = _ => false)
      runAndCheck(
        loggerFactory,
        { seq =>
          seq.foreach { entry =>
            assert(
              entry.warningMessage.contains("is stuck or overloaded") || entry.warningMessage
                .contains("is just overloaded"),
              s"did not match expected warning messages: ${entry.toString}",
            )
          }
          seq should not be empty
        },
      )
    }

    "default SuppressingLogger skips its warning" in {
      runAndCheck(loggerFactory, seq => seq shouldBe empty)
    }
  }
}
