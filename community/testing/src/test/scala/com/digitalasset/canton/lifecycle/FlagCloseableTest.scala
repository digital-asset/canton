// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.*

class TestResource extends FlagCloseable with NamedLogging {
  override protected val timeouts: ProcessingTimeout = ProcessingTimeout()
  override val loggerFactory = NamedLoggerFactory.root
}

class FlagCloseableTest extends AnyWordSpec with Matchers with NoTracing {
  "FlagCloseable" should {
    "run shutdown tasks in order they were added" in {

      var shutdownTasks: Seq[String] = Seq.empty

      val closeable = new TestResource()
      closeable.runOnShutdown(new RunOnShutdown {
        override val name = "first"
        override val done = false
        override def run() = {
          shutdownTasks = shutdownTasks :+ "first"
        }
      })
      closeable.runOnShutdown(new RunOnShutdown {
        override val name = "second"
        override val done = false
        override def run() = {
          shutdownTasks = shutdownTasks :+ "second"
        }
      })
      closeable.close()

      shutdownTasks shouldBe Seq("first", "second")
    }
  }
}
