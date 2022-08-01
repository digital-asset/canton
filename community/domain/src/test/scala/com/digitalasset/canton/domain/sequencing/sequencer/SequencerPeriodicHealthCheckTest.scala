// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class SequencerPeriodicHealthCheckTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  private val interval = NonNegativeFiniteDuration.ofSeconds(5)
  private val health = Future.successful(SequencerHealthStatus(isActive = true))

  class Env() {
    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
    var numberOfCalls = 0

    private val flagCloseable: FlagCloseable = new FlagCloseable {
      override protected def timeouts: ProcessingTimeout = ProcessingTimeout()
      override protected def logger: TracedLogger = SequencerPeriodicHealthCheckTest.this.logger
    }
    private implicit val closeContext = CloseContext(flagCloseable)

    def close(): Unit = flagCloseable.close()

    new SequencerPeriodicHealthCheck(
      clock,
      interval,
      loggerFactory,
    )(_ => {
      numberOfCalls += 1
      health
    })
  }

  "should call health function periodically" in {
    val env = new Env()
    import env._

    numberOfCalls shouldBe 0

    clock.advance(interval.duration)
    numberOfCalls shouldBe 1
  }

  "should not call health function after we are closed" in {
    val env = new Env()
    import env._

    numberOfCalls shouldBe 0

    close()

    clock.advance(interval.duration.multipliedBy(2L))
    numberOfCalls shouldBe 0
  }
}
