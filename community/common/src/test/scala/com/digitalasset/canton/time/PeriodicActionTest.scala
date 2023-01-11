// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class PeriodicActionTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  private val interval = NonNegativeFiniteDuration.ofSeconds(5)
  private val result = Future.successful(())

  class Env() {
    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
    var numberOfCalls = 0

    val sut = new PeriodicAction(
      clock,
      interval,
      loggerFactory,
      ProcessingTimeout(),
      "test",
    )(_ => {
      numberOfCalls += 1
      result
    })
  }

  "should call function periodically" in {
    val env = new Env()
    import env.*

    numberOfCalls shouldBe 0

    clock.advance(interval.duration)
    numberOfCalls shouldBe 1
  }

  "should not call function after we are closed" in {
    val env = new Env()
    import env.*

    numberOfCalls shouldBe 0

    sut.close()

    clock.advance(interval.duration.multipliedBy(2L))
    numberOfCalls shouldBe 0
  }
}
