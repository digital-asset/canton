// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{PromiseUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.Success

class PeriodicActionTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  private val interval = NonNegativeFiniteDuration.tryOfSeconds(5)

  class Env(createPromise: () => PromiseUnlessShutdown[Unit] = () => PromiseUnlessShutdown.unit) {

    val promiseRef: AtomicReference[PromiseUnlessShutdown[Unit]] = new AtomicReference(
      PromiseUnlessShutdown.unit
    )
    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
    val numberOfCalls = new AtomicInteger(0)

    val sut = new PeriodicAction(
      clock,
      interval,
      loggerFactory,
      ProcessingTimeout(),
      "test",
    )(_ => {
      numberOfCalls.incrementAndGet()
      promiseRef.set(createPromise())
      promiseRef.get().futureUS
    })

    // Sometimes we need to make sure that the asynchronous scheduling of the next task has happened (in real time)
    def eventuallyOneTaskIsScheduled(): Assertion =
      eventually()(clock.numberOfScheduledTasks shouldBe 1)
  }

  "should call function periodically" in {
    val env = new Env()
    import env.*

    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 0

    clock.advance(interval.duration)
    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 1

    clock.advance(interval.duration)
    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 2

    clock.advance(interval.duration)
    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 3

    sut.close()

    clock.advance(interval.duration)
    numberOfCalls.get shouldBe 3
    clock.advance(interval.duration)
    numberOfCalls.get shouldBe 3
  }

  "should not call function after we are closed" in {
    val env = new Env()
    import env.*

    numberOfCalls.get shouldBe 0

    sut.close()

    clock.advance(interval.duration.multipliedBy(2L))
    numberOfCalls.get shouldBe 0
  }

  "should wait in close for running action to finish" in {
    val env = new Env(() => PromiseUnlessShutdown.unsupervised())
    import env.*

    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 0
    clock.advance(interval.duration)
    numberOfCalls.get shouldBe 1

    // The call to close will block until the running action is finished, so we run it in a separate thread
    val closingFuture = Future(sut.close())

    // sleep in real time too, so the closingFuture has a chance to complete on a separate thread
    Threading.sleep(200)
    closingFuture.isCompleted shouldBe false

    promiseRef.get().complete(Success(UnlessShutdown.Outcome(())))
    Await.ready(closingFuture, 5.seconds).discard

    succeed
  }

  "should cancel any scheduled jobs on close" in {
    val env = new Env()
    import env.*

    eventuallyOneTaskIsScheduled()
    sut.close()
    clock.numberOfScheduledTasks shouldBe 0
    numberOfCalls.get shouldBe 0
  }

}
