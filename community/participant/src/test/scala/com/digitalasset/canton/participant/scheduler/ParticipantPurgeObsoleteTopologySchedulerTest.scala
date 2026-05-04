// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.scheduler

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.scheduler.IndividualSchedule
import com.digitalasset.canton.scheduler.JobSchedule.NextRun
import com.digitalasset.canton.scheduler.JobScheduler.{Done, MoreWorkToPerform, ScheduledRunResult}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.store.DummyCopyTopologyStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import org.scalatest.Assertion

import java.util.concurrent.LinkedBlockingQueue

/** This test sets up a scheduler which will delete chunks from two obsolete stores. We use fakes so
  * that we can do assertions on the last result and in between runs.
  */
class ParticipantPurgeObsoleteTopologySchedulerTest
    extends BaseTestWordSpec
    with HasExecutionContext {

  /** Fake schedule for which each call to `determineNextRun` will wait for the test to call `step`,
    * so that we can make assertions on the last result and the state of the system in between runs.
    */
  class SteppingSchedule extends IndividualSchedule {
    val gotResults = new LinkedBlockingQueue[ScheduledRunResult](1)
    val nextRuns = new LinkedBlockingQueue[Option[NextRun]](1)

    override def determineNextRun(result: ScheduledRunResult)(implicit
        traceContext: TraceContext
    ): Option[NextRun] = {
      gotResults.put(result) // pass control to test thread
      nextRuns.take() // receive control back from the test thread
    }

    // Runs some assertions and allows the next run to proceed
    def step(assertions: ScheduledRunResult => Assertion): Unit = {
      val gotResult = gotResults.take()
      // At this point the last scheduler job should have run, and we can make assertions about what should have happened.
      assertions(gotResult)
      // Allow the cycle to loop
      nextRuns.put(Some(NextRun(NonNegativeFiniteDuration.Zero, this)))
    }
  }

  /** Fake topology store, defining deleteDataChunk to decrement the `size` to a minimum of 0, and
    * return whether the size ended up being decremented.
    */
  case class SizeTopologyStore(startingSize: Int)
      extends DummyCopyTopologyStore(None, loggerFactory) {
    var size = startingSize

    override def deleteDataChunk(chunkSize: PositiveInt)(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Boolean] = FutureUnlessShutdown.pure {
      if (size == 0) false
      else {
        size = (size - chunkSize.value) max 0
        true
      }
    }
  }

  "A ParticipantCleanObsoleteTopologyScheduler" should {
    "delete a chunk from each obsolete store until done" in {
      val store1 = SizeTopologyStore(startingSize = 3)
      val store2 = SizeTopologyStore(startingSize = 1)

      val schedule = new SteppingSchedule()

      val scheduler = new ParticipantPurgeObsoleteTopologyScheduler(
        schedule = Some(schedule),
        getObsoleteTopologyStores = () => FutureUnlessShutdown.pure(Seq(store1, store2)),
        chunkSize = PositiveInt.tryCreate(2),
        timeouts,
        loggerFactory,
      )

      val f = scheduler.start()

      // On startup it assumes there's work to perform. Before it proceeds, no state has changed.
      schedule.step { result =>
        result shouldBe MoreWorkToPerform
        store1.size shouldBe 3
        store2.size shouldBe 1
      }
      // The previous run deleted from both stores, so it assumes there's more work to do
      schedule.step { result =>
        result shouldBe MoreWorkToPerform
        store1.size shouldBe 1
        store2.size shouldBe 0
      }
      // The previous run deleted from store1, so assumes there's more work to do
      schedule.step { result =>
        result shouldBe MoreWorkToPerform
        store1.size shouldBe 0
        store2.size shouldBe 0
      }
      // The previous run did not delete anything (both stores were already empty), so report that we're done
      schedule.step(_ shouldBe Done)

      // Cleanup
      scheduler.stop()
      f.futureValue
    }
  }
}
