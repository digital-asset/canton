// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Promise}

class JournalGarbageCollectorTest extends BaseTestWordSpec with HasExecutionContext {

  private class TestScheduler extends JournalGarbageCollector.Scheduler() {

    val runningPromise = new AtomicReference[Option[Promise[Unit]]](None)
    override def timeouts: ProcessingTimeout = JournalGarbageCollectorTest.this.timeouts

    override protected def run()(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Unit] = {
      val ret = Promise[Unit]()
      runningPromise.getAndSet(Some(ret)) match {
        case Some(_value) => fail("should not be running")
        case None =>
          FutureUnlessShutdown.outcomeF(ret.future.map { _ =>
            runningPromise.set(None)
          })
      }
    }

    override protected implicit def executionContext: ExecutionContext =
      JournalGarbageCollectorTest.this.directExecutionContext

    override protected def loggerFactory: NamedLoggerFactory =
      JournalGarbageCollectorTest.this.loggerFactory
  }

  "journal cleaning" should {
    "rerun if scheduled while running" in {
      val t = new TestScheduler()
      t.flush(TraceContext.empty)
      val promise = t.runningPromise.get()
      promise should not be None
      // flush again
      t.flush(TraceContext.empty)
      // and flush again (multiple)
      t.flush(TraceContext.empty)
      // complete previous promise
      promise.value.success(())
      // eventually, the second flush should have run
      eventually() {
        val cur = t.runningPromise.get()
        // should be next run, not the same one
        cur should not be promise
        cur should not be empty
      }
      // shut down in background
      t.runningPromise.get().value.success(())
    }
  }
}
