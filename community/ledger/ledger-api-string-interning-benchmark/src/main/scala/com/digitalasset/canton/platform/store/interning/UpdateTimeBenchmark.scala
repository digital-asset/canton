// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.daml.logging.LoggingContext
import org.openjdk.jmh.annotations.*

import scala.concurrent.Await

class UpdateTimeBenchmark extends BenchmarkState {
  // Set up some extra entries for the repeated update() calls.
  // Give a large number so that not all of the strings can be ingested
  override def extraStringCount = 15000000

  @Setup(Level.Iteration)
  def setupIteration(): Unit = {
    interning = new StringInterningView()

    interningEnd = stringCount
    Await.result(
      interning.update(interningEnd)(BenchmarkState.loadStringInterningEntries(entries))(
        LoggingContext.ForTesting
      ),
      perfTestTimeout,
    )
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @Fork(value = 5)
  @Warmup(iterations = 5)
  @Measurement(iterations = 5)
  def run(): Unit = {
    interningEnd = interningEnd + 1
    if (interningEnd > entries.length) throw new RuntimeException("Can't ingest any more strings")

    Await.result(
      interning.update(interningEnd)(BenchmarkState.loadStringInterningEntries(entries))(
        LoggingContext.ForTesting
      ),
      perfTestTimeout,
    )
  }
}
