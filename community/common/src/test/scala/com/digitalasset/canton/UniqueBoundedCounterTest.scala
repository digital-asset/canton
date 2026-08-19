// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.File
import cats.syntax.parallel.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.util.FutureInstances.*
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class UniqueBoundedCounterTest
    extends AnyFlatSpec
    with BaseTest
    with BeforeAndAfterEach
    with HasExecutionContext {

  private var dataFile: File = _
  private var lockFile: File = _
  private val testLogger = logger.underlying

  override def beforeEach(): Unit = {
    dataFile = File.newTemporaryFile("unique_bounded_counter_test", ".dat")
    lockFile = File(dataFile.pathAsString + ".lock")
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    dataFile.delete(swallowIOExceptions = true)
    lockFile.delete(swallowIOExceptions = true)
    super.afterEach()
  }

  behavior of "UniqueBoundedCounter"

  it should "initialize counter to the start value if the file is new" in {
    val initial = 50
    val counter = new UniqueBoundedCounter(
      dataFile,
      startValue = initial,
      maxValue = 100,
    )(testLogger)

    val result = counter.get()
    result.success.value should be(initial)
  }

  it should "use the existing value if the file already exists" in {
    val existingValue = 124
    val maxValue = 200

    // Purely simulate an existing file created by another process
    dataFile.overwrite(existingValue.toString)(charset = StandardCharsets.UTF_8)

    // Create a new instance pointing to the same file, with a different startValue
    val counter =
      new UniqueBoundedCounter(dataFile, startValue = 166, maxValue = maxValue)(
        testLogger
      )

    // Get should return the value written to the file, completely ignoring startValue (166)
    counter.get().success.value should be(existingValue)

    // Increment should work from the existing value
    counter.incrementAndGet().success.value should be(existingValue + 1)
  }

  it should "increment correctly" in {
    val initial = 10
    val counter = new UniqueBoundedCounter(
      dataFile,
      startValue = initial,
      maxValue = 100,
    )(testLogger)
    counter.incrementAndGet().success.value should be(initial + 1)
    counter.incrementAndGet().success.value should be(initial + 2)
    counter.get().success.value should be(initial + 2)
  }

  it should "add a delta and wrap around correctly if the max is exceeded" in {
    val initial = 10
    val counter = new UniqueBoundedCounter(
      dataFile,
      startValue = initial,
      maxValue = 100,
    )(testLogger)

    counter.addAndGet(50).success.value should be(initial + 50)
    // 60 + 50 = 110. Since 110 > maxValue (100), it should hard-reset to startValue (10)
    counter.addAndGet(50).success.value should be(initial)
  }

  it should "wrap around correctly when maximum value is reached" in {
    val initial = 2
    val maxVal = 5
    val counter = new UniqueBoundedCounter(
      dataFile,
      startValue = initial,
      maxValue = maxVal,
    )(testLogger)

    counter.get().success.value should be(initial)
    counter.incrementAndGet().success.value should be(initial + 1)
    counter.incrementAndGet().success.value should be(initial + 2)
    counter.incrementAndGet().success.value should be(maxVal)
    counter.incrementAndGet().success.value should be(initial)
  }

  it should "generate unique counters concurrently without lock exceptions" in {
    val numThreads = Threading.detectNumberOfThreads(noTracingLogger).unwrap

    // 200 per thread should be plenty to trigger race conditions on the file lock.
    // (Higher values (e.g., 10,000) turn this test into a load rather than a concurrency test! And cause timeouts!)
    val incrementsPerThread = 200

    val totalIncrements = numThreads * incrementsPerThread
    val startValue = 1000
    // Use Int.MaxValue to effectively prevent wrap-around during this concurrency test.
    val counter = new UniqueBoundedCounter(dataFile, startValue, Int.MaxValue)(testLogger)

    val obtainedValues = ConcurrentHashMap.newKeySet[Int]()

    // Uses parTraverse instead of MonadUtil.parTraverseWithLimit because the collection
    // size is already naturally bounded by the configured execution context. We actively
    // want all workers to execute simultaneously to create the high contention needed to
    // properly test the file lock.
    val futures = (1 to numThreads).toList.parTraverse { threadId =>
      Future {
        for (i <- 1 to incrementsPerThread) {
          withClue(s"worker-$threadId on increment $i: ") {
            val value = counter.incrementAndGet().success.value

            // Attempt to add the obtained value. add() returns false if it was already present.
            if (!obtainedValues.add(value)) {
              fail(s"Duplicate value detected: $value")
            }
          }
        }
      }
    }

    // Wait long enough; may run slower when run with other tests concurrently.
    Await.result(futures, 5.minutes)

    obtainedValues.size() should be(totalIncrements)
    counter.get().success.value should be(startValue + totalIncrements)
  }
}
