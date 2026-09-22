// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.platform.indexer.parallel.BatchingParallelIngestionPipeSpec.{
  BatchedItem,
  SequencedItem,
}
import com.digitalasset.canton.util.Mutex
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, Semaphore}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}

class BatchingParallelIngestionPipeSpec
    extends AsyncFlatSpec
    with Matchers
    with OptionValues
    with PekkoBeforeAndAfterAll {

  // AsyncFlatSpec is with serial execution context
  private implicit val ec: ExecutionContext = system.dispatcher

  private val input = Iterator.continually(util.Random.nextInt()).take(1000).toList
  private val MaxBatchSize = 300
  private val MaxTailerBatchSize = 4

  it should "end the stream successfully in a happy path case" in {
    runPipe().map { case (ingested, ingestedTail, err) =>
      err shouldBe empty

      ingested.sortBy(_.index) shouldBe input.map(_.toString).zipWithIndex.map { case (s, i) =>
        BatchedItem(index = i + 1, value = s)
      }
      ingestedTail.last shouldBe 1000
    }
  }

  // Parameterized tests for component error handling
  Seq(
    ("input mapper", (ex: Exception) => runPipe(inputMapperHook = () => throw ex)),
    ("seqMapper", (ex: Exception) => runPipe(seqMapperHook = () => throw ex)),
    ("batcher", (ex: Exception) => runPipe(batcherHook = () => throw ex)),
    ("ingester", (ex: Exception) => runPipe(ingesterHook = _ => throw ex)),
    ("ingestTail", (ex: Exception) => runPipe(ingestTailHook = _ => throw ex)),
  ).foreach { case (component, runWithError) =>
    it should s"terminate the stream upon error in $component" in {
      val ex = new Exception(s"$component failed")
      runWithError(ex).map { case (_, _, err) =>
        err.value.getMessage shouldBe ex.getMessage
      }
    }
  }

  /*
   * Ensures that if a downstream stage hangs indefinitely, the stream respects the configured idle timeout.
   *
   * Deterministic approach:
   * Because `ingestingParallelism` is 2 (default), the `ingesterHook` can run concurrently for multiple batches.
   * If we start the timeout the moment batch 201 arrives, it might race with the preceding batch (1-200)
   * which may still be processing or in transit to `ingestTail`. We must use a handshake to ensure
   * `ingestTail` has fully processed all pre-201 elements BEFORE we arm the timeout clock.
   */
  it should "hold the stream if a single ingestion takes too long and timeouts" in {
    val ingestedTailAcc = new AtomicInteger(0)
    val tailSeenAcc = new AtomicInteger(0)
    val waitingForTail = new AtomicBoolean(false)
    val blockLatch = new CountDownLatch(1)
    val startTimeout = Promise[Unit]()

    runPipe(
      ingesterHook = batch => {
        val max = batch.map(_.index).max
        if (max < 201) {
          ingestedTailAcc.accumulateAndGet(max, _ max _)
        }
        if (batch.exists(_.index == 201)) {
          // Derive the preceding index from this batch itself to prevent mapAsync out-of-order execution bugs.
          // Publish the required tail index before allowing postIngestTailHook to arm the timeout.
          val expected = batch.head.index - 1
          ingestedTailAcc.accumulateAndGet(expected, _ max _)
          waitingForTail.set(true)

          // expected == 0 handles the edge case where 201 is in the very first batch, meaning there is no prior work to wait for
          if (expected == 0 || tailSeenAcc.get() >= expected) {
            startTimeout.trySuccess(())
          }
          blocking(blockLatch.await())
        }
      },
      postIngestTailHook = batchOfBatches => {
        val max = batchOfBatches.last.last.index
        val current = tailSeenAcc.accumulateAndGet(max, _ max _)
        // If the ingester is waiting and we just fulfilled the prior work requirement, trigger the timeout
        // This runs AFTER the state has been successfully appended to `ingestedTail`
        if (waitingForTail.get() && current >= ingestedTailAcc.get()) {
          startTimeout.trySuccess(())
        }
      },
      timeout = 100.millis,
      startTimeout = startTimeout.future,
    )
      .transform { result =>
        // Cleanly unblock the ingester thread the moment the test naturally concludes (via the timeout)
        blockLatch.countDown()
        result
      }
      .map { case (ingested, ingestedTail, err) =>
        err.value.getMessage shouldBe "timed out"
        // worst case: 200 elements will be ingested before 201
        // + 300 elements in the batch that does not contain it and is back-pressured by the batch containing 201
        ingested.size should be <= 500
        ingestedTail.last should be < 201
        ingestedTail.last shouldBe ingestedTailAcc.get()
      }
  }

  /*
   * Verifies that backpressure correctly causes the BatchN operator to accumulate elements up to its maximum capacity.
   *
   * Deterministic pipeline math:
   * 1. The batcher groups elements into lists of up to `MaxBatchSize` (300).
   * 2. The immediate downstream stage is `mapAsync(inputMappingParallelism = 2)`.
   * 3. Pekko Stream's `mapAsync(2)` eagerly pulls exactly 2 elements (in this case, 2 batches)
   *    into its internal execution buffer to process them concurrently.
   * 4. Therefore, the total capacity of the blocked boundary is:
   *    2 parallel lanes * 300 elements per batch = 600 total elements.
   *
   * By blocking the `inputMapperHook` (all lanes) and pushing exactly 600 elements,
   * we perfectly fill the `mapAsync` internal buffers without deadlocking the Source.
   *
   * Note on Strictness: In theory, we should be able to strictly assert that it formed
   * two perfectly max-sized batches (300 == 300). However, in reality, when the latch
   * unblocks downstream at exactly 600 elements, 1 or 2 elements might still be micro-seconds
   * away in Pekko's upstream `.async` transit buffers. The Batcher immediately emits what it
   * physically holds (e.g., 299) to satisfy downstream demand. Therefore, we use a tight
   * lower bound (`>= MaxBatchSize - 5`) rather than strict equality (`== 300`). The tight
   * bound absorbs this async reality while still proving massive backpressure.
   */
  it should "form max-sized batches when back-pressured by downstream" in {
    val batchSizes = ArrayBuffer.empty[Int]
    val batchSizesMutex = Mutex()
    val pushed = new AtomicInteger(0)
    val backpressureLatch = new CountDownLatch(1)
    val mapperStarted = new CountDownLatch(1)

    runPipe(
      inputMapperHook = () => {
        // Block all lanes of the downstream consumer to ensure total backpressure
        mapperStarted.countDown()
        blocking(backpressureLatch.await())
      },
      ingesterHook = batch => {
        batchSizesMutex.exclusive {
          batchSizes.addOne(batch.size)
        }
      },
      inputSource = Source(input)
        .take(700L) // Push beyond capacity to observe strict max-batching on release
        .map { x =>
          val current = pushed.incrementAndGet()
          if (current > 1) {
            // Gate upstream until the first hook actually starts blocking
            blocking(mapperStarted.await())
          }
          // Exact capacity hit: unblock the pipeline to observe the split chunks
          if (current == 600) {
            backpressureLatch.countDown()
          }
          x
        }
        .async,
    )
      .transform { result =>
        // Clean up locks in case of an early stream failure
        mapperStarted.countDown()
        backpressureLatch.countDown()
        result
      }
      .map { case (_, _, err) =>
        val measurementBatchSizes = batchSizesMutex.exclusive {
          batchSizes.toList
        }

        measurementBatchSizes should not be empty
        // A tight lower bound proves massive backpressure occurred,
        // while allowing 1-5 elements to be caught in upstream async transit buffers when the latch releases.
        measurementBatchSizes.max should be >= (MaxBatchSize - 5)
        err shouldBe empty
      }
  }

  /*
   * Verifies that when downstream is fast and upstream is slow, elements pass through quickly in small batches
   * rather than waiting to fill MaxBatchSize.
   */
  it should "form small batch sizes under no load" in {
    val allowNext = new Semaphore(1)

    runPipe(
      ingesterHook = batch => {
        batch.size should be <= 2
        // Release exact number of permits consumed to keep the pipeline completely starved
        allowNext.release(batch.size)
      },
      inputSource = Source(input)
        .take(n = 10L)
        .map { x =>
          // Slow down source to ensure downstream completes processing before next element arrives
          blocking(allowNext.acquire())
          x
        }
        .async,
    )
      .transform { result =>
        // Ensure parked source thread is released if the stream fails early
        allowNext.release(1000)
        result
      }
      .map { case (ingested, _, err) =>
        err shouldBe empty
        ingested.size shouldBe 10
      }
  }

  /*
   * Tests the secondary batching stage (`ingestTail`). We want to ensure it forms large batches of batches
   * when the final database insertion is slow.
   *
   * Deterministic approach:
   * 1. Set submissionBatchSize = 1 so BatchN emits batches of 1, isolating the tailer.
   * 2. Block the tailer (ingestTailHook) and signal tailStarted.
   * 3. MaxTailerBatchSize is 4. mapAsync(ingestingParallelism = 2) can hold 2 elements in flight.
   *    If the 7th element starts ingesterHook, at least 5 must have been passed downstream
   *    (1 in ingestTail, 4 in the tailer's .batch buffer). This guarantees the tailer's batch
   *    is maximally full before releasing the latch.
   */
  it should "form big batch sizes of batches before ingestTail under load" in {
    val batchSizes = ArrayBuffer.empty[Int]
    val batchSizesMutex = Mutex()
    val tailLatch = new CountDownLatch(1)
    val tailStarted = new CountDownLatch(1)
    val elementsReachedIngester = new AtomicInteger(0)
    val pushed = new AtomicInteger(0)
    val sourcePermits = new Semaphore(1)

    runPipe(
      submissionBatchSize = 1, // Force BatchN out of the equation
      inputMappingParallelism = 1,
      ingesterHook = batch => {
        // Count elements directly at the input to the tail batch
        if (elementsReachedIngester.addAndGet(batch.size) >= 7) {
          tailLatch.countDown()
        }
        // Let the source push the next element
        sourcePermits.release()
      },
      ingestTailHook = batchOfBatches => {
        // Block the tail consumer to ensure total backpressure
        tailStarted.countDown()
        blocking(tailLatch.await())
        batchSizesMutex.exclusive {
          batchSizes.addOne(batchOfBatches.size)
        }
      },
      inputSource = Source(input)
        .take(12L)
        .map { x =>
          if (pushed.incrementAndGet() > 1) {
            // Gate upstream until the first hook actually starts blocking
            blocking(tailStarted.await())
          }
          blocking(sourcePermits.acquire())
          x
        }
        .async,
    )
      .transform { result =>
        // Clean up locks in case of an early stream failure
        tailStarted.countDown()
        tailLatch.countDown()
        sourcePermits.release(1000)
        result
      }
      .map { case (_, _, err) =>
        val measurementBatchSizes = batchSizesMutex.exclusive {
          batchSizes.toList
        }

        measurementBatchSizes should not be empty
        // Tolerate 1 element caught in upstream async transit buffers when the latch releases
        measurementBatchSizes.max should be >= (MaxTailerBatchSize - 1)
        err shouldBe empty
      }
  }

  /*
   * Verifies that the tailer doesn't unnecessarily wait for `MaxTailerBatchSize` if the upstream is slow.
   * We pace the upstream based on what the tailer just finished consuming, ensuring zero backpressure.
   */
  it should "form small batch sizes of batches before ingestTail under no load" in {
    val batchSizes = ArrayBuffer.empty[Int]
    val batchSizesMutex = Mutex()
    val allowNext = new Semaphore(1)

    runPipe(
      submissionBatchSize = 1, // Force BatchN out of the equation
      ingestTailHook = batchOfBatches => {
        batchSizesMutex.exclusive {
          batchSizes.addOne(batchOfBatches.size)
        }
        val totalElementsInBatch = batchOfBatches.map(_.size).sum
        // Unblock exactly enough upstream permits to process what we just consumed
        allowNext.release(totalElementsInBatch)
      },
      inputSource = Source(input)
        .take(n = 100L)
        .map { x =>
          // Slow down source to ensure ingestTail is faster
          blocking(allowNext.acquire())
          x
        }
        .async,
    )
      .transform { result =>
        // Ensure parked source thread is released if the stream fails early
        allowNext.release(1000)
        result
      }
      .map { case (_, _, err) =>
        val measurementBatchSizes = batchSizesMutex.exclusive {
          batchSizes.toList
        }
        measurementBatchSizes should not be empty
        measurementBatchSizes.sum.toDouble / measurementBatchSizes.size should be < (MaxTailerBatchSize.toDouble * 0.3)
        err shouldBe empty
      }
  }

  def runPipe(
      inputMapperHook: () => Unit = () => (),
      seqMapperHook: () => Unit = () => (),
      batcherHook: () => Unit = () => (),
      ingesterHook: List[BatchedItem] => Unit = _ => (),
      ingestTailHook: Vector[List[BatchedItem]] => Unit = _ => (),
      postIngestTailHook: Vector[List[BatchedItem]] => Unit = _ => (),
      timeout: FiniteDuration = 10.seconds,
      startTimeout: Future[Unit] = Future.successful(()),
      inputSource: Source[Int, NotUsed] = Source(input),
      submissionBatchSize: Int = MaxBatchSize,
      inputMappingParallelism: Int = 2,
  ): Future[(Vector[BatchedItem], Vector[Int], Option[Throwable])] = {

    val stateMutex = Mutex()
    var ingested: Vector[BatchedItem] = Vector.empty
    var ingestedTail: Vector[Int] = Vector.empty

    val indexingFlow =
      BatchingParallelIngestionPipe[Int, List[SequencedItem], List[BatchedItem]](
        submissionBatchSize = submissionBatchSize.toLong,
        inputMappingParallelism = inputMappingParallelism,
        contractReInsertion = Future.successful,
        inputMapper = ins =>
          Future {
            inputMapperHook()
            ins.map(in => SequencedItem(index = 0, value = in)).toList
          },
        seqMapperZero = List(SequencedItem(index = 0, value = 0)),
        seqMapper = (prev, current) => {
          seqMapperHook()
          val lastIndex = prev.last.index
          current.zipWithIndex.map { case (item, index) =>
            item.copy(index = index + lastIndex + 1)
          }
        },
        dbPrepareParallelism = 2,
        dbPrepare = inBatch => Future.successful(inBatch),
        batchingParallelism = 2,
        batcher = inBatch =>
          Future {
            batcherHook()
            inBatch.map { item =>
              BatchedItem(index = item.index, value = item.value.toString)
            }
          },
        ingestingParallelism = 2,
        ingester = dbBatch =>
          Future {
            ingesterHook(dbBatch)
            stateMutex.exclusive {
              ingested = ingested ++ dbBatch
            }
            dbBatch
          },
        maxTailerBatchSize = MaxTailerBatchSize,
        ingestTail = dbBatch =>
          Future {
            ingestTailHook(dbBatch)
            stateMutex.exclusive {
              ingestedTail = ingestedTail :+ dbBatch.last.last.index
            }
            postIngestTailHook(dbBatch)
            dbBatch
          },
      )

    val p = Promise[(Vector[BatchedItem], Vector[Int], Option[Throwable])]()

    // Defer the start of the intended timeout mechanism until explicitly signaled by the handshake
    val timeoutF = startTimeout.flatMap(_ =>
      org.apache.pekko.pattern.after(timeout, system.scheduler) {
        Future.failed(new Exception("timed out"))
      }
    )

    val indexingF = inputSource
      .via(indexingFlow)
      .idleTimeout(10.seconds) // Stream-native deadlock watchdog prevents test hangs if hooks fail
      .run()
      .map { _ =>
        stateMutex.exclusive((ingested, ingestedTail, Option.empty[Throwable]))
      }

    timeoutF.onComplete(p.tryComplete)
    indexingF.onComplete(p.tryComplete)

    p.future.recover { case t =>
      stateMutex.exclusive((ingested, ingestedTail, Some(t)))
    }
  }
}

object BatchingParallelIngestionPipeSpec {
  final case class SequencedItem(index: Int, value: Int)
  final case class BatchedItem(index: Int, value: String)
}
