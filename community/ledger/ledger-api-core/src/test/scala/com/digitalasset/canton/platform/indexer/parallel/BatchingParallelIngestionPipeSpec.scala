// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.platform.indexer.parallel.BatchingParallelIngestionPipeSpec.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{ExecutionContext, Future, Promise}

/** These tests do not depend on timing:
  *   - No stage sleeps or blocks a thread. A test holds back a stage by letting it wait for a
  *     `Promise` that the test completes later.
  *   - A test only waits for something to happen (`eventually`), never for something not to happen.
  *     Only the test thread waits, never a thread of the actor system.
  *   - Expected batch sizes follow from counting the elements that queue up in front of a held-back
  *     stage.
  */
class BatchingParallelIngestionPipeSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with PekkoBeforeAndAfterAll {

  // Only reached if the pipe is broken; the tests themselves take milliseconds.
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(5, Millis))

  private implicit val ec: ExecutionContext = system.dispatcher

  behavior of "BatchingParallelIngestionPipe"

  it should "pass all elements through all stages in order" in {
    val run = runPipe()

    run.output.futureValue.flatten shouldBe (1 to InputSize).map(i =>
      BatchedItem(index = i, value = i.toString)
    )
  }

  // Parameterized tests for component error handling
  Seq[(String, Exception => PipeRun)](
    ("contractReInsertion", ex => runPipe(contractReInsertionHook = _ => Future.failed(ex))),
    ("inputMapper", ex => runPipe(inputMapperHook = () => throw ex)),
    ("seqMapper", ex => runPipe(seqMapperHook = () => throw ex)),
    ("batcher", ex => runPipe(batcherHook = () => throw ex)),
    ("ingester", ex => runPipe(ingesterHook = _ => Future.failed(ex))),
    ("ingestTail", ex => runPipe(ingestTailHook = _ => Future.failed(ex))),
  ).foreach { case (stage, runFailing) =>
    it should s"fail the stream if $stage fails" in {
      val ex = new Exception(s"$stage failed")

      runFailing(ex).output.failed.futureValue shouldBe ex
    }
  }

  it should "not advance ingestTail past a batch that is still being ingested" in {
    val stuck = 5
    val stuckIngestion = Promise[Unit]()
    val run = runPipe(
      submissionBatchSize = 1, // one element per batch
      ingestionMaxBatchSize = 1, // one element per second batch
      ingesterHook = batch => if (batch.head.index == stuck) stuckIngestion.future else Future.unit,
    )
    def ingested = run.ingesterCalls.all.flatten.map(_.index)
    def tailed = run.ingestTailCalls.all.flatten.map(_.index)

    // Ingestion continues in parallel with the stuck element ...
    eventually(ingested should contain(stuck + 1))
    // ... and everything before the stuck element reaches ingestTail ...
    eventually(tailed should contain(stuck - 1))
    // ... but nothing after it.
    tailed.max shouldBe stuck - 1
    // The ingester does not run further ahead than its parallelism allows.
    ingested.max shouldBe stuck + IngestionParallelism - 1

    stuckIngestion.success(())
    run.output.futureValue.flatten.map(_.index) shouldBe (1 to InputSize)
  }

  /*
   * Verifies that backpressure correctly causes the BatchN operator to accumulate elements up to its maximum capacity.
   */
  it should "form max-sized batches while contractReInsertion back-pressures" in {
    val release = Promise[Unit]()
    val run = runPipe(
      inputMappingParallelism = 1, // BatchN then forms one batch of up to MaxBatchSize at a time
      // Block all lanes of the downstream consumer to ensure total backpressure
      contractReInsertionHook = _ => release.future,
    )

    // Wait until contractReInsertion is busy with a first batch and BatchN holds a full second one.
    // Elements that left the source but have not reached contractReInsertion are in BatchN, except
    // for at most one still being handed over. So once there are MaxBatchSize + 1 of them, BatchN
    // holds a full batch. (BatchN takes one element beyond a full batch, so this is reached.)
    eventually {
      run.contractReInsertionCalls.all should have size 1
      run.emitted.get() - run.contractReInsertionCalls.all.flatten.size should be >=
        MaxBatchSize + 1
    }
    release.success(())

    run.output.futureValue
    val batchSizes = run.contractReInsertionCalls.all.map(_.size)
    // The first batch was formed before contractReInsertion was busy, the second one while it was.
    batchSizes(1) shouldBe MaxBatchSize
    all(batchSizes) should be <= MaxBatchSize
  }

  /*
   * Tests the secondary batching stage (before batcher). We want to ensure it forms large batches of batches
   * when the final database insertion is slow.
   */
  it should "form max-sized db batches when ingestion back-pressures" in {
    val release = Promise[Unit]()
    val run = runPipe(
      submissionBatchSize = 1, // Force the first BatchN out of the equation
      ingestionParallelism =
        1, // Second BatchN then forms one batch of up to MaxIngestionBatchSize at a time
      // Block the consumer of the second BatchN stage to ensure total backpressure
      batcherHook = () => release.future,
    )

    // Same reasoning as for the first BatchN above, except that up to IngestionParallelism more batches can
    // wait in the ingester stage in front of the tailer.
    eventually {
      run.batcherCalls.all.size should be >= 1
      run.dbPrepareCalls.all.size - run.batcherCalls.all.flatten.size should be >=
        MaxIngestionBatchSize + 1 + IngestionParallelism
    }
    release.success(())

    run.output.futureValue
    val batchSizes = run.batcherCalls.all.map(_.size)
    // The first batch was formed before batcher was busy, the second one while it was.
    // +3 due to the 0 weight elements (at most 3 of them can present in a batch of 23)
    batchSizes(1) should (be >= MaxIngestionBatchSize + 2 and be <= MaxIngestionBatchSize + 3)
    all(batchSizes) should be <= MaxIngestionBatchSize + 3
  }

  /*
   * Verifies that when downstream is fast and upstream is slow, elements pass through quickly in small batches
   * rather than waiting to fill MaxBatchSize.
   *
   * Verifies that the tailer doesn't unnecessarily wait for `MaxTailerBatchSize` if the upstream is slow.
   * We pace the upstream based on what the tailer just finished consuming, ensuring zero backpressure.
   */
  it should "not wait for batches to fill up while the pipe is idle" in {
    val numElements = 10
    val tailed = (1 to numElements).map(_ -> Promise[Unit]()).toMap
    val run = runPipe(
      // The source hands out element i only after element i - 1 reached ingestTail
      input = Source(1 to numElements).mapAsync(parallelism = 1)(i =>
        (if (i == 1) Future.unit else tailed(i - 1).future).map(_ => i)
      ),
      ingestTailHook = batch => {
        batch.foreach(item => tailed(item.index).success(()))
        Future.unit
      },
    )

    run.output.futureValue
    run.contractReInsertionCalls.all.map(_.size) shouldBe Seq.fill(numElements)(1)
    run.ingestTailCalls.all.map(_.size) shouldBe Seq.fill(numElements)(1)
  }

  /** Runs the pipe over `input` and records the calls to some of its stages.
    *
    * Every stage completes only after its hook completes, so a hook can hold back or fail its
    * stage.
    */
  private def runPipe(
      input: Source[Int, NotUsed] = Source(1 to InputSize),
      submissionBatchSize: Int = MaxBatchSize,
      inputMappingParallelism: Int = 2,
      contractReInsertionHook: Iterable[Int] => Future[Unit] = _ => Future.unit,
      inputMapperHook: () => Unit = () => (),
      seqMapperHook: () => Unit = () => (),
      batcherHook: () => Future[Unit] = () => Future.unit,
      ingestionParallelism: Int = IngestionParallelism,
      ingestionMaxBatchSize: Int = MaxIngestionBatchSize,
      ingesterHook: DbBatch => Future[Unit] = _ => Future.unit,
      ingestTailHook: List[BatchedItem] => Future[Unit] = _ => Future.unit,
  ): PipeRun = {
    val emitted = new AtomicInteger(0)
    val contractReInsertionCalls = new CallLog[Iterable[Int]]
    val dbPrepareCalls = new CallLog[List[SequencedItem]]
    val batcherCalls = new CallLog[List[PreBatchItem]]
    val ingesterCalls = new CallLog[DbBatch]
    val ingestTailCalls = new CallLog[List[BatchedItem]]

    val pipe =
      BatchingParallelIngestionPipe[Int, List[SequencedItem], PreBatchItem, List[BatchedItem], List[
        BatchedItem
      ]](
        submissionBatchSize = submissionBatchSize,
        inputMappingParallelism = inputMappingParallelism,
        contractReInsertion = batch => {
          contractReInsertionCalls.record(batch)
          contractReInsertionHook(batch).map(_ => batch)
        },
        inputMapper = batch =>
          Future {
            inputMapperHook()
            batch.map(value => SequencedItem(index = 0, value = value)).toList
          },
        seqMapperZero = List(SequencedItem(index = 0, value = 0)),
        seqMapper = (previous, current) => {
          seqMapperHook()
          val lastIndex = previous.last.index
          current.zipWithIndex.map { case (item, i) => item.copy(index = lastIndex + 1 + i) }
        },
        dbPrepareParallelism = 2,
        dbPrepare = inBatch => {
          dbPrepareCalls.record(inBatch.toList)
          Future.successful(inBatch.map { case SequencedItem(index, value) =>
            PreBatchItem(index, Vector(value))
          }.toVector)
        },
        batcher = inBatch => {
          batcherCalls.record(inBatch.toList)
          batcherHook().flatMap { _ =>
            Future {
              inBatch.map { item =>
                BatchedItem(index = item.index, value = item.batch.mkString)
              }.toList
            }
          }
        },
        batcherCostFn = item => if (item.index % 10 == 0) 0L else 1L,
        ingestionParallelism = ingestionParallelism,
        ingestionMaxBatchSize = ingestionMaxBatchSize,
        ingestionMinBatchSize = 1,
        ingester = batch => {
          ingesterCalls.record(batch)
          ingesterHook(batch).map(_ => batch)
        },
        tailConflateFlow = Flow
          .apply[List[BatchedItem]]
          .map(b => b),
        ingestTail = batch => {
          ingestTailCalls.record(batch)
          ingestTailHook(batch).map(_ => batch)
        },
      )

    val output = input
      .map { value =>
        emitted.incrementAndGet().discard
        value
      }
      .via(pipe)
      .runWith(Sink.seq)

    PipeRun(
      emitted,
      contractReInsertionCalls,
      dbPrepareCalls,
      batcherCalls,
      ingesterCalls,
      ingestTailCalls,
      output,
    )
  }
}

object BatchingParallelIngestionPipeSpec {
  private val InputSize = 1000
  private val MaxBatchSize = 300
  private val MaxIngestionBatchSize = 20
  private val IngestionParallelism = 2

  final case class SequencedItem(index: Int, value: Int)
  final case class PreBatchItem(index: Int, batch: Vector[Int])
  final case class BatchedItem(index: Int, value: String)
  type DbBatch = List[BatchedItem]

  /** What a pipe run has done so far. It only grows, so tests can poll it with `eventually`. */
  private final case class PipeRun(
      emitted: AtomicInteger, // elements that left the input source
      contractReInsertionCalls: CallLog[Iterable[Int]], // i.e. the batches formed by BatchN
      dbPrepareCalls: CallLog[List[SequencedItem]],
      batcherCalls: CallLog[List[PreBatchItem]],
      ingesterCalls: CallLog[DbBatch],
      ingestTailCalls: CallLog[List[BatchedItem]],
      output: Future[Seq[DbBatch]],
  )

  /** Thread-safe list of the arguments a stage was called with. */
  private final class CallLog[A] {
    private val calls = new AtomicReference[Vector[A]](Vector.empty)
    def record(call: A): Unit = calls.updateAndGet(_ :+ call).discard
    def all: Vector[A] = calls.get()
  }
}
