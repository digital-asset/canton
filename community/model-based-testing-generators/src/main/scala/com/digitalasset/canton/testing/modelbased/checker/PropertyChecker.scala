// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.checker

import com.daml.scalautil.Statement.discard
import org.scalacheck.Shrink

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Success, Try}

object PropertyChecker {

  private def formatDuration(d: FiniteDuration): String =
    s"${d.toMinutes}m${d.toSeconds % 60}s"

  // -- shrinkToFailure and related types --

  /** Result of shrinking a failing value.
    *
    * @param value
    *   the smallest failing value found
    * @param error
    *   the error message associated with the smallest failing value
    * @param shrinkSteps
    *   the number of successful shrink steps performed
    * @param timedOut
    *   if the shrinking process was stopped due to a timeout, the elapsed duration
    */
  final case class ShrinkResult[A](
      value: A,
      error: String,
      shrinkSteps: Int,
      timedOut: Option[FiniteDuration],
  ) {
    def summary: String =
      s"Shrinking completed after $shrinkSteps step(s)${timedOut
          .fold("")(d => s", timed out after ${formatDuration(d)}")}"
  }

  private object ShrinkToFailureAuxiliaryTypes {
    // Used by shrinkToFailure to represent the outcome of evaluating a single shrink candidate.
    sealed trait CandidateResult[+A]
    final case class CandidateFailed[A](value: A, error: String) extends CandidateResult[A]
    case object CandidatePassed extends CandidateResult[Nothing]
    case object CandidateTimedOut extends CandidateResult[Nothing]

    // Used by findFailingCandidate to represent the outcome of searching through all candidates.
    sealed trait SearchResult[+A]
    final case class SearchFoundSmallerValue[A](value: A, error: String) extends SearchResult[A]
    case object SearchDidNotFindSmallerValue extends SearchResult[Nothing]
    case object SearchTimedOut extends SearchResult[Nothing]
  }

  /** Shrinks a failing value by repeatedly trying smaller variants (as defined by the given
    * `Shrink` instance) and keeping the first one that still fails the given property. Returns the
    * smallest failing value together with its error message.
    *
    * The shrinking process will stop after `timeout` and return the smallest failing value found so
    * far. Each property evaluation is run in a separate thread and interrupted if the remaining
    * time runs out.
    *
    * @param value
    *   the initial failing value
    * @param shrink
    *   the shrink instance defining how to produce smaller candidates
    * @param error
    *   the error message associated with the initial failure
    * @param property
    *   a property that returns `Right(())` on success and `Left(errorMessage)` on failure. The
    *   `AtomicBoolean` parameter is a cancellation flag: when set to `true` by the checker, the
    *   property should stop asap.
    * @param timeout
    *   maximum duration for the entire shrinking process (defaults to 365 days)
    */
  private def shrinkToFailure[A](
      value: A,
      shrink: Shrink[A],
      error: String,
      property: (A, AtomicBoolean) => Either[String, Unit],
      timeout: FiniteDuration,
  ): ShrinkResult[A] = {
    import ShrinkToFailureAuxiliaryTypes.*

    val deadline = timeout.fromNow

    def elapsedSoFar(): FiniteDuration = timeout - deadline.timeLeft

    val executor = Executors.newSingleThreadExecutor()
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    val cancelled = new AtomicBoolean(false)

    def evaluateWithTimeout(candidate: A, remaining: FiniteDuration): CandidateResult[A] =
      try
        Await.result(Future(property(candidate, cancelled)), remaining) match {
          case Left(e) => CandidateFailed(candidate, e)
          case Right(()) => CandidatePassed
        }
      catch {
        case _: TimeoutException => CandidateTimedOut
      }

    @scala.annotation.tailrec
    def findFailingCandidate(
        candidates: Iterator[A]
    ): SearchResult[A] =
      if (deadline.isOverdue()) SearchTimedOut
      else if (!candidates.hasNext) SearchDidNotFindSmallerValue
      else {
        val candidate = candidates.next()
        val remaining = deadline.timeLeft
        evaluateWithTimeout(candidate, remaining) match {
          case CandidateFailed(v, e) => SearchFoundSmallerValue(v, e)
          case CandidatePassed => findFailingCandidate(candidates)
          case CandidateTimedOut => SearchTimedOut
        }
      }

    @scala.annotation.tailrec
    def shrinkLoop(
        value: A,
        error: String,
        steps: Int,
    ): ShrinkResult[A] =
      findFailingCandidate(shrink.shrink(value).iterator) match {
        case SearchFoundSmallerValue(smallerValue, smallerError) =>
          shrinkLoop(smallerValue, smallerError, steps + 1)
        case SearchDidNotFindSmallerValue =>
          ShrinkResult(value, error, steps, timedOut = None)
        case SearchTimedOut =>
          ShrinkResult(value, error, steps, timedOut = Some(elapsedSoFar()))
      }

    try shrinkLoop(value, error, 0)
    finally {
      cancelled.set(true)
      executor.shutdown()
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        discard(executor.shutdownNow())
      }
    }
  }

  // -- checkProperty and related types --

  /** Outcome of running `checkProperty`. */
  sealed trait CheckResult[+A] {
    def summary: String
  }

  /** All samples generated during the allotted time passed the property. */
  final case class CheckPassed(
      samplesChecked: Int,
      elapsed: FiniteDuration,
      timedOut: Boolean,
  ) extends CheckResult[Nothing] {
    def summary: String =
      if (timedOut)
        s"Timed out after ${formatDuration(elapsed)}, $samplesChecked sample(s) passed"
      else
        s"All $samplesChecked sample(s) passed in ${formatDuration(elapsed)}"
  }

  /** A sample failed the property. Contains the original failing value, the result of shrinking,
    * and statistics about the run.
    */
  final case class CheckFailed[A](
      originalValue: A,
      originalError: String,
      shrinkResult: ShrinkResult[A],
      samplesChecked: Int,
      elapsed: FiniteDuration,
  ) extends CheckResult[A] {
    def summary: String =
      s"Failed after $samplesChecked sample(s) in ${formatDuration(elapsed)}. ${shrinkResult.summary}"
  }

  private object CheckPropertyAuxiliaryTypes {
    // Used by checkProperty to represent the outcome of generating and evaluating a single sample.
    sealed trait SampleResult[+A]
    case object SamplePassed extends SampleResult[Nothing]
    final case class SampleFailed[A](value: A, error: String) extends SampleResult[A]
    case object SampleTimedOut extends SampleResult[Nothing]

    // Used by dequeue to represent the outcome of polling the pre-generation queue.
    sealed trait DequeueResult[+A]
    final case class Dequeued[A](value: A) extends DequeueResult[A]
    case object DequeueTimedOut extends DequeueResult[Nothing]
    final case class DequeuedGeneratorFailure(error: Throwable) extends DequeueResult[Nothing]
  }

  /** Repeatedly generates random values, checks `property` on each, and shrinks the first failing
    * value.
    *
    * Samples are pre-generated in parallel by `generatorParallelism` threads, filling a bounded
    * queue of size `sampleBufferSize`. The main loop dequeues samples one at a time and evaluates
    * the property. Running several generators in parallel improves the latency of the generation if
    * generation time has a large variance. Running the generation and the evaluation in parallel
    * improves the throughput in case both generation and evaluation are slow.
    *
    * The process stops when `maxSamples` have been checked, the `timeout` is reached, or a sample
    * fails the property. In the failure case the value is shrunk using `shrinkToFailure` with
    * whatever time remains from the overall timeout.
    *
    * @param generate
    *   a function that produces one random value when called
    * @param shrink
    *   the shrink instance defining how to produce smaller candidates on failure
    * @param property
    *   a property that returns `Right(())` on success and `Left(errorMessage)` on failure. The
    *   `AtomicBoolean` parameter is a cancellation flag: when set to `true` by the checker, the
    *   property should stop evaluating asap.
    * @param maxSamples
    *   maximum number of samples to check
    * @param timeout
    *   maximum duration for the entire process including shrinking
    * @param sampleBufferSize
    *   maximum number of pre-generated samples to buffer (defaults to 100)
    * @param generatorParallelism
    *   number of threads used to generate samples in parallel (defaults to 1)
    */
  def checkProperty[A](
      generate: () => A,
      shrink: Shrink[A],
      property: (A, AtomicBoolean) => Either[String, Unit],
      maxSamples: Int,
      timeout: FiniteDuration,
      sampleBufferSize: Int,
      generatorParallelism: Int,
  ): CheckResult[A] = {
    import CheckPropertyAuxiliaryTypes.*

    val deadline = timeout.fromNow

    def elapsedSoFar(): FiniteDuration = timeout - deadline.timeLeft

    val sampleQueue = new LinkedBlockingQueue[Try[A]](sampleBufferSize)
    val generatorPool = Executors.newFixedThreadPool(generatorParallelism)
    val evaluatorExecutor = Executors.newSingleThreadExecutor()
    val evaluatorExecutionContext: ExecutionContext =
      ExecutionContext.fromExecutor(evaluatorExecutor)

    // Currently only one evaluation runs at a time so a single cancellation flag suffices.
    // When multiple evaluators run in parallel this will need to become a per-evaluation flag.
    val cancelled = new AtomicBoolean(false)

    @scala.annotation.tailrec
    def generateForever(): Unit = {
      sampleQueue.put(Try(generate()))
      generateForever()
    }

    def shutdownAll(): Unit = {
      // Signal the property evaluation to stop cooperatively, then use graceful shutdown for the evaluator
      // to avoid interrupting a property evaluation mid-flight (e.g. a gRPC streaming call), which would cause Canton
      // to log an internal error.
      cancelled.set(true)
      discard(generatorPool.shutdownNow())
      evaluatorExecutor.shutdown()
      if (!evaluatorExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        discard(evaluatorExecutor.shutdownNow())
      }
    }

    def dequeue(remaining: FiniteDuration): DequeueResult[A] =
      Option(sampleQueue.poll(remaining.toMillis, TimeUnit.MILLISECONDS)) match {
        case None => DequeueTimedOut
        case Some(Failure(e)) => DequeuedGeneratorFailure(e)
        case Some(Success(value)) => Dequeued(value)
      }

    def evaluateWithTimeout(sample: A, remaining: FiniteDuration): SampleResult[A] =
      try
        Await.result(
          Future(property(sample, cancelled))(evaluatorExecutionContext),
          remaining,
        ) match {
          case Left(e) => SampleFailed(sample, e)
          case Right(()) => SamplePassed
        }
      catch {
        case _: TimeoutException =>
          cancelled.set(true)
          SampleTimedOut
      }

    def dequeueThenEvaluate(): SampleResult[A] =
      dequeue(deadline.timeLeft) match {
        case DequeueTimedOut => SampleTimedOut
        case DequeuedGeneratorFailure(e) => throw e
        case Dequeued(sample) => evaluateWithTimeout(sample, deadline.timeLeft)
      }

    @scala.annotation.tailrec
    def sampleLoop(checked: Int): CheckResult[A] =
      if (checked >= maxSamples) CheckPassed(checked, elapsedSoFar(), timedOut = false)
      else if (deadline.isOverdue()) CheckPassed(checked, elapsedSoFar(), timedOut = true)
      else
        dequeueThenEvaluate() match {
          case SamplePassed => sampleLoop(checked + 1)
          case SampleTimedOut => CheckPassed(checked, elapsedSoFar(), timedOut = true)
          case SampleFailed(value, error) =>
            shutdownAll()
            val shrinkResult =
              shrinkToFailure(
                value,
                shrink,
                error,
                property,
                deadline.timeLeft,
              )
            CheckFailed(
              originalValue = value,
              originalError = error,
              shrinkResult = shrinkResult,
              samplesChecked = checked,
              elapsed = elapsedSoFar(),
            )
        }

    // Start the generator threads and the main loop

    (1 to generatorParallelism).foreach(_ =>
      generatorPool.execute { () =>
        try generateForever()
        catch {
          case _: InterruptedException => ()
        }
      }
    )

    try sampleLoop(0)
    finally shutdownAll()
  }

  /** Convenience overload for properties that don't need cooperative cancellation. */
  def checkProperty[A](
      generate: () => A,
      shrink: Shrink[A],
      property: A => Either[String, Unit],
      maxSamples: Int = Int.MaxValue,
      timeout: FiniteDuration = 365.days,
      bufferSize: Int = 100,
      generatorParallelism: Int = 1,
  ): CheckResult[A] =
    checkProperty(
      generate = generate,
      shrink = shrink,
      property = (a: A, _: AtomicBoolean) => property(a),
      maxSamples = maxSamples,
      timeout = timeout,
      sampleBufferSize = bufferSize,
      generatorParallelism = generatorParallelism,
    )
}
