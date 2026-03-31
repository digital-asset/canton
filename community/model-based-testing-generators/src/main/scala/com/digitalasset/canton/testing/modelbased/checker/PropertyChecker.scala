// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.checker

import com.daml.scalautil.Statement.discard
import org.scalacheck.Shrink

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.control.NonFatal

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
    // Placed by evaluator threads into the result queue.
    sealed trait EvaluationOutcome[+A]
    case object EvaluationPassed extends EvaluationOutcome[Nothing]
    final case class EvaluationFailed[A](value: A, error: String) extends EvaluationOutcome[A]
    final case class EvaluationError(error: Throwable) extends EvaluationOutcome[Nothing]
    final case class EvaluationGeneratorFailure(error: Throwable) extends EvaluationOutcome[Nothing]

    sealed trait SampleQueueEntry[+A]
    final case class GeneratedSample[A](value: A) extends SampleQueueEntry[A]
    final case class GeneratorFailure(error: Throwable) extends SampleQueueEntry[Nothing]
  }

  /** Repeatedly generates random values, checks `property` on each, and shrinks the first failing
    * value.
    *
    * Samples are pre-generated in parallel by `generatorParallelism` threads, filling a bounded
    * queue of size `sampleBufferSize`. Evaluator threads pull samples from the queue, evaluate the
    * property in parallel, and put results into a result queue. The main loop polls the result
    * queue and reacts accordingly.
    *
    * If your samples are expensive to generate but cheap to evaluate, increase
    * `generatorParallelism` and set `evaluatorParallelism` to 1. If your samples are cheap to
    * generate but expensive to evaluate, do the opposite. If both are expensive, experiment with
    * different ratios. Finally, if evaluating the property is not thread safe, set
    * `evaluatorParallelism` to 1.
    *
    * The process stops when `maxSamples` have been checked, the `timeout` is reached, or a sample
    * fails the property. In the failure case the value is shrunk using `shrinkToFailure` with
    * whatever time remains from the overall timeout.
    *
    * Up to `evaluatorParallelism - 1` extra evaluations may run after the first failure is
    * detected. This is inherent to the parallel design: other workers may have already started
    * evaluating by the time the main loop processes the failure. A shared `stopped` flag minimizes
    * waste by cooperatively stopping them.
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
    * @param evaluatorParallelism
    *   number of threads used to evaluate the property in parallel (defaults to 1)
    */
  def checkProperty[A](
      generate: () => A,
      shrink: Shrink[A],
      property: (A, AtomicBoolean) => Either[String, Unit],
      maxSamples: Int,
      timeout: FiniteDuration,
      sampleBufferSize: Int,
      generatorParallelism: Int,
      evaluatorParallelism: Int,
  ): CheckResult[A] = {
    import CheckPropertyAuxiliaryTypes.*

    // == Implementation overview ==
    //
    // Three groups of threads cooperate through two shared data structures:
    //
    //   generators -> sampleQueue -> evaluators -> resultQueue -> main loop
    //
    // 1. Generator threads (generatorPool) run generateForever loops that call
    //    generate() and put samples into a bounded LinkedBlockingQueue. They
    //    block on put when the queue is full.
    //
    // 2. Evaluator threads (evaluatorPool) run evaluateForever loops that poll
    //    the sample queue for a value, evaluate the property against it, and
    //    put the resulting EvaluationOutcome into the result queue.
    //
    // 3. The main loop runs on the caller's thread. It polls the result queue
    //    for completed evaluations. On success, it loops. On failure, it shuts
    //    everything down and delegates to shrinkToFailure. On timeout or
    //    maxSamples, it returns.
    //
    // Cooperative cancellation: the `stopped` flag is passed to each property
    // evaluation. When set to true, evaluator threads stop polling the sample
    // queue and in-flight property evaluations should exit as soon as possible.
    //
    // Shutdown: shutdownAll interrupts the generator threads via shutdownNow()
    // (which is assumed to be a safe operation, as the generators are not
    // supposed to log errors on shutdown), and gracefully shuts down evaluators
    // by setting `stopped` to true. A finally block ensures that shutdownAll
    // runs on every exit path.

    val deadline = timeout.fromNow

    def elapsedSoFar(): FiniteDuration = timeout - deadline.timeLeft

    // Bounded buffer between generator threads and evaluator threads.
    val sampleQueue = new LinkedBlockingQueue[SampleQueueEntry[A]](sampleBufferSize)
    // Thread pool running `generateForever` loops that fill the sample queue.
    val generatorPool = Executors.newFixedThreadPool(generatorParallelism)
    // Thread pool running `evaluateForever` loops that drain the sample queue.
    val evaluatorPool = Executors.newFixedThreadPool(evaluatorParallelism)
    // Unbounded buffer between evaluator threads and the main loop. Should stay
    // mostly empty, as the main loop consumes results very quickly.
    val resultQueue = new LinkedBlockingQueue[EvaluationOutcome[A]]()

    // Set to true when the main loop decides to stop (failure, timeout, or
    // maxSamples reached). Also passed to each property evaluation for
    // cooperative cancellation.
    val stopped = new AtomicBoolean(false)

    // Runs in a generator-pool thread. Repeatedly generates samples and puts
    // them into the sample queue. Blocks on `put` when the queue is full.
    @scala.annotation.tailrec
    def generateForever(): Unit = {
      sampleQueue.put(
        try GeneratedSample(generate())
        catch { case NonFatal(e) => GeneratorFailure(e) }
      )
      generateForever()
    }

    // Stops all activity: sets `stopped` to true, interrupts generators, and
    // waits for evaluator threads to exit. Generators are interrupted
    // immediately. Evaluators get a graceful shutdown: the `stopped` flag
    // signals them to exit cooperatively between gRPC calls, avoiding mid-call
    // interrupts that would cause them to log internal errors.
    def shutdownAll(): Unit = {
      stopped.set(true)
      discard(generatorPool.shutdownNow())
      evaluatorPool.shutdown()
      if (!evaluatorPool.awaitTermination(60, TimeUnit.SECONDS)) {
        discard(evaluatorPool.shutdownNow())
      }
    }

    // Runs in an evaluator-pool thread. Polls the sample queue with short
    // timeouts, rechecking `stopped` between attempts so the worker exits
    // promptly on shutdown. When a sample is obtained, evaluates the property
    // and puts the result into the result queue.
    @scala.annotation.tailrec
    def evaluateForever(): Unit =
      if (!stopped.get()) {
        Option(sampleQueue.poll(100L, TimeUnit.MILLISECONDS)) match {
          case None =>
            evaluateForever()
          case Some(GeneratorFailure(e)) =>
            resultQueue.put(EvaluationGeneratorFailure(e))
            evaluateForever()
          case Some(GeneratedSample(sample)) =>
            val outcome: EvaluationOutcome[A] =
              try
                property(sample, stopped) match {
                  case Left(error) => EvaluationFailed(sample, error)
                  case Right(()) => EvaluationPassed
                }
              catch {
                case NonFatal(e) => EvaluationError(e)
              }
            resultQueue.put(outcome)
            evaluateForever()
        }
      }

    // Main loop. Consumes completed evaluations from the result queue and:
    // - on success, loops
    // - on failure, shuts everything down and shrinks
    // - on timeout/maxSamples, returns CheckPassed immediately (in-flight
    //   evaluations are discarded)
    @scala.annotation.tailrec
    def mainLoop(checked: Int): CheckResult[A] =
      if (checked >= maxSamples || deadline.isOverdue())
        CheckPassed(checked, elapsedSoFar(), timedOut = deadline.isOverdue())
      else
        Option(resultQueue.poll(deadline.timeLeft.toMillis, TimeUnit.MILLISECONDS)) match {
          case None =>
            CheckPassed(checked, elapsedSoFar(), timedOut = true)

          case Some(outcome) =>
            outcome match {
              case EvaluationPassed =>
                mainLoop(checked + 1)

              case EvaluationFailed(value, error) =>
                // Shut down eagerly (rather than relying on `finally`) to free the
                // generator and evaluator pools before the potentially long shrink.
                shutdownAll()
                val shrinkResult =
                  shrinkToFailure(value, shrink, error, property, deadline.timeLeft)
                CheckFailed(
                  originalValue = value,
                  originalError = error,
                  shrinkResult = shrinkResult,
                  samplesChecked = checked,
                  elapsed = elapsedSoFar(),
                )

              case EvaluationGeneratorFailure(e) =>
                throw e

              case EvaluationError(e) =>
                throw e
            }
        }

    // Start the generator and evaluator threads

    (1 to generatorParallelism).foreach(_ =>
      generatorPool.execute { () =>
        try generateForever()
        catch {
          // Expected: shutdownAll() calls generatorPool.shutdownNow(), which
          // interrupts generator threads blocked on sampleQueue.put().
          case _: InterruptedException => ()
        }
      }
    )

    (1 to evaluatorParallelism).foreach(_ =>
      evaluatorPool.execute { () =>
        try evaluateForever()
        catch {
          // Expected: shutdownAll() may call evaluatorPool.shutdownNow() as a
          // last resort, which interrupts threads blocked on sampleQueue.poll().
          case _: InterruptedException => ()
        }
      }
    )

    try mainLoop(0)
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
      evaluatorParallelism: Int = 1,
  ): CheckResult[A] =
    checkProperty(
      generate = generate,
      shrink = shrink,
      property = (a: A, _: AtomicBoolean) => property(a),
      maxSamples = maxSamples,
      timeout = timeout,
      sampleBufferSize = bufferSize,
      generatorParallelism = generatorParallelism,
      evaluatorParallelism = evaluatorParallelism,
    )
}
