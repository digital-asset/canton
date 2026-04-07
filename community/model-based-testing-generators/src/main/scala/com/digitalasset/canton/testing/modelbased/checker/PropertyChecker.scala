// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.checker

import com.daml.scalautil.Statement.discard
import org.scalacheck.Shrink

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{
  ConcurrentHashMap,
  CountDownLatch,
  Executors,
  LinkedBlockingQueue,
  TimeUnit,
}
import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.util.control.NonFatal

object PropertyChecker {

  private def formatDuration(d: FiniteDuration): String =
    s"${d.toMinutes}m${d.toSeconds % 60}s"

  /** Tracks elapsed time against a timeout.
    */
  private class Timer(timeout: FiniteDuration) {
    private val deadline: Deadline = timeout.fromNow
    def elapsed: FiniteDuration = timeout - deadline.timeLeft
    def remaining: FiniteDuration = deadline.timeLeft
    def isOverdue: Boolean = deadline.isOverdue()
  }

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

    /** Per-round shared state for parallel shrink candidate evaluation.
      *
      * Stored in a single `AtomicReference` and updated via compare-and-swap.
      *
      * Transitions:
      *   - `RoundNoFailureYet` -> `RoundBestFailure(i, v, e)` : first failing candidate found.
      *   - `RoundBestFailure(i, ...)` -> `RoundBestFailure(j, ...)` where `j < i` : found a better
      *     failure
      *   - `RoundNoFailureYet | RoundBestFailure` -> `RoundEvaluatorError(t)` : exception thrown by
      *     some evaluator
      */
    sealed trait RoundState[+A]
    case object RoundNoFailureYet extends RoundState[Nothing]
    final case class RoundBestFailure[A](index: Int, value: A, error: String) extends RoundState[A]
    final case class RoundEvaluatorError(exception: Throwable) extends RoundState[Nothing]
  }

  /** Shrinks a failing value by repeatedly trying smaller variants (as defined by the given
    * `Shrink` instance) and keeping the first one that still fails the given property. Returns the
    * smallest failing value together with its error message.
    *
    * When `evaluatorParallelism > 1`, multiple shrink candidates are evaluated in parallel while
    * preserving the guarantee that earliest candidates in the shrink stream are prioritized over
    * later ones. This is handy because debugging counter-examples with noise is much harder than
    * debugging minimal one, yet it can take more than an hour to shrink a scenario of size 50 when
    * running it against canton.
    *
    * The shrinking process will stop after `timeout` and return the smallest failing value found so
    * far.
    *
    * @param value
    *   the initial failing value
    * @param shrink
    *   the shrink instance defining how to produce smaller candidates
    * @param error
    *   the error message associated with the initial failure
    * @param property
    *   a property that returns `Right(())` on success and `Left(errorMessage)` on failure. The `()
    *   \=> Boolean` parameter is a cancellation predicate: when it returns `true`, the property
    *   should stop asap.
    * @param timeout
    *   maximum duration for the entire shrinking process (defaults to 365 days)
    * @param evaluatorParallelism
    *   number of threads evaluating shrink candidates concurrently
    */
  private def shrinkToFailure[A](
      value: A,
      shrink: Shrink[A],
      error: String,
      property: (A, () => Boolean) => Either[String, Unit],
      timeout: FiniteDuration,
      evaluatorParallelism: Int,
      cacheSuccesses: Boolean,
  ): ShrinkResult[A] = {
    import ShrinkToFailureAuxiliaryTypes.*

    // == Implementation overview ==
    //
    // Shrinking proceeds in rounds: we start with a candidate, consider its shrink candidates and
    // evaluate them against the property. The first candidate that fails the property becomes the
    // new current value. Rinse and repeat until no smaller failing candidate is found, or we run out
    // of time.
    //
    // When `evaluatorParallelism > 1`, candidates within a round are evaluated in parallel by a fixed
    // pool of workers. A shared `AtomicReference` tracks the best (i.e. earliest in the stream of candidates)
    // failure found so far, and workers update it via compare-and-swap. This allows us to preserve the
    // guarantee that earlier candidates in the shrink stream are preferred over later ones,
    // while still evaluating multiple candidates concurrently. Specifically, this saves time when
    // candidates 0..i succeed and candidate i+1 fails. Then we discard 0..i faster than if they were
    // evaluated sequentially.
    //
    // When a new best failure is found, we cooperatively cancel the evaluation of later candidates
    // in the same round. This is achieved by passing a cancellation function to the property that checks
    // the shared `AtomicReference` to know when it has been superseded by a better failure or an error,
    // or when the timer has expired.
    //
    // One last optimization is caching values that are known to pass the property, to avoid re-evaluating
    // them in later rounds. Cache hits are frequent because shrinking often produces the same candidate
    // from different paths.

    val timer = new Timer(timeout)
    val pool = Executors.newFixedThreadPool(evaluatorParallelism)
    // Cache of values known to pass the property.
    val successCache = ConcurrentHashMap.newKeySet[A]()

    try {
      @tailrec
      def shrinkLoop(
          value: A,
          error: String,
          steps: Int,
      ): ShrinkResult[A] =
        if (timer.isOverdue)
          ShrinkResult(value, error, steps, timedOut = Some(timer.elapsed))
        else {

          // -- Per-round shared state --

          // Synchronized iterator wrapper. Evaluator threads claim candidates from the shrink stream one
          // at a time. The synchronization cost should be negligible compared to property evaluation for
          // a typical shrinker.
          val iter = shrink.shrink(value).iterator
          @SuppressWarnings(Array("org.wartremover.warts.Var"))
          var nextIndex = 0
          def claimNext(): Option[(Int, A)] = this.synchronized {
            if (!iter.hasNext) None
            else {
              val i = nextIndex
              nextIndex += 1
              Some((i, iter.next()))
            }
          }

          // The outcome of the current shrink round, updated by evaluator threads via compare-and-set.
          val roundState: AtomicReference[RoundState[A]] = new AtomicReference(RoundNoFailureYet)
          // Barrier: ensures all evaluator threads have finished before reading roundState.
          val latch = new CountDownLatch(evaluatorParallelism)

          // -- Evaluator task --

          // Each task claims candidates, evaluates the property, and CAS-updates roundState.
          def evaluatorTask(): Unit =
            try {
              @tailrec
              def claimAndEvaluateUntilFailure(): Unit =
                if (!timer.isOverdue) {

                  claimNext() match {
                    case None => () // stream exhausted, exit

                    case Some((index, candidate)) =>
                      roundState.get() match {
                        case RoundBestFailure(bestIdx, _, _) if index >= bestIdx => ()
                        case RoundEvaluatorError(_) => ()
                        case _ if successCache.contains(candidate) => claimAndEvaluateUntilFailure()
                        case _ =>
                          // Cancellation function passed to the property for cooperative shutdown. An evaluation
                          // is canceled when a better failure has been found, an error occurs, or the timer expires.
                          val isCancelled: () => Boolean = () =>
                            roundState.get() match {
                              case RoundBestFailure(bestIdx, _, _) =>
                                index >= bestIdx || timer.isOverdue
                              case RoundEvaluatorError(_) => true
                              case RoundNoFailureYet => timer.isOverdue
                            }

                          property(candidate, isCancelled) match {
                            case Right(()) =>
                              // The !isCancelled() check is in theory redundant, as cancelled evaluations should
                              // return a Left. But we cover our bases in case of a misbehaving property that wrongly
                              // returns a Right without having properly finished evaluating after cancellation.
                              if (cacheSuccesses && !isCancelled())
                                discard(successCache.add(candidate))
                              claimAndEvaluateUntilFailure()

                            case Left(candidateError) =>
                              // Compare-and-swap loop: update roundState only if this failure improves upon the
                              // current best failure.
                              @tailrec
                              def updateBestFailure(): Unit = {
                                val current = roundState.get()
                                current match {
                                  case RoundEvaluatorError(_) =>
                                    () // terminal, don't overwrite
                                  case RoundBestFailure(bestIdx, _, _) if index >= bestIdx =>
                                    () // not better
                                  case _ => // RoundNoFailureYet or RoundBestFailure with worse index
                                    val updated = !roundState.compareAndSet(
                                      current,
                                      RoundBestFailure(index, candidate, candidateError),
                                    )
                                    if (!updated)
                                      updateBestFailure()
                                }
                              }
                              updateBestFailure()
                          }
                      }
                  }
                }
              claimAndEvaluateUntilFailure()
            } catch {
              case NonFatal(e) =>
                @tailrec
                def setError(): Unit = {
                  val current = roundState.get()
                  current match {
                    case RoundEvaluatorError(_) => () // first error wins
                    case _ =>
                      if (!roundState.compareAndSet(current, RoundEvaluatorError(e)))
                        setError()
                  }
                }
                setError()
              case _: InterruptedException =>
                () // expected during shutdown
            } finally {
              latch.countDown()
            }

          // Submit evaluator tasks for this round
          (1 to evaluatorParallelism).foreach(_ => pool.execute(() => evaluatorTask()))

          // Block until all evaluators have returned
          latch.await()

          roundState.get() match {
            case RoundEvaluatorError(exception) =>
              throw exception
            case _ if timer.isOverdue =>
              ShrinkResult(value, error, steps, timedOut = Some(timer.elapsed))
            case RoundBestFailure(_, smallerValue, smallerError) =>
              shrinkLoop(smallerValue, smallerError, steps + 1)
            case RoundNoFailureYet =>
              ShrinkResult(value, error, steps, timedOut = None) // locally minimal
          }
        }

      shrinkLoop(value, error, 0)
    } finally {
      pool.shutdown()
      if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
        discard(pool.shutdownNow())
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
    *   a property that returns `Right(())` on success and `Left(errorMessage)` on failure. The `()
    *   \=> Boolean` parameter is a cancellation function: when it returns `true`, the property
    *   should stop evaluating asap.
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
    * @param cacheSuccesses
    *   if true, cache values that pass the property during shrinking to avoid re-evaluating them
    */
  def checkProperty[A](
      generate: () => A,
      shrink: Shrink[A],
      property: (A, () => Boolean) => Either[String, Unit],
      maxSamples: Int,
      timeout: FiniteDuration,
      sampleBufferSize: Int,
      generatorParallelism: Int,
      evaluatorParallelism: Int,
      cacheSuccesses: Boolean,
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

    val timer = new Timer(timeout)

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
                property(sample, () => stopped.get()) match {
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
      if (checked >= maxSamples || timer.isOverdue)
        CheckPassed(checked, timer.elapsed, timedOut = timer.isOverdue)
      else
        Option(resultQueue.poll(timer.remaining.toMillis, TimeUnit.MILLISECONDS)) match {
          case None =>
            CheckPassed(checked, timer.elapsed, timedOut = true)

          case Some(outcome) =>
            outcome match {
              case EvaluationPassed =>
                mainLoop(checked + 1)

              case EvaluationFailed(value, error) =>
                // Shut down eagerly (rather than relying on `finally`) to free the
                // generator and evaluator pools before the potentially long shrink.
                shutdownAll()
                val shrinkResult =
                  shrinkToFailure(
                    value,
                    shrink,
                    error,
                    property,
                    timer.remaining,
                    evaluatorParallelism,
                    cacheSuccesses,
                  )
                CheckFailed(
                  originalValue = value,
                  originalError = error,
                  shrinkResult = shrinkResult,
                  samplesChecked = checked,
                  elapsed = timer.elapsed,
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
      cacheSuccesses: Boolean = true,
  ): CheckResult[A] =
    checkProperty(
      generate = generate,
      shrink = shrink,
      property = (a: A, _: () => Boolean) => property(a),
      maxSamples = maxSamples,
      timeout = timeout,
      sampleBufferSize = bufferSize,
      generatorParallelism = generatorParallelism,
      evaluatorParallelism = evaluatorParallelism,
      cacheSuccesses = cacheSuccesses,
    )
}
