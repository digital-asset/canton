// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import cats.syntax.flatMap._
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  RunOnShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.retry.RetryUtil.{
  AllExnRetryable,
  ErrorKind,
  ExceptionRetryable,
  NoErrorKind,
}
import com.digitalasset.canton.util.retry.RetryWithDelay.{RetryOutcome, RetryTermination}
import com.digitalasset.canton.util.{DelayUtil, LoggerUtil}
import org.slf4j.event.Level

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Try}

/** A retry [[com.digitalasset.canton.util.retry.Policy]] defines an interface for retrying a future-based task with
  * retry semantics specific to implementations. If the task throws a non-fatal exceptions synchronously, the exception is
  * converted into an asynchronous one, i.e., it is returned as a failed future or retried.
  *
  * If unsure about what retry policy to pick, [[com.digitalasset.canton.util.retry.Backoff]] is a good default.
  */
abstract class Policy(logger: TracedLogger) {

  protected val directExecutionContext: DirectExecutionContext = DirectExecutionContext(logger)

  def apply[T](task: => Future[T], retryOk: ExceptionRetryable)(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[T]

  def unlessShutdown[T](task: => FutureUnlessShutdown[T], retryOk: ExceptionRetryable)(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[T]

}

object Policy {

  /** Repeatedly execute the task until it doesn't throw an exception or the `flagCloseable` is closing. */
  def noisyInfiniteRetry[A](
      task: => Future[A],
      flagCloseable: FlagCloseable,
      retryInterval: FiniteDuration,
      operationName: String,
      actionable: String,
  )(implicit
      loggingContext: ErrorLoggingContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[A] =
    Pause(
      loggingContext.logger,
      flagCloseable,
      maxRetries = Int.MaxValue,
      retryInterval,
      operationName,
      actionable,
    ).unlessShutdown(FutureUnlessShutdown.outcomeF(task), AllExnRetryable)(
      Success.always,
      executionContext,
      loggingContext.traceContext,
    )
}

abstract class RetryWithDelay(
    logger: TracedLogger,
    operationName: String,
    longDescription: String,
    initialDelay: FiniteDuration,
    totalMaxRetries: Int,
    flagCloseable: FlagCloseable,
    retryLogLevel: Option[Level],
) extends Policy(logger) {

  private val complainAfterRetries: Int = 10

  protected def nextDelay(nextCount: Int, delay: FiniteDuration): FiniteDuration

  /** A [[com.digitalasset.canton.util.retry.Success]] criteria is supplied
    * to determine whether the future-based task has succeeded, or if it should perhaps be retried. Retries are not
    * performed after the [[com.digitalasset.canton.lifecycle.FlagCloseable]] has been closed.
    */
  override def apply[T](
      task: => Future[T],
      retryable: ExceptionRetryable,
  )(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[T] =
    retryWithDelay(task, retryable, executionContext).transform {
      case util.Success(RetryOutcome(outcome, _termination)) =>
        outcome
      case Failure(failure) =>
        logger.error("retryWithDelay failed unexpectedly", failure)
        Failure(failure)
    }(directExecutionContext)

  override def unlessShutdown[T](
      task: => FutureUnlessShutdown[T],
      retryable: ExceptionRetryable,
  )(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[T] =
    FutureUnlessShutdown {
      retryWithDelay(task.unwrap, retryable, executionContext)(Success.onShutdown, implicitly)
        .transform {
          case util.Success(outcome) =>
            util.Success(outcome.toUnlessShutdown.flatten)
          case Failure(failure) =>
            logger.error("retryWithDelay failed unexpectedly", failure)
            Failure(failure)
        }(directExecutionContext)
    }

  private def retryWithDelay[T](
      task: => Future[T],
      retryable: ExceptionRetryable,
      executionContext: ExecutionContext,
  )(implicit success: Success[T], traceContext: TraceContext): Future[RetryOutcome[T]] = {
    implicit val loggingContext: ErrorLoggingContext = ErrorLoggingContext.fromTracedLogger(logger)

    import LoggerUtil.logOnThrow

    def runTask(): Future[T] = Future.fromTry(Try(task)).flatten

    def run(
        previousResult: Future[T],
        totalRetries: Int,
        lastErrorKind: ErrorKind,
        retriesOfLastErrorKind: Int,
        delay: FiniteDuration,
    ): Future[RetryOutcome[T]] = logOnThrow {
      previousResult.transformWith { x =>
        logOnThrow(x match {
          case succ @ util.Success(result) if success.predicate(result) =>
            logger.trace(
              s"The operation '$operationName' was successful. No need to retry. $longDescription"
            )
            Future.successful(RetryOutcome(succ, RetryTermination.Success))

          case outcome if flagCloseable.isClosing =>
            Future.successful(RetryOutcome(outcome, RetryTermination.Shutdown))

          case outcome if totalMaxRetries < Int.MaxValue && totalRetries >= totalMaxRetries =>
            logger.info(
              messageOfOutcome(
                outcome,
                s"Total maximum number of retries $totalMaxRetries exceeded. Giving up.",
              ),
              throwableOfOutcome(outcome),
            )
            Future.successful(RetryOutcome(outcome, RetryTermination.GiveUp))

          case outcome =>
            // this will also log the exception in outcome
            val errorKind = retryable.retryOK(outcome, logger)
            val retriesOfErrorKind =
              if (errorKind == lastErrorKind) {
                logger.trace(s"Kind of error has not changed since last attempt: $errorKind")
                retriesOfLastErrorKind
              } else {
                logger.info(messageOfOutcome(outcome, s"New kind of error: $errorKind."))
                // No need to log the exception in outcome, as this has been logged by retryable.retryOk.
                0
              }
            if (errorKind.maxRetries == Int.MaxValue || retriesOfErrorKind < errorKind.maxRetries) {

              val level = retryLogLevel.getOrElse {
                if (totalRetries < complainAfterRetries || totalMaxRetries != Int.MaxValue)
                  Level.INFO
                else Level.WARN
              }

              LoggerUtil.logAtLevel(
                level,
                messageOfOutcome(outcome, show"Retrying after $delay."),
                // No need to log the exception in outcome, as this has been logged by retryable.retryOk.
              )

              val invocationP = Promise[Future[RetryOutcome[T]]]()
              val abortedOnShutdownTask = new RunOnShutdown {
                override def done: Boolean = invocationP.isCompleted

                override def name: String = s"'$operationName' / $longDescription"

                override def run(): Unit = logOnThrow {
                  val wrappedOutcome =
                    Future.successful(RetryOutcome(outcome, RetryTermination.Shutdown))
                  val previouslyCompleted = invocationP.trySuccess(wrappedOutcome)
                  if (!previouslyCompleted) {
                    logger.info(
                      s"The operation '$operationName' has been cancelled. Aborting. $longDescription"
                    )
                  }
                }
              }
              flagCloseable.runOnShutdown(abortedOnShutdownTask)

              val delayedF = DelayUtil.delay(delay, flagCloseable)
              flagCloseable
                .performUnlessClosing {
                  // if delayedF doesn't complete, then we can be sure that the `abortedOnShutdownTask.run` will run due to shutdown
                  delayedF.onComplete {
                    case util.Success(()) =>
                      logOnThrow { // if this one doesn't run, then the `abortedOnShutdownTask` will run
                        flagCloseable.performUnlessClosing {
                          val retryP = Promise[RetryOutcome[T]]()
                          // ensure that the abort task doesn't get executed anymore (because we
                          // want to return the "last outcome" and here, we can be sure that `run` will give us a new outcome
                          invocationP.trySuccess(retryP.future)
                          LoggerUtil.logAtLevel(
                            level,
                            s"Now retrying operation '$operationName'. $longDescription",
                          )
                          // Run the task again on the normal execution context as the task might take a long time.
                          // `performUnlessClosingF` guards against closing the execution context.
                          val nextRunUnlessShutdown =
                            flagCloseable
                              .performUnlessClosingF(runTask())(executionContext, traceContext)
                          @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
                          val nextRunF = nextRunUnlessShutdown
                            .onShutdown {
                              // If we're closing, report the previous `outcome` and recurse.
                              // This will enter the case branch with `flagCloseable.isClosing`
                              // and therefore yield the termination reason `Shutdown`.
                              outcome.get
                            }(
                              // Use the direct execution context as this is a small task.
                              // The surrounding `performUnlessClosing` ensures that this post-processing
                              // is registered with the normal execution context before it can close.
                              directExecutionContext
                            )
                          val retryF = run(
                            nextRunF,
                            totalRetries + 1,
                            errorKind,
                            retriesOfErrorKind + 1,
                            nextDelay(totalRetries + 1, delay),
                          )
                          retryP.completeWith(retryF).future
                        }(traceContext)
                      }
                    case failure: Failure[_] =>
                      logger.error("DelayUtil failed unexpectedly", failure)
                  }(
                    // It is safe to use the general execution context here by the following argument.
                    // - If the `onComplete` executes before `DelayUtil` completes the returned promise,
                    //   then the completion of the promise will schedule the function immediately.
                    //   Since this completion is guarded by `performUnlessClosing`,
                    //   the body gets scheduled with `executionContext` before `flagCloseable`'s close method completes.
                    // - If `DelayUtil` completes the returned promise before the `onComplete` call executes,
                    //   the `onComplete` call itself will schedule the body
                    //   and this is guarded by the `performUnlessClosing` above.
                    // Therefore the execution context is still open when the scheduling happens.
                    executionContext
                  )
                }
                .onShutdown(
                  // If the `onComplete` does not run, then `abortedDueToShutdownTask.run` will run due to shutdown
                  ()
                )
              invocationP.future.flatten
            } else {
              logger.info(
                messageOfOutcome(
                  outcome,
                  s"Maximum number of retries ${errorKind.maxRetries} exceeded. Giving up.",
                  // No need to log the exception in outcome, as this has been logged by retryable.retryOk.
                )
              )
              Future.successful(RetryOutcome(outcome, RetryTermination.GiveUp))
            }
        })
      // Although in most cases, it may be ok to schedule the body on executionContext,
      // there is a chance that executionContext is closed when the body is scheduled.
      // By choosing directExecutionContext, we avoid a RejectedExecutionException in this case.
      }(directExecutionContext)
    }

    // Run 0: Run task without checking `flagCloseable`. If necessary, the client has to check for closing.
    // Run 1 onwards: Only run this if `flagCloseable` is not closing.
    //  (The check is performed at the recursive call.)
    //  Checking at the client would be very difficult, because the client would have to deal with a closed EC.
    run(runTask(), 0, NoErrorKind, 0, initialDelay)
  }

  private def messageOfOutcome(outcome: Try[Any], consequence: String): String = outcome match {
    case util.Success(result) =>
      s"The operation '$operationName' was not successful. $consequence Result: $result. $longDescription"
    case Failure(_) =>
      s"The operation '$operationName' has failed with an exception. $consequence $longDescription"
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def throwableOfOutcome(outcome: Try[Any]): Throwable = outcome.failed.getOrElse(null)
}

object RetryWithDelay {

  /** The outcome of the last run of the task,
    * along with the condition that stopped the retry.
    */
  private case class RetryOutcome[A](outcome: Try[A], termination: RetryTermination) {

    /** @throws java.lang.Throwable Rethrows the exception if [[outcome]] is a [[scala.util.Failure]] */
    @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
    def toUnlessShutdown: UnlessShutdown[A] = {
      if (termination == RetryTermination.Shutdown) AbortedDueToShutdown
      else Outcome(outcome.get)
    }
  }
  private sealed trait RetryTermination extends Product with Serializable
  private[RetryWithDelay] object RetryTermination {

    /** The task completed successfully */
    case object Success extends RetryTermination

    /** The retry limit was exceeded or an exception was deemed not retryable */
    case object GiveUp extends RetryTermination

    /** The retry stopped due to shutdown */
    case object Shutdown extends RetryTermination
  }
}

/** Retry immediately after failure for a max number of times */
case class Directly(
    logger: TracedLogger,
    flagCloseable: FlagCloseable,
    maxRetries: Int,
    operationName: String,
    longDescription: String = "",
    retryLogLevel: Option[Level] = None,
) extends RetryWithDelay(
      logger,
      operationName,
      longDescription,
      Duration.Zero,
      maxRetries,
      flagCloseable,
      retryLogLevel,
    ) {

  override def nextDelay(nextCount: Int, delay: FiniteDuration): FiniteDuration = Duration.Zero
}

/** Retry with a pause between attempts for a max number of times */
case class Pause(
    logger: TracedLogger,
    flagCloseable: FlagCloseable,
    maxRetries: Int,
    delay: FiniteDuration,
    operationName: String,
    longDescription: String = "",
    retryLogLevel: Option[Level] = None,
) extends RetryWithDelay(
      logger,
      operationName,
      longDescription,
      delay,
      maxRetries,
      flagCloseable,
      retryLogLevel,
    ) {

  override def nextDelay(nextCount: Int, delay: FiniteDuration): FiniteDuration = delay
}

/** A retry policy which will back off using a configurable policy which
  *  incorporates random jitter. This has the advantage of reducing contention
  *  if you have threaded clients using the same service.
  *
  *  {{{
  *  val policy = retry.Backoff()
  *  val future = policy(issueRequest)
  *  }}}
  *
  *  The following pre-made jitter algorithms are available for you to use:
  *
  *  - [[Jitter.none]]
  *  - [[Jitter.full]]
  *  - [[Jitter.equal]]
  *  - [[Jitter.decorrelated]]
  *
  *  You can choose one like this:
  *  {{{
  *  implicit val jitter = retry.Jitter.full(cap = 5.minutes)
  *  val policy = retry.Backoff(1 second)
  *  val future = policy(issueRequest)
  *  }}}
  *
  *  If a jitter policy isn't in scope, it will use [[Jitter.full]] by
  *  default which tends to cause clients slightly less work at the cost of
  *  slightly more time.
  *
  *  For more information about the algorithms, see the following article:
  *
  *  [[https://www.awsarchitectureblog.com/2015/03/backoff.html]]
  */
case class Backoff(
    logger: TracedLogger,
    flagCloseable: FlagCloseable,
    maxRetries: Int,
    initialDelay: FiniteDuration,
    maxDelay: Duration,
    operationName: String,
    longDescription: String = "",
    retryLogLevel: Option[Level] = None,
)(implicit jitter: Jitter = Jitter.full(maxDelay))
    extends RetryWithDelay(
      logger,
      operationName,
      longDescription,
      initialDelay,
      maxRetries,
      flagCloseable,
      retryLogLevel,
    ) {

  override def nextDelay(nextCount: Int, delay: FiniteDuration): FiniteDuration =
    jitter(initialDelay, delay, nextCount)
}

/** A retry policy in which the failure determines the way a future should be retried.
  *  The partial function `depends` provided may define the domain of both the success OR exceptional
  *  failure of a future fails explicitly.
  *
  *  {{{
  *  val policy = retry.When {
  *    case RetryAfter(retryAt) => retry.Pause(delay = retryAt)
  *  }
  *  val future = policy(issueRequest)
  *  }}}
  *
  *  If the result is not defined for the depends block, the future will not
  *  be retried.
  */
case class When(
    logger: TracedLogger,
    depends: PartialFunction[Any, Policy],
) extends Policy(logger) {

  override def apply[T](task: => Future[T], retryable: ExceptionRetryable)(implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[T] = {
    val fut = Future.fromTry(Try(task)).flatten
    fut
      .flatMap { res =>
        if (success.predicate(res) || !depends.isDefinedAt(res)) fut
        else depends(res)(task, retryable)
      }(directExecutionContext)
      .recoverWith { case NonFatal(e) =>
        if (depends.isDefinedAt(e) && retryable.retryOK(Failure(e), logger).maxRetries > 0)
          depends(e)(task, retryable)
        else fut
      }(directExecutionContext)
  }

  override def unlessShutdown[T](task: => FutureUnlessShutdown[T], retryOk: ExceptionRetryable)(
      implicit
      success: Success[T],
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[T] =
    FutureUnlessShutdown(apply(task.unwrap, retryOk)(Success.onShutdown, implicitly, implicitly))
}
