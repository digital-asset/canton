// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import cats.data.{EitherT, OptionT}
import com.digitalasset.canton.concurrent.{DirectExecutionContext, ExecutionContextMonitor}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.Thereafter.syntax._
import org.scalactic.source
import org.scalatest.AppendedClues._
import org.scalatest.Assertion
import org.scalatest.Inside._
import org.scalatest.Inspectors._
import org.scalatest.matchers.should.Matchers._
import org.slf4j.Logger
import org.slf4j.event.Level
import org.slf4j.event.Level.{ERROR, WARN}

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/** A version of [[NamedLoggerFactory]] that allows suppressing and intercepting warnings and errors in the log.
  * Intended for testing.
  *
  * Suppressed warnings and errors will still be logged, but with level `INFO` and the prefix `"Suppressed WARN: "`
  * or `"Suppressed ERROR: "`.
  *
  * The methods of this class will check whether the computation during which logging is suppressed yields a future.
  * If so, suppression will finish after completion of the future (instead of after creation of the future).
  *
  * Nested suppression / recording of log messages is not supported to keep the implementation simple.
  *
  * Only affects logging by Canton code.
  * To suppress logging in libraries configure a [[com.digitalasset.canton.logging.Rewrite]] rule in `logback-test.xml`.
  *
  * @param skipLogEntry Log entries satisfying this predicate are suppressed, but not included in the queue of suppressed log entries.
  */
class SuppressingLogger private[logging] (
    underlyingLogger: NamedLoggerFactory,
    pollTimeout: FiniteDuration,
    skipLogEntry: LogEntry => Boolean,
    activeSuppressionRule: AtomicReference[SuppressionRule] =
      new AtomicReference[SuppressionRule](SuppressionRule.NoSuppression),
    private[logging] val recordedLogEntries: java.util.concurrent.BlockingQueue[LogEntry] =
      new java.util.concurrent.LinkedBlockingQueue[LogEntry](),
) extends NamedLoggerFactory {

  /** Logger used to log internal problems of this class.
    */
  private val internalLogger: Logger = underlyingLogger.getLogger(getClass)

  private val directExecutionContext: ExecutionContext = DirectExecutionContext(
    TracedLogger(internalLogger)
  )

  private val SuppressionPrefix: String = "Suppressed %s: %s"

  override val name: String = underlyingLogger.name
  override val properties: ListMap[String, String] = underlyingLogger.properties

  override def appendUnnamedKey(key: String, value: String): NamedLoggerFactory =
    // intentionally share suppressedLevel and queues so suppression on a parent logger will effect a child and collect all suppressed messages
    new SuppressingLogger(
      underlyingLogger.appendUnnamedKey(key, value),
      pollTimeout,
      skipLogEntry,
      activeSuppressionRule,
      recordedLogEntries,
    )

  override def append(key: String, value: String): SuppressingLogger =
    // intentionally share suppressedLevel and queues so suppression on a parent logger will effect a child and collect all suppressed messages
    new SuppressingLogger(
      underlyingLogger.append(key, value),
      pollTimeout,
      skipLogEntry,
      activeSuppressionRule,
      recordedLogEntries,
    )

  override private[logging] def getLogger(fullName: String): Logger = {
    val actualLogger = underlyingLogger.getLogger(fullName)
    val suppressedMessageLogger = new BufferingLogger(recordedLogEntries, fullName, skipLogEntry(_))

    val logger =
      new SuppressingLoggerDispatcher(
        actualLogger.getName,
        suppressedMessageLogger,
        activeSuppressionRule,
        SuppressionPrefix,
      )
    logger.setDelegate(actualLogger)
    logger
  }

  def assertThrowsAndLogs[T <: Throwable](
      within: => Any,
      assertions: (LogEntry => Assertion)*
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    assertLogs(checkThrowable[T](the[Throwable] thrownBy within), assertions: _*)

  def assertThrowsAndLogsAsync[T <: Throwable](
      within: => Future[_],
      assertion: T => Assertion,
      entryChecks: (LogEntry => Assertion)*
  )(implicit c: ClassTag[T], pos: source.Position): Future[Assertion] =
    assertLogs(
      within.transform {
        case Success(_) =>
          fail(s"An exception of type $c was expected, but no exception was thrown.")
        case Failure(c(t)) => Success(assertion(t))
        case Failure(t) => fail(s"Exception has wrong type. Expected type: $c. Got: $t.", t)
      }(directExecutionContext),
      entryChecks: _*
    )

  def assertThrowsAndLogsSeq[T <: Throwable](
      within: => Any,
      assertion: Seq[LogEntry] => Assertion,
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    checkThrowable[T](assertLoggedWarningsAndErrorsSeq(the[Throwable] thrownBy within, assertion))

  def assertThrowsAndLogsUnordered[T <: Throwable](
      within: => Any,
      assertions: (LogEntry => Assertion)*
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    checkThrowable[T](assertLogsUnordered(the[Throwable] thrownBy within, assertions: _*))

  private def checkThrowable[T <: Throwable](
      within: => Throwable
  )(implicit c: ClassTag[T], pos: source.Position): Assertion =
    inside(within) {
      case _: T => succeed
      case t: Throwable => fail(s"The throwable has an incorrect type ${t.getClass}.")
    }

  def assertInternalError[T <: Throwable](within: => Any, assertion: T => Assertion)(implicit
      c: ClassTag[T],
      pos: source.Position,
  ): Assertion = {
    assertLogs(
      {
        val t = the[T] thrownBy within
        assertion(t) withClue ErrorUtil.messageWithStacktrace(t)
      },
      checkLogsInternalError(assertion),
    )
  }

  def checkLogsInternalError[T <: Throwable](
      assertion: T => Assertion
  )(entry: LogEntry)(implicit c: ClassTag[T], pos: source.Position): Assertion = {
    entry.errorMessage shouldBe ErrorUtil.internalErrorMessage
    entry.throwable match {
      case Some(c(t)) => assertion(t) withClue ErrorUtil.messageWithStacktrace(t)
      case Some(t) => fail(s"Internal error logging throwable of wrong type. Expected type: $c.", t)
      case None => fail("Internal error not logging a throwable.")
    }
  }

  def assertInternalErrorAsync[T <: Throwable](
      within: => Future[_],
      assertion: T => Assertion,
  )(implicit c: ClassTag[T], pos: source.Position): Future[Assertion] =
    assertLogs(
      within.transform {
        case Success(_) =>
          fail(s"An exception of type $c was expected, but no exception was thrown.")
        case Failure(c(t)) => Success(assertion(t))
        case Failure(t) => fail(s"Exception has wrong type. Expected type: $c.", t)
      }(directExecutionContext),
      checkLogsInternalError(assertion),
    )

  /** Asserts that the sequence of logged warnings/errors meets a given sequence of assertions.
    * Use this if the expected sequence of logged warnings/errors is deterministic.
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * The method will delete logged messages up to and including the first message on which an assertion fails.
    *
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogs[A](within: => A, assertions: (LogEntry => Assertion)*)(implicit
      pos: source.Position
  ): A =
    suppress(SuppressionRule.LevelAndAbove(WARN)) {
      runWithCleanup(
        {
          within
        },
        { () =>
          // check the log

          // Check that every assertion succeeds on the corresponding log entry
          forEvery(assertions) { assertion =>
            // Poll with a timeout to wait for asynchronously logged messages,
            // i.e., logged by: myFuture.onFailure{ case t => logger.error("...", t) }
            Option(recordedLogEntries.poll(pollTimeout.length, pollTimeout.unit)) match {
              case Some(logEntry) =>
                assertion(logEntry) withClue s"\n\nRemaining log entries:${LogEntry.format(recordedLogEntries.asScala)}"
              case None => fail(s"Missing log message.")
            }
          }

          // Check that there are no unexpected log entries
          if (recordedLogEntries.asScala.nonEmpty) {
            fail(s"Found unexpected log messages:${LogEntry.format(recordedLogEntries.asScala)}")
          }
        },
        () => (),
      )
    }

  /** Overload of `assertLogsSeq` that defaults to only capturing Warnings and Errors. */
  def assertLoggedWarningsAndErrorsSeq[A](within: => A, assertion: Seq[LogEntry] => Assertion): A =
    assertLogsSeq(SuppressionRule.LevelAndAbove(WARN))(within, assertion)

  /** Asserts that the sequence of logged warnings/errors meets a given assertion.
    * Use this if the expected sequence of logged warnings/errors is non-deterministic.
    *
    * On success, the method will delete all logged messages. So this method is not idempotent.
    *
    * On failure, the method will not delete any logged message to support retrying with [[com.digitalasset.canton.BaseTest#eventually]].
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogsSeq[A](
      rule: SuppressionRule
  )(within: => A, assertion: Seq[LogEntry] => Assertion): A =
    suppress(rule) {
      runWithCleanup(
        {
          within
        },
        { () =>
          // check the log

          val logEntries = recordedLogEntries.asScala.toSeq

          assertion(logEntries)

          // Remove checked log entries only if check succeeds.
          // This is to allow for retries, if the check fails.
          logEntries.foreach(_ => recordedLogEntries.remove())
        },
        () => (),
      )
    }

  /** Asserts that the sequence of logged warnings/errors meets a set of expected log messages. */
  def assertLogsSeqString[A](rule: SuppressionRule, expectedLogs: Seq[String])(within: => A): A =
    assertLogsSeq(rule)(
      within,
      logs =>
        forEvery(logs)(log =>
          assert(
            expectedLogs.exists(msg => log.message.contains(msg)),
            s"line $log contained unexpected log",
          )
        ),
    )

  /** Asserts that the sequence of logged warnings/errors matches a set of assertions.
    * Use this if the order of logged warnings/errors is nondeterministic.
    * Matching the assertions against the logged messages is sequential in the order of the assertions.
    * That is, more specific assertions must be listed earlier.
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * The method will delete as many logged messages as there are assertions.
    *
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogsUnordered[A](within: => A, assertions: (LogEntry => Assertion)*)(implicit
      pos: source.Position
  ): A =
    assertLogsUnorderedOptional(within, assertions.map(LogEntryOptionality.Required -> _): _*)

  /** Asserts that the sequence of logged warnings/errors matches a set of assertions.
    * Use this if the order of logged warnings/errors is nondeterministic and some of them are optional.
    * Matching the assertions against the logged messages is sequential in the order of the assertions.
    * That is, more specific assertions must be listed earlier.
    *
    * This method will automatically use asynchronous suppression if `A` is `Future[_]`.
    *
    * The method will delete as many logged messages as there are assertions.
    *
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`, because the method cannot detect
    *                                            whether asynchronous suppression is needed in this case.
    *                                            Use `EitherT.value` or `OptionT`.value to work around this.
    */
  def assertLogsUnorderedOptional[A](
      within: => A,
      assertions: (LogEntryOptionality, LogEntry => Assertion)*
  )(implicit pos: source.Position): A =
    suppress(SuppressionRule.LevelAndAbove(WARN)) {
      runWithCleanup(
        within,
        () => {
          val unmatchedAssertions =
            mutable.SortedMap[Int, (LogEntryOptionality, LogEntry => Assertion)]() ++
              assertions.zipWithIndex.map { case (assertion, index) => index -> assertion }
          val matchedLogEntries = mutable.ListBuffer[(Int, LogEntry)]()
          val unexpectedLogEntries = mutable.ListBuffer[LogEntry]()

          @tailrec
          def checkLogEntries(): Unit =
            Option(recordedLogEntries.poll(pollTimeout.length, pollTimeout.unit)) match {
              case Some(logEntry) =>
                unmatchedAssertions.collectFirst {
                  case (index, (_required, assertion)) if Try(assertion(logEntry)).isSuccess =>
                    index
                } match {
                  case Some(index) =>
                    unmatchedAssertions.remove(index)
                    matchedLogEntries += index -> logEntry
                  case None => unexpectedLogEntries += logEntry
                }
                checkLogEntries()
              case None => ()
            }

          checkLogEntries()

          val errors = mutable.ListBuffer[String]()

          if (unexpectedLogEntries.nonEmpty) {
            errors += s"Found unexpected log messages:${LogEntry.format(unexpectedLogEntries)}"
          }

          val requiredUnmatchedAssertions = unmatchedAssertions.filter { case (_, (required, _)) =>
            required == LogEntryOptionality.Required
          }
          if (requiredUnmatchedAssertions.nonEmpty) {
            errors += s"No log message has matched the assertions with index ${requiredUnmatchedAssertions.keys
              .mkString(", ")}.\n"
          }

          if (errors.nonEmpty) {
            errors += s"Matched log messages:${matchedLogEntries
              .map { case (index, entry) => s"$index:\t$entry" }
              .mkString("\n", "\n", "\n")}"

            fail(errors.mkString("\n"))
          }
        },
        () => (),
      )
    }

  def numberOfRecordedEntries: Int = recordedLogEntries.size()

  def pollRecordedLogEntry(timeout: FiniteDuration = pollTimeout): Option[LogEntry] =
    Option(recordedLogEntries.poll(timeout.length, timeout.unit))

  def fetchRecordedLogEntries: Seq[LogEntry] = recordedLogEntries.asScala.toSeq

  /** Use this only in very early stages of development.
    * Try to use [[assertLogs]] instead which lets you specify the specific messages that you expected to suppress.
    * This avoids the risk of hiding unrelated warnings and errors.
    */
  def suppressWarnings[A](within: => A): A = suppress(SuppressionRule.Level(WARN))(within)

  /** Use this only in very early stages of development.
    * Try to use [[assertLogs]] instead which lets you specify the specific messages that you expected to suppress.
    * This avoids the risk of hiding unrelated warnings and errors.
    */
  def suppressWarningsAndErrors[A](within: => A): A =
    suppress(SuppressionRule.LevelAndAbove(WARN))(within)

  /** Use this only in very early stages of development.
    * Try to use [[assertLogs]] instead which lets you specify the specific messages that you expected to suppress.
    * This avoids the risk of hiding unrelated warnings and errors.
    */
  def suppressErrors[A](within: => A): A = suppress(SuppressionRule.Level(ERROR))(within)

  /** Use this only in very early stages of development.
    * Try to use [[assertThrowsAndLogs]] instead which lets you specify the specific messages that you expected to suppress.
    * This avoids the risk of hiding unrelated warnings and errors.
    */
  def suppressWarningsErrorsExceptions[T <: Throwable](
      within: => Any
  )(implicit classTag: ClassTag[T], pos: source.Position): Any = suppressWarningsAndErrors {
    a[T] should be thrownBy within
  }

  def suppress[A](rule: SuppressionRule)(within: => A): A = {
    val endSuppress = beginSuppress(rule)
    runWithCleanup(within, () => (), endSuppress)
  }

  /** First runs `body`, `onSuccess`, and then `doFinally`.
    * Runs `onSuccess` after `body` if `body` completes successfully
    * Runs `doFinally` after `body` and `onSuccess`, even if they terminate abruptly.
    *
    * @return The result of `body`. If `body` is of type `Future[_]`, a future with the same result as `body` that
    *         completes after completion of `doFinally`.
    * @throws java.lang.IllegalArgumentException if `T` is `EitherT` or `OptionT`
    */
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.AsInstanceOf"))
  private def runWithCleanup[T](body: => T, onSuccess: () => Unit, doFinally: () => Unit): T = {
    var isAsync = false
    try {
      // Run the computation in body
      val result = body
      result match {
        case asyncResult: Future[_] =>
          implicit val ec: ExecutionContext = directExecutionContext

          // Cleanup after completion of the future.
          val asyncResultWithCleanup = asyncResult
            .map(r => {
              onSuccess()
              r
            })
            .thereafter(_ => doFinally())

          // Switch off cleanup in finally block, as that would be performed too early
          isAsync = true

          // Return future that completes after termination of body and cleanup
          asyncResultWithCleanup.asInstanceOf[T]

        case _: EitherT[_, _, _] | _: OptionT[_, _] =>
          throw new IllegalArgumentException(
            "Suppression for EitherT and OptionT is currently not supported. Please unwrap the result by calling `_.value`."
          )

        // Unable to support this, because we can't access the first type parameter.
        // Therefore, we don't know whether the suppression needs to be asynchronous.

        case syncResult =>
          onSuccess()
          syncResult
      }
    } finally {
      if (!isAsync)
        // Cleanup, if body fails synchronously, i.e.
        // - body is not of type Future, or
        // - body is of type Future, but it terminates abruptly without constructing a future
        doFinally()
    }
  }

  private def beginSuppress(rule: SuppressionRule): () => Unit = {
    withClue("Trying to suppress warnings/errors several times") {
      // Nested usages are not supported, because we clear the message queue when the suppression begins.
      // So a second call of this method would purge the messages collected by previous calls.

      val previousRule = activeSuppressionRule.getAndUpdate { (previous: SuppressionRule) =>
        if (previous == SuppressionRule.NoSuppression) rule else previous
      }
      previousRule shouldBe SuppressionRule.NoSuppression
    }

    recordedLogEntries.clear()

    () => activeSuppressionRule.set(SuppressionRule.NoSuppression)
  }
}

object SuppressingLogger {
  def apply(
      testClass: Class[_],
      pollTimeout: FiniteDuration = 1.second,
      skipLogEntry: LogEntry => Boolean = defaultSkipLogEntry,
  ): SuppressingLogger =
    new SuppressingLogger(
      NamedLoggerFactory.unnamedKey("test", testClass.getSimpleName),
      pollTimeout,
      skipLogEntry = skipLogEntry,
    )

  case class LogEntryCriterion(level: Level, loggerNamePrefix: String, pattern: Regex) {
    def matches(logEntry: LogEntry): Boolean =
      logEntry.level == level &&
        logEntry.loggerName.startsWith(loggerNamePrefix) &&
        (logEntry.message match {
          case pattern(_*) => true
          case _ => false
        })
  }

  def assertThatLogDoesntContainUnexpected(
      expectedProblems: List[String]
  )(logs: Seq[LogEntry]): Assertion = {
    forEvery(logs) { x =>
      assert(
        expectedProblems.exists(msg => x.toString.contains(msg)),
        s"line $x contained unexpected problem",
      )
    }
  }

  /** Lists criteria for log entries that are skipped during suppression in all test cases */
  val skippedLogEntries: Seq[LogEntryCriterion] = Seq(
    LogEntryCriterion(
      WARN,
      classOf[ExecutionContextMonitor].getName,
      ("""(?s)Task runner .* is .* overloaded.*""").r,
    )
  )

  def defaultSkipLogEntry(logEntry: LogEntry): Boolean =
    skippedLogEntries.exists(_.matches(logEntry))

  sealed trait LogEntryOptionality extends Product with Serializable
  object LogEntryOptionality {
    case object Required extends LogEntryOptionality
    case object Optional extends LogEntryOptionality
  }
}
