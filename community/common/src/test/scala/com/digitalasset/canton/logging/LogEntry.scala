// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.daml.error.ErrorCode
import com.digitalasset.canton.error.BaseCantonError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil._
import org.scalactic.source
import org.scalatest.AppendedClues._
import org.scalatest.Assertion
import org.scalatest.Inspectors.{forAll, forAtLeast, forEvery}
import org.scalatest.matchers.should.Matchers.{include, _}
import org.slf4j.MDC
import org.slf4j.event.Level
import org.slf4j.event.Level._
import org.slf4j.helpers.FormattingTuple

case class LogEntry(
    level: Level,
    loggerName: String,
    message: String,
    throwable: Option[Throwable] = None,
    mdc: Map[String, String] = LogEntry.copyMDC,
) extends PrettyPrinting {

  def errorMessage(implicit pos: source.Position): String = {
    if (level != ERROR) {
      fail(s"Incorrect log level $level. Expected: ERROR\n$this")
    }
    message
  }

  def warningMessage(implicit pos: source.Position): String = {
    if (level != WARN) {
      fail(s"Incorrect log level $level. Expected: WARN\n$this")
    }
    message
  }

  /** test if the message contains a specific error code */
  def shouldBeCantonErrorCode(code: ErrorCode)(implicit pos: source.Position): Assertion = {
    this.message should include(code.id)
  }

  /** test if a log message corresponds to a particular canton error
    *
    * @param additionalContext additional context that should be checked. it can be used to remove an argument from
    *                          the context assertion by supplying an empty string for that particular key
    * @param strict            if strict is true, then the context arguments will be evaluated in a strict sense (need to match).
    *                          if strict is false, the context values passed need to "include" the passed context arguments
    */
  def shouldBeCantonError(
      err: BaseCantonError,
      additionalContext: Map[String, String] = Map.empty,
      strict: Boolean = true,
  )(implicit pos: source.Position): Assertion = {
    // first, test the error message
    this.message should include(err.code.id)
    if (!err.code.category.securitySensitive) { // TODO(i10133): we can't just ignore the message!
      this.message should include(err.cause)
    }
    // test context
    forAll((err.context ++ additionalContext).filter(_._2.nonEmpty)) {
      // getOrElse will not fail unless BaseError is faulty
      case (key, value) =>
        val item = mdc.getOrElse(key, s"MISSING ${key}")
        if (strict)
          item shouldBe value
        else
          item should include(value)
    }
    succeed
  }

  val CommandFailureLoggerNames: Seq[String] =
    Seq(
      "com.digitalasset.canton.integration.CommunityEnvironmentDefinition",
      "com.digitalasset.canton.integration.EnterpriseEnvironmentDefinition",
    )

  def shouldBeCommandFailure(code: ErrorCode, message: String = "")(implicit
      pos: source.Position
  ): Assertion = {
    commandFailureMessage should (include(code.id) and include(message))
  }

  def commandFailureMessage(implicit pos: source.Position): String = {
    val errors = new StringBuilder()

    if (level != ERROR) {
      errors ++= s"Incorrect log level $level. Expected: ERROR\n"
    }

    if (CommandFailureLoggerNames.forall(!loggerName.startsWith(_))) {
      errors ++=
        s"""Incorrect logger name $loggerName. Expected one of:
           |  ${CommandFailureLoggerNames.mkString(", ")}
           |""".stripMargin
    }

    if (errors.nonEmpty) {
      errors ++= toString
      fail(errors.toString())
    }

    message
  }

  override def pretty: Pretty[LogEntry] = prettyOfString {
    case entry @ LogEntry(level, _, message, maybeThrowable, mdc) =>
      val throwableStr = maybeThrowable match {
        case Some(throwable) => "\n" + ErrorUtil.messageWithStacktrace(throwable)
        case None => ""
      }

      val optTraceId = mdc.get(CanLogTraceContext.traceIdMdcKey)
      val traceIdStr = optTraceId match {
        case Some(traceId) => s" tid:$traceId"
        case None => ""
      }

      val remainingMdc = mdc - CanLogTraceContext.traceIdMdcKey
      val remainingMdcStr =
        if (remainingMdc.isEmpty) ""
        else s"\nMDC: ${remainingMdc.map { case (key, value) => s"$key -> $value" }.mkString(", ")}"

      f"$level%-5s ${entry.loggerName.readableLoggerName(30)}$traceIdStr - $message$remainingMdcStr$throwableStr"
        .split('\n')
        .mkString("## ", "\n## \t", "")
  }
}

object LogEntry {
  import scala.jdk.CollectionConverters._

  private[logging] def copyMDC: Map[String, String] =
    Option(MDC.getCopyOfContextMap).map(_.asScala.toMap).getOrElse(Map.empty)

  def apply(level: Level, loggerName: String, formattingTuple: FormattingTuple): LogEntry =
    new LogEntry(
      level,
      loggerName,
      formattingTuple.getMessage,
      Option(formattingTuple.getThrowable),
    )

  def apply(level: Level, loggerName: String, message: String, throwable: Throwable): LogEntry =
    new LogEntry(level, loggerName, message, Option(throwable))

  def format(entries: IterableOnce[LogEntry]): String =
    entries.iterator.mkString("\n\t", "\n\t", "\n")

  /** Verifies a sequence of log entries.
    *
    * @param mustContainWithClue describes entries that must occur inside of `entries`;
    *                            the string component is a clue that will be output in case of failure
    * @param mayContain describes entries that may optionally occur inside of `entries`
    * @param entries the log entries to be checked
    * @return
    */
  def assertLogSeq(
      mustContainWithClue: Seq[(LogEntry => Assertion, String)],
      mayContain: Seq[LogEntry => Assertion],
  )(entries: Iterable[LogEntry]): Assertion = {
    val mustContain = mustContainWithClue.map { case (assertion, _) => assertion }

    forEvery(entries) { entry =>
      withClue(show"Unexpected log entry:\n\t$entry") {
        forAtLeast(1, mustContain ++ mayContain) { assertion => assertion(entry) }
      }
    }

    forEvery(mustContainWithClue) { case (assertion, clue) =>
      withClue(s"Missing log entry: $clue") {
        forAtLeast(1, entries) { entry => assertion(entry) }
      }
    }
  } withClue s"\n\nAll log entries:${LogEntry.format(entries)}"
}
