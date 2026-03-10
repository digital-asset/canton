// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.base.error.ErrorCode
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import org.scalactic.source
import org.scalatest.AppendedClues.*
import org.scalatest.Assertion
import org.scalatest.Checkpoints.Checkpoint
import org.scalatest.Inspectors.forAtLeast
import org.scalatest.matchers.should.Matchers.{include, *}
import org.slf4j.MDC
import org.slf4j.event.Level
import org.slf4j.event.Level.*
import org.slf4j.helpers.FormattingTuple

import scala.annotation.unused

final case class LogEntry(
    level: Level,
    loggerName: String,
    message: String,
    throwable: Option[Throwable] = None,
    mdc: Map[String, String] = LogEntry.copyMDC,
) extends PrettyPrinting {

  def errorMessage(implicit pos: source.Position): String = {
    if (level != ERROR) {
      fail(s"Incorrect log level $level. Expected: ERROR. Message: $message")
    }
    message
  }

  def warningMessage(implicit pos: source.Position): String = {
    if (level != WARN) {
      fail(s"Incorrect log level $level. Expected: WARN. Message: $message")
    }
    message
  }

  def infoMessage(implicit pos: source.Position): String = {
    if (level != INFO) {
      fail(s"Incorrect log level $level. Expected: INFO. Message: $message")
    }
    message
  }

  def debugMessage(implicit pos: source.Position): String = {
    if (level != DEBUG) {
      fail(s"Incorrect log level $level. Expected: DEBUG. Message: $message")
    }
    message
  }

  /** test if the message contains a specific error code */
  def shouldBeCantonErrorCode(code: ErrorCode)(implicit pos: source.Position): Assertion =
    this.message should include(code.id)

  /** test if a log message corresponds to a particular canton error
    *
    * @param errorCode
    *   the error code that should be checked
    * @param messageAssertion
    *   a check on the log entry's message text; this function receives the log entry's message text
    *   only, stripped of the error code
    * @param contextAssertion
    *   a check on the log entry's context map; the default is to not check anything
    */
  def shouldBeCantonError(
      errorCode: ErrorCode,
      messageAssertion: String => Assertion,
      contextAssertion: Map[String, String] => Assertion = _ => succeed,
      loggerAssertion: String => Assertion = _ => succeed,
  )(implicit pos: source.Position): Assertion = {
    // Decompose the log entry's message
    // NOTE: The format is defined by code in `ErrorCode`
    val (msgCode, msgDesc) = message match {
      case s"$a: $b" => (a, b)
      case _ => fail("Malformed log entry message")
    }

    // Check the error code and group ID
    msgCode should fullyMatch regex s"${errorCode.id}\\(${errorCode.category.asInt},.*\\)"

    // Check the message contents
    messageAssertion(msgDesc)

    // Check the context
    mdc.keySet should contain("location")
    contextAssertion(mdc)

    // Check the logger name
    loggerAssertion(loggerName)

  } withClue s"\n\nThe LogEntry is:\n$this\n\nand contains:\n- message: \"$message\"\n- context: $mdc"

  val CommandFailureLoggerNames: Seq[String] =
    Seq(
      "com.digitalasset.canton.integration.EnvironmentDefinition",
      "com.digitalasset.canton.integration.EnvironmentDefinition",
    )

  def shouldBeCommandFailure(code: ErrorCode, message: String = "")(implicit
      pos: source.Position
  ): Assertion = shouldBeOneOfCommandFailure(Seq(code), message)

  def shouldBeOneOfCommandFailure(codes: Seq[ErrorCode], message: String = "")(implicit
      pos: source.Position
  ): Assertion =
    forAtLeast(1, codes) { code =>
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

  override protected def pretty: Pretty[LogEntry] = prettyOfString {
    case entry @ LogEntry(level, _, message, maybeThrowable, mdc) =>
      val throwableStr = maybeThrowable match {
        case Some(t) => "\n" + ErrorUtil.messageWithStacktrace(t)
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
  import scala.jdk.CollectionConverters.*

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
    * @param mustContainWithClue
    *   describes entries that must occur inside of `entries`; the string component is a clue that
    *   will be output in case of failure
    * @param mayContain
    *   describes entries that may optionally occur inside of `entries`
    * @param entries
    *   the log entries to be checked
    * @return
    */
  def assertLogSeq(
      mustContainWithClue: Seq[(LogEntry => Assertion, String)],
      mayContain: Seq[LogEntry => Assertion] = Seq.empty,
  )(entries: Iterable[LogEntry])(implicit pos: source.Position): Assertion = {
    // Compute entries covered by no assertion
    // 1st element: the entry
    // 2nd element: all assertion failures for the entry
    val unexpectedLogEntries: Iterable[(LogEntry, Checkpoint)] = for {
      entry <- entries
      checkpoint <- {
        val checkpoint = new Checkpoint
        var success = false

        def go(assertions: Seq[(LogEntry => Assertion, String)]): Unit =
          for {
            (assertion, clue) <- assertions
            if !success
          }
            checkpoint {
              withClue(s"\tAssertion $clue: ") {
                assertion(entry)
              }
              success = true
            }

        go(mustContainWithClue.map { case (assertion, clue) =>
          assertion -> clue.doubleQuoted.toString
        })
        go(mayContain.zipWithIndex.map { case (assertion, index) =>
          assertion -> show"mayContain[$index]"
        })

        if (success) Seq.empty else Seq(checkpoint)
      }

    } yield entry -> checkpoint

    // Compute assertions in mustContainWithClue that do not match any log entry.
    // 1st element: the clue of the assertion
    // 2nd element: failures of the assertion on all log entries
    val assertionsNotMatchingAnyLogEntry: Seq[(String, Checkpoint)] = for {
      (assertion, clue) <- mustContainWithClue
      checkpoint <- {
        val checkpoint = new Checkpoint
        var success = false
        for {
          (entry, index) <- entries.zipWithIndex
          if !success
        } checkpoint {
          withClue(show"\tEntry $index: ") {
            assertion(entry)
          }
          success = true
        }

        if (success) Seq.empty else Seq(checkpoint)
      }

    } yield clue -> checkpoint

    val globalCheckpoint = new Checkpoint

    // Summary report for unexpected log entries
    if (unexpectedLogEntries.nonEmpty) {
      val unexpectedEntriesStr =
        unexpectedLogEntries.map { case (entry, _) => entry }.mkShow("\n\t", "\n\t", "\n")
      globalCheckpoint(
        fail(
          show"\nUnexpected log entries:$unexpectedEntriesStr"
        )
      )
    }

    // Summary report for assertions not matching any log entry
    if (assertionsNotMatchingAnyLogEntry.nonEmpty) {
      val missingEntriesStr = assertionsNotMatchingAnyLogEntry
        .map { case (clue, _) => clue }
        .mkShow("\n\t", "\n\t", "\n")
      globalCheckpoint(
        fail(show"\nAssertions not matching any log entry:$missingEntriesStr")
      )
    }

    // Details for unexpected log entries
    unexpectedLogEntries.foreach { case (entry, checkpoint) =>
      globalCheckpoint {
        withClue(show"\nDetails for unexpected entry: $entry\n") {
          checkpoint.reportAll()
        } withClue "\n"
      }
    }

    // Details for assertions not matching any log entry
    assertionsNotMatchingAnyLogEntry.foreach { case (clue, checkpoint) =>
      globalCheckpoint {
        withClue(show"\nDetails for assertion ${clue.doubleQuoted} without matching log entry:\n") {
          checkpoint.reportAll()
        } withClue "\n"
      }
    }

    // All log entries
    if (unexpectedLogEntries.nonEmpty || assertionsNotMatchingAnyLogEntry.nonEmpty) {
      globalCheckpoint(fail(s"\nAll log entries:${LogEntry.format(entries)}\n"))
    }

    // Finally, print all reports
    globalCheckpoint.reportAll()
    succeed
  }

  /** Helper to pprint logs to help with debugging Usage is to LogEntry.pprintLogs andThen
    * LogEntry.assertLogSeq(...
    */
  @unused def pprintLogSeq(entries: Iterable[LogEntry]): Iterable[LogEntry] = {
    println("=== Log Entries ===")
    entries.foreach(pprint.pprintln(_))
    entries
  }

  val SECURITY_SENSITIVE_MESSAGE_ON_API =
    "An error occurred. Please contact the operator and inquire about the request"
}
