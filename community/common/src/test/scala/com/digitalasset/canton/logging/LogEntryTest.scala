// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.BaseTest
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

class LogEntryTest extends AnyWordSpec with BaseTest {

  val ex = new RuntimeException("test exception")

  "LogEntry" should {
    "pretty print nicely" in {
      val mdc = Map(CanLogTraceContext.traceIdMdcKey -> "mytraceid", "otherKey" -> "otherValue")

      val entry =
        LogEntry(
          Level.WARN,
          "com.digitalasset.canton.MyClass:MyNode",
          "message line 1\nmessage line 2",
          Some(ex),
          mdc,
        )

      entry.toString should startWith(
        """## WARN  c.d.canton.MyClass:MyNode tid:mytraceid - message line 1
          |## 	message line 2
          |## 	MDC: otherKey -> otherValue
          |## 	java.lang.RuntimeException: test exception
          |## 		at com.digitalasset.canton.logging.LogEntryTest.<init>""".stripMargin
      )
    }

    "mention incorrect log level" in {
      val entry = LogEntry(Level.WARN, "", "test")

      val failure = the[TestFailedException] thrownBy entry.errorMessage
      failure.message.value shouldBe
        """Incorrect log level WARN. Expected: ERROR. Message: test""".stripMargin
    }

    "mention incorrect log level and logger" in {
      val entry = LogEntry(Level.WARN, "MyLogger", "test")

      val failure = the[TestFailedException] thrownBy entry.commandFailureMessage
      failure.message.value shouldBe
        """Incorrect log level WARN. Expected: ERROR
          |Incorrect logger name MyLogger. Expected one of:
          |  com.digitalasset.canton.integration.EnvironmentDefinition, com.digitalasset.canton.integration.EnvironmentDefinition
          |## WARN  MyLogger - test""".stripMargin
    }

    "produce nice reports for unexpected log entries" in {
      val failure = the[TestFailedException] thrownBy LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          (_.warningMessage shouldBe "line 1", "nudelsalat")
        ),
        mayContain = Seq(_.warningMessage shouldBe "line 2"),
      )(Seq(LogEntry(Level.WARN, "", "line 1"), LogEntry(Level.WARN, "", "line 3")))

      failure.message.value.replaceAll("LogEntryTest\\.scala:\\d+", "LogEntryTest.scala") shouldBe """
                                       |Unexpected log entries:
                                       |	## WARN   - line 3
                                       | (in Checkpoint) at LogEntryTest.scala
                                       |
                                       |Details for unexpected entry: ## WARN   - line 3
                                       |	Assertion "nudelsalat": "line [3]" was not equal to "line [1]" (in Checkpoint) at LogEntryTest.scala
                                       |	Assertion mayContain[0]: "line [3]" was not equal to "line [2]" (in Checkpoint) at LogEntryTest.scala
                                       | (in Checkpoint) at LogEntryTest.scala
                                       |
                                       |All log entries:
                                       |	## WARN   - line 1
                                       |	## WARN   - line 3
                                       |
                                       | (in Checkpoint) at LogEntryTest.scala""".stripMargin
    }

    "produce nice reports for assertions without a matching log entry" in {
      val failure = the[TestFailedException] thrownBy LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          (_.warningMessage shouldBe "line 1", "nudelsalat")
        ),
        mayContain = Seq(_.warningMessage shouldBe "line 2"),
      )(Seq(LogEntry(Level.WARN, "", "line 2")))

      failure.message.value.replaceAll("LogEntryTest\\.scala:\\d+", "LogEntryTest.scala") shouldBe """
                                       |Assertions not matching any log entry:
                                       |	nudelsalat
                                       | (in Checkpoint) at LogEntryTest.scala
                                       |
                                       |Details for assertion "nudelsalat" without matching log entry:
                                       |	Entry 0: "line [2]" was not equal to "line [1]" (in Checkpoint) at LogEntryTest.scala
                                       | (in Checkpoint) at LogEntryTest.scala
                                       |
                                       |All log entries:
                                       |	## WARN   - line 2
                                       |
                                       | (in Checkpoint) at LogEntryTest.scala""".stripMargin
    }

    "produce nice reports for unexpected log entries and assertions without a matching log entry" in {
      val failure = the[TestFailedException] thrownBy LogEntry.assertLogSeq(
        mustContainWithClue = Seq(
          (_.warningMessage shouldBe "line 1", "nudelsalat")
        ),
        mayContain = Seq(_.warningMessage shouldBe "line 2"),
      )(Seq(LogEntry(Level.WARN, "", "line 2"), LogEntry(Level.WARN, "", "line 3")))

      failure.message.value.replaceAll("LogEntryTest\\.scala:\\d+", "LogEntryTest.scala") shouldBe """
                                       |Unexpected log entries:
                                       |	## WARN   - line 3
                                       | (in Checkpoint) at LogEntryTest.scala
                                       |
                                       |Assertions not matching any log entry:
                                       |	nudelsalat
                                       | (in Checkpoint) at LogEntryTest.scala
                                       |
                                       |Details for unexpected entry: ## WARN   - line 3
                                       |	Assertion "nudelsalat": "line [3]" was not equal to "line [1]" (in Checkpoint) at LogEntryTest.scala
                                       |	Assertion mayContain[0]: "line [3]" was not equal to "line [2]" (in Checkpoint) at LogEntryTest.scala
                                       | (in Checkpoint) at LogEntryTest.scala
                                       |
                                       |Details for assertion "nudelsalat" without matching log entry:
                                       |	Entry 0: "line [2]" was not equal to "line [1]" (in Checkpoint) at LogEntryTest.scala
                                       |	Entry 1: "line [3]" was not equal to "line [1]" (in Checkpoint) at LogEntryTest.scala
                                       | (in Checkpoint) at LogEntryTest.scala
                                       |
                                       |All log entries:
                                       |	## WARN   - line 2
                                       |	## WARN   - line 3
                                       |
                                       | (in Checkpoint) at LogEntryTest.scala""".stripMargin
    }
  }
}
