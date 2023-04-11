// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.daml.scalautil.Statement
import com.digitalasset.canton.testing.TestingLogCollector.{Entry, ThrowableCause, ThrowableEntry}
import org.scalatest.Checkpoints.Checkpoint
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{AppendedClues, Assertion, OptionValues}
import org.slf4j.Marker

import scala.beans.BeanProperty
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.reflect.ClassTag

// An appender and a logger must be defined in the logback.xml or logback-test.xml to capture the log entries.
object TestingLogCollector {

  final case class ThrowableCause(className: String, message: String)

  final case class ThrowableEntry(
      className: String,
      message: String,
      causeO: Option[ThrowableCause] = None,
  )
  final case class Entry(
      level: Level,
      msg: String,
      marker: Option[Marker],
      throwableEntryO: Option[ThrowableEntry] = None,
  )
  final case class ExpectedLogEntry(level: Level, msg: String, markerRegex: Option[String])

  private val log = TrieMap.empty[String, TrieMap[String, mutable.Builder[Entry, Vector[Entry]]]]

  /** This method is deemed unsafe as it does not guarantee visibility over all the log events
    * issued before its call and it must be retried until all expected logs are present.
    * For convenience, use [[LoggingAssertions.assertLogEntries]].
    *
    * Explanation: During the initialization process of the static LoggerFactory internals,
    * calls to [[org.slf4j.LoggerFactory.getLogger]] can return [[org.slf4j.helpers.SubstituteLogger]]
    * logger instances that defer emitting events to their appenders (see https://www.slf4j.org/codes.html#replay).
    * As a result, logging calls to such loggers trigger the [[TestingLogCollector.append]]
    * sometime later (after the static LoggerFactory initialization finished and all events have been replayed)
    * and, in result, can break the happens-before relationship with this method.
    */
  private[testing] def unsafeReadAsEntries[Test](loggerName: String)(implicit
      test: ClassTag[Test]
  ): Seq[Entry] =
    log
      .get(test.runtimeClass.getName.stripSuffix("$"))
      .flatMap(_.get(loggerName))
      .fold(IndexedSeq.empty[Entry])(_.result())

  def clear[Test](implicit test: ClassTag[Test]): Unit = {
    log.remove(test.runtimeClass.getName)
    ()
  }
}

final class TestingLogCollector extends AppenderBase[ILoggingEvent] {

  @BeanProperty
  var test: String = _

  override def append(e: ILoggingEvent): Unit = {
    if (test == null) {
      addError("Test identifier undefined, skipping logging")
    } else {
      val log = TestingLogCollector.log
        .getOrElseUpdate(test, TrieMap.empty)
        .getOrElseUpdate(e.getLoggerName, Vector.newBuilder)
      val _ = log.synchronized {
        val throwableO = Option(e.getThrowableProxy)
        val causeEntryO = throwableO
          .flatMap(t => Option(t.getCause))
          .map(cause => ThrowableCause(cause.getClassName, cause.getMessage))
        val throwableEntryO =
          throwableO.map(t => ThrowableEntry(t.getClassName, t.getMessage, causeEntryO))
        log += Entry(
          e.getLevel,
          e.getMessage,
          Option(e.getMarker),
          throwableEntryO = throwableEntryO,
        )
      }
    }
  }
}

trait LoggingAssertions extends OptionValues with AppendedClues {
  self: Matchers
    with Eventually
    // Guarantee enough headroom for the LoggerFactory initialization
    with IntegrationPatience =>

  def assertLogEntries[Test, Logger](
      assert: Seq[Entry] => Assertion
  )(implicit
      _test: ClassTag[Test],
      logger: ClassTag[Logger],
  ): Assertion =
    assertLogEntries[Test](logger.runtimeClass.getName.stripSuffix("$"))(assert)

  def assertLogEntries[Test: ClassTag](loggerName: String)(
      assert: Seq[Entry] => Assertion
  ): Assertion =
    eventually(assert(TestingLogCollector.unsafeReadAsEntries[Test](loggerName)))

  /** @param expectedMarkerAsString use "<line-number>" where a line number would've been expected
    */
  def assertSingleLogEntry[Test: ClassTag, Logger: ClassTag](
      expectedLogLevel: Level,
      expectedMsg: String,
      expectedMarkerAsString: String,
      expectedThrowableEntry: Option[ThrowableEntry],
  ): Unit =
    assertLogEntries[Test, Logger] { actual =>
      actual should have size 1 withClue ("expected exactly one log entry")
      val actualEntry = actual.head
      val actualMarker =
        actualEntry.marker.value.toString.replaceAll("\\.scala:\\d+", ".scala:<line-number>")
      val cp = new Checkpoint
      cp { Statement.discard { actualEntry.level shouldBe expectedLogLevel } }
      cp { Statement.discard { actualEntry.msg shouldBe expectedMsg } }
      cp { Statement.discard { actualMarker shouldBe expectedMarkerAsString } }
      cp { Statement.discard { actualEntry.throwableEntryO shouldBe expectedThrowableEntry } }
      cp.reportAll()
      succeed
    }

  def assertLogEntry(
      actual: TestingLogCollector.Entry,
      expected: TestingLogCollector.ExpectedLogEntry,
  ): Unit = {
    assertLogEntry(actual, expected.level, expected.msg, expected.markerRegex)
  }

  def assertLogEntry(
      actual: TestingLogCollector.Entry,
      expectedLogLevel: Level,
      expectedMsg: String,
      expectedMarkerRegex: Option[String] = None,
  ): Unit = {
    val cp = new Checkpoint
    cp { Statement.discard { actual.level shouldBe expectedLogLevel } }
    cp { Statement.discard { actual.msg shouldBe expectedMsg } }
    expectedMarkerRegex match {
      case Some(markerRegex) =>
        cp {
          Statement.discard {
            actual.marker.value.toString should fullyMatch regex markerRegex
          }
        }
      case None =>
        cp {
          Statement.discard {
            actual.marker shouldBe None
          }
        }
    }
    cp.reportAll()
  }
}
