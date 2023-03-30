// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import org.slf4j.event
import org.slf4j.event.Level

/** Specify which log messages should be suppressed and captured. */
trait SuppressionRule {

  /** Determines whether an event we the given level should be suppressed.
    */
  def isSuppressed(loggerName: String, eventLevel: Level): Boolean
}

object SuppressionRule {

  /** Suppress events that are at this level or above (error is highest). */
  final case class LevelAndAbove(level: event.Level) extends SuppressionRule {
    def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      eventLevel.toInt >= level.toInt
  }

  /** Supress only this level of events */
  final case class Level(level: event.Level) extends SuppressionRule {
    def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean =
      eventLevel.toInt == level.toInt
  }

  /** No suppression */
  case object NoSuppression extends SuppressionRule {
    override def isSuppressed(loggerName: String, eventLevel: event.Level): Boolean = false
  }
}
