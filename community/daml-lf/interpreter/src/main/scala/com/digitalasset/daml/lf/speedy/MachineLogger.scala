// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.canton.logging.NamedLoggingContext
import com.digitalasset.canton.logging.LoggerNameFromClass
import com.digitalasset.daml.lf.data.Ref.Location
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.event.Level

private[lf] trait MachineLogger {
  def trace(message: String, location: Option[Location])(implicit ln: LoggerNameFromClass): Unit
  def warn(message: String, location: Option[Location])(implicit ln: LoggerNameFromClass): Unit
}

private[lf] object MachineLogger {
  def apply(
    enabled: Boolean = true,
    logLevel: Level = Level.DEBUG,
    matching: Seq[String] = Seq.empty,
  )(implicit loggingContext: NamedLoggingContext): MachineLogger = {
    val traceFilter =
      if (!enabled) (_: String) => false
      else if (matching.isEmpty) (_: String) => true
      else {
        val predicates = matching.map { expr =>
          if (expr.contains("*") || expr.contains("?")) {
            val regex = expr.replace("?", ".?").replace("*", ".*?")
            (s: String) => s.matches(regex)
          } else (s: String) => s.contains(expr)
        }
        (message: String) => predicates.exists(_(message))
      }

    new MachineLogger {
      def trace(message: String, location: Option[Location])(
        implicit loggerName: LoggerNameFromClass
      ): Unit = {
        implicit val traceContext = loggingContext.traceContext
        val logger = loggingContext.tracedLogger
        if (traceFilter(message)) {
          lazy val prettyLocation = Pretty.prettyLoc(location).renderWideStream.mkString
          lazy val prettyMessage = s"$prettyLocation: ${StringEscapeUtils.escapeJava(message)}"
          logLevel match {
            case Level.DEBUG => logger.debug(prettyMessage)
            case Level.INFO => logger.info(prettyMessage)
            case Level.TRACE => logger.debug(prettyMessage)
            case Level.WARN => logger.warn(prettyMessage)
            case Level.ERROR => logger.error(prettyMessage)
            case _ => ()
          }
        }
      }
      def warn(message: String, location: Option[Location])(
        implicit loggerName: LoggerNameFromClass
      ): Unit = {
        val prettyLocation = Pretty.prettyLoc(location).renderWideStream.mkString
        loggingContext.warn(s"$prettyLocation: $message")
      } 
    }
  }
}
