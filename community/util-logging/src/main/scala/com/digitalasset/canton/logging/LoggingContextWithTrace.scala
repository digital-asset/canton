// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.daml.logging.LoggingContext
import com.daml.logging.entries.{LoggingEntries, LoggingEntry}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.tracing.TraceContext

/** Class to enrich [[com.digitalasset.canton.logging.ErrorLoggingContext]] with [[com.digitalasset.canton.tracing.TraceContext]]
  */
class LoggingContextWithTrace(
    override val entries: LoggingEntries,
    val traceContext: TraceContext,
) extends LoggingContext(entries)
object LoggingContextWithTrace {
  implicit def implicitExtractTraceContext(implicit source: LoggingContextWithTrace): TraceContext =
    source.traceContext

  def apply(telemetry: Telemetry)(implicit
      loggingContext: LoggingContext
  ): LoggingContextWithTrace = {
    val traceContext =
      TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
    new LoggingContextWithTrace(loggingContext.entries, traceContext)
  }

  def apply(traceContext: TraceContext)(implicit
      loggingContext: LoggingContext
  ): LoggingContextWithTrace = {
    new LoggingContextWithTrace(loggingContext.entries, traceContext)
  }

  /** ## Principles to follow when enriching the logging context
    *
    * ### Don't add values coming from a scope outside of the current method
    *
    * If a method receives a value as a parameter, it should trust that,
    * if it was relevant, the caller already added this value to the context.
    * Add values to the context as upstream as possible in the call chain.
    * This ensures to not add duplicates, possibly using slightly different
    * names to track the same value. The context was implemented to ensure
    * that values did not have to be passed down the entire call stack to
    * be logged at relevant points.
    *
    * ### Don't dump string representations of complex objects
    *
    * The purpose of the context is to be consumed by structured logging
    * frameworks. Dumping the string representation of an object, like a
    * Scala case class instance, means embedding some form of string
    * formatting in another (likely to be JSON). This can be difficult
    * to manage and parse, so stick to simple values (strings, numbers,
    * dates, etc.).
    */
  def withEnrichedLoggingContext[A](
      telemetry: Telemetry
  )(entry: LoggingEntry, entries: LoggingEntry*)(
      f: LoggingContextWithTrace => A
  )(implicit loggingContext: LoggingContext): A = {
    LoggingContext.withEnrichedLoggingContext(entry, entries: _*) { implicit loggingContext =>
      f(LoggingContextWithTrace(telemetry)(loggingContext))
    }
  }
}