// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.tracing.TraceContext
import com.typesafe.scalalogging.Logger
import org.slf4j
import org.slf4j.helpers.NOPLogger
import com.daml.error.{BaseError, ContextualizedErrorLogger}
import org.slf4j.MDC
import org.slf4j.event.Level

case class ErrorLoggingContext(
    logger: TracedLogger,
    properties: Map[String, String],
    traceContext: TraceContext,
) extends ContextualizedErrorLogger {
  override def correlationId: Option[String] = traceContext.traceId

  /** Log the cause while adding the context into the MDC
    *
    * We add the context twice to the MDC: first, every map item is added directly
    * and then we add a second string version as "err-context". When we log to file,
    * we add the err-context to the log output.
    * When we log to JSON, we ignore the err-context field.
    */
  override def logError(err: BaseError, extra: Map[String, String]): Unit = {
    implicit val traceContextImplicit: TraceContext = traceContext

    val mergedContext = err.context ++ err.location.map(("location", _)).toList.toMap ++ extra
    // we are putting the context into the MDC twice, once as a serialised string, once argument by argument
    // for text logging, we'll use the err-context string, for json logging, we use the arguments and ignore the err-context
    val arguments = mergedContext ++ Map(
      "error-code" -> err.code.codeStr(traceContext.traceId),
      "err-context" -> ("{" + ContextualizedErrorLogger.formatContextAsString(mergedContext) + "}"),
    )
    val message = err.code.toMsg(err.cause, traceContext.traceId)
    arguments.foreach { case (name, value) =>
      MDC.put(name, value)
    }
    (err.code.logLevel, err.throwableO) match {
      case (Level.INFO, None) => logger.info(message)
      case (Level.INFO, Some(tr)) => logger.info(message, tr)
      case (Level.WARN, None) => logger.warn(message)
      case (Level.WARN, Some(tr)) => logger.warn(message, tr)
      // an error that is logged with < INFO is not an error ...
      case (_, None) => logger.error(message)
      case (_, Some(tr)) => logger.error(message, tr)
    }
    arguments.keys.foreach(key => MDC.remove(key))
  }

  override def info(message: String): Unit = logger.info(message)(traceContext)
  override def info(message: String, throwable: Throwable): Unit =
    logger.info(message, throwable)(traceContext)
  override def warn(message: String): Unit = logger.warn(message)(traceContext)
  override def warn(message: String, throwable: Throwable): Unit =
    logger.warn(message, throwable)(traceContext)
  override def error(message: String): Unit = logger.error(message)(traceContext)
  override def error(message: String, throwable: Throwable): Unit =
    logger.error(message, throwable)(traceContext)
}

object ErrorLoggingContext {
  def fromTracedLogger(tracedLogger: TracedLogger)(implicit
      traceContext: TraceContext
  ): ErrorLoggingContext =
    ErrorLoggingContext(tracedLogger, Map.empty, traceContext)
}

trait NamedLogging {

  private implicit val canLogTraceContext = CanLogTraceContext

  protected implicit def loggingContext(implicit traceContext: TraceContext): ErrorLoggingContext =
    ErrorLoggingContext(theLoggerWithContext, loggerFactory.properties, traceContext)

  protected def loggerFactory: NamedLoggerFactory

  private[this] lazy val underlying: slf4j.Logger = loggerFactory.getLogger(getClass)

  private[this] lazy val theLogger: Logger =
    Logger(underlying)

  private[this] lazy val theLoggerWithContext: TracedLogger =
    Logger.takingImplicit[TraceContext](underlying)

  protected def noTracingLogger: Logger = theLogger
  protected def logger: TracedLogger = theLoggerWithContext
}

object NamedLogging {
  private implicit val canLogTraceContext = CanLogTraceContext

  def loggerWithoutTracing(logger: TracedLogger): Logger = Logger(logger.underlying)
  def todoTracedLogger(logger: TracedLogger): Logger = loggerWithoutTracing(logger)

  lazy val noopLogger = Logger.takingImplicit[TraceContext](NOPLogger.NOP_LOGGER)
}
