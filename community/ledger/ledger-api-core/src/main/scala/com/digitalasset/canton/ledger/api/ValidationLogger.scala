// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.error.ErrorCode.LoggedApiException
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, TracedLogger}

object ValidationLogger {
  def logFailure[Request](request: Request, t: Throwable)(implicit
      logger: ContextualizedLogger,
      loggingContext: LoggingContext,
  ): Throwable = {
    logger.debug(s"Request validation failed for $request, message: ${t.getMessage}")
    t match {
      case _: LoggedApiException => ()
      case _ => logger.info(t.getMessage)
    }
    t
  }

  def logFailureWithTrace[Request](logger: TracedLogger, request: Request, t: Throwable)(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Throwable = {
    logger.debug(
      s"Request validation failed for $request, message: ${t.getMessage}, ${loggingContextWithTrace.makeString}"
    )
    t match {
      case _: LoggedApiException => ()
      case _ => logger.info(t.getMessage)
    }
    t
  }
}
